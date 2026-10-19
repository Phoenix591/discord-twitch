"""Regression checks for the review fixes.

Tests call the bot with stubs. No Discord, Twitch, or AWS credentials are used.
"""

import asyncio
import datetime
import hashlib
import hmac
import time
from types import SimpleNamespace

from botocore.exceptions import ClientError

import discord
import pytest
from aiohttp.base_protocol import BaseProtocol
from aiohttp.streams import StreamReader
from aiohttp.test_utils import make_mocked_request

from discord_twitch import bot, manage_streamer


_CONFIG_GLOBALS = (
    "PUBLIC_URL",
    "SERVER_DOMAIN",
    "TWITCH_EVENTSUB_SECRET",
    "YOUTUBE_WEBHOOK_SECRET",
    "TWITCH_EVENTSUB_SECRET_GENERATED",
    "YOUTUBE_WEBHOOK_SECRET_GENERATED",
    "YOUTUBE_API_KEY",
    "YOUTUBE_BACKFILL_CHECK",
    "INTERNAL_API_SECRET",
    "DISCORD_TOKEN",
    "DISCORD_CHANNEL_ID",
    "TWITCH_CLIENT_ID",
    "TWITCH_CLIENT_SECRET",
    "DYNAMODB_TABLE_NAME",
    "DYNAMODB_REGION",
    "STREAMERS_TABLE_NAME",
    "LOCAL_PORT",
    "twitch_bot",
)


def _reset_trackers():
    for task in list(bot.twitch_active_tasks.values()):
        if task is not None and not task.done():
            task.cancel()
    bot.twitch_active_messages.clear()
    bot.twitch_active_tasks.clear()
    bot.twitch_active_stream_ids.clear()
    bot.youtube_active_messages.clear()
    bot._twitch_locks.clear()
    bot.TWITCH_STREAMERS.clear()
    bot.YOUTUBE_STREAMERS.clear()
    bot._lease_held = True
    for job in bot.scheduler.get_jobs():
        job.remove()


@pytest.fixture(autouse=True)
def _restore_config_globals():
    saved = {name: getattr(bot, name) for name in _CONFIG_GLOBALS}
    _reset_trackers()
    yield
    for name, value in saved.items():
        setattr(bot, name, value)
    _reset_trackers()


class _Embed:
    def __init__(self):
        self.title = "Live"
        self.url = "https://twitch.tv/ada"
        self.image = None


class _Msg:
    def __init__(self):
        self.id = 42
        self.embeds = [_Embed()]

    async def edit(self, **kwargs):
        self.last_edit = kwargs


def _secret_file(path, twitch_extra=""):
    path.write_text(
        "[discord]\n"
        "token = token\n"
        "channelid = 1\n"
        "[twitch]\n"
        "clientid = cid\n"
        "clientsecret = csecret\n"
        f"{twitch_extra}"
        "[server]\n"
        "domain = example.com\n"
        "public_url = https://example.com/hooks\n"
        "port = 8080\n"
        "internal_api_secret = internal-secret\n"
        "dynamodb_region = us-east-1\n",
        encoding="utf-8",
    )


def test_one_failed_restore_does_not_drop_the_other(monkeypatch):
    """gather(return_exceptions=True): a bad row must not cancel a good restore."""
    items = [
        {"video_id": "ytlive_vid", "message_id": "11"},
        {
            "video_id": "tw_7",
            "message_id": "22",
            "login": "ada",
            "stream_id": "stream-1",
        },
    ]
    gate = asyncio.Event()
    good_done = []

    async def bad_restore(*args, **kwargs):
        await asyncio.sleep(0)
        raise RuntimeError("restore failed")

    async def good_restore(*args, **kwargs):
        await gate.wait()
        good_done.append(True)

    saved = []

    async def save():
        saved.append(list(good_done))

    monkeypatch.setattr(bot, "restore_youtube_state", bad_restore)
    monkeypatch.setattr(bot, "restore_twitch_state", good_restore)
    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)
    monkeypatch.setattr(bot, "_db_scan", lambda: items)

    async def channel():
        return object()

    monkeypatch.setattr(bot, "discord_channel", channel)

    async def run():
        async def release():
            await asyncio.sleep(0.02)
            gate.set()

        asyncio.create_task(release())
        await bot.sync_state_from_dynamodb(SimpleNamespace())

    asyncio.run(run())
    assert saved == [[True]]


def test_youtube_maintenance_continues_after_an_exception(monkeypatch):
    calls = []
    waits = []
    closed = {"value": False}

    async def ready():
        return None

    async def ensure(channel_id, label, mode):
        calls.append(channel_id)
        if len(calls) == 1:
            raise RuntimeError("hub down")
        return True

    async def wait(seconds):
        waits.append(seconds)
        if len(waits) >= 2:
            closed["value"] = True

    bot.YOUTUBE_STREAMERS["UC1"] = "Ada"
    monkeypatch.setattr(bot.discord_bot, "wait_until_ready", ready)
    monkeypatch.setattr(bot.discord_bot, "is_closed", lambda: closed["value"])

    async def run():
        client = bot.HybridBot()
        client._ensure_youtube_sub = ensure
        client._wait_youtube_maint = wait
        await client.maintain_youtube_subs()

    asyncio.run(run())
    assert calls == ["UC1", "UC1"]
    assert waits[0] == 3600


def test_poll_survives_a_reconcile_exception(monkeypatch):
    async def boom(initial=False):
        raise RuntimeError("dynamo down")

    monkeypatch.setattr(
        bot, "twitch_bot", SimpleNamespace(reconcile_streamers=boom)
    )

    async def run():
        await bot.poll_streamers_task.coro()

    asyncio.run(run())


def test_invisible_youtube_video_is_retried_when_an_api_key_exists():
    async def missing(video_id):
        return None

    async def run():
        client = bot.HybridBot()
        client.fetch_youtube_data = missing
        bot.YOUTUBE_API_KEY = "key"
        await client.initial_youtube_check("vid-retry", save=False)
        assert bot.scheduler.get_job("yt_vid-retry") is not None

        bot.YOUTUBE_API_KEY = ""
        await client.initial_youtube_check("vid-gone", save=False)
        assert bot.scheduler.get_job("yt_vid-gone") is None

    asyncio.run(run())


def test_removing_a_twitch_channel_sweeps_streams_still_tracked(monkeypatch):
    swept = []

    async def announce(user_ids=None):
        swept.append((user_ids, set(bot.TWITCH_STREAMERS)))

    async def delete_all():
        return None

    async def subscribe(s_id, s_name):
        return True

    monkeypatch.setattr(
        bot,
        "_db_get_streamers",
        lambda: [
            {"platform": "twitch", "channel_id": "2", "display_name": "Bob"}
        ],
    )

    async def run():
        client = bot.HybridBot()
        client.announce_current_twitch_streams = announce
        client.delete_all_eventsub_subscriptions = delete_all
        client._subscribe_twitch = subscribe
        bot.TWITCH_STREAMERS["1"] = "Ada"
        bot.TWITCH_STREAMERS["2"] = "Bob"
        await client.reconcile_streamers(initial=False)

    asyncio.run(run())
    assert swept == [(None, {"2"})]


def test_shutdown_saves_state_without_a_twitch_client(monkeypatch):
    saved = []

    async def save():
        saved.append(True)

    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)
    monkeypatch.setattr(bot, "twitch_bot", None)
    bot._lease_held = False

    async def run():
        await bot.DiscordTwitchBot().close()

    asyncio.run(run())
    assert saved == [True]


def test_upcoming_broadcast_stays_tracked(monkeypatch):
    async def no_sync():
        raise AssertionError("upcoming broadcast should stay tracked")

    monkeypatch.setattr(bot, "sync_state_to_dynamodb", no_sync)

    async def run():
        client = bot.HybridBot()
        msg = _Msg()
        bot.youtube_active_messages["vid"] = msg

        async def upcoming(video_id):
            return {"snippet": {"liveBroadcastContent": "upcoming"}}

        client.fetch_youtube_data = upcoming
        await client.check_youtube_offline("vid")
        assert bot.youtube_active_messages["vid"] is msg
        assert msg.embeds[0].title == "Live"

    asyncio.run(run())


def test_youtube_alert_sends_when_thumbnails_are_missing(monkeypatch):
    sent = []

    class Chan:
        async def send(self, **kwargs):
            sent.append(kwargs)
            return _Msg()

    async def channel():
        return Chan()

    async def save():
        return None

    monkeypatch.setattr(bot, "discord_channel", channel)
    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)
    bot.YOUTUBE_STREAMERS["UC1"] = "Ada"

    async def post(data):
        client = bot.HybridBot()
        assert await client.send_youtube_notification(data) is True

    payload = {
        "id": "vid-thumbs",
        "snippet": {
            "channelId": "UC1",
            "channelTitle": "Ada",
            "title": "Hello",
        },
        "statistics": {"viewCount": "3"},
    }
    asyncio.run(post(payload))
    assert sent
    assert "MEMBERS ONLY" not in sent[0]["content"]
    bot.youtube_active_messages.clear()
    sent.clear()
    payload["id"] = "vid-empty"
    payload["snippet"]["thumbnails"] = {}
    asyncio.run(post(payload))
    assert sent
    assert bot.youtube_active_messages["vid-empty"].id == 42


class _Batch:
    def __init__(self, deleted):
        self._deleted = deleted

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def put_item(self, Item):
        return None

    def delete_item(self, Key):
        self._deleted.append(Key["video_id"])


class _Table:
    def __init__(self, deleted):
        self._deleted = deleted

    def batch_writer(self):
        return _Batch(self._deleted)


def test_sync_keeps_rows_this_process_does_not_own(monkeypatch):
    deleted = []
    bot._lease_held = False

    def rows(table):
        return [
            {
                "video_id": "tw_mine",
                "message_id": "1",
                "login": "ada",
                "stream_id": "s",
            },
            {
                "video_id": "tw_other",
                "message_id": "9",
                "login": "bob",
                "stream_id": "o",
            },
        ]

    monkeypatch.setattr(bot, "scan_all", rows)
    monkeypatch.setattr(bot, "_dynamodb_table", lambda name: _Table(deleted))
    bot.twitch_active_messages["mine"] = _Msg()
    bot.twitch_active_stream_ids["mine"] = "s"

    async def run():
        await bot.sync_state_to_dynamodb()

    asyncio.run(run())
    assert deleted == []


def test_sync_holder_drops_untracked_rows_and_keeps_the_lease(monkeypatch):
    deleted = []

    def rows(table):
        return [
            {
                "video_id": "tw_mine",
                "message_id": "1",
                "login": "ada",
                "stream_id": "s",
            },
            {
                "video_id": "tw_other",
                "message_id": "9",
                "login": "bob",
                "stream_id": "o",
            },
            {"video_id": "lease", "owner": "previous", "expires_at": 1},
        ]

    monkeypatch.setattr(bot, "scan_all", rows)
    monkeypatch.setattr(bot, "_dynamodb_table", lambda name: _Table(deleted))
    bot._lease_held = True
    bot.twitch_active_messages["mine"] = _Msg()
    bot.twitch_active_stream_ids["mine"] = "s"

    async def run():
        await bot.sync_state_to_dynamodb()

    asyncio.run(run())
    assert "tw_other" in deleted
    assert "lease" not in deleted


class _LeaseTable:
    def __init__(self, item):
        self.item = item
        self.calls = []

    def update_item(self, **kwargs):
        self.calls.append("update")
        values = kwargs["ExpressionAttributeValues"]
        now = values[":now"]
        owner = values[":o"]
        expires = values[":e"]
        current = self.item
        allowed = (
            current is None
            or current.get("expires_at") is None
            or int(current["expires_at"]) < now
            or current.get("owner") == owner
        )
        if not allowed:
            raise ClientError(
                {
                    "Error": {
                        "Code": "ConditionalCheckFailedException",
                        "Message": "The conditional request failed",
                    }
                },
                "UpdateItem",
            )
        self.item = {
            "video_id": kwargs["Key"]["video_id"],
            "owner": owner,
            "expires_at": expires,
        }

    def delete_item(self, **kwargs):
        self.calls.append("delete")
        owner = kwargs["ExpressionAttributeValues"][":o"]
        if not self.item or self.item.get("owner") != owner:
            raise ClientError(
                {
                    "Error": {
                        "Code": "ConditionalCheckFailedException",
                        "Message": "The conditional request failed",
                    }
                },
                "DeleteItem",
            )
        self.item = None


def test_missing_lease_is_acquired(monkeypatch):
    table = _LeaseTable(None)
    monkeypatch.setattr(bot, "_dynamodb_table", lambda name: table)
    bot._lease_held = False
    assert bot._acquire_or_renew_lease() is True
    assert table.item["owner"] == bot.INSTANCE_ID
    assert table.item["video_id"] == "lease"
    assert bot._lease_held is True


def test_expired_lease_is_taken_without_a_release(monkeypatch):
    table = _LeaseTable({"video_id": "lease", "owner": "dead", "expires_at": 1})
    monkeypatch.setattr(bot, "_dynamodb_table", lambda name: table)
    bot._lease_held = False
    assert bot._acquire_or_renew_lease() is True
    assert table.item["owner"] == bot.INSTANCE_ID
    assert "delete" not in table.calls
    assert bot._lease_held is True


def test_live_lease_blocks_another_instance(monkeypatch):
    future = int(time.time()) + 3600
    table = _LeaseTable(
        {"video_id": "lease", "owner": "other", "expires_at": future}
    )
    monkeypatch.setattr(bot, "_dynamodb_table", lambda name: table)
    bot._lease_held = True
    assert bot._acquire_or_renew_lease() is False
    assert table.item["owner"] == "other"
    assert table.item["expires_at"] == future
    assert bot._lease_held is False


def test_release_without_a_lease_does_not_call_dynamodb(monkeypatch):
    def boom(name):
        raise AssertionError("dynamodb")

    monkeypatch.setattr(bot, "_dynamodb_table", boom)
    bot._lease_held = False
    bot._release_lease()


def test_state_restore_ignores_the_lease_row(monkeypatch):
    seen = []

    async def record_youtube(bot_instance, channel, vid, msg_id):
        seen.append(("yt", vid))

    async def record_twitch(
        bot_instance, channel, s_id, login, msg_id, stream_id=""
    ):
        seen.append(("tw", s_id))

    async def save():
        return None

    monkeypatch.setattr(bot, "restore_youtube_state", record_youtube)
    monkeypatch.setattr(bot, "restore_twitch_state", record_twitch)
    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)
    monkeypatch.setattr(
        bot,
        "_db_scan",
        lambda: [
            {"video_id": "lease", "owner": "other", "expires_at": 1},
            {
                "video_id": "tw_7",
                "message_id": "22",
                "login": "ada",
                "stream_id": "stream-1",
            },
        ],
    )

    async def channel():
        return object()

    monkeypatch.setattr(bot, "discord_channel", channel)

    async def run():
        await bot.sync_state_from_dynamodb(SimpleNamespace())

    asyncio.run(run())
    assert seen == [("tw", "7")]


def test_setup_twitch_subs_does_not_wipe_the_client():
    wiped = []
    deleted = []
    subscribed = []
    callback = "https://twitch.example.com/callback"

    class Sub:
        def __init__(self, sub_id, url, broadcaster):
            self.id = sub_id
            self.transport = SimpleNamespace(callback=url)
            self.condition = {"broadcaster_user_id": broadcaster}

        async def delete(self):
            deleted.append(self.id)

    async def subscriptions():
        yield Sub("stale", callback, "999")
        yield Sub("kept", callback, "1")
        yield Sub("foreign", "https://other.example/hook", "999")

    async def fetch(**kwargs):
        return SimpleNamespace(subscriptions=subscriptions())

    async def delete_all():
        wiped.append(True)

    async def subscribe(s_id, s_name):
        subscribed.append(s_id)
        return True

    async def run():
        bot.PUBLIC_URL = callback
        bot.TWITCH_EVENTSUB_SECRET_GENERATED = False
        bot.YOUTUBE_WEBHOOK_SECRET_GENERATED = True
        client = bot.HybridBot()
        client.delete_all_eventsub_subscriptions = delete_all
        client.fetch_eventsub_subscriptions = fetch
        client._subscribe_twitch = subscribe
        bot.TWITCH_STREAMERS["1"] = "Ada"
        await client.setup_twitch_subs()

    asyncio.run(run())
    assert wiped == []
    assert deleted == ["stale"]
    assert subscribed == ["1"]


def test_eventsub_path_follows_public_url():
    bot.PUBLIC_URL = "https://twitch.example.com/hooks/twitch"
    bot.SERVER_DOMAIN = "twitch.example.com"
    bot.TWITCH_EVENTSUB_SECRET = "a" * 20
    client = bot.HybridBot()
    assert client.web_adapter._eventsub_path == "/hooks/twitch"


def test_added_youtube_channel_backfills_current_videos(monkeypatch):
    called = []

    async def ensure(channel_id, label, mode):
        return True

    async def backfill():
        called.append("backfill")

    monkeypatch.setattr(
        bot,
        "_db_get_streamers",
        lambda: [
            {
                "platform": "youtube",
                "channel_id": "UC1",
                "display_name": "Ada",
            }
        ],
    )

    async def run():
        client = bot.HybridBot()
        client._ensure_youtube_sub = ensure
        client.run_youtube_backfill = backfill
        await client.reconcile_streamers(initial=False)

    asyncio.run(run())
    assert called == ["backfill"]


def test_public_live_without_viewcount_is_not_members_only(monkeypatch):
    sent = []

    class Chan:
        async def send(self, **kwargs):
            sent.append(kwargs)
            return _Msg()

    async def channel():
        return Chan()

    async def save():
        return None

    monkeypatch.setattr(bot, "discord_channel", channel)
    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)
    bot.YOUTUBE_STREAMERS["UC1"] = "Ada"

    async def run():
        client = bot.HybridBot()
        await client.send_youtube_notification(
            {
                "id": "vid-public",
                "snippet": {
                    "channelId": "UC1",
                    "channelTitle": "Ada",
                    "title": "Public live",
                    "liveBroadcastContent": "live",
                    "thumbnails": {},
                },
                "statistics": {},
            }
        )

    asyncio.run(run())
    assert sent
    assert "MEMBERS ONLY" not in sent[0]["content"]
    assert "MEMBERS ONLY" not in sent[0]["embed"].title


def _eventsub_request(loop, body, headers):
    protocol = BaseProtocol(loop)
    payload = StreamReader(protocol, 2**16, loop=loop)
    payload.feed_data(body)
    payload.feed_eof()
    return make_mocked_request(
        "POST",
        "/callback",
        headers={**headers, "Content-Length": str(len(body))},
        payload=payload,
        loop=loop,
    )


def test_eventsub_retry_with_the_same_id_is_verified():
    secret = "s" * 16
    bot.TWITCH_EVENTSUB_SECRET = secret
    body = b'{"challenge":"abc","subscription":{"type":"webhook_callback_verification"}}'
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    message_id = "msg-1"
    good_sig = "sha256=" + hmac.new(
        secret.encode(),
        f"{message_id}{timestamp}{body.decode()}".encode(),
        hashlib.sha256,
    ).hexdigest()
    headers = {
        "Twitch-Eventsub-Message-Type": "webhook_callback_verification",
        "Twitch-Eventsub-Message-Id": message_id,
        "Twitch-Eventsub-Message-Timestamp": timestamp,
    }

    async def run():
        client = bot.HybridBot()
        loop = asyncio.get_running_loop()
        bad = await client.web_adapter.eventsub_callback(
            _eventsub_request(
                loop,
                body,
                {**headers, "Twitch-Eventsub-Message-Signature": "sha256=" + ("ab" * 32)},
            )
        )
        assert bad.status == 400
        good = await client.web_adapter.eventsub_callback(
            _eventsub_request(
                loop,
                body,
                {**headers, "Twitch-Eventsub-Message-Signature": good_sig},
            )
        )
        assert good.status == 200
        assert good.text == "abc"

    asyncio.run(run())


def test_oauth_routes_are_not_mounted():
    client = bot.HybridBot()
    mounted = {
        (route.method, route.get_info().get("path"))
        for route in client.web_adapter.router.routes()
    }
    assert ("GET", "/oauth") not in mounted
    assert ("GET", "/oauth/callback") not in mounted


def test_remove_missing_streamer_is_not_success(tmp_path, monkeypatch, capsys):
    _secret_file(tmp_path / "secret.cfg")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))

    class Table:
        def delete_item(self, Key, **kwargs):
            if "ConditionExpression" not in kwargs:
                return {}
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    "Error": {
                        "Code": "ConditionalCheckFailedException",
                        "Message": "The conditional request failed",
                    }
                },
                "DeleteItem",
            )

    class Resource:
        def Table(self, name):
            return Table()

    monkeypatch.setattr(
        manage_streamer.boto3, "resource", lambda *args, **kwargs: Resource()
    )
    monkeypatch.setattr(
        "sys.argv", ["manage_streamer", "--remove", "twitch", "missing-id"]
    )
    with pytest.raises(SystemExit) as caught:
        manage_streamer.main()
    assert caught.value.code not in (0, None)
    output = capsys.readouterr().out
    assert "Removed" not in output
    assert "missing-id" in output


class _HttpResponse:
    def __init__(self, payload):
        self.status = 200
        self._payload = payload

    async def json(self):
        return self._payload


class _HttpGet:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return _HttpResponse(self._payload)

    async def __aexit__(self, *args):
        return False


def test_youtube_backfill_limits_concurrency(monkeypatch):
    inflight = {"current": 0, "peak": 0, "started": 0}

    async def initial(video_id, save=True):
        inflight["started"] += 1
        inflight["current"] += 1
        inflight["peak"] = max(inflight["peak"], inflight["current"])
        await asyncio.sleep(0.05)
        inflight["current"] -= 1

    async def save():
        return None

    class Session:
        def get(self, url, params=None, headers=None):
            if url.endswith("/channels"):
                playlist = "PL" + params["id"]
                payload = {
                    "items": [
                        {
                            "contentDetails": {
                                "relatedPlaylists": {"uploads": playlist}
                            }
                        }
                    ]
                }
            else:
                payload = {
                    "items": [
                        {"contentDetails": {"videoId": "v" + params["playlistId"]}}
                    ]
                }
            return _HttpGet(payload)

    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)
    bot.YOUTUBE_API_KEY = "key"
    bot.YOUTUBE_BACKFILL_CHECK = 1
    for index in range(4):
        bot.YOUTUBE_STREAMERS[f"UC{index}"] = f"Channel {index}"

    async def run():
        client = bot.HybridBot()
        client.session = Session()
        client.initial_youtube_check = initial
        await client.run_youtube_backfill()

    asyncio.run(run())
    assert inflight["started"] == 4
    assert inflight["peak"] == 2


def test_blank_helix_stream_type_is_treated_as_live():
    assert bot._helix_stream_is_live(SimpleNamespace(type="")) is True
    assert bot._helix_stream_is_live(SimpleNamespace(type="live")) is True


def test_generated_eventsub_secret_replaces_only_our_subscriptions():
    wiped = []
    deleted = []
    subscribed = []
    callback = "https://twitch.example.com/callback"

    class Sub:
        def __init__(self, sub_id, url, broadcaster):
            self.id = sub_id
            self.transport = SimpleNamespace(callback=url)
            self.condition = {"broadcaster_user_id": broadcaster}

        async def delete(self):
            deleted.append(self.id)

    async def subscriptions():
        yield Sub("stale", callback, "999")
        yield Sub("kept", callback, "1")
        yield Sub("foreign", "https://other.example/hook", "999")

    async def fetch(**kwargs):
        return SimpleNamespace(subscriptions=subscriptions())

    async def delete_all():
        wiped.append(True)

    async def subscribe(s_id, s_name):
        subscribed.append(s_id)
        return True

    async def run():
        bot.PUBLIC_URL = callback
        bot.TWITCH_EVENTSUB_SECRET_GENERATED = True
        bot.YOUTUBE_WEBHOOK_SECRET_GENERATED = False
        client = bot.HybridBot()
        client.delete_all_eventsub_subscriptions = delete_all
        client.fetch_eventsub_subscriptions = fetch
        client._subscribe_twitch = subscribe
        bot.TWITCH_STREAMERS["1"] = "Ada"
        await client.setup_twitch_subs()

    asyncio.run(run())
    assert wiped == []
    assert deleted == ["stale", "kept"]
    assert subscribed == ["1"]


def test_omitted_secrets_are_generated_for_this_process(tmp_path, monkeypatch):
    _secret_file(tmp_path / "secret.cfg")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))
    bot.load_config()
    assert bot.TWITCH_EVENTSUB_SECRET_GENERATED is True
    assert bot.YOUTUBE_WEBHOOK_SECRET_GENERATED is True
    assert 10 <= len(bot.TWITCH_EVENTSUB_SECRET) <= 100
    assert bot.YOUTUBE_WEBHOOK_SECRET


def test_discord_client_has_no_command_prefix():
    client = bot.DiscordTwitchBot()
    assert isinstance(client, discord.Client)
    assert not hasattr(client, "command_prefix")


def test_overlong_eventsub_secret_is_rejected_at_config(tmp_path, monkeypatch):
    _secret_file(tmp_path / "secret.cfg", twitch_extra="eventsub_secret = " + ("a" * 128) + "\n")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))
    with pytest.raises(ValueError, match="eventsub_secret"):
        bot.load_config()
    assert bot.TWITCH_EVENTSUB_SECRET != "a" * 128

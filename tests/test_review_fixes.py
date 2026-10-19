"""Stubbed checks for the announcement, webhook, and config bugs.

No Discord, Twitch, or AWS credentials are used. Network clients are replaced
before any call that would otherwise leave the process.
"""

import asyncio
import datetime
import hashlib
import hmac
import signal
from types import SimpleNamespace

import discord
import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from discord_twitch import bot, config, dynamo


def reset_state():
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
def _clean_bot_state():
    reset_state()
    yield
    reset_state()


class _Embed:
    def __init__(self):
        self.title = "Live"
        self.url = "https://example.invalid/live"
        self.image = None


class _Msg:
    def __init__(self):
        self.id = 42
        self.edits = 0
        self.embeds = [_Embed()]

    async def edit(self, **kwargs):
        self.edits += 1
        self.last_edit = kwargs


def test_secret_with_percent_sign_loads(tmp_path, monkeypatch):
    secret = tmp_path / "secret.cfg"
    secret.write_text(
        "[server]\ninternal_api_secret = abc%def\ndynamodb_region = us-west-2\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))
    parser, path = config.load_parser()
    assert path == str(secret)
    assert parser["server"]["internal_api_secret"] == "abc%def"
    assert config.dynamodb_settings(parser)[0] == "us-west-2"


def test_scan_all_follows_pagination():
    class Table:
        def __init__(self):
            self.calls = []

        def scan(self, **kwargs):
            self.calls.append(kwargs)
            if "ExclusiveStartKey" not in kwargs:
                return {
                    "Items": [{"video_id": "a"}],
                    "LastEvaluatedKey": {"video_id": "a"},
                }
            return {"Items": [{"video_id": "b"}]}

    table = Table()
    assert dynamo.scan_all(table) == [{"video_id": "a"}, {"video_id": "b"}]
    assert table.calls[1]["ExclusiveStartKey"] == {"video_id": "a"}


def test_youtube_verification_rejects_unsubscribe_of_tracked_channel():
    topic = "https://www.youtube.com/xml/feeds/videos.xml?channel_id=UC123"
    streamers = {"UC123": "Ada"}
    assert (
        bot.youtube_verification_challenge("unsubscribe", topic, "chal", streamers)
        is None
    )
    assert (
        bot.youtube_verification_challenge("subscribe", topic, "chal", streamers)
        == "chal"
    )
    assert (
        bot.youtube_verification_challenge("unsubscribe", topic, "chal", {}) == "chal"
    )
    assert (
        bot.youtube_verification_challenge(
            "subscribe", "https://example.invalid/feed", "chal", streamers
        )
        is None
    )


def test_overdue_scheduler_job_still_runs():
    ran = []

    async def job():
        ran.append(True)

    async def main():
        sched = AsyncIOScheduler(
            job_defaults={"misfire_grace_time": None, "coalesce": True}
        )
        past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            minutes=5
        )
        sched.add_job(job, "date", run_date=past, id="overdue")
        sched.start()
        await asyncio.sleep(0.3)
        sched.shutdown(wait=False)

    asyncio.run(main())
    assert ran == [True]


def test_unresolved_twitch_id_keeps_existing_row(monkeypatch):
    captured = {}

    def fake_push(jobs, tw, yt, pending=None):
        captured["tw"] = tw
        captured["pending"] = set(pending or ())

    monkeypatch.setattr(bot, "_db_push", fake_push)
    bot.twitch_active_messages["42"] = 99

    async def run():
        await bot.sync_state_to_dynamodb()

    asyncio.run(run())
    assert "tw_42" in captured["pending"]
    assert "tw_42" not in captured["tw"]


def test_nan_takeover_timestamp_is_rejected():
    async def run():
        client = bot.HybridBot()
        request = SimpleNamespace(
            headers={"X-Signature": "deadbeef", "X-Timestamp": "nan"},
            remote="203.0.113.4",
        )
        response = await client.internal_takeover_handler(request)
        assert response.status == 401

    asyncio.run(run())


def test_stale_offline_event_does_not_clear_the_new_stream(monkeypatch):
    pushed = []
    monkeypatch.setattr(bot, "_db_push", lambda *args, **kwargs: pushed.append(args))

    async def run():
        client = bot.HybridBot()
        msg = _Msg()
        bot.twitch_active_messages["7"] = msg
        bot.twitch_active_stream_ids["7"] = "new-stream"
        stale = SimpleNamespace(
            broadcaster=SimpleNamespace(id="7", name="ada"), id="old-stream"
        )
        await client.event_stream_offline(stale)
        assert msg.edits == 0
        assert bot.twitch_active_messages["7"] is msg
        assert pushed == []

        current = SimpleNamespace(
            broadcaster=SimpleNamespace(id="7", name="ada"), id="new-stream"
        )
        await client.event_stream_offline(current)
        assert msg.edits == 1
        assert "7" not in bot.twitch_active_messages
        assert pushed

    asyncio.run(run())


def test_failed_announce_clears_the_placeholder_for_the_next_event():
    async def run():
        client = bot.HybridBot()

        class Thumb:
            base_url = "https://example.invalid/{width}x{height}.jpg"

            def url_for(self, width, height):
                raise RuntimeError("thumbnail failed")

        data = SimpleNamespace(
            title="Live", game_name="Game", started_at=None, thumbnail=Thumb()
        )
        with pytest.raises(RuntimeError):
            await client._handle_twitch_online("9", "ada", "strm", stream_data=data)
        assert "9" not in bot.twitch_active_messages

    asyncio.run(run())


def test_empty_twitch_thumbnail_is_omitted():
    client = bot.HybridBot()

    class Thumb:
        base_url = ""

        def url_for(self, width, height):
            raise AssertionError("empty thumbnail should not be formatted")

    data = SimpleNamespace(
        title="Live", game_name="Game", started_at=None, thumbnail=Thumb()
    )
    embed = client.build_twitch_embed("ada", data)
    assert "image" not in embed.to_dict()


def test_second_online_event_runs_after_the_first_fails():
    async def run():
        client = bot.HybridBot()
        order = []

        async def announce(s_id, s_login, stream_id, stream_data=None):
            order.append(stream_id)
            if stream_id == "1":
                await asyncio.sleep(0.05)
                raise RuntimeError("discord down")

        client._announce_twitch = announce

        async def call(stream_id):
            try:
                await client._handle_twitch_online("1", "ada", stream_id)
            except RuntimeError:
                pass

        await asyncio.gather(call("1"), call("2"))
        assert order == ["1", "2"]

    asyncio.run(run())


def test_added_twitch_channel_sweeps_streams_already_live(monkeypatch):
    monkeypatch.setattr(
        bot,
        "_db_get_streamers",
        lambda: [
            {"platform": "twitch", "channel_id": "55", "display_name": "Ada"}
        ],
    )

    async def run():
        client = bot.HybridBot()
        swept = []

        async def sweep(user_ids=None):
            swept.append(list(user_ids) if user_ids is not None else None)

        async def subscribe(*args, **kwargs):
            return None

        client.announce_current_twitch_streams = sweep
        client.subscribe_webhook = subscribe
        await client.reconcile_streamers(initial=False)
        assert swept == [["55"]]
        assert bot.TWITCH_STREAMERS == {"55": "Ada"}

    asyncio.run(run())


def test_twitch_subscribe_retries_then_records_failure(monkeypatch):
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(bot.asyncio, "sleep", fake_sleep)

    async def run():
        client = bot.HybridBot()
        calls = {"n": 0}

        async def subscribe(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("429")
            return None

        client.subscribe_webhook = subscribe
        assert await client._subscribe_twitch("8", "Bea") is True
        assert calls["n"] == 4
        assert "8" not in client._twitch_sub_failed

        async def always_fail(*args, **kwargs):
            raise RuntimeError("429")

        client.subscribe_webhook = always_fail
        assert await client._subscribe_twitch("8", "Bea") is False
        assert "8" in client._twitch_sub_failed

    asyncio.run(run())
    assert sleeps


def test_failed_twitch_subs_are_retried_on_the_next_poll(monkeypatch):
    monkeypatch.setattr(
        bot,
        "_db_get_streamers",
        lambda: [
            {"platform": "twitch", "channel_id": "55", "display_name": "Ada"}
        ],
    )

    async def run():
        client = bot.HybridBot()
        calls = []
        swept = []

        async def subscribe(*args, **kwargs):
            calls.append(kwargs.get("payload"))
            return None

        async def sweep(user_ids=None):
            swept.append(list(user_ids) if user_ids is not None else None)

        client.subscribe_webhook = subscribe
        client.announce_current_twitch_streams = sweep
        bot.TWITCH_STREAMERS["55"] = "Ada"
        client._twitch_sub_failed.add("55")
        await client.reconcile_streamers(initial=False)
        assert len(calls) == 2
        assert swept == [["55"]]
        assert "55" not in client._twitch_sub_failed

    asyncio.run(run())


def test_youtube_send_failure_drops_the_lock(monkeypatch):
    async def channel():
        class Chan:
            async def send(self, **kwargs):
                raise RuntimeError("discord down")

        return Chan()

    monkeypatch.setattr(bot, "discord_channel", channel)
    bot.YOUTUBE_STREAMERS["UC1"] = "Ada"
    data = {
        "id": "vid",
        "snippet": {
            "channelId": "UC1",
            "channelTitle": "Ada",
            "title": "Hello",
            "thumbnails": {},
        },
        "statistics": {"viewCount": "1"},
    }

    async def run():
        client = bot.HybridBot()
        assert await client.send_youtube_notification(data) is False
        assert "vid" not in bot.youtube_active_messages

    asyncio.run(run())


def test_sniper_retries_when_discord_send_fails():
    async def run():
        client = bot.HybridBot()
        bot.YOUTUBE_STREAMERS["UC1"] = "Ada"

        async def fetch(video_id):
            return {
                "snippet": {
                    "channelId": "UC1",
                    "liveBroadcastContent": "live",
                }
            }

        async def send(data):
            return False

        client.fetch_youtube_data = fetch
        client.send_youtube_notification = send
        when = datetime.datetime.now(datetime.timezone.utc)
        await client.check_youtube_status("vid123", when)
        job = bot.scheduler.get_job("yt_vid123")
        assert job is not None
        assert job.args[0] == "vid123"

    asyncio.run(run())


def test_youtube_offline_keeps_state_when_api_or_discord_fails(monkeypatch):
    async def no_sync():
        raise AssertionError("should not drop state")

    monkeypatch.setattr(bot, "sync_state_to_dynamodb", no_sync)

    async def run():
        client = bot.HybridBot()
        msg = _Msg()
        bot.youtube_active_messages["vid"] = msg

        async def missing(video_id):
            return None

        client.fetch_youtube_data = missing
        await client.check_youtube_offline("vid")
        assert bot.youtube_active_messages["vid"] is msg

        async def ended(video_id):
            return {"snippet": {"liveBroadcastContent": "none"}}

        class Resp:
            status = 500
            reason = "error"

        async def edit_fails(**kwargs):
            raise discord.HTTPException(Resp(), "unavailable")

        msg.edit = edit_fails
        client.fetch_youtube_data = ended
        await client.check_youtube_offline("vid")
        assert bot.youtube_active_messages["vid"] is msg

        upcoming = {"snippet": {"liveBroadcastContent": "upcoming"}}
        assert bot.youtube_broadcast_ended(upcoming) is False
        assert bot.youtube_broadcast_ended(None) is None
        assert (
            bot.youtube_broadcast_ended({"snippet": {"liveBroadcastContent": "none"}})
            is True
        )

    asyncio.run(run())


def test_youtube_webhook_signature_and_intent():
    async def run():
        client = bot.HybridBot()
        bot.YOUTUBE_WEBHOOK_SECRET = "test-secret"
        bot.YOUTUBE_STREAMERS["UC1"] = "Ada"
        seen = []

        async def initial(video_id, save=True):
            seen.append(video_id)

        client.initial_youtube_check = initial
        topic = "https://www.youtube.com/xml/feeds/videos.xml?channel_id=UC1"

        class Request:
            def __init__(self, method, query=None, headers=None, body=b""):
                self.method = method
                self.query = query or {}
                self.headers = headers or {}
                self.remote = "203.0.113.9"
                self._body = body

            async def read(self):
                return self._body

        denied = await client.youtube_webhook_handler(
            Request(
                "GET",
                {
                    "hub.mode": "unsubscribe",
                    "hub.topic": topic,
                    "hub.challenge": "chal",
                },
            )
        )
        assert denied.status == 404

        allowed = await client.youtube_webhook_handler(
            Request(
                "GET",
                {
                    "hub.mode": "subscribe",
                    "hub.topic": topic,
                    "hub.challenge": "chal",
                },
            )
        )
        assert allowed.status == 200
        assert allowed.text == "chal"

        bad = await client.youtube_webhook_handler(
            Request(
                "POST",
                headers={"X-Hub-Signature": "sha1=abcd"},
                body=b"<feed/>",
            )
        )
        assert bad.status == 403

        short = await client.youtube_webhook_handler(
            Request("POST", headers={"X-Hub-Signature": "nope"}, body=b"<feed/>")
        )
        assert short.status == 403

        body = (
            '<feed xmlns="http://www.w3.org/2005/Atom" '
            'xmlns:yt="http://purl.org/yt/2012"><entry>'
            "<yt:videoId>abc12345678</yt:videoId>"
            "<yt:channelId>UC1</yt:channelId>"
            "</entry></feed>"
        ).encode()
        signature = "sha1=" + hmac.new(
            b"test-secret", body, hashlib.sha1
        ).hexdigest()
        ok = await client.youtube_webhook_handler(
            Request("POST", headers={"X-Hub-Signature": signature}, body=body)
        )
        assert ok.status == 200
        await asyncio.sleep(0)
        assert seen == ["abc12345678"]

    asyncio.run(run())


def test_sigterm_schedules_one_shutdown():
    async def run():
        discord_bot = bot.DiscordTwitchBot()
        calls = []

        async def close():
            calls.append("close")

        discord_bot.close = close
        loop = asyncio.get_running_loop()
        await discord_bot.setup_hook()
        try:
            assert signal.SIGTERM in loop._signal_handlers
            discord_bot._on_sigterm()
            discord_bot._on_sigterm()
            await asyncio.sleep(0)
            assert calls == ["close"]
        finally:
            loop.remove_signal_handler(signal.SIGTERM)

    asyncio.run(run())

"""Regression tests for the 2026-11-20 review.

Plain tests, no xfail. The three startup-window tests fail on main until the
fix is merged. test_startup_sweep_announces_a_different_live_broadcast also
passes on main and locks that a newer broadcast is still announced.
"""

import asyncio
import datetime
import hashlib
import hmac
from types import SimpleNamespace

from discord_twitch import bot


def _reset():
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
    bot._lease_until = 0
    bot._lease_closing = False
    bot._youtube_checks_in_flight.clear()
    for job in bot.scheduler.get_jobs():
        job.remove()


def setup_function():
    _reset()


def teardown_function():
    _reset()


class _Msg:
    def __init__(self, mid):
        self.id = mid
        self.edits = 0
        self.embeds = [
            SimpleNamespace(title="Live", url="https://twitch.tv/ada", image=None)
        ]

    async def edit(self, **kwargs):
        self.edits += 1


class _Chan:
    def __init__(self):
        self.sent = []

    async def send(self, **kwargs):
        msg = _Msg(300 + len(self.sent))
        self.sent.append(kwargs.get("content"))
        return msg


def _stream(stream_id):
    return SimpleNamespace(
        id=stream_id,
        type="live",
        user=SimpleNamespace(id="7", name="ada"),
        title="Live",
        game_name="Chess",
        started_at=datetime.datetime.now(datetime.timezone.utc),
        thumbnail=None,
    )


def _offline(stream_id):
    return SimpleNamespace(
        broadcaster=SimpleNamespace(id="7", name="ada"), id=stream_id
    )


def _startup(monkeypatch, client, ready, restore, streams):
    async def channel():
        return client._chan

    async def fetch_streams(user_ids=None, first=None):
        for stream in streams:
            yield stream

    async def no_sync(*args, **kwargs):
        return None

    async def noop(*args, **kwargs):
        return None

    client._chan = _Chan()
    client.fetch_streams = fetch_streams
    client.reconcile_streamers = restore
    client.setup_twitch_subs = noop
    client.run_youtube_backfill = noop
    client.maintain_youtube_subs = noop
    monkeypatch.setattr(bot, "discord_channel", channel)
    monkeypatch.setattr(bot, "sync_state_to_dynamodb", no_sync)
    monkeypatch.setattr(bot, "sync_state_from_dynamodb", no_sync)
    monkeypatch.setattr(bot, "ClientSession", lambda *args, **kwargs: client._chan)
    monkeypatch.setattr(bot, "TCPConnector", lambda **kwargs: None)
    monkeypatch.setattr(bot.discord_bot, "wait_until_ready", ready)
    monkeypatch.setattr(bot.autosave_state_task, "is_running", lambda: True)
    monkeypatch.setattr(bot.poll_streamers_task, "is_running", lambda: True)
    # event_ready starts the process-wide scheduler. Leave that to the tests
    # that own a scheduler, so this file does not bind it to a closed loop.
    monkeypatch.setattr(bot.scheduler, "start", lambda *args, **kwargs: None)


def _restore_ada(old):
    async def restore(initial=False):
        bot.TWITCH_STREAMERS["7"] = "Ada"
        bot.twitch_active_messages["7"] = old
        bot.twitch_active_stream_ids["7"] = "old-stream"

    return restore


def test_startup_sweep_does_not_revive_an_offline_stream(monkeypatch):
    """Helix often still lists a stream for a few seconds after stream.offline.

    Replaying that offline and then sweeping Get Streams posted a new LIVE
    alert for the broadcast that had just ended.
    """
    old = _Msg(100)

    async def run():
        client = bot.HybridBot()

        async def ready():
            await client.event_stream_offline(_offline("old-stream"))

        _startup(
            monkeypatch,
            client,
            ready,
            _restore_ada(old),
            [_stream("old-stream")],
        )
        await client.event_ready()
        return client

    client = asyncio.run(run())
    assert client._chan.sent == []
    assert "7" not in bot.twitch_active_messages
    assert "7" not in bot.twitch_active_stream_ids
    assert old.edits == 1


def test_startup_sweep_announces_a_different_live_broadcast(monkeypatch):
    """A newer Helix stream id is a new broadcast and must still be announced."""
    old = _Msg(100)

    async def run():
        client = bot.HybridBot()

        async def ready():
            await client.event_stream_offline(_offline("old-stream"))

        _startup(
            monkeypatch,
            client,
            ready,
            _restore_ada(old),
            [_stream("new-stream")],
        )
        await client.event_ready()
        return client

    client = asyncio.run(run())
    assert client._chan.sent == ["🔴 **ada** is LIVE! https://twitch.tv/ada"]
    assert "7" in bot.twitch_active_messages
    assert bot.twitch_active_stream_ids["7"] == "new-stream"
    assert old.edits == 1


def test_youtube_verification_waits_until_streamers_load(monkeypatch):
    """A hub handshake during startup used to 404 because the channel list was empty.

    The hub needs the challenge in this response, so the GET has to wait until
    restore has loaded the channel.
    """
    seen = {}

    async def run():
        client = bot.HybridBot()

        class Request:
            method = "GET"
            remote = "203.0.113.9"
            query = {
                "hub.mode": "subscribe",
                "hub.topic": (
                    "https://www.youtube.com/xml/feeds/videos.xml?channel_id=UC1"
                ),
                "hub.challenge": "chal",
            }

        async def ready():
            task = asyncio.create_task(client.youtube_webhook_handler(Request()))
            await asyncio.sleep(0)
            seen["done_before_restore"] = task.done()
            seen["task"] = task

        async def restore(initial=False):
            bot.YOUTUBE_STREAMERS["UC1"] = "Ada"

        _startup(monkeypatch, client, ready, restore, [])
        await client.event_ready()
        return await seen["task"]

    response = asyncio.run(run())
    assert seen["done_before_restore"] is False
    assert response.status == 200
    assert response.text == "chal"


def test_deferred_youtube_check_does_not_run_before_the_scheduler_starts(
    monkeypatch,
):
    """A held YouTube notification used to be awaited before the scheduler started.

    videos.list has no short timeout, so that check kept restored snipers and
    EventSub setup waiting.
    """
    bot.YOUTUBE_WEBHOOK_SECRET = "test-secret"
    order = []
    body = (
        '<feed xmlns="http://www.w3.org/2005/Atom" '
        'xmlns:yt="http://purl.org/yt/2012"><entry>'
        "<yt:videoId>abc12345678</yt:videoId>"
        "<yt:channelId>UC1</yt:channelId>"
        "</entry></feed>"
    ).encode()
    signature = "sha1=" + hmac.new(b"test-secret", body, hashlib.sha1).hexdigest()

    class Request:
        method = "POST"
        headers = {"X-Hub-Signature": signature}
        remote = "203.0.113.9"

        async def read(self):
            return body

    async def run():
        client = bot.HybridBot()

        async def initial(video_id, save=True):
            order.append("check")

        async def subs():
            order.append("subs")

        client.initial_youtube_check = initial

        async def ready():
            await client.youtube_webhook_handler(Request())

        async def restore(initial=False):
            bot.YOUTUBE_STREAMERS["UC1"] = "Ada"

        _startup(monkeypatch, client, ready, restore, [])
        monkeypatch.setattr(
            bot.scheduler, "start", lambda *args, **kwargs: order.append("scheduler")
        )
        client.setup_twitch_subs = subs
        await client.event_ready()
        await asyncio.sleep(0)

    asyncio.run(run())
    assert order.index("scheduler") < order.index("check")
    assert order.index("subs") < order.index("check")

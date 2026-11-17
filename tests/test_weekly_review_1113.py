"""Regression tests for the 2026-11-13 review.

Plain tests, no xfail. They fail on main until the fix is merged, and this
branch should merge after or together with that fix.
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
        self.sent.append(msg)
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


def _online(stream_id):
    return SimpleNamespace(
        broadcaster=SimpleNamespace(id="7", name="ada"),
        id=stream_id,
        type="live",
    )


def _silence_startup(monkeypatch, client, ready, restore):
    async def channel():
        return client._chan

    async def fetch_streams(user_ids=None, first=None):
        yield _stream("new-stream")

    async def no_sync():
        return None

    async def noop(*args, **kwargs):
        return None

    client._chan = _Chan()
    client.fetch_streams = fetch_streams
    client.reconcile_streamers = restore
    client.setup_twitch_subs = noop
    client.announce_current_twitch_streams = noop
    client.run_youtube_backfill = noop
    client.maintain_youtube_subs = noop
    monkeypatch.setattr(bot, "discord_channel", channel)
    monkeypatch.setattr(bot, "sync_state_to_dynamodb", no_sync)
    monkeypatch.setattr(bot, "sync_state_from_dynamodb", noop)
    monkeypatch.setattr(bot, "ClientSession", lambda *args, **kwargs: client._chan)
    monkeypatch.setattr(bot, "TCPConnector", lambda **kwargs: None)
    monkeypatch.setattr(bot.discord_bot, "wait_until_ready", ready)
    monkeypatch.setattr(bot.autosave_state_task, "is_running", lambda: True)
    monkeypatch.setattr(bot.poll_streamers_task, "is_running", lambda: True)
    monkeypatch.setattr(bot.scheduler, "start", lambda *args, **kwargs: None)


def test_startup_online_event_keeps_one_live_alert(monkeypatch):
    """A stream.online retry can arrive while Discord is still connecting.

    Restore used to replace the alert that retry just posted, so Discord kept
    a second LIVE message that this process no longer tracked.
    """
    old = _Msg(100)

    async def run():
        client = bot.HybridBot()

        async def ready():
            await client.event_stream_online(_online("new-stream"))

        async def restore(initial=False):
            bot.TWITCH_STREAMERS["7"] = "Ada"
            bot.twitch_active_messages["7"] = old
            bot.twitch_active_stream_ids["7"] = "old-stream"

        _silence_startup(monkeypatch, client, ready, restore)
        await client.event_ready()
        return client

    client = asyncio.run(run())
    assert len(client._chan.sent) == 1
    assert bot.twitch_active_messages["7"] is client._chan.sent[0]
    assert old.edits == 1
    assert bot.twitch_active_stream_ids["7"] == "new-stream"


def test_startup_offline_event_clears_the_restored_alert(monkeypatch):
    """stream.offline during Discord connect used to land on an empty memory.

    Restore then put the old alert back, and it stayed up until the next poll.
    """
    old = _Msg(100)

    async def run():
        client = bot.HybridBot()

        async def ready():
            await client.event_stream_offline(
                SimpleNamespace(
                    broadcaster=SimpleNamespace(id="7", name="ada"), id="old-stream"
                )
            )

        async def restore(initial=False):
            bot.TWITCH_STREAMERS["7"] = "Ada"
            bot.twitch_active_messages["7"] = old
            bot.twitch_active_stream_ids["7"] = "old-stream"

        _silence_startup(monkeypatch, client, ready, restore)
        await client.event_ready()

    asyncio.run(run())
    assert "7" not in bot.twitch_active_messages
    assert old.edits == 1


def test_youtube_notification_during_startup_is_not_dropped(monkeypatch):
    """The webhook server is up before YouTube channels are loaded.

    A signed post in that window used to be acknowledged and ignored.
    """
    bot.YOUTUBE_WEBHOOK_SECRET = "test-secret"
    seen = []

    async def run():
        client = bot.HybridBot()

        async def initial(video_id, save=True):
            seen.append(video_id)

        client.initial_youtube_check = initial
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

        async def ready():
            await client.youtube_webhook_handler(Request())

        async def restore(initial=False):
            bot.YOUTUBE_STREAMERS["UC1"] = "Ada"

        _silence_startup(monkeypatch, client, ready, restore)
        await client.event_ready()

    asyncio.run(run())
    assert seen == ["abc12345678"]


def test_sniper_row_is_protected_before_the_coroutine_starts():
    """APScheduler removes a due date job before the coroutine runs.

    A sync that snapshots in that gap deletes the premiere. The row has to
    be protected before _process_jobs returns to the event loop.
    """
    seen = {}
    started = bot.scheduler.running
    orig = bot.scheduler._process_jobs

    def wrapped():
        try:
            return orig()
        finally:
            job_ids = {job.id for job in bot.scheduler.get_jobs()}
            seen["kept"] = (
                "yt_gapvid" in job_ids or "yt_gapvid" in bot._youtube_checks_in_flight
            )

    async def idle(*args):
        return None

    async def run():
        if not bot.scheduler.running:
            bot.scheduler.start()
        bot.scheduler._process_jobs = wrapped
        try:
            past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
                seconds=1
            )
            bot.scheduler.add_job(
                idle,
                "date",
                run_date=past,
                args=["gapvid", past],
                id="yt_gapvid",
                replace_existing=True,
            )
            for _ in range(10):
                await asyncio.sleep(0)
                if "kept" in seen:
                    break
        finally:
            bot.scheduler._process_jobs = orig
            if bot.scheduler.running and not started:
                bot.scheduler.shutdown(wait=False)
                await asyncio.sleep(0)

    asyncio.run(run())
    assert seen.get("kept") is True

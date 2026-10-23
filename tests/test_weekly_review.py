"""Regression tests for the 2026-10-23 review.

Findings that the fix PR addresses are xfail on main and run for real once
that code is present. Delete the xfail markers after the fix merges.
"""

import asyncio
import datetime
import inspect
import signal
import time
from types import SimpleNamespace

import pytest

from discord_twitch import bot


def _fixed_lease() -> bool:
    return hasattr(bot, "_lease_write_allowed")


def _fixed_signals() -> bool:
    return hasattr(bot.EventSubAdapter, "_restore_shutdown_signals")


def _fixed_youtube_schedule() -> bool:
    return hasattr(bot.HybridBot, "_adopt_youtube_schedule")


def _fixed_stream_id() -> bool:
    return hasattr(bot.HybridBot, "_set_twitch_stream_id")


def _fixed_offline_edit() -> bool:
    source = inspect.getsource(bot.HybridBot.check_youtube_offline)
    return "keep tracking until a later check can edit the message" in source


def _fixed_restore_row() -> bool:
    return hasattr(bot, "_queue_restored_item")


_XFAIL_LEASE = pytest.mark.xfail(
    not _fixed_lease(),
    strict=True,
    reason=(
        "A failed renew past the lease TTL still writes state on main. "
        "Remove this xfail once the fix PR merges."
    ),
)
_XFAIL_SIGNALS = pytest.mark.xfail(
    not _fixed_signals(),
    strict=True,
    reason=(
        "aiohttp replaces SIGTERM with SystemExit(1) on main. "
        "Remove this xfail once the fix PR merges."
    ),
)
_XFAIL_SCHEDULE = pytest.mark.xfail(
    not _fixed_youtube_schedule(),
    strict=True,
    reason=(
        "A YouTube sniper ignores the API start time on main and gives up. "
        "Remove this xfail once the fix PR merges."
    ),
)
_XFAIL_STREAM = pytest.mark.xfail(
    not _fixed_stream_id(),
    strict=True,
    reason=(
        "A new Helix stream id is kept only in memory on main. "
        "Remove this xfail once the fix PR merges."
    ),
)
_XFAIL_RESTORE = pytest.mark.xfail(
    not _fixed_restore_row(),
    strict=True,
    reason=(
        "One unreadable state row aborts the rest of the restore on main. "
        "Remove this xfail once the fix PR merges."
    ),
)
_XFAIL_OFFLINE = pytest.mark.xfail(
    not _fixed_offline_edit(),
    strict=True,
    reason=(
        "A non-HTTPException while editing a finished YouTube alert drops it "
        "on main. Remove this xfail once the fix PR merges."
    ),
)


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
    if hasattr(bot, "_lease_until"):
        bot._lease_until = 0
    for job in bot.scheduler.get_jobs():
        job.remove()


@pytest.fixture(autouse=True)
def _clean():
    _reset()
    yield
    _reset()


class _Msg:
    def __init__(self):
        self.id = 42
        self.edits = 0
        self.embeds = []

    async def edit(self, **kwargs):
        self.edits += 1
        self.last_edit = kwargs


def _future_start(days):
    when = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days)
    return when, when.strftime("%Y-%m-%dT%H:%M:%SZ")


async def _fail_one_renew():
    """Run the renew loop through one failing acquire, then cancel the sleep."""

    def boom():
        raise TimeoutError("dynamodb down")

    async def cancel_sleep(_seconds):
        raise asyncio.CancelledError

    original_sleep = asyncio.sleep
    original_acquire = bot._acquire_or_renew_lease
    original_closed = bot.discord_bot.is_closed
    bot._acquire_or_renew_lease = boom
    bot.discord_bot.is_closed = lambda: False
    asyncio.sleep = cancel_sleep
    try:
        await bot._renew_state_lease()
    except asyncio.CancelledError:
        pass
    finally:
        asyncio.sleep = original_sleep
        bot._acquire_or_renew_lease = original_acquire
        bot.discord_bot.is_closed = original_closed


def test_failed_renew_before_the_deadline_keeps_the_lease():
    bot._lease_held = True
    bot._lease_until = int(time.time()) + 40

    asyncio.run(_fail_one_renew())
    assert bot._lease_held is True


@_XFAIL_LEASE
def test_failed_renew_after_the_deadline_stops_writes(monkeypatch):
    pushed = []
    monkeypatch.setattr(bot, "_db_push", lambda *args, **kwargs: pushed.append(args))
    bot._lease_held = True
    bot._lease_until = int(time.time()) - 5

    async def run():
        await _fail_one_renew()
        await bot.sync_state_to_dynamodb()

    asyncio.run(run())
    assert bot._lease_held is False
    assert pushed == []


@_XFAIL_SIGNALS
def test_sigterm_handler_survives_the_web_server():
    async def run():
        client = bot.HybridBot()
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, bot.discord_bot._on_sigterm)
        await client.web_adapter.run(port=0)
        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                handle = loop._signal_handlers[sig]
                callback = getattr(handle, "_callback", handle)
                assert callback == bot.discord_bot._on_sigterm
        finally:
            await client.web_adapter.close()
            for sig in (signal.SIGTERM, signal.SIGINT):
                if sig in getattr(loop, "_signal_handlers", {}):
                    loop.remove_signal_handler(sig)

    asyncio.run(run())


@_XFAIL_SCHEDULE
def test_sniper_follows_a_premiere_the_api_learns_later(monkeypatch):
    async def save():
        return None

    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)
    bot.YOUTUBE_STREAMERS["UC1"] = "Ada"
    start, stamp = _future_start(2)

    async def run():
        client = bot.HybridBot()

        async def fetch(video_id):
            return {
                "snippet": {
                    "channelId": "UC1",
                    "liveBroadcastContent": "upcoming",
                },
                "liveStreamingDetails": {"scheduledStartTime": stamp},
            }

        client.fetch_youtube_data = fetch
        placeholder = datetime.datetime.now(datetime.timezone.utc)
        await client.check_youtube_status("premiere", placeholder)
        job = bot.scheduler.get_job("yt_premiere")
        assert job is not None
        assert job.args[1] > placeholder + datetime.timedelta(days=1)
        assert abs(job.args[1] - start) < datetime.timedelta(seconds=2)

    asyncio.run(run())


@_XFAIL_SCHEDULE
def test_sniper_drops_a_premiere_outside_the_tracking_window(monkeypatch):
    async def save():
        return None

    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)
    bot.YOUTUBE_STREAMERS["UC1"] = "Ada"
    _start, stamp = _future_start(60)

    async def run():
        client = bot.HybridBot()

        async def fetch(video_id):
            return {
                "snippet": {
                    "channelId": "UC1",
                    "liveBroadcastContent": "upcoming",
                },
                "liveStreamingDetails": {"scheduledStartTime": stamp},
            }

        client.fetch_youtube_data = fetch
        now = datetime.datetime.now(datetime.timezone.utc)
        await client.check_youtube_status("far", now)
        assert bot.scheduler.get_job("yt_far") is None

    asyncio.run(run())


def test_sniper_keeps_polling_when_the_api_start_matches(monkeypatch):
    async def save():
        return None

    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)
    bot.YOUTUBE_STREAMERS["UC1"] = "Ada"
    start = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
    stamp = start.strftime("%Y-%m-%dT%H:%M:%SZ")

    async def run():
        client = bot.HybridBot()

        async def fetch(video_id):
            return {
                "snippet": {
                    "channelId": "UC1",
                    "liveBroadcastContent": "upcoming",
                },
                "liveStreamingDetails": {"scheduledStartTime": stamp},
            }

        client.fetch_youtube_data = fetch
        await client.check_youtube_status("soon", start)
        job = bot.scheduler.get_job("yt_soon")
        assert job is not None
        assert abs(job.args[1] - start) < datetime.timedelta(seconds=2)

    asyncio.run(run())


@_XFAIL_OFFLINE
def test_youtube_offline_edit_error_keeps_the_alert(monkeypatch):
    async def save():
        raise AssertionError("should not drop state")

    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)

    async def run():
        client = bot.HybridBot()
        msg = _Msg()
        bot.youtube_active_messages["vid"] = msg

        async def ended(video_id):
            return {"snippet": {"liveBroadcastContent": "none"}}

        async def edit_fails(**kwargs):
            raise RuntimeError("discord rejected the edit")

        msg.edit = edit_fails
        client.fetch_youtube_data = ended
        await client.check_youtube_offline("vid")
        assert bot.youtube_active_messages["vid"] is msg

    asyncio.run(run())


@_XFAIL_OFFLINE
def test_youtube_offline_without_an_embed_still_marks_ended(monkeypatch):
    saved = []

    async def save():
        saved.append(True)

    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)

    async def run():
        client = bot.HybridBot()
        msg = _Msg()
        bot.youtube_active_messages["vid"] = msg

        async def ended(video_id):
            return {"snippet": {"liveBroadcastContent": "none"}}

        client.fetch_youtube_data = ended
        await client.check_youtube_offline("vid")
        assert msg.edits == 1
        assert msg.last_edit["embed"].description == "**Stream Ended**"
        assert "vid" not in bot.youtube_active_messages
        assert saved == [True]

    asyncio.run(run())


def _install_stream(stream_id):
    stream = SimpleNamespace(
        id=stream_id,
        type="live",
        user=SimpleNamespace(id="7", name="ada"),
        title="Live",
        game_name="Chess",
        started_at=datetime.datetime.now(datetime.timezone.utc),
        thumbnail=None,
    )

    async def fetch_streams(user_ids=None, first=None):
        yield stream

    return fetch_streams


@_XFAIL_RESTORE
def test_one_unreadable_state_row_does_not_drop_the_next(monkeypatch):
    seen = []

    async def record_twitch(
        bot_instance, channel, s_id, login, msg_id, stream_id=""
    ):
        seen.append(s_id)

    async def save():
        return None

    monkeypatch.setattr(bot, "restore_twitch_state", record_twitch)
    monkeypatch.setattr(bot, "sync_state_to_dynamodb", save)
    monkeypatch.setattr(
        bot,
        "_db_scan",
        lambda: [
            {"video_id": "yt_bad", "scheduled_time": "not-a-timestamp"},
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
    assert seen == ["7"]


@_XFAIL_STREAM
def test_live_sweep_persists_a_changed_stream_id(monkeypatch):
    pushed = []

    def push(jobs, tw, yt, pending=None):
        pushed.append(tw)

    monkeypatch.setattr(bot, "_db_push", push)
    bot.TWITCH_STREAMERS["7"] = "ada"
    bot.twitch_active_messages["7"] = _Msg()
    bot.twitch_active_stream_ids["7"] = "old-stream"

    async def run():
        client = bot.HybridBot()
        client.fetch_streams = _install_stream("new-stream")
        await client.announce_current_twitch_streams(["7"])

    asyncio.run(run())
    assert pushed
    assert pushed[-1]["tw_7"]["stream_id"] == "new-stream"


@_XFAIL_STREAM
def test_hourly_check_persists_a_changed_stream_id(monkeypatch):
    pushed = []

    def push(jobs, tw, yt, pending=None):
        pushed.append(tw)

    monkeypatch.setattr(bot, "_db_push", push)
    bot.TWITCH_STREAMERS["7"] = "ada"
    bot.twitch_active_messages["7"] = _Msg()
    bot.twitch_active_stream_ids["7"] = "old-stream"

    async def run():
        client = bot.HybridBot()
        client.fetch_streams = _install_stream("new-stream")
        await client.delayed_check("7", "ada", delay=0)

    asyncio.run(run())
    assert pushed
    assert pushed[-1]["tw_7"]["stream_id"] == "new-stream"

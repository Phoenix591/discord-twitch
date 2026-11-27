"""Regression tests for the 2026-11-27 review.

Plain tests, no xfail. The three subscribe-gap tests fail on main until the
fix is merged. The other two already pass and lock the boundaries that fix
has to keep: a later queued online still wins, and an offline for a channel
we were not tracking does not block the sweep.
"""

import asyncio
import datetime
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


def _online(stream_id):
    return SimpleNamespace(
        broadcaster=SimpleNamespace(id="7", name="ada"),
        id=stream_id,
        type="live",
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


def _restore_ada(message, stream_id):
    async def restore(initial=False):
        bot.TWITCH_STREAMERS["7"] = "Ada"
        if message is not None:
            bot.twitch_active_messages["7"] = message
            bot.twitch_active_stream_ids["7"] = stream_id

    return restore


def _restore_untracked():
    async def restore(initial=False):
        bot.TWITCH_STREAMERS["7"] = "Ada"

    return restore


def _record_posted_ids(client):
    posted = []
    original = client._post_twitch_announcement

    async def wrapped(s_id, s_login, stream_id, stream_data):
        posted.append(stream_id)
        return await original(s_id, s_login, stream_id, stream_data)

    client._post_twitch_announcement = wrapped
    return posted


def test_deferred_online_does_not_revive_a_stream_that_ended_during_subscribe(
    monkeypatch,
):
    """A held stream.online is replayed after EventSub setup.

    A stream.offline in that gap retires the restored alert and is recorded
    so the sweep will not post it again. The replay never looked at that set,
    so the older online posted a new LIVE alert for the broadcast that had
    just ended.
    """
    old = _Msg(100)

    async def run():
        client = bot.HybridBot()

        async def ready():
            await client.event_stream_online(_online("old-stream"))

        async def subs():
            await client.event_stream_offline(_offline("old-stream"))

        _startup(
            monkeypatch,
            client,
            ready,
            _restore_ada(old, "old-stream"),
            [_stream("old-stream")],
        )
        client.setup_twitch_subs = subs
        await client.event_ready()
        return client

    client = asyncio.run(run())
    assert client._chan.sent == []
    assert "7" not in bot.twitch_active_messages
    assert "7" not in bot.twitch_active_stream_ids
    assert old.edits == 1


def test_offline_during_subscribe_drops_a_deferred_online_with_no_alert(
    monkeypatch,
):
    """The same gap when restore had no alert for the channel.

    The offline was ignored because nothing was tracked yet, then the queued
    online announced a broadcast Twitch had already ended. Helix still listing
    that id must not bring it back either.
    """

    async def run():
        client = bot.HybridBot()

        async def ready():
            await client.event_stream_online(_online("old-stream"))

        async def subs():
            await client.event_stream_offline(_offline("old-stream"))

        _startup(
            monkeypatch,
            client,
            ready,
            _restore_untracked(),
            [_stream("old-stream")],
        )
        client.setup_twitch_subs = subs
        await client.event_ready()
        return client

    client = asyncio.run(run())
    assert client._chan.sent == []
    assert "7" not in bot.twitch_active_messages
    assert "7" not in bot.twitch_active_stream_ids


def test_newer_broadcast_is_announced_after_the_deferred_one_ends(monkeypatch):
    """Ending the queued broadcast must not hide a different live stream id."""
    old = _Msg(100)

    async def run():
        client = bot.HybridBot()
        posted = _record_posted_ids(client)

        async def ready():
            await client.event_stream_online(_online("old-stream"))

        async def subs():
            await client.event_stream_offline(_offline("old-stream"))

        _startup(
            monkeypatch,
            client,
            ready,
            _restore_ada(old, "old-stream"),
            [_stream("new-stream")],
        )
        client.setup_twitch_subs = subs
        await client.event_ready()
        return client, posted

    client, posted = asyncio.run(run())
    assert posted == ["new-stream"]
    assert client._chan.sent == ["🔴 **ada** is LIVE! https://twitch.tv/ada"]
    assert bot.twitch_active_stream_ids["7"] == "new-stream"
    assert old.edits == 1


def test_later_queued_online_still_announces(monkeypatch):
    """An offline and a later online held in the same queue stay in order.

    The offline is older. Replaying it must not swallow the online that
    arrived after it.
    """
    old = _Msg(100)

    async def run():
        client = bot.HybridBot()
        posted = _record_posted_ids(client)

        async def ready():
            await client.event_stream_offline(_offline("old-stream"))
            await client.event_stream_online(_online("new-stream"))

        _startup(
            monkeypatch,
            client,
            ready,
            _restore_ada(old, "old-stream"),
            [_stream("new-stream")],
        )
        await client.event_ready()
        return client, posted

    client, posted = asyncio.run(run())
    assert posted == ["new-stream"]
    assert client._chan.sent == ["🔴 **ada** is LIVE! https://twitch.tv/ada"]
    assert bot.twitch_active_stream_ids["7"] == "new-stream"
    assert old.edits == 1


def test_untracked_offline_without_a_deferred_online_still_sweeps(monkeypatch):
    """An offline for a channel we were not tracking stays eligible for the sweep.

    Helix listing that channel is the first time this process has seen it.
    Only an offline that ends a still-queued online is dropped.
    """

    async def run():
        client = bot.HybridBot()
        posted = _record_posted_ids(client)

        async def ready():
            return None

        async def subs():
            await client.event_stream_offline(_offline("live-stream"))

        _startup(
            monkeypatch,
            client,
            ready,
            _restore_untracked(),
            [_stream("live-stream")],
        )
        client.setup_twitch_subs = subs
        await client.event_ready()
        return posted

    posted = asyncio.run(run())
    assert posted == ["live-stream"]
    assert bot.twitch_active_stream_ids["7"] == "live-stream"

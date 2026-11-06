"""Regression tests for the 2026-11-06 review.

Both tests are plain (no xfail). They fail on main until the shutdown fixes
are present, and they should merge after or together with that fix.
"""

import asyncio
import threading
import time

from discord_twitch import bot


def _reset():
    bot.twitch_active_messages.clear()
    bot.twitch_active_stream_ids.clear()
    bot.youtube_active_messages.clear()
    bot.TWITCH_STREAMERS.clear()
    bot.YOUTUBE_STREAMERS.clear()
    bot._lease_held = True
    bot._lease_until = 0
    if hasattr(bot, "_lease_closing"):
        bot._lease_closing = False
    if hasattr(bot, "_youtube_checks_in_flight"):
        bot._youtube_checks_in_flight.clear()
    for job in bot.scheduler.get_jobs():
        job.remove()


def setup_function():
    _reset()


def teardown_function():
    _reset()


def test_cancelled_sync_does_not_overlap_the_next_push(monkeypatch):
    """close() cancels an in-flight sync and then syncs again.

    The cancelled task must not still be inside _db_push when the next write
    starts. Overlapping batch writes delete rows the shutdown snapshot kept.
    """
    active = {"n": 0, "max": 0}
    gate = threading.Event()
    started = threading.Event()
    counter = threading.Lock()

    def push(*_args, **_kwargs):
        with counter:
            active["n"] += 1
            active["max"] = max(active["max"], active["n"])
        started.set()
        gate.wait(3)
        with counter:
            active["n"] -= 1

    monkeypatch.setattr(bot, "_db_push", push)
    bot._lease_held = True
    bot._lease_until = 0

    async def run():
        first = asyncio.create_task(bot.sync_state_to_dynamodb())
        assert await asyncio.to_thread(started.wait, 5)
        first.cancel()
        second = asyncio.create_task(bot.sync_state_to_dynamodb())
        for _ in range(50):
            await asyncio.sleep(0.02)
            with counter:
                if active["max"] >= 2:
                    break
        with counter:
            seen = active["max"]
        gate.set()
        try:
            await first
        except asyncio.CancelledError:
            pass
        await second
        return seen

    try:
        assert asyncio.run(run()) == 1
    finally:
        gate.set()


def test_in_flight_renew_cannot_recreate_the_lease_after_release(monkeypatch):
    """A renew blocked in UpdateItem survives task.cancel().

    _release_lease has to wait for that update and then delete. If the update
    lands after the delete, the next process waits out the 60 second TTL.
    """
    entered = threading.Event()

    class Table:
        def __init__(self):
            self.item = {
                "video_id": "lease",
                "owner": bot.INSTANCE_ID,
                "expires_at": 1,
            }

        def update_item(self, **kwargs):
            entered.set()
            time.sleep(0.5)
            values = kwargs["ExpressionAttributeValues"]
            self.item = {
                "video_id": "lease",
                "owner": values[":o"],
                "expires_at": values[":e"],
            }

        def delete_item(self, **kwargs):
            owner = kwargs["ExpressionAttributeValues"][":o"]
            if not self.item or self.item.get("owner") != owner:
                raise AssertionError("deleted a lease this process does not hold")
            self.item = None

    table = Table()
    monkeypatch.setattr(bot, "_dynamodb_table", lambda name: table)
    bot._lease_held = True
    bot._lease_until = time.time() + 60
    original_closed = bot.discord_bot.is_closed
    bot.discord_bot.is_closed = lambda: False

    async def run():
        task = asyncio.create_task(bot._renew_state_lease())
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await asyncio.to_thread(bot._release_lease)
        # Let a write that raced past the delete show up.
        await asyncio.sleep(0.7)

    try:
        asyncio.run(run())
    finally:
        bot.discord_bot.is_closed = original_closed
    assert table.item is None
    assert bot._lease_held is False

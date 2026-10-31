"""Regression tests for the 2026-10-30 review.

test_graceful_release_deletes_the_lease_row and
test_sync_during_a_sniper_fetch_keeps_the_premiere cover the companion fix.
They fail on main until that change is present, and they stay plain tests
because the fix is already in a pending PR.
"""

import asyncio
import datetime
import re

import pytest
from botocore.exceptions import ClientError

from discord_twitch import bot


def _reset():
    bot.twitch_active_messages.clear()
    bot.twitch_active_stream_ids.clear()
    bot.youtube_active_messages.clear()
    bot.TWITCH_STREAMERS.clear()
    bot.YOUTUBE_STREAMERS.clear()
    bot._lease_held = True
    bot._lease_until = 0
    if hasattr(bot, "_youtube_checks_in_flight"):
        bot._youtube_checks_in_flight.clear()
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
        self.embeds = []


def _validation_error():
    return ClientError(
        {
            "Error": {
                "Code": "ValidationException",
                "Message": (
                    "Invalid ConditionExpression: Attribute name is a reserved "
                    "keyword; reserved keyword: owner"
                ),
            }
        },
        "DeleteItem",
    )


def _condition_error():
    return ClientError(
        {
            "Error": {
                "Code": "ConditionalCheckFailedException",
                "Message": "The conditional request failed",
            }
        },
        "DeleteItem",
    )


def _bare_owner(expression):
    """True when owner is used as an attribute, not as a #placeholder."""
    return re.search(r"(?<!#)\bowner\b", expression or "") is not None


class _LeaseTable:
    """DeleteItem that rejects a bare reserved word, as DynamoDB does."""

    def __init__(self, item):
        self.item = item
        self.calls = []

    def delete_item(self, **kwargs):
        self.calls.append("delete")
        expression = kwargs.get("ConditionExpression", "")
        if _bare_owner(expression):
            raise _validation_error()
        names = kwargs.get("ExpressionAttributeNames") or {}
        values = kwargs.get("ExpressionAttributeValues") or {}
        if expression != "#owner = :o" or names.get("#owner") != "owner":
            raise _validation_error()
        if not self.item or self.item.get("owner") != values.get(":o"):
            raise _condition_error()
        self.item = None


class _Batch:
    def __init__(self, store):
        self._store = store
        self._puts = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        for item in self._puts:
            self._store[item["video_id"]] = item
        return False

    def put_item(self, Item):
        self._puts.append(Item)

    def delete_item(self, Key):
        self._store.pop(Key["video_id"], None)


class _StateTable:
    def __init__(self, store):
        self._store = store

    def batch_writer(self):
        return _Batch(self._store)


def _install_state(monkeypatch, store):
    monkeypatch.setattr(bot, "scan_all", lambda table: list(store.values()))
    monkeypatch.setattr(bot, "_dynamodb_table", lambda name: _StateTable(store))


def test_graceful_release_deletes_the_lease_row(monkeypatch):
    table = _LeaseTable(
        {"video_id": "lease", "owner": bot.INSTANCE_ID, "expires_at": 1}
    )
    monkeypatch.setattr(bot, "_dynamodb_table", lambda name: table)
    bot._lease_held = True

    bot._release_lease()

    assert table.calls == ["delete"]
    assert table.item is None
    assert bot._lease_held is False


def test_release_does_not_delete_another_owners_lease(monkeypatch):
    table = _LeaseTable({"video_id": "lease", "owner": "other", "expires_at": 1})
    monkeypatch.setattr(bot, "_dynamodb_table", lambda name: table)
    bot._lease_held = True

    bot._release_lease()

    assert table.calls == ["delete"]
    assert table.item["owner"] == "other"
    assert bot._lease_held is False


def test_sync_during_a_sniper_fetch_keeps_the_premiere(monkeypatch):
    store = {"yt_premiere": {"video_id": "yt_premiere", "scheduled_time": "old"}}
    _install_state(monkeypatch, store)

    async def run():
        client = bot.HybridBot()

        async def fetch(video_id):
            await bot.sync_state_to_dynamodb()
            return None

        client.fetch_youtube_data = fetch
        started = datetime.datetime.now(datetime.timezone.utc)
        await client.check_youtube_status("premiere", started)

    asyncio.run(run())
    assert "yt_premiere" in store
    job = bot.scheduler.get_job("yt_premiere")
    assert job is not None
    assert job.args[0] == "premiere"


def test_sniper_give_up_still_deletes_the_premiere(monkeypatch):
    store = {"yt_old": {"video_id": "yt_old", "scheduled_time": "old"}}
    _install_state(monkeypatch, store)

    async def run():
        client = bot.HybridBot()

        async def fetch(video_id):
            return None

        client.fetch_youtube_data = fetch
        started = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            minutes=30
        )
        await client.check_youtube_status("old", started)

    asyncio.run(run())
    assert "yt_old" not in store
    assert bot.scheduler.get_job("yt_old") is None


def test_live_sniper_drops_the_schedule_row(monkeypatch):
    store = {"yt_livevid": {"video_id": "yt_livevid", "scheduled_time": "old"}}
    _install_state(monkeypatch, store)
    bot.YOUTUBE_STREAMERS["UC1"] = "Ada"

    class Chan:
        async def send(self, **kwargs):
            return _Msg()

    async def channel():
        return Chan()

    monkeypatch.setattr(bot, "discord_channel", channel)

    async def run():
        client = bot.HybridBot()

        async def fetch(video_id):
            return {
                "id": "livevid",
                "snippet": {
                    "channelId": "UC1",
                    "channelTitle": "Ada",
                    "title": "Live",
                    "liveBroadcastContent": "live",
                    "thumbnails": {},
                },
            }

        client.fetch_youtube_data = fetch
        when = datetime.datetime.now(datetime.timezone.utc)
        await client.check_youtube_status("livevid", when)

    asyncio.run(run())
    assert "yt_livevid" not in store
    assert store["ytlive_livevid"]["message_id"] == "42"
    assert bot.scheduler.get_job("yt_livevid") is None
    assert bot.scheduler.get_job("yt_monitor_livevid") is not None

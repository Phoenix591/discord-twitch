#!/usr/bin/env python
import asyncio
import logging
import discord
import os
import sys
import datetime
import json
import signal
import subprocess
import socket
import traceback
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, parse_qs
from typing import Any
from aiohttp import web, ClientSession, TCPConnector
from discord.ext import commands, tasks
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import twitchio
from twitchio.web import AiohttpAdapter
from twitchio.eventsub import StreamOnlineSubscription, StreamOfflineSubscription
import uuid
import time
import hmac
import hashlib
import secrets

from discord_twitch.config import dynamodb_settings, load_parser
from discord_twitch.dynamo import scan_all

INSTANCE_ID = str(uuid.uuid4())

# Import boto3 for S3 access
try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("❌ Critical: 'boto3' is missing. Please run: pip install boto3")
    sys.exit(1)

# Setup & Logging
logger = logging.getLogger("Bot")
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%H:%M:%S"
)

# Systemd Journal Handler (stdout). The log file is attached in main(), once
# LogsDirectory= exists. Doing it at import crashes under ProtectSystem=strict.
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Global Variables
twitch_bot = None
twitch_active_messages = {}
twitch_active_tasks = {}
twitch_active_stream_ids = {}
youtube_active_messages = {}
_twitch_announcing = set()
# misfire_grace_time of 1 second drops date jobs that are already due when start()
# runs. None means run them no matter how late they are.
scheduler = AsyncIOScheduler(
    job_defaults={"misfire_grace_time": None, "coalesce": True}
)
YOUTUBE_WEBHOOK_SECRET = ""
TWITCH_EVENTSUB_SECRET = ""
_sync_lock = asyncio.Lock()

# Config Placeholders
DYNAMODB_REGION = "us-east-1"
DYNAMODB_TABLE_NAME = ""
STREAMERS_TABLE_NAME = "discord-twitch-streamers"
DISCORD_TOKEN = ""
DISCORD_CHANNEL_ID = 0
TWITCH_CLIENT_ID = ""
TWITCH_CLIENT_SECRET = ""
YOUTUBE_API_KEY = ""
YOUTUBE_BACKFILL_CHECK = 2
SERVER_DOMAIN = ""
PUBLIC_URL = ""
LOCAL_PORT = 8080
TWITCH_STREAMERS = {}
YOUTUBE_STREAMERS = {}
INTERNAL_API_SECRET = ""


# Discord Bot Setup
class DiscordTwitchBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self) -> None:
        if twitch_bot:
            self.loop.create_task(twitch_bot.start())

    async def close(self):
        logger.info("🛑 Received shutdown signal. Saving state...")
        try:
            await sync_state_to_dynamodb()
        except Exception as e:
            logger.error(f"Error saving state on shutdown: {e}")

        if twitch_bot:
            logger.info("🛑 Shutting down Twitch Bot...")
            try:
                await twitch_bot.close()
            except Exception as e:
                logger.error(f"Error closing Twitch bot: {e}")

        logger.info("🛑 Shutting down Scheduler...")
        try:
            scheduler.shutdown(wait=False)
        except Exception as e:
            logger.error(f"Error closing scheduler: {e}")

        logger.info("🛑 Shutting down Discord Bot...")
        try:
            await super().close()
        except Exception as e:
            logger.error(f"Error closing Discord bot: {e}")

        logger.info("👋 Exiting successfully (End of close sequence)")


# Create the global instance using the new class
discord_bot = DiscordTwitchBot()


def load_config():
    global DISCORD_TOKEN, DISCORD_CHANNEL_ID, TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET
    global YOUTUBE_API_KEY, YOUTUBE_BACKFILL_CHECK
    global DYNAMODB_TABLE_NAME, DYNAMODB_REGION, STREAMERS_TABLE_NAME
    global SERVER_DOMAIN, PUBLIC_URL, LOCAL_PORT
    global TWITCH_STREAMERS, YOUTUBE_STREAMERS, INTERNAL_API_SECRET
    global YOUTUBE_WEBHOOK_SECRET, TWITCH_EVENTSUB_SECRET

    config, secret_path = load_parser()
    logger.info(f"🔒 Loading secrets from: {secret_path}")

    DISCORD_TOKEN = config["discord"]["token"]
    DISCORD_CHANNEL_ID = int(config["discord"]["channelid"])
    TWITCH_CLIENT_ID = config["twitch"]["clientid"]
    TWITCH_CLIENT_SECRET = config["twitch"]["clientsecret"]
    YOUTUBE_API_KEY = (
        config["youtube"].get("api_key", "") if "youtube" in config else ""
    )
    YOUTUBE_BACKFILL_CHECK = (
        int(config["youtube"].get("backfill_check", 2)) if "youtube" in config else 2
    )
    DYNAMODB_REGION, DYNAMODB_TABLE_NAME, STREAMERS_TABLE_NAME = dynamodb_settings(
        config
    )
    SERVER_DOMAIN = config["server"]["domain"]
    PUBLIC_URL = config["server"]["public_url"]
    LOCAL_PORT = int(config["server"]["port"])
    INTERNAL_API_SECRET = config["server"]["internal_api_secret"]

    yt_secret = ""
    tw_secret = ""
    if "youtube" in config:
        yt_secret = config["youtube"].get("webhook_secret", "").strip()
    if "twitch" in config:
        tw_secret = config["twitch"].get("eventsub_secret", "").strip()
    YOUTUBE_WEBHOOK_SECRET = yt_secret or secrets.token_hex(32)
    TWITCH_EVENTSUB_SECRET = tw_secret or secrets.token_hex(32)
    if not tw_secret:
        logger.info("No EventSub secret in config; generated one for this process.")
    if not yt_secret:
        logger.info("No YouTube webhook secret in config; generated one for this process.")


async def restore_twitch_state(
    bot_instance, channel, s_id, login, msg_id, stream_id=""
):
    if s_id not in twitch_active_messages:
        twitch_active_messages[s_id] = msg_id
    try:
        msg = await channel.fetch_message(msg_id)
        twitch_active_messages[s_id] = msg
        if stream_id:
            twitch_active_stream_ids[s_id] = stream_id
        twitch_active_tasks[s_id] = asyncio.create_task(
            bot_instance.delayed_check(s_id, login, delay=0)
        )
    except discord.NotFound:
        # Message was deleted. Drop the lock so the live sweep can post again.
        twitch_active_messages.pop(s_id, None)
        twitch_active_stream_ids.pop(s_id, None)
    except Exception as e:
        logger.error(f"Failed to fetch twitch message {msg_id}: {e}")
        twitch_active_tasks[s_id] = asyncio.create_task(
            bot_instance.delayed_check(s_id, login, delay=0)
        )


async def restore_youtube_state(bot_instance, channel, vid, msg_id):
    if vid not in youtube_active_messages:
        youtube_active_messages[vid] = msg_id

    try:
        msg = await channel.fetch_message(msg_id)
        youtube_active_messages[vid] = msg
        bot_instance.schedule_youtube_monitor(vid, immediate=True)
    except discord.NotFound:
        # Message was deleted. Keep the id so we do not announce it again,
        # and still watch for the stream ending.
        bot_instance.schedule_youtube_monitor(vid, immediate=True)
    except Exception as e:
        logger.error(f"Failed to fetch youtube message {msg_id}: {e}")
        bot_instance.schedule_youtube_monitor(vid, immediate=True)


# --- Helper functions for background threads ---
def _dynamodb_table(name):
    dynamodb = boto3.resource("dynamodb", region_name=DYNAMODB_REGION)
    return dynamodb.Table(name)


def _db_get_streamers():
    try:
        return scan_all(_dynamodb_table(STREAMERS_TABLE_NAME))
    except Exception as e:
        logger.error(f"Failed to fetch streamers from DB: {e}")
        return None  # Return None on failure to prevent accidental un-subscribes


def _db_scan():
    return scan_all(_dynamodb_table(DYNAMODB_TABLE_NAME))


def _db_push(jobs_data, tw_data, yt_data, pending_ids=None):
    table = _dynamodb_table(DYNAMODB_TABLE_NAME)
    db_items = scan_all(table)
    active_db_keys = set()
    pending_ids = pending_ids or set()

    # Using batch_writer handles throttling and groups requests automatically!
    with table.batch_writer() as batch:
        for db_key, stime in jobs_data.items():
            active_db_keys.add(db_key)
            batch.put_item(Item={"video_id": db_key, "scheduled_time": stime})

        for db_key, item_data in tw_data.items():
            active_db_keys.add(db_key)
            batch.put_item(Item=item_data)

        for db_key, item_data in yt_data.items():
            active_db_keys.add(db_key)
            batch.put_item(Item=item_data)

        for item in db_items:
            video_id = item["video_id"]
            if video_id not in active_db_keys and video_id not in pending_ids:
                batch.delete_item(Key={"video_id": video_id})


def _helix_stream_is_live(stream):
    # Helix uses "live", or "" when the type could not be determined.
    return getattr(stream, "type", "live") in ("live", "")


async def discord_channel():
    channel = discord_bot.get_channel(DISCORD_CHANNEL_ID)
    if channel:
        return channel
    try:
        return await discord_bot.fetch_channel(DISCORD_CHANNEL_ID)
    except Exception as e:
        logger.error(f"❌ Could not fetch Discord channel: {e}")
        return None


# --- Main Async Sync Functions ---
async def sync_state_from_dynamodb(bot_instance):
    try:
        logger.info("☁️  Downloading state from DynamoDB...")
        items = await asyncio.to_thread(_db_scan)
        now = datetime.datetime.now(datetime.timezone.utc)

        # 1. Bypass the cache to guarantee we get the channel
        channel = await discord_channel()
        if not channel:
            logger.error("❌ Critical Error: Could not fetch Discord channel")
            return  # Abort sync! Do not proceed with an empty memory state!

        # 2. Collect all restoration tasks so we can wait for them
        restore_tasks = []

        for item in items:
            db_key = item["video_id"]

            if db_key.startswith("ytlive_"):
                vid = db_key[7:]
                msg_id = int(item.get("message_id", 0))
                if msg_id:
                    restore_tasks.append(
                        restore_youtube_state(bot_instance, channel, vid, msg_id)
                    )
            elif db_key.startswith("yt_"):
                vid = db_key[3:]
                s_time = datetime.datetime.fromisoformat(item["scheduled_time"])
                if s_time < now - datetime.timedelta(
                    days=2
                ) or s_time > now + datetime.timedelta(days=45):
                    continue
                run_date = s_time - datetime.timedelta(minutes=3)
                if run_date < now:
                    run_date = now + datetime.timedelta(seconds=5)
                bot_instance.schedule_youtube_sniper(vid, s_time, run_date)
            elif db_key.startswith("tw_"):
                s_id = db_key[3:]
                login = item.get("login", "")
                msg_id = int(item.get("message_id", 0))
                stream_id = item.get("stream_id", "")
                if msg_id:
                    restore_tasks.append(
                        restore_twitch_state(
                            bot_instance, channel, s_id, login, msg_id, stream_id
                        )
                    )

        # 3. Wait for ALL Discord messages to be fetched into memory before proceeding
        if restore_tasks:
            await asyncio.gather(*restore_tasks)

        logger.info(f"✅ State loaded from DynamoDB. Restored {len(items)} items.")
        await sync_state_to_dynamodb()
    except Exception as e:
        logger.error(f"❌ Failed to load state from DynamoDB: {e}")


async def sync_state_to_dynamodb():
    async with _sync_lock:
        try:
            # Snapshot with no await. Placeholder 0 is an in-flight send: keep the
            # existing row, and do not overwrite it with incomplete data.
            jobs_data = {}
            for job in scheduler.get_jobs():
                if job.id.startswith("yt_") and not job.id.startswith("yt_monitor_"):
                    try:
                        jobs_data[f"yt_{job.args[0]}"] = job.args[1].isoformat()
                    except IndexError:
                        pass

            pending_ids = set()
            tw_data = {}
            for s_id, msg in list(twitch_active_messages.items()):
                if msg is None or msg == 0:
                    pending_ids.add(f"tw_{s_id}")
                    continue

                if isinstance(msg, int):
                    m_id = str(msg)
                    login = "Unknown"
                else:
                    m_id = str(msg.id)
                    login = "Unknown"
                    if msg.embeds and msg.embeds[0].url:
                        login = msg.embeds[0].url.split("/")[-1].lower()

                tw_data[f"tw_{s_id}"] = {
                    "video_id": f"tw_{s_id}",
                    "message_id": m_id,
                    "login": login,
                    "stream_id": twitch_active_stream_ids.get(s_id, ""),
                }

            yt_data = {}
            for vid, msg in list(youtube_active_messages.items()):
                if msg is None or msg == 0:
                    pending_ids.add(f"ytlive_{vid}")
                    continue

                if isinstance(msg, int):
                    m_id = str(msg)
                else:
                    m_id = str(msg.id)

                yt_data[f"ytlive_{vid}"] = {
                    "video_id": f"ytlive_{vid}",
                    "message_id": m_id,
                }
            await asyncio.to_thread(
                _db_push, jobs_data, tw_data, yt_data, pending_ids
            )
            logger.info("☁️  State synced to DynamoDB.")
        except Exception as e:
            logger.error(f"❌ DynamoDB Sync failed: {e}")


# Main Hybrid Bot Class
class HybridBot(twitchio.Client):
    def __init__(self) -> None:
        self.web_adapter = AiohttpAdapter(
            host="0.0.0.0",
            port=LOCAL_PORT,
            domain=SERVER_DOMAIN,
            eventsub_secret=TWITCH_EVENTSUB_SECRET,
        )
        self.session = None
        super().__init__(
            client_id=TWITCH_CLIENT_ID,
            client_secret=TWITCH_CLIENT_SECRET,
            adapter=self.web_adapter,
        )

        if hasattr(self.web_adapter, "router"):
            path = urlparse(PUBLIC_URL).path.rstrip("/")
            route = path + "/youtube"
            self.web_adapter.router.add_post(route, self.youtube_webhook_handler)
            self.web_adapter.router.add_get(route, self.youtube_webhook_handler)
            logger.info(f"✅ Registered YouTube Route: {route}")
            self.web_adapter.router.add_post(
                "/internal/takeover", self.internal_takeover_handler
            )
            logger.info(f"Registered takeover handler")

    async def event_ready(self) -> None:
        logger.info(f"✅ Hybrid Bot Listening on {LOCAL_PORT} (IPv4)")
        conn = TCPConnector(family=socket.AF_INET)
        self.session = ClientSession(connector=conn)

        await discord_bot.wait_until_ready()
        await self.reconcile_streamers(initial=True)
        await sync_state_from_dynamodb(self)
        if not scheduler.running:
            scheduler.start()
        await self.setup_twitch_subs()
        await self.announce_current_twitch_streams()
        await self.run_youtube_backfill()
        asyncio.create_task(self.maintain_youtube_subs())
        if not autosave_state_task.is_running():
            autosave_state_task.start()
        if not poll_streamers_task.is_running():
            poll_streamers_task.start()

    async def close(self):
        if self.session:
            await self.session.close()
        await super().close()

    def schedule_youtube_sniper(self, video_id, scheduled_time, run_time):
        scheduler.add_job(
            self.check_youtube_status,
            "date",
            run_date=run_time,
            args=[video_id, scheduled_time],
            id=f"yt_{video_id}",
            replace_existing=True,
        )

    def schedule_youtube_monitor(self, video_id, immediate=False):
        kwargs = {}
        if immediate:
            # A restored interval otherwise waits the full 30 minutes.
            kwargs["next_run_time"] = datetime.datetime.now(datetime.timezone.utc)
        scheduler.add_job(
            self.check_youtube_offline,
            "interval",
            minutes=30,
            args=[video_id],
            id=f"yt_monitor_{video_id}",
            replace_existing=True,
            **kwargs,
        )

    async def _youtube_hub(self, mode, channel_id):
        if not self.session:
            return False, "HTTP session is not open"
        data = {
            "hub.mode": mode,
            "hub.topic": f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={channel_id}",
            "hub.callback": f"{PUBLIC_URL}/youtube",
            "hub.secret": YOUTUBE_WEBHOOK_SECRET,
        }
        if mode == "subscribe":
            data["hub.lease_seconds"] = 432000
        try:
            async with self.session.post(
                "https://pubsubhubbub.appspot.com/subscribe", data=data
            ) as resp:
                if resp.status >= 400:
                    return False, f"HTTP {resp.status}"
                return True, ""
        except Exception as e:
            return False, str(e)

    async def internal_takeover_handler(self, request):
        signature = request.headers.get("X-Signature")
        timestamp = request.headers.get("X-Timestamp")

        if not signature or not timestamp:
            return web.Response(status=401, text="Missing authentication headers")

        # 1. Validate Timestamp to prevent replay attacks (must be within 60 seconds)
        try:
            ts = float(timestamp)
            now = datetime.datetime.now(datetime.timezone.utc).timestamp()
            if abs(now - ts) > 60:
                logger.warning(
                    f"⚠️ Takeover attempt rejected: Expired timestamp ({timestamp})"
                )
                return web.Response(status=401, text="Expired timestamp")
        except ValueError:
            return web.Response(status=400, text="Invalid timestamp format")

        # 2. Cryptographic HMAC-SHA256 Verification
        message = timestamp.encode("utf-8")
        secret = INTERNAL_API_SECRET.encode("utf-8")
        expected_mac = hmac.new(secret, message, hashlib.sha256).hexdigest()

        if not hmac.compare_digest(expected_mac, signature):
            logger.warning(
                f"⚠️ Unauthorized takeover attempt! Invalid signature from {request.remote}"
            )
            return web.Response(status=403, text="Invalid signature")

        # 3. Success! Yield the server
        logger.info(
            "🛑 Authorized takeover signal received via API! Yielding gracefully..."
        )

        # Schedule the shutdown so we can return the 200 OK response to Ansible immediately
        asyncio.create_task(self.delayed_shutdown())

        return web.Response(status=200, text="Takeover accepted. Shutting down.")

    async def delayed_shutdown(self):
        # Wait 1 second to ensure the HTTP 200 OK is sent back to the client
        await asyncio.sleep(1)

        # Saves state to DynamoDB and closes Discord
        await discord_bot.close()

        # Exit with code 0 so systemd's 'Restart=on-failure' leaves this dead process in the grave
        _exit(0)

    async def youtube_webhook_handler(self, request):
        if request.method == "GET":
            challenge = request.query.get("hub.challenge")
            return (
                web.Response(text=challenge) if challenge else web.Response(status=404)
            )

        # 1. Read the raw body bytes for signature verification
        body = await request.read()

        # 2. Verify Google's Signature (YouTube uses SHA1)
        signature = request.headers.get("X-Hub-Signature")
        if not signature:
            logger.warning("⚠️ YouTube Webhook missing signature! (Spoof attempt?)")
            return web.Response(status=403, text="Forbidden")

        secret = YOUTUBE_WEBHOOK_SECRET.encode("utf-8")
        expected_mac = "sha1=" + hmac.new(secret, body, hashlib.sha1).hexdigest()

        if not hmac.compare_digest(expected_mac, signature):
            logger.warning(
                f"⚠️ YouTube Webhook signature mismatch! Unauthorized access attempt from {request.remote}"
            )
            return web.Response(status=403, text="Invalid signature")

        # 3. Parse the verified XML
        try:
            xml_text = body.decode("utf-8")
            root = ET.fromstring(xml_text)
            ns = {
                "atom": "http://www.w3.org/2005/Atom",
                "yt": "http://purl.org/yt/2012",
            }
            entry = root.find("atom:entry", ns)
            if entry is not None:
                vid_elem = entry.find("yt:videoId", ns)
                cid_elem = entry.find("yt:channelId", ns)
                if vid_elem is not None and cid_elem is not None:
                    video_id = vid_elem.text
                    channel_id = cid_elem.text
                    if channel_id in YOUTUBE_STREAMERS:
                        asyncio.create_task(self.initial_youtube_check(video_id))
        except Exception as e:
            logger.error(f"YouTube XML Parse Error: {e}")

        return web.Response(text="OK")

    async def run_youtube_backfill(self):
        logger.info(f"🔎 Backfilling YouTube State (limit {YOUTUBE_BACKFILL_CHECK})...")
        if not YOUTUBE_API_KEY:
            logger.warning("   ⚠️ No API Key found. Skipping Backfill.")
            return

        tasks = []
        rss_headers = {
            "User-Agent": "Mozilla/5.0 (compatible; DiscordTwitchBot/2.0; +http://discordapp.com)"
        }

        for channel_id in list(YOUTUBE_STREAMERS):
            playlist_id = None
            try:
                url = "https://www.googleapis.com/youtube/v3/channels"
                params = {
                    "part": "contentDetails",
                    "id": channel_id,
                    "key": YOUTUBE_API_KEY,
                }
                async with self.session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("items"):
                            playlist_id = data["items"][0]["contentDetails"][
                                "relatedPlaylists"
                            ]["uploads"]
                    elif resp.status == 403:
                        err = await resp.json()
                        reason = err.get("error", {}).get("message", "Unknown 403")
                        logger.warning(
                            f"   ⚠️ API Lookup 403 for {channel_id}: {reason}"
                        )
            except Exception as e:
                logger.debug(f"   ⚠️ API Lookup exc for {channel_id}: {e}")

            if not playlist_id and channel_id.startswith("UC"):
                playlist_id = "UU" + channel_id[2:]

            success = False
            if playlist_id:
                try:
                    url = "https://www.googleapis.com/youtube/v3/playlistItems"
                    params = {
                        "part": "contentDetails",
                        "playlistId": playlist_id,
                        "maxResults": YOUTUBE_BACKFILL_CHECK,
                        "key": YOUTUBE_API_KEY,
                    }
                    async with self.session.get(url, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for item in data.get("items", []):
                                vid = item["contentDetails"]["videoId"]
                                tasks.append(
                                    self.initial_youtube_check(vid, save=False)
                                )
                            success = True
                        elif resp.status == 403:
                            err = await resp.json()
                            reason = err.get("error", {}).get("message", "Unknown 403")
                            logger.warning(
                                f"   ⚠️ Playlist Fetch 403 for {channel_id}: {reason}"
                            )
                except Exception as e:
                    logger.debug(f"   ⚠️ Playlist Fetch exc: {e}")

            if not success:
                try:
                    url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
                    async with self.session.get(url, headers=rss_headers) as resp:
                        if resp.status == 200:
                            xml_text = await resp.text()
                            root = ET.fromstring(xml_text)
                            ns = {
                                "atom": "http://www.w3.org/2005/Atom",
                                "yt": "http://purl.org/yt/2012",
                            }
                            for entry in root.findall("atom:entry", ns)[
                                :YOUTUBE_BACKFILL_CHECK
                            ]:
                                vid_elem = entry.find("yt:videoId", ns)
                                if vid_elem is not None and vid_elem.text:
                                    tasks.append(
                                        self.initial_youtube_check(
                                            vid_elem.text, save=False
                                        )
                                    )
                        else:
                            logger.warning(
                                f"   ❌ RSS Fallback failed for {channel_id}: {resp.status}"
                            )
                except Exception as e:
                    logger.warning(f"   ❌ RSS Fallback exc for {channel_id}: {e}")

        if tasks:
            await asyncio.gather(*tasks)
            await sync_state_to_dynamodb()

    async def initial_youtube_check(self, video_id, save=True):
        try:
            data = await self.fetch_youtube_data(video_id)
        except Exception as e:
            logger.error(f"API Error fetching {video_id}: {e}")
            return
        if not data:
            await self.remove_youtube_job(video_id, save)
            return

        snippet = data["snippet"]
        channel_id = snippet.get("channelId")
        if channel_id not in YOUTUBE_STREAMERS:
            logger.info(
                f"   ℹ️ Skipping {video_id}: channel {channel_id} is no longer tracked."
            )
            await self.remove_youtube_job(video_id, save)
            return

        live_details = data.get("liveStreamingDetails", {})
        is_live = snippet.get("liveBroadcastContent") == "live"
        scheduled_start = live_details.get("scheduledStartTime")
        actual_end = live_details.get("actualEndTime")

        # 1. If YouTube confirms it has an actual end time, it's definitively over. Drop it!
        if actual_end:
            await self.remove_youtube_job(video_id, save)
            return

        if is_live:
            await self.send_youtube_notification(data)
            await self.remove_youtube_job(video_id, save)
        elif scheduled_start:
            dt = datetime.datetime.fromisoformat(scheduled_start.replace("Z", "+00:00"))
            now = datetime.datetime.now(datetime.timezone.utc)

            # 2. Ignore placeholder "waiting rooms" scheduled more than 45 days out
            if dt > now + datetime.timedelta(days=45):
                logger.info(
                    f"   ⏭️ Skipping {video_id}: Scheduled > 45 days in the future."
                )
                await self.remove_youtube_job(video_id, save)
                return

            # 3. Ignore stale schedules that missed their time by > 2 days and never went live
            if dt < now - datetime.timedelta(days=2):
                logger.info(
                    f"   ⏭️ Skipping {video_id}: Scheduled > 2 days in the past."
                )
                await self.remove_youtube_job(video_id, save)
                return

            logger.info(f"   🗓️ Scheduled for {dt}. Queueing Sniper.")
            run_time = dt - datetime.timedelta(minutes=3)
            if run_time < now:
                run_time = now + datetime.timedelta(seconds=10)
            self.schedule_youtube_sniper(video_id, dt, run_time)
            if save:
                await sync_state_to_dynamodb()

    async def check_youtube_status(self, video_id, scheduled_time):
        now = datetime.datetime.now(datetime.timezone.utc)
        try:
            data = await self.fetch_youtube_data(video_id)
        except Exception as e:
            logger.error(f"API Error sniper {video_id}: {e}")
            # The date job has already been consumed. Give up once the old window
            # has passed so a dead video cannot poll every 90 seconds forever.
            if now >= scheduled_time + datetime.timedelta(minutes=21):
                logger.info(f"   🛑 Giving up on {video_id} after API errors.")
                await sync_state_to_dynamodb()
                return
            self.schedule_youtube_sniper(
                video_id, scheduled_time, now + datetime.timedelta(seconds=90)
            )
            return
        if not data:
            logger.info(f"   🛑 Giving up on {video_id} (video not found).")
            await sync_state_to_dynamodb()
            return

        channel_id = data["snippet"].get("channelId")
        if channel_id not in YOUTUBE_STREAMERS:
            logger.info(
                f"   🛑 Giving up on {video_id}: channel {channel_id} is no longer tracked."
            )
            await sync_state_to_dynamodb()
            return

        is_live = data["snippet"].get("liveBroadcastContent") == "live"

        if is_live:
            logger.info(f"🎯 Sniper Hit! {video_id} is LIVE.")
            await self.send_youtube_notification(data)
            return

        if now < (scheduled_time + datetime.timedelta(minutes=3)):
            self.schedule_youtube_sniper(
                video_id, scheduled_time, now + datetime.timedelta(seconds=90)
            )
        elif now < (scheduled_time + datetime.timedelta(minutes=21)):
            self.schedule_youtube_sniper(
                video_id, scheduled_time, now + datetime.timedelta(minutes=3)
            )
        else:
            logger.info(f"   🛑 Giving up on {video_id} (Never went live).")
            await sync_state_to_dynamodb()

    async def check_youtube_offline(self, video_id):
        if video_id not in youtube_active_messages:
            self.remove_youtube_monitor(video_id)
            return
        try:
            data = await self.fetch_youtube_data(video_id)
        except Exception as e:
            logger.error(f"API Error checking offline status for {video_id}: {e}")
            return  # Abort check, keep assuming it's live!

        is_live = False
        if data:
            snippet = data.get("snippet")
            if snippet and snippet.get("liveBroadcastContent") == "live":
                is_live = True

        if is_live:
            return

        try:
            msg = youtube_active_messages[video_id]
            if msg and not isinstance(msg, int):
                old_embed = msg.embeds[0]
                new_embed = discord.Embed(
                    title=old_embed.title,
                    url=old_embed.url,
                    description="**Stream Ended**",
                    color=0x2C2F33,
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                )
                if old_embed.image:
                    new_embed.set_image(url=old_embed.image.url)
                await msg.edit(content=None, embed=new_embed)
        except Exception as e:
            logger.error(f"Failed to edit offline message for {video_id}: {e}")

        if video_id in youtube_active_messages:
            del youtube_active_messages[video_id]
        self.remove_youtube_monitor(video_id)
        await sync_state_to_dynamodb()

    def remove_youtube_monitor(self, video_id):
        job_id = f"yt_monitor_{video_id}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)

    async def fetch_youtube_data(self, video_id):
        if not YOUTUBE_API_KEY:
            return None
        if not self.session:
            return None
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "snippet,liveStreamingDetails,statistics",
            "id": video_id,
            "key": YOUTUBE_API_KEY,
        }
        async with self.session.get(url, params=params) as resp:
            if resp.status != 200:
                raise Exception(f"YouTube API Error {resp.status}")
            js = await resp.json()
            return js["items"][0] if js["items"] else None

    async def force_stream_offline(self, s_id: str, s_login: str):
        msg = twitch_active_messages.get(s_id)
        if s_id in twitch_active_messages and not isinstance(msg, int):
            try:
                ts = int(datetime.datetime.now().timestamp())
                embed = discord.Embed(
                    title=f"⚫ {s_login} ended.",
                    description=f"Ended at <t:{ts}:T>.",
                    color=0x2C2F33,
                )
                await twitch_active_messages[s_id].edit(content=None, embed=embed)
            except Exception as e:
                logger.debug(f"Could not edit offline message for {s_login}: {e}")
        twitch_active_messages.pop(s_id, None)
        twitch_active_stream_ids.pop(s_id, None)

        # delayed_check calls this and then syncs. Cancelling the current task
        # would drop that sync on the floor.
        task = twitch_active_tasks.pop(s_id, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    async def send_youtube_notification(self, data):
        vid_id = data["id"]
        channel_id = data["snippet"]["channelId"]
        if channel_id not in YOUTUBE_STREAMERS:
            logger.info(
                f"   ℹ️ Skipping {vid_id}: channel {channel_id} is no longer tracked."
            )
            return

        if vid_id in youtube_active_messages:
            logger.info(f"   ℹ️ Skipping duplicate notification for {vid_id}")
            return
        youtube_active_messages[vid_id] = 0
        try:
            channel_name = YOUTUBE_STREAMERS.get(
                channel_id, data["snippet"]["channelTitle"]
            )
            url = f"https://www.youtube.com/watch?v={vid_id}"

            stats = data.get("statistics", {})
            is_members_only = "viewCount" not in stats

            if is_members_only:
                title_prefix = "( MEMBERS ONLY )"
                desc = f"🔒 **{channel_name}** is live for **MEMBERS ONLY**!"
                color = 0xFFD700  # Gold
            else:
                title_prefix = "🔴"
                desc = f"**{channel_name}** is LIVE on YouTube!"
                color = 0xFF0000  # Red

            embed = discord.Embed(
                title=f"{title_prefix} {data['snippet']['title']}",
                url=url,
                description=desc,
                color=color,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
            )
            thumbs = data["snippet"]["thumbnails"]
            thumb_url = thumbs.get("maxres", thumbs.get("high", thumbs.get("default")))[
                "url"
            ]
            embed.set_image(url=thumb_url)

            chan = await discord_channel()
            if not chan:
                youtube_active_messages.pop(vid_id, None)
                logger.error("Failed to send youtube message: Discord channel unavailable")
                return
            msg = await chan.send(
                content=f"{title_prefix} **{channel_name}** is LIVE! {url}",
                embed=embed,
            )
            youtube_active_messages[vid_id] = msg
            self.schedule_youtube_monitor(vid_id, immediate=False)
            await sync_state_to_dynamodb()
        except Exception as e:
            # If Discord fails to send, delete the eager lock so the backfill can try again!
            youtube_active_messages.pop(vid_id, None)
            logger.error(f"Failed to send youtube message to Discord: {e}")

    async def remove_youtube_job(self, video_id, save=True):
        job_id = f"yt_{video_id}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
            if save:
                await sync_state_to_dynamodb()

    async def maintain_youtube_subs(self):
        await discord_bot.wait_until_ready()
        while not discord_bot.is_closed():
            logger.info("📡 Renewing YouTube WebSub Leases...")
            any_failed = False
            for cid, name in list(YOUTUBE_STREAMERS.items()):
                ok = False
                err = ""
                for attempt in range(4):
                    ok, err = await self._youtube_hub("subscribe", cid)
                    if ok:
                        logger.info(f"   ➜ Subscribed to YouTube: {name} ({cid})")
                        break
                    logger.error(
                        f"   ❌ Failed sub for {cid} ({err}) - Attempt {attempt + 1}/4"
                    )
                    if attempt < 3:
                        await asyncio.sleep(15)
                if not ok:
                    any_failed = True
            # A failed lease should be retried within the hour, not in four days.
            await asyncio.sleep(3600 if any_failed else 345600)

    # Twitch Logic
    async def setup_twitch_subs(self):
        try:
            # This clears every EventSub subscription on the client id, not just ours.
            await self.delete_all_eventsub_subscriptions()
        except Exception as e:
            logger.error(f"Failed to delete existing EventSub subscriptions: {e}")
        logger.info(f"📋 Subscribing {len(TWITCH_STREAMERS)} Twitch channels...")
        for s_id, s_name in list(TWITCH_STREAMERS.items()):
            try:
                await self.subscribe_webhook(
                    payload=StreamOnlineSubscription(
                        broadcaster_user_id=s_id, version="1"
                    ),
                    callback_url=PUBLIC_URL,
                )
                await self.subscribe_webhook(
                    payload=StreamOfflineSubscription(
                        broadcaster_user_id=s_id, version="1"
                    ),
                    callback_url=PUBLIC_URL,
                )
                logger.info(f"   ➜ Subscribed to Twitch: {s_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed Twitch {s_name}: {e}")

    async def event_stream_offline(self, payload: twitchio.StreamOffline) -> None:
        s_id = str(payload.broadcaster.id)
        s_login = payload.broadcaster.name or TWITCH_STREAMERS.get(s_id, s_id)
        await self.force_stream_offline(s_id, s_login)
        await sync_state_to_dynamodb()

    async def event_stream_online(self, payload: twitchio.StreamOnline) -> None:
        s_id = str(payload.broadcaster.id)
        s_login = payload.broadcaster.name or TWITCH_STREAMERS.get(s_id, s_id)
        # Premieres, reruns, playlists, and watch parties are not a live broadcast.
        if payload.type != "live":
            logger.info(f"   ℹ️ Ignoring {payload.type} event for {s_login}")
            return
        await self._handle_twitch_online(s_id, s_login, payload.id)

    async def announce_current_twitch_streams(self):
        ids = list(TWITCH_STREAMERS.keys())
        if not ids:
            return
        logger.info(
            f"🔎 Checking {len(ids)} Twitch channels for streams already live..."
        )
        for start in range(0, len(ids), 100):
            chunk = ids[start : start + 100]
            try:
                async for stream in self.fetch_streams(user_ids=chunk, first=100):
                    if not _helix_stream_is_live(stream):
                        logger.info(
                            f"   ℹ️ Skipping Twitch stream {stream.id} ({stream.type})"
                        )
                        continue
                    s_id = str(stream.user.id)
                    if s_id not in TWITCH_STREAMERS:
                        continue
                    if s_id in twitch_active_messages:
                        if stream.id:
                            twitch_active_stream_ids[s_id] = str(stream.id)
                        continue
                    s_login = stream.user.name or TWITCH_STREAMERS.get(s_id, s_id)
                    await self._handle_twitch_online(
                        s_id, s_login, stream.id, stream_data=stream
                    )
            except Exception as e:
                logger.error(f"Failed to sweep live Twitch streams: {e}")

    async def _handle_twitch_online(self, s_id, s_login, stream_id, stream_data=None):
        if s_id in _twitch_announcing:
            logger.info(f"   ℹ️ Announcement already in flight for {s_login}")
            return
        _twitch_announcing.add(s_id)
        try:
            await self._announce_twitch(s_id, s_login, stream_id, stream_data)
        finally:
            _twitch_announcing.discard(s_id)

    async def _announce_twitch(self, s_id, s_login, stream_id, stream_data=None):
        if stream_id is not None:
            stream_id = str(stream_id)

        if s_id in twitch_active_messages:
            old_stream_id = twitch_active_stream_ids.get(s_id)
            # If the stream IDs match perfectly, it's a true duplicate event
            if old_stream_id and stream_id and old_stream_id == stream_id:
                logger.info(f"   ℹ️ Ignoring duplicate online event for {s_login}")
                return
            if twitch_active_messages.get(s_id) == 0 and not old_stream_id:
                logger.info(f"   ℹ️ Announcement already in flight for {s_login}")
                return
            logger.warning(
                f"   ⚠️ Stale stream detected for {s_login} (Old: {old_stream_id}, New: {stream_id}). Retiring old message."
            )
            await self.force_stream_offline(s_id, s_login)

        # Hold the slot before any further await so a duplicate event cannot
        # announce the same stream twice.
        twitch_active_messages[s_id] = 0

        if stream_data is None:
            for attempt in range(3):
                try:
                    streams = [
                        s
                        async for s in self.fetch_streams(user_ids=[s_id])
                        if _helix_stream_is_live(s)
                    ]
                    if streams:
                        stream_data = streams[0]
                        if not stream_id and getattr(stream_data, "id", None):
                            stream_id = str(stream_data.id)
                        break
                except Exception:
                    pass

                if attempt < 2:
                    logger.info(
                        f"   ⏳ Stream data unavailable, retrying in 5s... ({attempt+1}/3)"
                    )
                    await asyncio.sleep(5)

        logger.info(f"📣 Twitch LIVE: {s_login} (Stream: {stream_id})")
        embed = self.build_twitch_embed(s_login, stream_data)
        chan = await discord_channel()
        if not chan:
            twitch_active_messages.pop(s_id, None)
            twitch_active_stream_ids.pop(s_id, None)
            logger.error(
                f"Failed to announce {s_login}: Discord channel unavailable"
            )
            return
        try:
            msg = await chan.send(
                content=f"🔴 **{s_login}** is LIVE! https://twitch.tv/{s_login}",
                embed=embed,
            )
        except Exception as e:
            twitch_active_messages.pop(s_id, None)
            twitch_active_stream_ids.pop(s_id, None)
            logger.error(f"Failed to send twitch message to Discord: {e}")
            return

        twitch_active_messages[s_id] = msg
        if stream_id:
            twitch_active_stream_ids[s_id] = stream_id

        existing = twitch_active_tasks.get(s_id)
        if existing is not None and existing is not asyncio.current_task():
            existing.cancel()
        twitch_active_tasks[s_id] = asyncio.create_task(
            self.delayed_check(s_id, s_login)
        )
        await sync_state_to_dynamodb()

    async def delayed_check(self, s_id: str, s_login: str, delay: int = 3600) -> None:
        try:
            if delay:
                await asyncio.sleep(delay)
            if s_id not in twitch_active_messages:
                return

            streams = [
                s
                async for s in self.fetch_streams(user_ids=[s_id])
                if _helix_stream_is_live(s)
            ]
            if not streams:
                await self.force_stream_offline(s_id, s_login)
                await sync_state_to_dynamodb()
            else:
                # Update the stream ID from the API fetch just in case it wasn't caught
                current_stream_id = getattr(streams[0], "id", None)
                if current_stream_id:
                    twitch_active_stream_ids[s_id] = str(current_stream_id)

                msg = twitch_active_messages.get(s_id)
                if msg and not isinstance(msg, int):
                    await msg.edit(embed=self.build_twitch_embed(s_login, streams[0]))

                twitch_active_tasks[s_id] = asyncio.create_task(
                    self.delayed_check(s_id, s_login)
                )

        except asyncio.CancelledError:
            # Task was canceled cleanly; do nothing
            pass
        except Exception as e:
            logger.error(f"Error in delayed check for {s_login}: {e}")
            # Try again in an hour if there was a temporary network error.
            # Skip this when the stream was just marked offline.
            if s_id in twitch_active_messages:
                twitch_active_tasks[s_id] = asyncio.create_task(
                    self.delayed_check(s_id, s_login)
                )

    def build_twitch_embed(self, login, data):
        if data:
            title = data.title if data.title else "Live Stream"
            game = data.game_name if data.game_name else "Unknown"

            if data.started_at:
                start_unix = int(data.started_at.timestamp())
                desc = f"**{login}** playing **{game}**\n*( started at <t:{start_unix}:t> )*"
            else:
                desc = f"**{login}** playing **{game}**"

            ts = datetime.datetime.now(datetime.timezone.utc)

            # Append a cache-busting parameter (?t=...) so Discord fetches the new frame
            if data.thumbnail:
                thumb_url = f"{data.thumbnail.url_for(1280, 720)}?t={int(time.time())}"
            else:
                thumb_url = None
        else:
            title = "Live Stream"
            desc = f"**{login}** is LIVE!"
            ts = datetime.datetime.now(datetime.timezone.utc)
            thumb_url = None

        embed = discord.Embed(
            title=title,
            url=f"https://twitch.tv/{login}",
            description=desc,
            color=0x9146FF,
            timestamp=ts,
        )

        embed.set_footer(text="Last Updated")

        if thumb_url:
            embed.set_image(url=thumb_url)

        return embed

    async def reconcile_streamers(self, initial=False):
        global TWITCH_STREAMERS, YOUTUBE_STREAMERS
        items = await asyncio.to_thread(_db_get_streamers)
        if items is None:
            return  # DB error, abort check to preserve current state

        new_tw, new_yt = {}, {}
        for item in items:
            platform = item.get("platform", "").lower()
            cid = str(item.get("channel_id", ""))
            name = item.get("display_name", cid)
            if not cid:
                continue

            if platform == "twitch":
                new_tw[cid] = name
            elif platform == "youtube":
                new_yt[cid] = name

        if initial:
            TWITCH_STREAMERS.clear()
            TWITCH_STREAMERS.update(new_tw)
            YOUTUBE_STREAMERS.clear()
            YOUTUBE_STREAMERS.update(new_yt)
            logger.info(
                f"📋 Loaded {len(TWITCH_STREAMERS)} Twitch and {len(YOUTUBE_STREAMERS)} YouTube streamers from DB."
            )
            return

        added_tw = set(new_tw.keys()) - set(TWITCH_STREAMERS.keys())
        removed_tw = set(TWITCH_STREAMERS.keys()) - set(new_tw.keys())
        added_yt = set(new_yt.keys()) - set(YOUTUBE_STREAMERS.keys())
        removed_yt = set(YOUTUBE_STREAMERS.keys()) - set(new_yt.keys())

        if not any([added_tw, removed_tw, added_yt, removed_yt]):
            # A display-name edit does not add or remove an id, but new alerts
            # should still use the name currently stored in DynamoDB.
            TWITCH_STREAMERS.clear()
            TWITCH_STREAMERS.update(new_tw)
            YOUTUBE_STREAMERS.clear()
            YOUTUBE_STREAMERS.update(new_yt)
            return

        TWITCH_STREAMERS.clear()
        TWITCH_STREAMERS.update(new_tw)
        YOUTUBE_STREAMERS.clear()
        YOUTUBE_STREAMERS.update(new_yt)

        # --- TWITCH RECONCILIATION ---
        if added_tw or removed_tw:
            logger.info(
                f"🔄 Twitch streamers changed. Added: {added_tw}, Removed: {removed_tw}"
            )
            if removed_tw:
                # Wiping and resubscribing is the cleanest way to clear old Twitch EventSubs
                # since we don't cache the active Subscription IDs in memory.
                logger.info(
                    "   Wiping Twitch EventSubs and resubscribing due to removals..."
                )
                await self.setup_twitch_subs()
            else:
                for s_id in added_tw:
                    try:
                        await self.subscribe_webhook(
                            payload=StreamOnlineSubscription(
                                broadcaster_user_id=s_id, version="1"
                            ),
                            callback_url=PUBLIC_URL,
                        )
                        await self.subscribe_webhook(
                            payload=StreamOfflineSubscription(
                                broadcaster_user_id=s_id, version="1"
                            ),
                            callback_url=PUBLIC_URL,
                        )
                        logger.info(f"   ➜ Subscribed to Twitch: {new_tw[s_id]}")
                    except Exception as e:
                        logger.error(f"   ❌ Failed Twitch Sub for {new_tw[s_id]}: {e}")

        # --- YOUTUBE RECONCILIATION ---
        if added_yt or removed_yt:
            logger.info(
                f"🔄 YouTube streamers changed. Added: {added_yt}, Removed: {removed_yt}"
            )
            for cid in added_yt:
                ok, err = await self._youtube_hub("subscribe", cid)
                if ok:
                    logger.info(f"   ➜ Sent Sub request for YouTube: {new_yt[cid]}")
                else:
                    logger.error(f"   ❌ Failed YouTube sub for {new_yt[cid]}: {err}")

            for cid in removed_yt:
                ok, err = await self._youtube_hub("unsubscribe", cid)
                if ok:
                    logger.info(f"   ➜ Sent Unsub request for YouTube: {cid}")
                else:
                    logger.error(f"   ❌ Failed YouTube unsub for {cid}: {err}")


@tasks.loop(minutes=90)
async def autosave_state_task():
    await sync_state_to_dynamodb()


@tasks.loop(minutes=10)
async def poll_streamers_task():
    if twitch_bot:
        await twitch_bot.reconcile_streamers(initial=False)


def configure_file_logging():
    # systemd's LogsDirectory=discord-twitch creates this folder and gives us write access
    try:
        file_handler = logging.FileHandler(
            "/var/log/discord-twitch/discord-twitch.log", encoding="utf-8"
        )
    except OSError as e:
        logger.warning(f"File logging unavailable ({e}). Continuing with stdout only.")
        return
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def _exit(code):
    for handler in logger.handlers:
        handler.flush()
    sys.stdout.flush()
    os._exit(code)


def main() -> None:
    global twitch_bot
    code = 0
    try:
        configure_file_logging()
        load_config()
        twitch_bot = HybridBot()
        discord_bot.run(DISCORD_TOKEN)
    except KeyboardInterrupt:
        code = 0
    except Exception as e:
        logger.critical(f"🔥 FATAL ERROR: {e}")
        traceback.print_exc()
        code = 1
    else:
        code = 0
    if code == 0:
        logger.info("Exiting sucessfully ( End of main )")
    _exit(code)


if __name__ == "__main__":
    main()

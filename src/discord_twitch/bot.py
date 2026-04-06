#!/usr/bin/env python
import asyncio
import logging
import discord
import configparser
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

# Systemd Journal Handler (stdout)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# File for cloudwatch etc
# systemd's LogsDirectory=discord-twitch creates this folder and gives us write access
file_handler = logging.FileHandler(
    "/var/log/discord-twitch/discord-twitch.log", encoding="utf-8"
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Global Variables
config = configparser.ConfigParser()
config.optionxform = str
twitch_bot = None
twitch_active_messages = {}
twitch_active_tasks = {}
twitch_active_stream_ids = {}
youtube_active_messages = {}
scheduler = AsyncIOScheduler()
YOUTUBE_WEBHOOK_SECRET = secrets.token_hex(32)
TWITCH_EVENTSUB_SECRET = secrets.token_hex(32)

# Config Placeholders
DYNAMODB_REGION = "us-east-1"
DYNAMODB_TABLE_NAME = ""
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
        intents.message_content = True
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
        # Force immediate success exit, bypassing asyncio's fragile background thread teardown
        for handler in logging.root.handlers:
            handler.flush()
        sys.stdout.flush()
        os._exit(0)


# Create the global instance using the new class
discord_bot = DiscordTwitchBot()


def load_config():
    global DISCORD_TOKEN, DISCORD_CHANNEL_ID, TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET
    global YOUTUBE_API_KEY, YOUTUBE_BACKFILL_CHECK
    global DYNAMODB_TABLE_NAME, DYNAMODB_REGION, SERVER_DOMAIN, PUBLIC_URL, LOCAL_PORT
    global TWITCH_STREAMERS, YOUTUBE_STREAMERS, INTERNAL_API_SECRET

    cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")
    secret_path = None
    secret_candidates = []
    if cred_dir:
        secret_candidates.append(os.path.join(cred_dir, "secret.cfg"))
    secret_candidates.extend(
        [
            "/etc/discord-twitch/secret.cfg",
            "/usr/local/discord-twitch/secret.cfg",
            "secret.cfg",
        ]
    )

    for candidate in secret_candidates:
        if os.path.exists(candidate):
            secret_path = candidate
            logger.info(f"🔒 Loading secrets from: {secret_path}")
            break
    if not secret_path:
        secret_path = "secret.cfg"

    streamers_path = None
    streamer_candidates = [
        "/etc/discord-twitch/streamers.cfg",
        "/usr/local/discord-twitch/streamers.cfg",
        "streamers.cfg",
    ]
    for candidate in streamer_candidates:
        if os.path.exists(candidate):
            streamers_path = candidate
            break

    files_to_read = [
        f for f in [secret_path, streamers_path] if f and os.path.exists(f)
    ]
    if not files_to_read:
        raise FileNotFoundError("❌ No config files found!")

    if not config.read(files_to_read):
        raise FileNotFoundError("❌ Failed to parse config files.")

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
    DYNAMODB_TABLE_NAME = config["server"].get("dynamodb_table", "discord-twitch-state")
    SERVER_DOMAIN = config["server"]["domain"]
    PUBLIC_URL = config["server"]["public_url"]
    LOCAL_PORT = int(config["server"]["port"])
    INTERNAL_API_SECRET = config["server"]["internal_api_secret"]
    logger.info(f"Current twitch secret for troubleshooting: {TWITCH_EVENTSUB_SECRET}")

    if "streamers" in config:
        logger.warning("⚠️ Legacy [streamers] section found. Moving to Twitch.")
        for s_id, s_name in config["streamers"].items():
            TWITCH_STREAMERS[str(s_id)] = s_name
    if "twitch" in config:
        ignore_keys = ["clientid", "clientsecret", "eventsub_secret"]
        for s_id, s_name in config["twitch"].items():
            if s_id.lower() in ignore_keys:
                continue
            TWITCH_STREAMERS[str(s_id)] = s_name
    if "youtube" in config:
        for c_id, c_name in config["youtube"].items():
            if c_id not in ["api_key", "backfill_check"]:
                YOUTUBE_STREAMERS[str(c_id)] = c_name


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
            bot_instance.delayed_check(s_id, login)
        )
    except discord.NotFound:
        pass  # Message was deleted manually while bot was offline
    except Exception as e:
        logger.error(f"Failed to fetch twitch message {msg_id}: {e}")


async def restore_youtube_state(bot_instance, channel, vid, msg_id):
    if vid not in youtube_active_messages:
        youtube_active_messages[vid] = msg_id

    try:
        msg = await channel.fetch_message(msg_id)
        youtube_active_messages[vid] = msg

        scheduler.add_job(
            bot_instance.check_youtube_offline,
            "interval",
            minutes=30,
            args=[vid],
            id=f"yt_monitor_{vid}",
            replace_existing=True,
        )
    except discord.NotFound:
        pass  # Message deleted, but keep lock to prevent duplicates
    except Exception as e:
        logger.error(f"Failed to fetch youtube message {msg_id}: {e}")
        # Keep lock to prevent duplicates on transient discord errors


# --- Helper functions for background threads ---
def _db_scan():
    dynamodb = boto3.resource("dynamodb", region_name=DYNAMODB_REGION)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    return table.scan().get("Items", [])


def _db_push(jobs_data, tw_data, yt_data):
    dynamodb = boto3.resource("dynamodb", region_name=DYNAMODB_REGION)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    response = table.scan()
    db_items = response.get("Items", [])
    active_db_keys = set()

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
            if item["video_id"] not in active_db_keys:
                batch.delete_item(Key={"video_id": item["video_id"]})


# --- Main Async Sync Functions ---
async def sync_state_from_dynamodb(bot_instance):
    try:
        logger.info("☁️  Downloading state from DynamoDB...")
        items = await asyncio.to_thread(_db_scan)
        now = datetime.datetime.now(datetime.timezone.utc)

        # 1. Bypass the cache to guarantee we get the channel
        channel = discord_bot.get_channel(DISCORD_CHANNEL_ID)
        if not channel:
            try:
                channel = await discord_bot.fetch_channel(DISCORD_CHANNEL_ID)
            except Exception as e:
                logger.error(f"❌ Critical Error: Could not fetch Discord channel: {e}")
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
                scheduler.add_job(
                    bot_instance.check_youtube_status,
                    "date",
                    run_date=run_date,
                    args=[vid, s_time],
                    id=f"yt_{vid}",
                    replace_existing=True,
                )
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
        asyncio.create_task(sync_state_to_dynamodb())
    except Exception as e:
        logger.error(f"❌ Failed to load state from DynamoDB: {e}")


async def sync_state_to_dynamodb():
    try:
        # 1. Safely snapshot all memory states on the main thread
        jobs_data = {}
        for job in scheduler.get_jobs():
            if job.id.startswith("yt_") and not job.id.startswith("yt_monitor_"):
                try:
                    jobs_data[f"yt_{job.args[0]}"] = job.args[1].isoformat()
                except IndexError:
                    pass

        tw_data = {}
        for s_id, msg in twitch_active_messages.items():
            if msg is None or msg == 0:
                continue

            # 👇 ADD THIS BLOCK 👇
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
        for vid, msg in youtube_active_messages.items():
            if msg is None or msg == 0:
                continue

            # 👇 ADD THIS BLOCK 👇
            if isinstance(msg, int):
                m_id = str(msg)
            else:
                m_id = str(msg.id)

            yt_data[f"ytlive_{vid}"] = {
                "video_id": f"ytlive_{vid}",
                "message_id": m_id,
            }
        # 2. Pass the safely copied data to the background thread to upload
        await asyncio.to_thread(_db_push, jobs_data, tw_data, yt_data)
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
        await sync_state_from_dynamodb(self)
        scheduler.start()
        await self.setup_twitch_subs()
        await self.run_youtube_backfill()
        asyncio.create_task(self.maintain_youtube_subs())
        if not autosave_state_task.is_running():
            autosave_state_task.start()

    async def close(self):
        if self.session:
            await self.session.close()
        await super().close()

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

        # Trigger your existing clean shutdown logic (Syncs S3 and closes Discord)
        await discord_bot.close()

        # Exit with code 0 so systemd's 'Restart=on-failure' leaves this dead process in the grave
        os._exit(0)

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

        for channel_id in YOUTUBE_STREAMERS:
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
            return

        snippet = data["snippet"]
        live_details = data.get("liveStreamingDetails", {})
        is_live = snippet.get("liveBroadcastContent") == "live"
        scheduled_start = live_details.get("scheduledStartTime")
        actual_end = live_details.get("actualEndTime")

        # 1. If YouTube confirms it has an actual end time, it's definitively over. Drop it!
        if actual_end:
            return

        if is_live:
            await self.send_youtube_notification(data)
            self.remove_youtube_job(video_id, save)
        elif scheduled_start:
            dt = datetime.datetime.fromisoformat(scheduled_start.replace("Z", "+00:00"))
            now = datetime.datetime.now(datetime.timezone.utc)

            # 2. Ignore placeholder "waiting rooms" scheduled more than 45 days out
            if dt > now + datetime.timedelta(days=45):
                logger.info(
                    f"   ⏭️ Skipping {video_id}: Scheduled > 45 days in the future."
                )
                return

            # 3. Ignore stale schedules that missed their time by > 2 days and never went live
            if dt < now - datetime.timedelta(days=2):
                logger.info(
                    f"   ⏭️ Skipping {video_id}: Scheduled > 2 days in the past."
                )
                return

            logger.info(f"   🗓️ Scheduled for {dt}. Queueing Sniper.")
            run_time = dt - datetime.timedelta(minutes=3)
            if run_time < now:
                run_time = now + datetime.timedelta(seconds=10)
            scheduler.add_job(
                self.check_youtube_status,
                "date",
                run_date=run_time,
                args=[video_id, dt],
                id=f"yt_{video_id}",
                replace_existing=True,
            )
            if save:
                await sync_state_to_dynamodb()

    async def check_youtube_status(self, video_id, scheduled_time):
        try:
            data = await self.fetch_youtube_data(video_id)
        except Exception as e:
            logger.error(f"API Error sniper {video_id}: {e}")
            # Reschedule sniper to try again in 90 seconds
            next_run = datetime.datetime.now(
                datetime.timezone.utc
            ) + datetime.timedelta(seconds=90)
            scheduler.add_job(
                self.check_youtube_status,
                "date",
                run_date=next_run,
                args=[video_id, scheduled_time],
                id=f"yt_{video_id}",
                replace_existing=True,
            )
            return
        if not data:
            return
        is_live = data["snippet"].get("liveBroadcastContent") == "live"
        now = datetime.datetime.now(datetime.timezone.utc)

        if is_live:
            logger.info(f"🎯 Sniper Hit! {video_id} is LIVE.")
            await self.send_youtube_notification(data)
            return

        if now < (scheduled_time + datetime.timedelta(minutes=3)):
            next_run = now + datetime.timedelta(seconds=90)
            scheduler.add_job(
                self.check_youtube_status,
                "date",
                run_date=next_run,
                args=[video_id, scheduled_time],
                id=f"yt_{video_id}",
            )
        elif now < (scheduled_time + datetime.timedelta(minutes=21)):
            next_run = now + datetime.timedelta(minutes=3)
            scheduler.add_job(
                self.check_youtube_status,
                "date",
                run_date=next_run,
                args=[video_id, scheduled_time],
                id=f"yt_{video_id}",
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

        if s_id in twitch_active_tasks:
            twitch_active_tasks[s_id].cancel()
            del twitch_active_tasks[s_id]

    async def send_youtube_notification(self, data):
        vid_id = data["id"]

        if vid_id in youtube_active_messages:
            logger.info(f"   ℹ️ Skipping duplicate notification for {vid_id}")
            return
        youtube_active_messages[vid_id] = 0

        channel_id = data["snippet"]["channelId"]
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

        chan = discord_bot.get_channel(DISCORD_CHANNEL_ID)
        if chan:
            try:
                msg = await chan.send(
                    content=f"{title_prefix} **{channel_name}** is LIVE! {url}",
                    embed=embed,
                )
                youtube_active_messages[vid_id] = msg
                scheduler.add_job(
                    self.check_youtube_offline,
                    "interval",
                    minutes=30,
                    args=[vid_id],
                    id=f"yt_monitor_{vid_id}",
                    replace_existing=True,
                )
                asyncio.create_task(sync_state_to_dynamodb())
            except Exception as e:
                # If Discord fails to send, delete the eager lock so the backfill can try again!
                del youtube_active_messages[vid_id]
                logger.error(f"Failed to send youtube message to Discord: {e}")

    def remove_youtube_job(self, video_id, save=True):
        job_id = f"yt_{video_id}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
            if save:
                asyncio.create_task(sync_state_to_dynamodb())

    async def maintain_youtube_subs(self):
        await discord_bot.wait_until_ready()
        hub_url = "https://pubsubhubbub.appspot.com/subscribe"
        while not discord_bot.is_closed():
            logger.info("📡 Renewing YouTube WebSub Leases...")
            for cid in YOUTUBE_STREAMERS:
                data = {
                    "hub.mode": "subscribe",
                    "hub.topic": f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={cid}",
                    "hub.callback": f"{PUBLIC_URL}/youtube",
                    "hub.lease_seconds": 432000,
                    "hub.secret": YOUTUBE_WEBHOOK_SECRET,
                }
                for attempt in range(4):
                    try:
                        if self.session:
                            async with self.session.post(hub_url, data=data) as resp:
                                if resp.status >= 400:
                                    logger.error(
                                        f"   ❌ Failed sub for {cid} (HTTP {resp.status}) - Attempt {attempt + 1}/4"
                                    )
                                    if attempt < 3:
                                        # Sleep for 15 seconds before retrying
                                        await asyncio.sleep(15)
                                else:
                                    logger.info(
                                        f"   ➜ Subscribed to YouTube: {YOUTUBE_STREAMERS[cid]} ({cid})"
                                    )
                                    break  # Success, break out of the retry loop
                    except Exception as e:
                        logger.error(
                            f"   ❌ Failed sub for {cid} ({e}) - Attempt {attempt + 1}/4"
                        )
                        if attempt < 3:
                            await asyncio.sleep(15)
            await asyncio.sleep(345600)

    # Twitch Logic
    async def setup_twitch_subs(self):
        try:
            await self.delete_all_eventsub_subscriptions()
        except:
            pass
        logger.info(f"📋 Subscribing {len(TWITCH_STREAMERS)} Twitch channels...")
        for s_id, s_name in TWITCH_STREAMERS.items():
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
        s_login = payload.broadcaster.name
        await self.force_stream_offline(s_id, s_login)
        await sync_state_to_dynamodb()

    async def event_stream_online(self, payload: twitchio.StreamOnline) -> None:
        s_id = str(payload.broadcaster.id)
        s_login = payload.broadcaster.name
        stream_id = getattr(payload, "id", None)

        if s_id in twitch_active_messages:
            old_stream_id = twitch_active_stream_ids.get(s_id)
            # If the stream IDs match perfectly, it's a true duplicate event
            if old_stream_id and stream_id and old_stream_id == stream_id:
                logger.info(f"   ℹ️ Ignoring duplicate online event for {s_login}")
                return
            else:
                logger.warning(
                    f"   ⚠️ Stale stream detected for {s_login} (Old: {old_stream_id}, New: {stream_id}). Retiring old message."
                )
                # Forcefully end the old message before creating the new one!
                await self.force_stream_offline(s_id, s_login)

        logger.info(f"📣 Twitch LIVE: {s_login} (Stream: {stream_id})")
        stream_data = None

        for attempt in range(3):
            try:
                streams = [s async for s in self.fetch_streams(user_ids=[s_id])]
                if streams:
                    stream_data = streams[0]
                    # Fallback just in case payload didn't have the ID
                    if not stream_id and hasattr(stream_data, "id"):
                        stream_id = stream_data.id
                    break
            except Exception:
                pass

            if attempt < 2:
                logger.info(
                    f"   ⏳ Stream data unavailable, retrying in 5s... ({attempt+1}/3)"
                )
                await asyncio.sleep(5)

        embed = self.build_twitch_embed(s_login, stream_data)
        chan = discord_bot.get_channel(DISCORD_CHANNEL_ID)
        if chan:
            msg = await chan.send(
                content=f"🔴 **{s_login}** is LIVE! https://twitch.tv/{s_login}",
                embed=embed,
            )
            twitch_active_messages[s_id] = msg
            if stream_id:
                twitch_active_stream_ids[s_id] = stream_id

            if s_id in twitch_active_tasks:
                twitch_active_tasks[s_id].cancel()
            twitch_active_tasks[s_id] = asyncio.create_task(
                self.delayed_check(s_id, s_login)
            )
            await sync_state_to_dynamodb()

    async def delayed_check(self, s_id: str, s_login: str) -> None:
        try:
            await asyncio.sleep(3600)
            if s_id not in twitch_active_messages:
                return

            streams = [s async for s in self.fetch_streams(user_ids=[s_id])]
            if not streams:
                await self.force_stream_offline(s_id, s_login)
                await sync_state_to_dynamodb()
            else:
                # Update the stream ID from the API fetch just in case it wasn't caught
                current_stream_id = getattr(streams[0], "id", None)
                if current_stream_id:
                    twitch_active_stream_ids[s_id] = current_stream_id

                msg = twitch_active_messages.get(s_id)
                if msg and not isinstance(msg, int):  # <--- Add the isinstance check
                    await msg.edit(embed=self.build_twitch_embed(s_login, streams[0]))

                twitch_active_tasks[s_id] = asyncio.create_task(
                    self.delayed_check(s_id, s_login)
                )

        except asyncio.CancelledError:
            # Task was canceled cleanly; do nothing
            pass
        except Exception as e:
            logger.error(f"Error in delayed check for {s_login}: {e}")
            # Try again in an hour if there was a temporary network error
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


@tasks.loop(minutes=90)
async def autosave_state_task():
    await sync_state_to_dynamodb()


def main() -> None:
    global twitch_bot
    try:
        load_config()
        twitch_bot = HybridBot()
        discord_bot.run(DISCORD_TOKEN)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.critical(f"🔥 FATAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
    logger.info("Exiting sucessfully ( End of main )")
    os._exit(0)


if __name__ == "__main__":
    main()

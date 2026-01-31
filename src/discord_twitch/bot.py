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
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from typing import Any
from aiohttp import web, ClientSession, TCPConnector
from discord.ext import commands, tasks
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import twitchio
from twitchio.web import AiohttpAdapter
from twitchio.eventsub import StreamOnlineSubscription, StreamOfflineSubscription

# Setup & Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("Bot")
config = configparser.ConfigParser()
config.optionxform = str

# Configuration Loading
cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")
secret_path = None
secret_candidates = []
if cred_dir:
    secret_candidates.append(os.path.join(cred_dir, "secret.cfg"))
secret_candidates.extend(["/etc/discord-twitch/secret.cfg", "/usr/local/discord-twitch/secret.cfg", "secret.cfg"])

for candidate in secret_candidates:
    if os.path.exists(candidate):
        secret_path = candidate
        logger.info(f"🔒 Loading secrets from: {secret_path}")
        break
if not secret_path:
    secret_path = "/usr/local/discord-twitch/secret.cfg"

streamers_path = None
streamer_candidates = ["/etc/discord-twitch/streamers.cfg", "/usr/local/discord-twitch/streamers.cfg", "streamers.cfg"]
for candidate in streamer_candidates:
    if os.path.exists(candidate):
        streamers_path = candidate
        break
if not streamers_path:
    streamers_path = "/usr/local/discord-twitch/streamers.cfg"

if not config.read([secret_path, streamers_path]):
    logger.error("❌ No config files found!")
    sys.exit(1)

# Constants & Config Parsing
DISCORD_TOKEN = config["discord"]["token"]
DISCORD_CHANNEL_ID = int(config["discord"]["channelid"])
TWITCH_CLIENT_ID = config["twitch"]["clientid"]
TWITCH_CLIENT_SECRET = config["twitch"]["clientsecret"]
TWITCH_EVENTSUB_SECRET = config["twitch"]["eventsub_secret"]
YOUTUBE_API_KEY = config["youtube"].get("api_key", "") if "youtube" in config else ""
YOUTUBE_BACKFILL_CHECK = int(config["youtube"].get("backfill_check", 2)) if "youtube" in config else 2
S3_BUCKET_URL = config["server"].get("s3_state_url", "s3://phoenix591/discord-twitch/state.json")
SERVER_DOMAIN = config["server"]["domain"]
PUBLIC_URL = config["server"]["public_url"]
LOCAL_PORT = int(config["server"]["port"])

TWITCH_STREAMERS = {}
YOUTUBE_STREAMERS = {}
if "streamers" in config:
    logger.warning("⚠️ Legacy [streamers] section found. Moving to Twitch.")
    for s_id, s_name in config["streamers"].items():
        TWITCH_STREAMERS[str(s_id)] = s_name
if "twitch" in config:
    ignore_keys = ["clientid", "clientsecret", "eventsub_secret"]
    for s_id, s_name in config["twitch"].items():
        if s_id.lower() in ignore_keys: continue
        TWITCH_STREAMERS[str(s_id)] = s_name
if "youtube" in config:
    for c_id, c_name in config["youtube"].items():
        if c_id not in ["api_key", "backfill_check"]:
            YOUTUBE_STREAMERS[str(c_id)] = c_name

# State & Scheduler
twitch_active_messages = {}
STATE_FILE = "state.json"
scheduler = AsyncIOScheduler()

def sync_state_from_s3():
    try:
        logger.info("☁️  Downloading state from S3...")
        env = {**os.environ, "HOME": "/tmp"}
        subprocess.run(["aws", "s3", "cp", S3_BUCKET_URL, STATE_FILE], check=True, timeout=10, env=env)
    except Exception as e:
        logger.warning(f"⚠️  Could not download state (First run?): {e}")

def sync_state_to_s3():
    try:
        save_local_state()
        env = {**os.environ, "HOME": "/tmp"}
        subprocess.run(["aws", "s3", "cp", STATE_FILE, S3_BUCKET_URL], check=False, env=env)
        logger.info("☁️  State synced to S3.")
    except Exception as e:
        logger.error(f"❌ S3 Sync failed: {e}")

def save_local_state():
    jobs = []
    for job in scheduler.get_jobs():
        if job.id.startswith("yt_"):
            jobs.append({"video_id": job.args[0], "scheduled_time": job.args[1].isoformat()})
    with open(STATE_FILE, "w") as f:
        json.dump({"pending_checks": jobs}, f)

def load_local_state(bot_instance):
    if not os.path.exists(STATE_FILE): return
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
        now = datetime.datetime.now(datetime.timezone.utc)
        for item in data.get("pending_checks", []):
            vid = item["video_id"]
            s_time = datetime.datetime.fromisoformat(item["scheduled_time"])
            run_date = s_time - datetime.timedelta(minutes=3)
            if run_date < now:
                run_date = now + datetime.timedelta(seconds=5)
            scheduler.add_job(bot_instance.check_youtube_status, 'date', run_date=run_date, args=[vid, s_time], id=f"yt_{vid}", replace_existing=True)
        logger.info("♻️  Restored pending YouTube checks.")
    except Exception as e:
        logger.error(f"❌ Failed to load state: {e}")

# Discord Bot Setup
intents = discord.Intents.default()
intents.message_content = True
discord_bot = commands.Bot(command_prefix="!", intents=intents)

# Main Hybrid Bot Class
class HybridBot(twitchio.Client):
    def __init__(self) -> None:
        # FORCE IPv4 on the Server (host="0.0.0.0")
        self.web_adapter = AiohttpAdapter(host="0.0.0.0", port=LOCAL_PORT, domain=SERVER_DOMAIN, eventsub_secret=TWITCH_EVENTSUB_SECRET)
        self.session = None
        super().__init__(client_id=TWITCH_CLIENT_ID, client_secret=TWITCH_CLIENT_SECRET, adapter=self.web_adapter)
        
        if hasattr(self.web_adapter, 'router'):
            path = urlparse(PUBLIC_URL).path.rstrip('/')
            route = path + '/youtube'
            self.web_adapter.router.add_post(route, self.youtube_webhook_handler)
            self.web_adapter.router.add_get(route, self.youtube_webhook_handler)
            logger.info(f"✅ Registered YouTube Route: {route}")

    async def event_ready(self) -> None:
        logger.info(f"✅ Hybrid Bot Listening on {LOCAL_PORT} (IPv4)")
        # FORCE IPv4 on the Client (outgoing requests)
        conn = TCPConnector(family=socket.AF_INET)
        self.session = ClientSession(connector=conn)
        
        await discord_bot.wait_until_ready()
        await self.populate_message_cache()
        sync_state_from_s3()
        load_local_state(self)
        scheduler.start()
        await self.setup_twitch_subs()
        await self.run_youtube_backfill()
        asyncio.create_task(self.maintain_youtube_subs())

    async def close(self):
        if self.session:
            await self.session.close()
        await super().close()

    # YouTube Logic
    async def youtube_webhook_handler(self, request):
        if request.method == 'GET':
            challenge = request.query.get('hub.challenge')
            return web.Response(text=challenge) if challenge else web.Response(status=404)
        try:
            xml_text = await request.text()
            root = ET.fromstring(xml_text)
            ns = {'atom': 'http://www.w3.org/2005/Atom', 'yt': 'http://purl.org/yt/2012'}
            entry = root.find('atom:entry', ns)
            if entry:
                video_id = entry.find('yt:videoId', ns).text
                channel_id = entry.find('yt:channelId', ns).text
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
        rss_headers = {"User-Agent": "Mozilla/5.0 (compatible; DiscordTwitchBot/2.0; +http://discordapp.com)"}

        for channel_id in YOUTUBE_STREAMERS:
            playlist_id = None
            
            # Step 1: Try Official API to get Playlist ID
            try:
                url = "https://www.googleapis.com/youtube/v3/channels"
                params = {"part": "contentDetails", "id": channel_id, "key": YOUTUBE_API_KEY}
                async with self.session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('items'):
                            playlist_id = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                    elif resp.status == 403:
                        err = await resp.json()
                        reason = err.get('error', {}).get('message', 'Unknown 403')
                        logger.warning(f"   ⚠️ API Lookup 403 for {channel_id}: {reason}")
            except Exception as e:
                logger.debug(f"   ⚠️ API Lookup exc for {channel_id}: {e}")

            # Step 2: Fallback - Force Derive Playlist ID
            if not playlist_id and channel_id.startswith("UC"):
                playlist_id = "UU" + channel_id[2:]

            # Step 3: Fetch Playlist Items
            success = False
            if playlist_id:
                try:
                    url = "https://www.googleapis.com/youtube/v3/playlistItems"
                    params = {
                        "part": "contentDetails",
                        "playlistId": playlist_id,
                        "maxResults": YOUTUBE_BACKFILL_CHECK,
                        "key": YOUTUBE_API_KEY
                    }
                    async with self.session.get(url, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for item in data.get('items', []):
                                vid = item['contentDetails']['videoId']
                                tasks.append(self.initial_youtube_check(vid, save=False))
                            success = True
                        elif resp.status == 403:
                            err = await resp.json()
                            reason = err.get('error', {}).get('message', 'Unknown 403')
                            logger.warning(f"   ⚠️ Playlist Fetch 403 for {channel_id}: {reason}")
                except Exception as e:
                    logger.debug(f"   ⚠️ Playlist Fetch exc: {e}")

            # Step 4: Final Fallback - RSS
            if not success:
                try:
                    url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
                    async with self.session.get(url, headers=rss_headers) as resp:
                        if resp.status == 200:
                            xml_text = await resp.text()
                            root = ET.fromstring(xml_text)
                            ns = {'atom': 'http://www.w3.org/2005/Atom', 'yt': 'http://purl.org/yt/2012'}
                            for entry in root.findall('atom:entry', ns)[:YOUTUBE_BACKFILL_CHECK]:
                                vid_elem = entry.find('yt:videoId', ns)
                                if vid_elem is not None and vid_elem.text:
                                    tasks.append(self.initial_youtube_check(vid_elem.text, save=False))
                        else:
                            logger.warning(f"   ❌ RSS Fallback failed for {channel_id}: {resp.status}")
                except Exception as e:
                    logger.warning(f"   ❌ RSS Fallback exc for {channel_id}: {e}")
        
        if tasks:
            await asyncio.gather(*tasks)
            sync_state_to_s3()

    async def initial_youtube_check(self, video_id, save=True):
        data = await self.fetch_youtube_data(video_id)
        if not data: return
        snippet = data['snippet']
        live_details = data.get('liveStreamingDetails', {})
        is_live = snippet.get('liveBroadcastContent') == 'live'
        scheduled_start = live_details.get('scheduledStartTime')

        if is_live:
            await self.send_youtube_notification(data)
            self.remove_youtube_job(video_id, save)
        elif scheduled_start:
            dt = datetime.datetime.fromisoformat(scheduled_start.replace('Z', '+00:00'))
            logger.info(f"   🗓️ Scheduled for {dt}. Queueing Sniper.")
            run_time = dt - datetime.timedelta(minutes=3)
            now = datetime.datetime.now(datetime.timezone.utc)
            if run_time < now:
                run_time = now + datetime.timedelta(seconds=10)
            scheduler.add_job(self.check_youtube_status, 'date', run_date=run_time, args=[video_id, dt], id=f"yt_{video_id}", replace_existing=True)
            if save: sync_state_to_s3()

    async def check_youtube_status(self, video_id, scheduled_time):
        data = await self.fetch_youtube_data(video_id)
        if not data: return
        is_live = data['snippet'].get('liveBroadcastContent') == 'live'
        now = datetime.datetime.now(datetime.timezone.utc)

        if is_live:
            logger.info(f"🎯 Sniper Hit! {video_id} is LIVE.")
            await self.send_youtube_notification(data)
            return
        
        if now < (scheduled_time + datetime.timedelta(minutes=3)):
            next_run = now + datetime.timedelta(seconds=90)
            scheduler.add_job(self.check_youtube_status, 'date', run_date=next_run, args=[video_id, scheduled_time], id=f"yt_{video_id}")
        elif now < (scheduled_time + datetime.timedelta(minutes=21)):
            next_run = now + datetime.timedelta(minutes=3)
            scheduler.add_job(self.check_youtube_status, 'date', run_date=next_run, args=[video_id, scheduled_time], id=f"yt_{video_id}")
        else:
            logger.info(f"   🛑 Giving up on {video_id} (Never went live).")
            sync_state_to_s3()

    async def fetch_youtube_data(self, video_id):
        if not YOUTUBE_API_KEY: return None
        if not self.session: return None
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {"part": "snippet,liveStreamingDetails,statistics", "id": video_id, "key": YOUTUBE_API_KEY}
        async with self.session.get(url, params=params) as resp:
            if resp.status != 200: return None
            js = await resp.json()
            return js['items'][0] if js['items'] else None

    async def send_youtube_notification(self, data):
        channel_id = data['snippet']['channelId']
        channel_name = YOUTUBE_STREAMERS.get(channel_id, data['snippet']['channelTitle'])
        vid_id = data['id']
        url = f"https://www.youtube.com/watch?v={vid_id}"
        
        stats = data.get('statistics', {})
        is_members_only = 'viewCount' not in stats

        if is_members_only:
            title_prefix = "( MEMBERS ONLY )"
            desc = f"🔒 **{channel_name}** is live for **MEMBERS ONLY**!"
            color = 0xFFD700 # Gold
        else:
            title_prefix = "🔴"
            desc = f"**{channel_name}** is LIVE on YouTube!"
            color = 0xFF0000 # Red

        embed = discord.Embed(title=f"{title_prefix} {data['snippet']['title']}", url=url, description=desc, color=color, timestamp=datetime.datetime.now(datetime.timezone.utc))
        thumbs = data['snippet']['thumbnails']
        thumb_url = thumbs.get('maxres', thumbs.get('high', thumbs.get('default')))['url']
        embed.set_image(url=thumb_url)

        chan = discord_bot.get_channel(DISCORD_CHANNEL_ID)
        if chan: await chan.send(content=f"{title_prefix} **{channel_name}** is LIVE! {url}", embed=embed)

    def remove_youtube_job(self, video_id, save=True):
        job_id = f"yt_{video_id}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
            if save: sync_state_to_s3()

    async def maintain_youtube_subs(self):
        await discord_bot.wait_until_ready()
        hub_url = "https://pubsubhubbub.appspot.com/subscribe"
        while not discord_bot.is_closed():
            logger.info("📡 Renewing YouTube WebSub Leases...")
            for cid in YOUTUBE_STREAMERS:
                data = {"hub.mode": "subscribe", "hub.topic": f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={cid}", "hub.callback": f"{PUBLIC_URL}/youtube", "hub.lease_seconds": 432000}
                try:
                    if self.session:
                        async with self.session.post(hub_url, data=data) as resp:
                            if resp.status >= 400:
                                logger.error(f"   ❌ Failed sub for {cid}: {resp.status}")
                            else:
                                logger.info(f"   ➜ Subscribed to YouTube: {YOUTUBE_STREAMERS[cid]} ({cid})")
                except Exception as e:
                    logger.error(f"   ❌ Failed sub for {cid}: {e}")
            await asyncio.sleep(345600)

    # Twitch Logic
    async def setup_twitch_subs(self):
        try:
            await self.delete_all_eventsub_subscriptions()
        except: pass
        logger.info(f"📋 Subscribing {len(TWITCH_STREAMERS)} Twitch channels...")
        for s_id, s_name in TWITCH_STREAMERS.items():
            try:
                await self.subscribe_webhook(payload=StreamOnlineSubscription(broadcaster_user_id=s_id, version="1"), callback_url=PUBLIC_URL)
                await self.subscribe_webhook(payload=StreamOfflineSubscription(broadcaster_user_id=s_id, version="1"), callback_url=PUBLIC_URL)
                logger.info(f"   ➜ Subscribed to Twitch: {s_name}")
            except Exception as e:
                logger.error(f"   ❌ Failed Twitch {s_name}: {e}")

    async def populate_message_cache(self) -> None:
        channel

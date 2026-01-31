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
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from typing import Any
from aiohttp import web, ClientSession
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
        if c_id != "api_key":
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
        self.web_adapter = AiohttpAdapter(port=LOCAL_PORT, domain=SERVER_DOMAIN, eventsub_secret=TWITCH_EVENTSUB_SECRET)
        self.session = None
        super().__init__(client_id=TWITCH_CLIENT_ID, client_secret=TWITCH_CLIENT_SECRET, adapter=self.web_adapter)

    async def event_ready(self) -> None:
        logger.info(f"✅ Hybrid Bot Listening on {LOCAL_PORT}")
        self.session = ClientSession()
        await discord_bot.wait_until_ready()
        await self.populate_message_cache()
        sync_state_from_s3()
        load_local_state(self)
        scheduler.start()
        await self.setup_twitch_subs()
        
        # FIX: Register route based on PUBLIC_URL path (e.g., /callback/youtube)
        if hasattr(self.web_adapter, '_app') and self.web_adapter._app:
            path = urlparse(PUBLIC_URL).path.rstrip('/')
            route = path + '/youtube'
            
            self.web_adapter._app.router.add_post(route, self.youtube_webhook_handler)
            self.web_adapter._app.router.add_get(route, self.youtube_webhook_handler)
            logger.info(f"✅ Registered YouTube Route: {route}")
            
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

    async def initial_youtube_check(self, video_id):
        data = await self.fetch_youtube_data(video_id)
        if not data: return
        snippet = data['snippet']
        live_details = data.get('liveStreamingDetails', {})
        is_live = snippet.get('liveBroadcastContent') == 'live'
        scheduled_start = live_details.get('scheduledStartTime')

        if is_live:
            await self.send_youtube_notification(data)
            self.remove_youtube_job(video_id)
        elif scheduled_start:
            dt = datetime.datetime.fromisoformat(scheduled_start.replace('Z', '+00:00'))
            logger.info(f"   🗓️ Scheduled for {dt}. Queueing Sniper.")
            run_time = dt - datetime.timedelta(minutes=3)
            now = datetime.datetime.now(datetime.timezone.utc)
            if run_time < now:
                run_time = now + datetime.timedelta(seconds=10)
            scheduler.add_job(self.check_youtube_status, 'date', run_date=run_time, args=[video_id, dt], id=f"yt_{video_id}", replace_existing=True)
            sync_state_to_s3()

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

    def remove_youtube_job(self, video_id):
        job_id = f"yt_{video_id}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
            sync_state_to_s3()

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
                            if resp.status >= 400: logger.error(f"   ❌ Failed sub for {cid}: {resp.status}")
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
            except Exception as e:
                logger.error(f"   ❌ Failed Twitch {s_name}: {e}")

    async def populate_message_cache(self) -> None:
        channel = discord_bot.get_channel(DISCORD_CHANNEL_ID)
        if not isinstance(channel, discord.TextChannel): return
        try:
            async for message in channel.history(limit=50):
                if message.author != discord_bot.user or not message.embeds: continue
                embed = message.embeds[0]
                if embed.color and embed.color.value == 9520895:
                    url = embed.url
                    if url:
                        login = url.split("/")[-1].lower()
                        found_id = next((i for i, n in TWITCH_STREAMERS.items() if n.lower() == login), None)
                        if found_id:
                            twitch_active_messages[found_id] = message
                            asyncio.create_task(self.delayed_check(found_id, login))
        except Exception as e:
            logger.error(f"❌ Cache rebuild fail: {e}")

    async def event_stream_online(self, payload: twitchio.StreamOnline) -> None:
        s_id = payload.broadcaster.id
        s_login = payload.broadcaster.name
        logger.info(f"📣 Twitch LIVE: {s_login}")
        stream_data = None
        try:
            streams = [s async for s in self.fetch_streams(user_ids=[s_id])]
            if streams: stream_data = streams[0]
        except: pass
        embed = self.build_twitch_embed(s_login, stream_data)
        chan = discord_bot.get_channel(DISCORD_CHANNEL_ID)
        if chan:
            msg = await chan.send(content=f"🔴 **{s_login}** is LIVE! https://twitch.tv/{s_login}", embed=embed)
            twitch_active_messages[s_id] = msg
            asyncio.create_task(self.delayed_check(s_id, s_login))

    async def event_stream_offline(self, payload: twitchio.StreamOffline) -> None:
        s_id = str(payload.broadcaster.id)
        if s_id in twitch_active_messages:
            try:
                ts = int(datetime.datetime.now().timestamp())
                embed = discord.Embed(title=f"⚫ {payload.broadcaster.name} ended.", description=f"Ended at <t:{ts}:T>.", color=0x2C2F33)
                await twitch_active_messages[s_id].edit(content=None, embed=embed)
            except: pass
            del twitch_active_messages[s_id]

    async def delayed_check(self, s_id: str, s_login: str) -> None:
        await asyncio.sleep(3600)
        if s_id not in twitch_active_messages: return
        try:
            streams = [s async for s in self.fetch_streams(user_ids=[s_id])]
            if not streams:
                ts = int(datetime.datetime.now().timestamp())
                embed = discord.Embed(title=f"⚫ {s_login} ended.", description=f"Ended at <t:{ts}:T>.", color=0x2C2F33)
                await twitch_active_messages[s_id].edit(content=None, embed=embed)
                del twitch_active_messages[s_id]
            else:
                await twitch_active_messages[s_id].edit(embed=self.build_twitch_embed(s_login, streams[0]))
                asyncio.create_task(self.delayed_check(s_id, s_login))
        except:
            asyncio.create_task(self.delayed_check(s_id, s_login))

    def build_twitch_embed(self, login, data):
        title = data.title if data else "Live Stream"
        game = data.game_name if data else "Unknown"
        embed = discord.Embed(title=title, url=f"https://twitch.tv/{login}", description=f"**{login}** playing **{game}**", color=0x9146FF, timestamp=datetime.datetime.now(datetime.timezone.utc))
        if data: embed.set_image(url=data.thumbnail_url.replace("{width}x{height}", "1280x720"))
        return embed

# Run & Lifecycle
twitch_bot = HybridBot()

@tasks.loop(minutes=90)
async def autosave_state_task():
    sync_state_to_s3()

@discord_bot.event
async def setup_hook() -> None:
    discord_bot.loop.create_task(twitch_bot.start())
    autosave_state_task.start()

async def shutdown_handler(signal_type):
    logger.info(f"🛑 Received {signal_type.name}...")
    sync_state_to_s3()
    await discord_bot.close()
    await twitch_bot.close()

def main() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown_handler(s)))
    try:
        discord_bot.run(DISCORD_TOKEN)
    except KeyboardInterrupt: pass

if __name__ == "__main__":
    main()

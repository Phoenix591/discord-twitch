#!/usr/local/discord-twitch/bin/python
import asyncio
import logging
import discord
import configparser
import os
from discord.ext import commands
import twitchio
from twitchio.web import AiohttpAdapter
from twitchio.eventsub import StreamOnlineSubscription, StreamOfflineSubscription

# ==========================================
#               CONFIGURATION
# ==========================================

config = configparser.ConfigParser()

# Systemd LoadCredential logic
credential_dir = os.environ.get("CREDENTIALS_DIRECTORY")
if credential_dir:
    # If running via Systemd, look here
    configfile = os.path.join(credential_dir, "secret.cfg")
else:
    # Fallback for manual testing/running
    configfile = os.path.expanduser("/usr/local/discord-twitch/secret.cfg")

if not os.path.exists(configfile):
    raise FileNotFoundError(f"Configuration file not found at: {configfile}")

config.read(configfile)

# 1. DISCORD CONFIG
DISCORD_TOKEN = config['discord']['token']
DISCORD_CHANNEL_ID = int(config['discord']['channelid']) 

# 2. TWITCH CONFIG
TWITCH_CLIENT_ID = config['twitch']['clientid']
TWITCH_CLIENT_SECRET = config['twitch']['clientsecret']
TWITCH_EVENTSUB_SECRET = config['twitch']['eventsub_secret']

# 3. SERVER CONFIG
SERVER_DOMAIN = config['server']['domain']
PUBLIC_URL = config['server']['public_url']
LOCAL_PORT = int(config['server']['port'])

# 4. LOAD STREAMERS
STREAMERS_TO_TRACK = {}
if 'streamers' in config:
    for streamer_id, display_name in config['streamers'].items():
        # Ensure ID is a string for TwitchIO v3
        STREAMERS_TO_TRACK[str(streamer_id)] = display_name
else:
    print("WARNING: No [streamers] section found in config file!")

# ==========================================
#           GLOBAL STATE & LOGGING
# ==========================================

active_messages = {}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Bot")

# ==========================================
#              DISCORD BOT
# ==========================================

intents = discord.Intents.default()
discord_bot = commands.Bot(command_prefix="!", intents=intents)

# ==========================================
#              TWITCH BOT
# ==========================================

class TwitchBot(twitchio.Client):
    def __init__(self):
        # Initialize the Web Adapter (Removed invalid 'api=True' arg)
        adapter = AiohttpAdapter(
            port=LOCAL_PORT,
            domain=SERVER_DOMAIN
        )
        
        super().__init__(
            client_id=TWITCH_CLIENT_ID,
            client_secret=TWITCH_CLIENT_SECRET,
            adapter=adapter
        )

    async def event_ready(self):
        logger.info(f"✅ Twitch Webhook Server listening on port {LOCAL_PORT}")
        logger.info(f"🌐 Configured Public URL: {PUBLIC_URL}")
        
        # Cleanup old subs to ensure fresh start
        try:
            await self.delete_all_eventsub_subscriptions()
        except Exception:
            pass

        logger.info(f"📋 Subscribing {len(STREAMERS_TO_TRACK)} channels...")

        for streamer_id, streamer_name in STREAMERS_TO_TRACK.items():
            try:
                # 1. Subscribe Online
                # v3 Subscription Objects
                online_sub = StreamOnlineSubscription(broadcaster_user_id=streamer_id, version="1")
                await self.subscribe_webhook(
                    payload=online_sub,
                    callback_url=PUBLIC_URL,
                    eventsub_secret=TWITCH_EVENTSUB_SECRET
                )

                # 2. Subscribe Offline
                offline_sub = StreamOfflineSubscription(broadcaster_user_id=streamer_id, version="1")
                await self.subscribe_webhook(
                    payload=offline_sub,
                    callback_url=PUBLIC_URL,
                    eventsub_secret=TWITCH_EVENTSUB_SECRET
                )
                
                logger.info(f"   ➜ Subscribed: {streamer_name} (ID: {streamer_id})")
            except Exception as e:
                logger.error(f"   ❌ Failed {streamer_name}: {e}")

    async def event_stream_online(self, payload):
        """Triggered when Twitch POSTs data to our server"""
        event = payload.event
        streamer_id = str(event.broadcaster_user_id)
        streamer_name = event.broadcaster_user_name
        
        logger.info(f"📣 WEBHOOK RECEIVED: {streamer_name} is LIVE")
        
        channel = discord_bot.get_channel(DISCORD_CHANNEL_ID)
        if not channel: 
            logger.error(f"❌ Discord Channel {DISCORD_CHANNEL_ID} not found.")
            return

        embed = discord.Embed(
            title=f"🔴 {streamer_name} is LIVE!",
            url=f"https://twitch.tv/{event.broadcaster_user_login}",
            color=0x9146FF
        )
        
        try:
            msg = await channel.send(
                content=f"Hey @everyone, **{streamer_name}** is live!", 
                embed=embed
            )
            active_messages[streamer_id] = msg
        except Exception as e:
            logger.error(f"Discord Send Error: {e}")

    async def event_stream_offline(self, payload):
        event = payload.event
        streamer_id = str(event.broadcaster_user_id)
        streamer_name = event.broadcaster_user_name
        
        logger.info(f"🌑 WEBHOOK RECEIVED: {streamer_name} is OFFLINE")

        if streamer_id in active_messages:
            old_msg = active_messages[streamer_id]
            try:
                new_embed = discord.Embed(
                    title=f"⚫ {streamer_name} was live.",
                    description="Stream ended.",
                    color=0x2c2f33
                )
                await old_msg.edit(content=None, embed=new_embed)
            except Exception:
                pass
            del active_messages[streamer_id]

    async def event_error(self, payload):
        logger.error(f"Twitch Error: {payload}")

twitch_bot = TwitchBot()

# ==========================================
#           RUNNING BOTH LOOPS
# ==========================================

@discord_bot.event
async def setup_hook():
    # Start the Twitch Client (which automatically starts the Web Server)
    discord_bot.loop.create_task(twitch_bot.start())

if __name__ == "__main__":
    discord_bot.run(DISCORD_TOKEN)

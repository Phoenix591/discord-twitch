#!/usr/local/discord-twitch/bin/python
import asyncio
import logging
import discord
import configparser
import os
import datetime
from discord.ext import commands, tasks  # <--- NEW: Import tasks
import twitchio
from twitchio.web import AiohttpAdapter
from twitchio.eventsub import StreamOnlineSubscription, StreamOfflineSubscription

config = configparser.ConfigParser()

cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")
if cred_dir:
    secret_path = os.path.join(cred_dir, "secret.cfg")
    logger.info(f"🔒 Loading secrets from Systemd Credential: {secret_path}")
else:
    secret_path = "/usr/local/discord-twitch/secret.cfg"
    logger.info(f"⚠️  Systemd Credential env not found. Trying local: {secret_path}")

streamers_path = "/usr/local/discord-twitch/streamers.cfg"

read_files = config.read([secret_path, streamers_path])

if not read_files:
    logger.error(f"❌ No config files found! Looked for: {secret_path}, {streamers_path}")
    sys.exit(1)

logger.info(f"✅ Config loaded from: {read_files}")

# 1. DISCORD
DISCORD_TOKEN = config['discord']['token']
DISCORD_CHANNEL_ID = int(config['discord']['channelid']) 

# 2. TWITCH
TWITCH_CLIENT_ID = config['twitch']['clientid']
TWITCH_CLIENT_SECRET = config['twitch']['clientsecret']
TWITCH_EVENTSUB_SECRET = config['twitch']['eventsub_secret']

# 3. SERVER
SERVER_DOMAIN = config['server']['domain']
PUBLIC_URL = config['server']['public_url']
LOCAL_PORT = int(config['server']['port'])
DEBUG_INTERVAL = int(config['server'].get('debug_interval', 30))

# 4. STREAMERS
STREAMERS_TO_TRACK = {}
if 'streamers' in config:
    for streamer_id, display_name in config['streamers'].items():
        STREAMERS_TO_TRACK[str(streamer_id)] = display_name

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
#              DISCORD BOT SETUP
# ==========================================

intents = discord.Intents.default()
intents.message_content = True
discord_bot = commands.Bot(command_prefix="!", intents=intents)

# ==========================================
#              TWITCH BOT
# ==========================================

class TwitchBot(twitchio.Client):
    def __init__(self):
        adapter = AiohttpAdapter(
            port=LOCAL_PORT,
            domain=SERVER_DOMAIN,
            eventsub_secret=TWITCH_EVENTSUB_SECRET 
        )
        super().__init__(
            client_id=TWITCH_CLIENT_ID,
            client_secret=TWITCH_CLIENT_SECRET,
            adapter=adapter
        )

    async def event_ready(self):
        logger.info(f"✅ Twitch Webhook Server listening on port {LOCAL_PORT}")

        await discord_bot.wait_until_ready()
        await self.populate_message_cache()
        # -----------------------------------------------

        # Cleanup old subs
        try:
            await self.delete_all_eventsub_subscriptions()
        except Exception:
            pass

        logger.info(f"📋 Subscribing {len(STREAMERS_TO_TRACK)} channels...")

        for streamer_id, streamer_name in STREAMERS_TO_TRACK.items():
            try:
                # Online
                online_sub = StreamOnlineSubscription(broadcaster_user_id=streamer_id, version="1")
                await self.subscribe_webhook(payload=online_sub, callback_url=PUBLIC_URL)
                # Offline
                offline_sub = StreamOfflineSubscription(broadcaster_user_id=streamer_id, version="1")
                await self.subscribe_webhook(payload=offline_sub, callback_url=PUBLIC_URL)
                logger.info(f"   ➜ Subscribed: {streamer_name} (ID: {streamer_id})")
            except Exception as e:
                logger.error(f"   ❌ Failed {streamer_name}: {e}")

    async def populate_message_cache(self):
        """
        Scans recent Discord messages to find active 'Live' alerts
        and restores them to memory so we can edit them later.
        """
        logger.info("🧠 Scanning Discord history to rebuild state...")
        channel = discord_bot.get_channel(DISCORD_CHANNEL_ID)

        if not channel:
            logger.error("❌ Cannot fetch history: Channel not found.")
            return

        try:
            # Scan the last 50 messages (Adjust limit if your channel is very busy)
            async for message in channel.history(limit=50):

                # 1. Ignore messages not by me
                if message.author != discord_bot.user:
                    continue

                # 2. Ignore messages without embeds
                if not message.embeds:
                    continue

                embed = message.embeds[0]

                # 3. Check for the "Live" color (Purple: 0x9146FF -> Decimal: 9520895)
                # If the embed is Grey (Offline), we ignore it.
                if embed.color and embed.color.value == 9520895:

                    # 4. Extract Streamer Name from URL (https://twitch.tv/ninja)
                    if embed.url:
                        # Get the last part of the URL (the login name)
                        login_name_from_url = embed.url.split('/')[-1].lower()

                        # 5. Reverse Lookup: Find the ID for this name
                        # We need the ID because `active_messages` keys are IDs (from Twitch events)
                        found_id = None
                        for s_id, s_name in STREAMERS_TO_TRACK.items():
                            # Compare against config names (case-insensitive)
                            if s_name.lower() == login_name_from_url:
                                found_id = s_id
                                break

                        if found_id:
                            active_messages[found_id] = message
                            logger.info(f"   ↳ ♻️  Restored state for: {login_name_from_url} (ID: {found_id})")

        except Exception as e:
            logger.error(f"❌ Failed to rebuild cache: {e}")
        logger.info(f"📋 Subscribing {len(STREAMERS_TO_TRACK)} channels...")

        for streamer_id, streamer_name in STREAMERS_TO_TRACK.items():
            try:
                # Subscribe Online
                online_sub = StreamOnlineSubscription(broadcaster_user_id=streamer_id, version="1")
                await self.subscribe_webhook(payload=online_sub, callback_url=PUBLIC_URL)

                # Subscribe Offline
                offline_sub = StreamOfflineSubscription(broadcaster_user_id=streamer_id, version="1")
                await self.subscribe_webhook(payload=offline_sub, callback_url=PUBLIC_URL)

                logger.info(f"   ➜ Subscribed: {streamer_name} (ID: {streamer_id})")
            except Exception as e:
                logger.error(f"   ❌ Failed {streamer_name}: {e}")

    async def event_stream_online(self, payload):
        """
        Triggered when a subscribed streamer goes LIVE.
        """
        streamer_id = payload.broadcaster.id
        streamer_login = payload.broadcaster.name
        stream_url = f"https://twitch.tv/{streamer_login}"

        logger.info(f"📣 WEBHOOK RECEIVED: {streamer_login} is LIVE")

        stream_title = "Live Stream"
        game_name = "Unknown Category"
        thumbnail_url = None

        try:
            streams = await self.fetch_streams(user_ids=[streamer_id])
            
            if streams:
                stream = streams[0]
                stream_title = stream.title
                game_name = stream.game_name
                
                thumb_asset = getattr(stream, "thumbnail", None) or getattr(stream, "thumbnail_url", None)
                
                if thumb_asset:
                    if hasattr(thumb_asset, 'url_for'):
                        thumbnail_url = thumb_asset.url_for(width=1280, height=720)
                    else:
                        # Fallback for strings (older versions)
                        thumbnail_url = str(thumb_asset).replace("{width}x{height}", "1280x720")
            
            else:
                logger.warning(f"   ⚠️ {streamer_login} is live, but API returned no stream data.")

        except Exception as e:
            logger.error(f"   ⚠️ Could not fetch stream details: {e}")

        embed = discord.Embed(
            title=stream_title,
            url=stream_url,
            description=f"**{streamer_login}** is playing **{game_name}**!",
            color=0x9146FF,
            timestamp=datetime.datetime.now(datetime.UTC)
        )
        
        if thumbnail_url:
            embed.set_image(url=thumbnail_url)

        embed.set_footer(text="Twitch Notification")

        # 4. Send to Discord
        channel = discord_bot.get_channel(DISCORD_CHANNEL_ID)
        if channel:
            try:
                msg = await channel.send(content=f"🔴 **{streamer_login}** is LIVE! {stream_url}", embed=embed)
                active_messages[streamer_id] = msg
                logger.info(f"   ➜ Notification sent to Discord.")
            except Exception as e:
                logger.error(f"   ❌ Discord Send Failed: {e}")
        else:
            logger.error("   ❌ Discord Channel not found.")

    async def event_stream_offline(self, payload):
        streamer_id = str(payload.broadcaster.id)
        streamer_name = payload.broadcaster.name

        logger.info(f"🌑 WEBHOOK RECEIVED: {streamer_name} is OFFLINE")

        if streamer_id in active_messages:
            old_msg = active_messages[streamer_id]
            try:
                timestamp = int(datetime.datetime.now().timestamp())
                new_embed = discord.Embed(
                    title=f"⚫ {streamer_name} was live.",
                    description=f"Stream ended at <t:{timestamp}:T>.",
                    url=f"https://twitch.tv/{streamer_name}",
                    color=0x2c2f33
                )
                await old_msg.edit(content=None, embed=new_embed)
            except Exception:
                pass
            del active_messages[streamer_id]

    async def event_error(self, payload):
        logger.error(f"❌ Twitch Event Error: {payload.error}")

twitch_bot = TwitchBot()


# DEBUG LOOP
@tasks.loop(seconds=DEBUG_INTERVAL)
async def debug_status_check():
    try:
        # 1. Get the wrapper object
        response = await twitch_bot.fetch_eventsub_subscriptions()

        # 2. Flatten the async iterator into a standard list
        # response.subscriptions is an HTTPAsyncIterator, so we must use [async for ...]
        current_subs = [s async for s in response.subscriptions]

        logger.info(f"🔎 DEBUG CHECK: Found {len(current_subs)} active subscriptions.")

        for sub in current_subs:
            # Safely get the user_id from the condition dict
            user_id = sub.condition.get('broadcaster_user_id', 'Unknown')
            name = STREAMERS_TO_TRACK.get(user_id, f"ID_{user_id}")

            # Create a clean status log
            status_icon = "⚠️"
            if sub.status == 'enabled':
                status_icon = "✅"
            elif sub.status == 'webhook_callback_verification_pending':
                status_icon = "⏳"
            elif sub.status == 'webhook_callback_verification_failed':
                status_icon = "❌"

            logger.info(f"   {status_icon} {name} | Type: {sub.type} | Status: {sub.status}")

    except Exception as e:
        logger.error(f"❌ Debug Loop Error: {e}")

@debug_status_check.before_loop
async def before_debug_loop():
# Wait for the bot to be fully ready before asking for data
    await twitch_bot.wait_until_ready()

@discord_bot.event
async def setup_hook():
    # Start the Twitch Client
    discord_bot.loop.create_task(twitch_bot.start())
    # Start the Debug Loop
    debug_status_check.start()

@discord_bot.command()
async def test(ctx):
    await ctx.send("✅ System Normal.")

if __name__ == "__main__":
    discord_bot.run(DISCORD_TOKEN)

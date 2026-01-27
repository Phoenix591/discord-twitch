#!/usr/local/discord-twitch/bin/python
import asyncio
import logging
import discord
import configparser
import os
import sys
import datetime
from typing import Any
from discord.ext import commands, tasks
import twitchio
from twitchio.web import AiohttpAdapter
from twitchio.eventsub import StreamOnlineSubscription, StreamOfflineSubscription

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("DiscordTwitchBot")

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
    logger.error(
        f"❌ No config files found! Looked for: {secret_path}, {streamers_path}"
    )
    sys.exit(1)

logger.info(f"✅ Config loaded from: {read_files}")

# 1. DISCORD
DISCORD_TOKEN = config["discord"]["token"]
DISCORD_CHANNEL_ID = int(config["discord"]["channelid"])

# 2. TWITCH
TWITCH_CLIENT_ID = config["twitch"]["clientid"]
TWITCH_CLIENT_SECRET = config["twitch"]["clientsecret"]
TWITCH_EVENTSUB_SECRET = config["twitch"]["eventsub_secret"]

# 3. SERVER
SERVER_DOMAIN = config["server"]["domain"]
PUBLIC_URL = config["server"]["public_url"]
LOCAL_PORT = int(config["server"]["port"])
DEBUG_INTERVAL = int(config["server"].get("debug_interval", 30))

# 4. STREAMERS
STREAMERS_TO_TRACK = {}
if "streamers" in config:
    for streamer_id, display_name in config["streamers"].items():
        STREAMERS_TO_TRACK[str(streamer_id)] = display_name

# ==========================================
#           GLOBAL STATE & LOGGING
# ==========================================

active_messages = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
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
    def __init__(self) -> None:
        adapter: AiohttpAdapter[Any] = AiohttpAdapter(
            port=LOCAL_PORT,
            domain=SERVER_DOMAIN,
            eventsub_secret=TWITCH_EVENTSUB_SECRET,
        )
        super().__init__(
            client_id=TWITCH_CLIENT_ID,
            client_secret=TWITCH_CLIENT_SECRET,
            adapter=adapter,
        )

    async def event_ready(self) -> None:
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
                online_sub = StreamOnlineSubscription(
                    broadcaster_user_id=streamer_id, version="1"
                )
                await self.subscribe_webhook(
                    payload=online_sub, callback_url=PUBLIC_URL
                )
                # Offline
                offline_sub = StreamOfflineSubscription(
                    broadcaster_user_id=streamer_id, version="1"
                )
                await self.subscribe_webhook(
                    payload=offline_sub, callback_url=PUBLIC_URL
                )
                logger.info(f"   ➜ Subscribed: {streamer_name} (ID: {streamer_id})")
            except Exception as e:
                logger.error(f"   ❌ Failed {streamer_name}: {e}")

    async def populate_message_cache(self) -> None:
        """
        Scans recent Discord messages to find active 'Live' alerts
        and restores them to memory so we can edit them later.
        """
        logger.info("🧠 Scanning Discord history to rebuild state...")
        channel = discord_bot.get_channel(DISCORD_CHANNEL_ID)

        if not isinstance(channel, discord.TextChannel):
            logger.error(
                "❌ Cannot fetch history: Channel not found or not a TextChannel."
            )
            return

        try:
            # Scan the last 50 messages
            async for message in channel.history(limit=50):

                if message.author != discord_bot.user:
                    continue

                if not message.embeds:
                    continue

                embed = message.embeds[0]

                # Purple color indicates a Live message
                if embed.color and embed.color.value == 9520895:
                    url = embed.url
                    if url:
                        login_name_from_url = url.split("/")[-1].lower()

                        found_id = None
                        for s_id, s_name in STREAMERS_TO_TRACK.items():
                            if s_name.lower() == login_name_from_url:
                                found_id = s_id
                                break

                        if found_id:
                            active_messages[found_id] = message
                            logger.info(
                                f"   ↳ ♻️  Restored state for: {login_name_from_url} (ID: {found_id})"
                            )
                            # Schedule a health check for this restored stream
                            asyncio.create_task(
                                self.delayed_check(found_id, login_name_from_url)
                            )

        except Exception as e:
            logger.error(f"❌ Failed to rebuild cache: {e}")

    # ---------------------------------------------------------------------
    # Helper: Build Embed (Shared by initial send and retry)
    # ---------------------------------------------------------------------
    def build_embed(
        self,
        streamer_login: str,
        stream_url: str,
        stream_data: twitchio.Stream | None = None,
    ) -> discord.Embed:
        title = "Live Stream"
        game = "Unknown Category"
        thumbnail_url = None

        if stream_data:
            title = stream_data.title
            game = stream_data.game_name or "Unknown Category"

            thumb_asset = getattr(stream_data, "thumbnail", None) or getattr(
                stream_data, "thumbnail_url", None
            )

            if thumb_asset:
                if hasattr(thumb_asset, "url_for"):
                    thumbnail_url = thumb_asset.url_for(width=1280, height=720)
                else:
                    thumbnail_url = str(thumb_asset).replace(
                        "{width}x{height}", "1280x720"
                    )

        embed = discord.Embed(
            title=title,
            url=stream_url,
            description=f"**{streamer_login}** is playing **{game}**!",
            color=0x9146FF,
            timestamp=datetime.datetime.now(datetime.UTC),
        )

        if thumbnail_url:
            embed.set_image(url=thumbnail_url)

        embed.set_footer(text="Twitch Notification")
        return embed

    # ---------------------------------------------------------------------
    # Periodic Delayed Check (Recursively schedules itself)
    # ---------------------------------------------------------------------
    async def delayed_check(self, streamer_id: str, streamer_login: str) -> None:
        logger.info(f"   ⏳ Scheduling 1-hour health check for {streamer_login}...")

        # Wait 1 hour (3600 seconds)
        await asyncio.sleep(3600)

        # If stream was manually ended/offline'd during the wait, stop.
        if streamer_id not in active_messages:
            return

        logger.info(f"   ⏰ Performing 1-hour check for {streamer_login}...")

        try:
            msg = active_messages[streamer_id]
            # TwitchIO v3: Consume async iterator into a list
            streams = [s async for s in self.fetch_streams(user_ids=[streamer_id])]

            if not streams:
                # CASE 1: Silent Offline detected
                logger.info(f"   📉 Detected Silent Offline for {streamer_login}")

                timestamp = int(datetime.datetime.now().timestamp())
                new_embed = discord.Embed(
                    title=f"⚫ {streamer_login} was live.",
                    description=f"Stream ended at <t:{timestamp}:T>.",
                    url=f"https://twitch.tv/{streamer_login}",
                    color=0x2C2F33,
                )

                try:
                    await msg.edit(content=None, embed=new_embed)
                except Exception:
                    pass

                if streamer_id in active_messages:
                    del active_messages[streamer_id]

            else:
                # CASE 2: Still Live (Update Info)
                logger.info(
                    f"   🔄 Stream still live. Updating info for {streamer_login}..."
                )
                stream = streams[0]
                login_name = stream.user.name or "Unknown"
                stream_url = f"https://twitch.tv/{login_name}"

                new_embed = self.build_embed(login_name, stream_url, stream)

                try:
                    await msg.edit(embed=new_embed)
                except Exception:
                    pass

                # RECURSIVE: Schedule the next check for 1 hour from now
                asyncio.create_task(self.delayed_check(streamer_id, login_name))

        except Exception as e:
            logger.error(f"   ❌ Delayed check failed for {streamer_login}: {e}")
            # Optional: On error (e.g. API fail), try again in 1 hour anyway
            asyncio.create_task(self.delayed_check(streamer_id, streamer_login))

    async def event_stream_online(self, payload: twitchio.StreamOnline) -> None:
        """
        Triggered when a subscribed streamer goes LIVE.
        """
        streamer_id = payload.broadcaster.id
        streamer_login = payload.broadcaster.name or "Unknown"
        stream_url = f"https://twitch.tv/{streamer_login}"

        logger.info(f"📣 WEBHOOK RECEIVED: {streamer_login} is LIVE")

        # 1. First Attempt: Fetch Data
        stream_data: twitchio.Stream | None = None
        try:
            # TwitchIO v3: Consume async iterator into a list
            streams = [s async for s in self.fetch_streams(user_ids=[streamer_id])]
            if streams:
                stream_data = streams[0]
            else:
                logger.warning(
                    f"   ⚠️ {streamer_login} is live, but API returned no stream data (API Lag)."
                )
        except Exception as e:
            logger.error(f"   ⚠️ Could not fetch stream details: {e}")

        # 2. Build Initial Embed (Uses Defaults if stream_data is None)
        embed = self.build_embed(streamer_login, stream_url, stream_data)

        # 3. Send Notification Immediately
        channel = discord_bot.get_channel(DISCORD_CHANNEL_ID)
        if not isinstance(channel, discord.TextChannel):
            logger.error("   ❌ Discord Channel not found or not a TextChannel.")
            return

        try:
            msg = await channel.send(
                content=f"🔴 **{streamer_login}** is LIVE! {stream_url}", embed=embed
            )
            active_messages[streamer_id] = msg
            logger.info(f"   ➜ Notification sent to Discord.")

            # 5. Schedule 1-Hour Health Check
            asyncio.create_task(self.delayed_check(streamer_id, streamer_login))

        except Exception as e:
            logger.error(f"   ❌ Discord Send Failed: {e}")
            return

        # 4. Retry Logic (Only if initial fetch failed)
        if not stream_data:
            logger.info(f"   ⏳ API Lag detected. Retrying in 5 seconds...")
            await asyncio.sleep(5)

            try:
                # TwitchIO v3: Consume async iterator into a list
                streams = [s async for s in self.fetch_streams(user_ids=[streamer_id])]
                if streams:
                    logger.info(
                        f"   ✅ Data found on retry! Updating message for {streamer_login}."
                    )
                    # Build new embed with the valid data
                    new_embed = self.build_embed(streamer_login, stream_url, streams[0])
                    # Edit the existing message
                    await msg.edit(embed=new_embed)
                else:
                    logger.warning(
                        f"   ❌ Still no data after 5s. Keeping default message."
                    )
            except Exception as e:
                logger.error(f"   ❌ Retry failed: {e}")

    async def event_stream_offline(self, payload: twitchio.StreamOffline) -> None:
        streamer_id = str(payload.broadcaster.id)
        streamer_name = payload.broadcaster.name or "Unknown"

        logger.info(f"🌑 WEBHOOK RECEIVED: {streamer_name} is OFFLINE")

        if streamer_id in active_messages:
            old_msg = active_messages[streamer_id]
            try:
                timestamp = int(datetime.datetime.now().timestamp())
                new_embed = discord.Embed(
                    title=f"⚫ {streamer_name} was live.",
                    description=f"Stream ended at <t:{timestamp}:T>.",
                    url=f"https://twitch.tv/{streamer_name}",
                    color=0x2C2F33,
                )
                await old_msg.edit(content=None, embed=new_embed)
            except Exception:
                pass
            del active_messages[streamer_id]

    async def event_error(self, payload: twitchio.EventErrorPayload) -> None:
        logger.error(f"❌ Twitch Event Error: {payload.error}")


twitch_bot = TwitchBot()


# DEBUG LOOP
@tasks.loop(seconds=DEBUG_INTERVAL)
async def debug_status_check() -> None:
    try:
        response = await twitch_bot.fetch_eventsub_subscriptions()
        current_subs = [s async for s in response.subscriptions]

        logger.info(f"🔎 DEBUG CHECK: Found {len(current_subs)} active subscriptions.")

        for sub in current_subs:
            user_id = sub.condition.get("broadcaster_user_id", "Unknown")
            name = STREAMERS_TO_TRACK.get(user_id, f"ID_{user_id}")

            status_icon = "⚠️"
            if sub.status == "enabled":
                status_icon = "✅"
            elif sub.status == "webhook_callback_verification_pending":
                status_icon = "⏳"
            elif sub.status == "webhook_callback_verification_failed":
                status_icon = "❌"

            logger.info(
                f"   {status_icon} {name} | Type: {sub.type} | Status: {sub.status}"
            )

    except Exception as e:
        logger.error(f"❌ Debug Loop Error: {e}")


@debug_status_check.before_loop
async def before_debug_loop() -> None:
    await twitch_bot.wait_until_ready()


@discord_bot.event
async def setup_hook() -> None:
    discord_bot.loop.create_task(twitch_bot.start())
    debug_status_check.start()


@discord_bot.command()
async def test(ctx: commands.Context[Any]) -> None:
    await ctx.send("✅ System Normal.")


if __name__ == "__main__":
    discord_bot.run(DISCORD_TOKEN)

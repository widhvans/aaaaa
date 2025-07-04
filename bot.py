# bot.py

import logging
import asyncio
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, PeerIdInvalid, MessageNotModified
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyromod import Client
from aiohttp import web
from config import Config
from database.db import (
    get_user, save_file_data, get_post_channel, get_index_db_channel
)
from utils.helpers import create_post, get_title_key, notify_and_remove_invalid_channel
from thefuzz import fuzz

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()])
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyromod").setLevel(logging.WARNING)
logging.getLogger("imdbpy").setLevel(logging.WARNING) # Suppress cinemagoer logs
logger = logging.getLogger(__name__)


# Web Server Redirect Handler
async def handle_redirect(request):
    composite_id = request.match_info.get('composite_id', None)
    if not composite_id:
        return web.Response(text="File ID missing.", status=400)
    
    try:
        with open(Config.BOT_USERNAME_FILE, 'r') as f:
            bot_username = f.read().strip().replace("@", "")
    except FileNotFoundError:
        logger.error(f"FATAL: Bot username file not found at {Config.BOT_USERNAME_FILE}")
        return web.Response(text="Bot configuration error.", status=500)
    
    return web.HTTPFound(f"https://t.me/{bot_username}?start=get_{composite_id}")


class Bot(Client):
    def __init__(self):
        super().__init__("FinalStorageBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN, plugins=dict(root="handlers"))
        self.me = None
        self.web_app = None
        self.web_runner = None
        
        self.owner_db_channel = Config.OWNER_DB_CHANNEL
        self.stream_channel_id = None
        self.file_queue = asyncio.Queue()
        self.open_batches = {}
        self.search_cache = {}
        
        self.vps_ip = Config.VPS_IP
        self.vps_port = Config.VPS_PORT

    async def send_with_protection(self, coro, *args, **kwargs):
        while True:
            try:
                return await coro(*args, **kwargs)
            except FloodWait as e:
                logger.warning(f"FloodWait of {e.value}s detected. Sleeping for {e.value + 5}s...")
                await asyncio.sleep(e.value + 5)
            except Exception as e:
                logger.error(f"An error occurred in send_with_protection: {e}", exc_info=True)
                return None

    async def find_matching_batch_key(self, user_id, title_key):
        """Finds the best matching batch key for a given title using aggressive fuzzy matching."""
        best_match_key = None
        highest_similarity = 82  # More inclusive threshold

        if user_id not in self.open_batches:
            return None

        # First pass: Check for high-quality matches based on word order and similarity
        for existing_key in self.open_batches[user_id]:
            similarity = fuzz.token_sort_ratio(title_key, existing_key)
            if similarity > highest_similarity:
                highest_similarity = similarity
                best_match_key = existing_key
        
        # Second pass: If no strong match, try a more lenient check for substrings
        if not best_match_key:
            for existing_key in self.open_batches[user_id]:
                if fuzz.partial_ratio(title_key, existing_key) > 92: # Needs to be a very high partial match
                    best_match_key = existing_key
                    logger.info(f"Found a batch match for '{title_key}' with '{existing_key}' using lenient partial ratio.")
                    return best_match_key
        
        return best_match_key

    async def _finalize_batch(self, user_id, batch_key):
        if user_id not in self.open_batches or batch_key not in self.open_batches[user_id]:
            return
        
        batch_data = self.open_batches[user_id].pop(batch_key)
        messages = batch_data.get('messages', [])
        dashboard_msg = batch_data.get('dashboard_message')

        try:
            if not messages:
                if dashboard_msg: await dashboard_msg.delete()
                return

            if dashboard_msg: await dashboard_msg.edit_text(f"**Processing Batch**\n\n🎬 **Batch:** `{batch_key}`\n"
                                                          f"⏳ **Status:** Processing `{len(messages)}` files...")

            user = await get_user(user_id)
            if not user: return

            post_channel_id = await get_post_channel(user_id)
            if not post_channel_id:
                if dashboard_msg: await dashboard_msg.edit_text("❌ **Error!**\n\nNo Post Channel is configured. Cannot create post.")
                return

            if not await notify_and_remove_invalid_channel(self, user_id, post_channel_id, "Post"):
                 if dashboard_msg: await dashboard_msg.edit_text("❌ **Error!**\n\nCould not access the configured Post Channel.")
                 return

            posts_to_send = await create_post(self, user_id, messages)
            
            if dashboard_msg: await dashboard_msg.edit_text(f"**Posting Batch**\n\n🎬 **Batch:** `{batch_key}`\n"
                                                          f"⏳ **Status:** Posting `{len(posts_to_send)}` message(s) to your channel...")

            for channel_id in [post_channel_id]:
                for post in posts_to_send:
                    poster, caption, footer = post
                    if poster:
                        await self.send_with_protection(self.send_photo, channel_id, photo=poster, caption=caption, reply_markup=footer)
                    else:
                        await self.send_with_protection(self.send_message, channel_id, caption, reply_markup=footer, disable_web_page_preview=True)
                    await asyncio.sleep(2) 

            if dashboard_msg: await dashboard_msg.edit_text(f"✅ **Batch Complete!**\n\n`{batch_key}` with `{len(messages)}` files posted successfully.")

        except Exception as e:
            logger.exception(f"CRITICAL Error finalizing batch {batch_key} for user {user_id}: {e}")
            if dashboard_msg: await dashboard_msg.edit_text(f"❌ **Error!**\n\nAn unexpected error occurred while processing batch `{batch_key}`.")
        finally:
            if user_id in self.open_batches and not self.open_batches[user_id]:
                del self.open_batches[user_id]

    async def file_processor_worker(self):
        logger.info("File Processor Worker started.")
        while True:
            try:
                message, user_id = await self.file_queue.get()
                
                self.stream_channel_id = await get_index_db_channel(user_id) or self.owner_db_channel
                if not self.stream_channel_id: continue

                copied_message = await self.send_with_protection(message.copy, self.owner_db_channel)
                if not copied_message: continue

                await save_file_data(user_id, message, copied_message, copied_message)
                
                filename = getattr(copied_message, copied_message.media.value).file_name
                title_key = await get_title_key(filename)
                if not title_key: continue

                self.open_batches.setdefault(user_id, {})
                loop = asyncio.get_event_loop()

                matched_key = await self.find_matching_batch_key(user_id, title_key)

                if matched_key:
                    batch_data = self.open_batches[user_id][matched_key]
                    if batch_data['timer']: batch_data['timer'].cancel()
                    
                    batch_data['messages'].append(copied_message)
                    
                    dashboard_msg = batch_data.get('dashboard_message')
                    if dashboard_msg:
                        try:
                           await dashboard_msg.edit_text(f"**Batch Update**\n\n🎬 **Batch:** `{matched_key}`\n"
                                                      f"📊 **Files Collected:** `{len(batch_data['messages'])}`\n"
                                                      f"⏳ **Status:** Added new file. Waiting 5 more seconds...")
                        except MessageNotModified: pass
                    
                    batch_data['timer'] = loop.call_later(5, lambda u=user_id, k=matched_key: asyncio.create_task(self._finalize_batch(u, k)))
                else:
                    dashboard_msg = await self.send_message(
                        chat_id=user_id,
                        text=f"**New Batch Detected**\n\n🎬 **Batch:** `{title_key}`\n"
                             f"📊 **Files Collected:** `1`\n"
                             f"⏳ **Status:** Collecting files. Waiting for 5 seconds..."
                    )
                    
                    self.open_batches[user_id][title_key] = {
                        'messages': [copied_message],
                        'timer': loop.call_later(5, lambda u=user_id, k=title_key: asyncio.create_task(self._finalize_batch(u, k))),
                        'dashboard_message': dashboard_msg
                    }
            except Exception as e:
                logger.exception(f"CRITICAL Error in file_processor_worker's main loop: {e}")
            finally:
                self.file_queue.task_done()
                await asyncio.sleep(0.5)

    async def start_web_server(self):
        from server.stream_routes import routes as stream_routes
        self.web_app = web.Application()
        self.web_app['bot'] = self
        self.web_app.router.add_get("/get/{composite_id}", handle_redirect)
        self.web_app.add_routes(stream_routes)
        self.web_runner = web.AppRunner(self.web_app)
        await self.web_runner.setup()
        site = web.TCPSite(self.web_runner, self.vps_ip, self.vps_port)
        await site.start()
        logger.info(f"Web server started at http://{self.vps_ip}:{self.vps_port}")

    async def start(self):
        await super().start()
        self.me = await self.get_me()

        logger.info("Hydrating session with dialogs to prevent Peer ID errors...")
        try:
            async for _ in self.get_dialogs():
                pass
            logger.info("Session hydration complete.")
        except Exception as e:
            logger.error(f"Could not hydrate session: {e}")
        
        if self.owner_db_channel:
            try:
                await self.get_chat(self.owner_db_channel)
                logger.info(f"Successfully connected to Owner DB (Log Channel) [{self.owner_db_channel}]")
            except Exception as e:
                logger.error(f"Could not verify Owner DB Channel. Ensure the bot is an admin. Error: {e}")
        else: 
            logger.warning("Owner DB ID (Log Channel) not set in config.py.")
            
        try:
            with open(Config.BOT_USERNAME_FILE, 'w') as f:
                f.write(f"@{self.me.username}")
            logger.info(f"Updated bot username to @{self.me.username}")
        except Exception as e:
            logger.error(f"Could not write to {Config.BOT_USERNAME_FILE}: {e}")
            
        asyncio.create_task(self.file_processor_worker())
        await self.start_web_server()
        
        logger.info(f"Bot @{self.me.username} started successfully.")

    async def stop(self, *args):
        logger.info("Stopping bot...")
        if self.web_runner:
            await self.web_runner.cleanup()
        await super().stop()
        logger.info("Bot stopped.")

if __name__ == "__main__":
    Bot().run()

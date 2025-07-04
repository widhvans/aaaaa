# bot.py

import logging
import asyncio
import time
import re
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
        
        # For dashboard edit throttling
        self.last_dashboard_edit = {}

        # Caching for performance
        self.imdb_cache = {}
        self.poster_cache = {}
        
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
    
    def _get_significant_words(self, title):
        """Extracts words of 5 or more letters from a title for matching."""
        if not title:
            return set()
        # Remove special characters and split into words
        words = re.split(r'[\s._-]+', title.lower())
        # Filter for words with 5 or more alphanumeric characters
        return {word for word in words if len(re.sub(r'[^a-zA-Z0-9]', '', word)) >= 5}

    async def find_matching_batch_key(self, user_id, title_key):
        """Finds a matching batch key based on shared significant words."""
        if user_id not in self.open_batches:
            return None

        new_file_words = self._get_significant_words(title_key)
        if not new_file_words:
            return None

        for existing_key in self.open_batches[user_id]:
            existing_key_words = self._get_significant_words(existing_key)
            if not new_file_words.isdisjoint(existing_key_words): # isdisjoint is False if there's an intersection
                return existing_key
        
        return None

    async def _update_processing_dashboard(self, dashboard_msg, total_files, processed_files_count, start_time):
        """A background task to update the dashboard with progress and ETA."""
        while not processed_files_count.is_set():
            await asyncio.sleep(5)
            processed_count = processed_files_count.get_value()
            if processed_count > 0:
                elapsed_time = time.time() - start_time
                time_per_file = elapsed_time / processed_count
                remaining_files = total_files - processed_count
                eta_seconds = int(time_per_file * remaining_files)
                eta_str = f"{eta_seconds // 60}m {eta_seconds % 60}s"
                progress_text = (
                    f"**Processing Batch...**\n\n"
                    f"⚙️ **Status:** Processing file `{processed_count}/{total_files}`\n"
                    f"⏱️ **ETA:** Approximately `{eta_str}`"
                )
                try:
                    await self.send_with_protection(dashboard_msg.edit_text, progress_text)
                except MessageNotModified:
                    pass

    async def _finalize_batch(self, user_id, batch_key):
        if user_id not in self.open_batches or batch_key not in self.open_batches[user_id]:
            return
        
        batch_data = self.open_batches[user_id].pop(batch_key)
        messages = batch_data.get('messages', [])
        dashboard_msg = batch_data.get('dashboard_message')

        # --- Live Dashboard Setup ---
        class Counter: # Helper to pass a mutable int to the background task
            def __init__(self): self.value = 0; self._event = asyncio.Event()
            def set_value(self, val): self.value = val
            def get_value(self): return self.value
            def is_set(self): return self._event.is_set()
            def set(self): self._event.set()

        processed_files_count = Counter()
        updater_task = None
        start_time = time.time()
        
        try:
            if not messages:
                if dashboard_msg: await self.send_with_protection(dashboard_msg.delete)
                return

            if dashboard_msg:
                updater_task = asyncio.create_task(
                    self._update_processing_dashboard(dashboard_msg, len(messages), processed_files_count, start_time)
                )

            user = await get_user(user_id)
            if not user: return
            post_channel_id = await get_post_channel(user_id)

            posts_to_send = await create_post(self, user_id, messages, self.imdb_cache, self.poster_cache, processed_files_count)
            processed_files_count.set() # Signal completion to updater task
            
            if dashboard_msg: await self.send_with_protection(dashboard_msg.edit_text, f"✅ Grouping complete. Now posting to your channel...")
            
            for post in posts_to_send:
                poster, caption, footer = post
                if poster:
                    await self.send_with_protection(self.send_photo, post_channel_id, photo=poster, caption=caption, reply_markup=footer)
                else:
                    await self.send_with_protection(self.send_message, post_channel_id, caption, reply_markup=footer, disable_web_page_preview=True)
                await asyncio.sleep(1.5)

            # --- Final Notification ---
            if dashboard_msg: await self.send_with_protection(dashboard_msg.delete)
            await self.send_message(user_id, f"✅ **Batch Complete!**\n\nYour batch for **'{batch_key}'** with `{len(messages)}` files has been posted.")

        except Exception as e:
            logger.exception(f"CRITICAL Error finalizing batch {batch_key} for user {user_id}: {e}")
            if dashboard_msg: await self.send_with_protection(dashboard_msg.edit_text, f"❌ **Error!**\n\nAn unexpected error occurred while processing batch `{batch_key}`.")
        finally:
            processed_files_count.set()
            if updater_task and not updater_task.done():
                updater_task.cancel()
            if user_id in self.open_batches and not self.open_batches[user_id]:
                del self.open_batches[user_id]
            self.last_dashboard_edit.pop((user_id, batch_key), None)

    async def file_processor_worker(self):
        logger.info("File Processor Worker started.")
        DASHBOARD_EDIT_THROTTLE_SECONDS = 4
        BATCH_INACTIVITY_SECONDS = 15
        while True:
            try:
                message, user_id = await self.file_queue.get()
                
                self.stream_channel_id = await get_index_db_channel(user_id) or self.owner_db_channel
                if not self.stream_channel_id: continue

                copied_message = await self.send_with_protection(message.copy, self.owner_db_channel)
                if not copied_message: continue

                await save_file_data(user_id, message, copied_message, copied_message)
                
                filename = getattr(copied_message, copied_message.media.value).file_name
                title_key = await get_title_key(filename, self.imdb_cache)
                if not title_key: continue

                self.open_batches.setdefault(user_id, {})
                loop = asyncio.get_event_loop()

                matched_key = await self.find_matching_batch_key(user_id, title_key)
                batch_key_to_use = matched_key if matched_key else title_key

                if batch_key_to_use in self.open_batches[user_id]:
                    batch_data = self.open_batches[user_id][batch_key_to_use]
                    if batch_data.get('timer'): batch_data['timer'].cancel()
                    
                    batch_data['messages'].append(copied_message)
                    
                    dashboard_msg = batch_data.get('dashboard_message')
                    edit_key = (user_id, batch_key_to_use)
                    last_edit_time = self.last_dashboard_edit.get(edit_key, 0)
                    
                    if (time.time() - last_edit_time) > DASHBOARD_EDIT_THROTTLE_SECONDS:
                        if dashboard_msg:
                            try:
                               await self.send_with_protection(
                                   dashboard_msg.edit_text,
                                   f"**Batch Update**\n\n"
                                   f"🎬 **Batch:** `{batch_key_to_use}`\n"
                                   f"📊 **Files Collected:** `{len(batch_data['messages'])}`\n"
                                   f"⏳ **Status:** Added new file. Waiting {BATCH_INACTIVITY_SECONDS} more seconds..."
                               )
                               self.last_dashboard_edit[edit_key] = time.time()
                            except MessageNotModified: pass
                    
                    batch_data['timer'] = loop.call_later(BATCH_INACTIVITY_SECONDS, lambda u=user_id, k=batch_key_to_use: asyncio.create_task(self._finalize_batch(u, k)))
                else:
                    dashboard_msg = await self.send_with_protection(
                        self.send_message,
                        chat_id=user_id,
                        text=f"**New Batch Detected**\n\n"
                             f"🎬 **Batch:** `{batch_key_to_use}`\n"
                             f"📊 **Files Collected:** `1`\n"
                             f"⏳ **Status:** Collecting files. Waiting for {BATCH_INACTIVITY_SECONDS} seconds..."
                    )
                    
                    self.open_batches[user_id][batch_key_to_use] = {
                        'messages': [copied_message],
                        'timer': loop.call_later(BATCH_INACTIVITY_SECONDS, lambda u=user_id, k=batch_key_to_use: asyncio.create_task(self._finalize_batch(u, k))),
                        'dashboard_message': dashboard_msg
                    }
                    self.last_dashboard_edit[(user_id, batch_key_to_use)] = time.time()

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

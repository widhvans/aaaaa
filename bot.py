# bot.py

import logging
import asyncio
import time
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, PeerIdInvalid, MessageNotModified
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyromod import Client
from aiohttp import web
from config import Config
from database.db import (
    get_user, save_file_data, get_post_channel, get_index_db_channel
)
from utils.helpers import create_post, clean_and_parse_filename, notify_and_remove_invalid_channel
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

        self.processing_users = set()
        self.waiting_files = {}
        self.last_dashboard_edit_time = {}
        self.imdb_cache = {}
        self.is_in_flood_wait = False # Global flag for smart timer
        
        self.vps_ip = Config.VPS_IP
        self.vps_port = Config.VPS_PORT

    async def send_with_protection(self, coro, *args, **kwargs):
        while True:
            try:
                return await coro(*args, **kwargs)
            except FloodWait as e:
                logger.warning(f"FloodWait of {e.value}s detected. Sleeping...")
                self.is_in_flood_wait = True
                await asyncio.sleep(e.value + 5)
                self.is_in_flood_wait = False
                logger.info("FloodWait sleep finished. Resuming operations.")
            except MessageNotModified:
                logger.warning("Attempted to edit message with the same content.")
                return None # Continue without error
            except Exception as e:
                logger.error(f"An error occurred in send_with_protection: {e}", exc_info=True)
                return None

    async def _generate_dashboard_text(self, collection_data, status_text):
        """Builds the full dashboard text string."""
        header = collection_data.get('header', '')
        processed_count = len(collection_data.get('messages', []))
        skipped_files = collection_data.get('skipped_files', [])
        
        text = f"{header}"
        text += f"**File Batch Update**\n\n"
        text += f"📊 **Files Collected:** `{processed_count}`\n"
        text += status_text
        
        if skipped_files:
            text += f"\n\n"
            text += f"🚫 **Skipped Files:** `{len(skipped_files)}`\n"
            for i, filename in enumerate(skipped_files):
                if i >= 5: # Limit displayed skipped files to 5
                    text += f"- `...and {len(skipped_files) - 5} more.`"
                    break
                text += f"- `{filename}`: Cannot be a movie/series.\n"
        
        return text

    async def _start_new_collection(self, user_id, initial_messages):
        """Helper function to create a new collection window and dashboard."""
        loop = asyncio.get_event_loop()
        
        # Get channel names for the dashboard header
        post_ch_id = await get_post_channel(user_id)
        db_ch_id = await get_index_db_channel(user_id) or self.owner_db_channel
        
        try: post_ch_title = (await self.get_chat(post_ch_id)).title if post_ch_id else "Not Set"
        except: post_ch_title = "Invalid Channel"
        
        try: db_ch_title = (await self.get_chat(db_ch_id)).title if db_ch_id else "Not Set"
        except: db_ch_title = "Invalid Channel"
            
        header_text = (f"**Post Channel:** `{post_ch_title}`\n"
                       f"**DB Channel:** `{db_ch_title}`\n"
                       f"--------------------------------\n")
        
        collection_data = {
            'messages': initial_messages,
            'skipped_files': [],
            'timer': loop.call_later(10, lambda u=user_id: asyncio.create_task(self._finalize_collection(u))),
            'dashboard_message': None,
            'header': header_text
        }
        
        initial_status = "⏳ **Status:** Collecting files..."
        initial_text = await self._generate_dashboard_text(collection_data, initial_status)

        dashboard_msg = await self.send_with_protection(self.send_message, chat_id=user_id, text=initial_text)
        
        collection_data['dashboard_message'] = dashboard_msg
        self.open_batches[user_id] = collection_data
        self.last_dashboard_edit_time[user_id] = time.time()


    async def _finalize_collection(self, user_id):
        if self.is_in_flood_wait:
            logger.warning(f"Finalize_collection for user {user_id} triggered during a flood wait. Rescheduling silently.")
            loop = asyncio.get_event_loop()
            collection_data = self.open_batches.get(user_id, {})
            if collection_data:
                collection_data['timer'] = loop.call_later(10, lambda u=user_id: asyncio.create_task(self._finalize_collection(u)))
            return

        self.processing_users.add(user_id)
        self.imdb_cache.clear()
        dashboard_msg = None
        try:
            if user_id not in self.open_batches or not self.open_batches[user_id]:
                return

            collection_data = self.open_batches.pop(user_id)
            messages = collection_data.get('messages', [])
            dashboard_msg = collection_data.get('dashboard_message')

            if not messages:
                if dashboard_msg: await self.send_with_protection(dashboard_msg.delete)
                return

            if dashboard_msg:
                status = f"⏳ **Status:** Analyzing and grouping `{len(messages)}` files..."
                await self.send_with_protection(dashboard_msg.edit_text, await self._generate_dashboard_text(collection_data, status))

            tasks = [clean_and_parse_filename(getattr(msg, msg.media.value).file_name, self.imdb_cache) for msg in messages]
            file_infos = await asyncio.gather(*tasks)

            logical_batches = {}
            for i, info in enumerate(file_infos):
                if not info or not info.get("batch_title"): continue
                current_msg = messages[i]
                current_title = info["batch_title"]
                best_match_key = None
                highest_similarity = 0
                SIMILARITY_THRESHOLD = 85
                for existing_key in logical_batches.keys():
                    similarity = fuzz.token_set_ratio(current_title, existing_key)
                    if similarity > highest_similarity:
                        highest_similarity = similarity
                        best_match_key = existing_key
                if highest_similarity > SIMILARITY_THRESHOLD: logical_batches[best_match_key].append(current_msg)
                else: logical_batches[current_title] = [current_msg]

            total_batches = len(logical_batches)
            if dashboard_msg:
                status = f"⏳ **Status:** Found `{total_batches}` batches. Processing..."
                await self.send_with_protection(dashboard_msg.edit_text, await self._generate_dashboard_text(collection_data, status))
            
            user = await get_user(user_id)
            if not user: return
            post_channel_id = await get_post_channel(user_id)
            if not post_channel_id or not await notify_and_remove_invalid_channel(self, user_id, post_channel_id, "Post"):
                if dashboard_msg: await self.send_with_protection(dashboard_msg.edit_text, "❌ **Error!** Could not access a valid Post Channel.")
                return

            for i, (batch_title, batch_messages) in enumerate(logical_batches.items()):
                if dashboard_msg:
                    status = f"⏳ **Status:** Posting batch {i + 1}/{total_batches} ('{batch_title}')..."
                    await self.send_with_protection(dashboard_msg.edit_text, await self._generate_dashboard_text(collection_data, status))
                
                posts_to_send = await create_post(self, user_id, batch_messages, self.imdb_cache)
                for post in posts_to_send:
                    poster, caption, footer = post
                    if poster: await self.send_with_protection(self.send_photo, post_channel_id, photo=poster, caption=caption, reply_markup=footer)
                    else: await self.send_with_protection(self.send_message, post_channel_id, caption, reply_markup=footer, disable_web_page_preview=True)
                    await asyncio.sleep(2)

            if dashboard_msg: await self.send_with_protection(dashboard_msg.delete)
            await self.send_message(user_id, "✅ **Batch processing complete!** All files have been successfully posted.")

        except Exception as e:
            logger.exception(f"CRITICAL Error finalizing collection for user {user_id}: {e}")
            if dashboard_msg: await self.send_with_protection(dashboard_msg.edit_text, f"❌ **Error!** An unexpected error occurred.")
        finally:
            self.processing_users.discard(user_id)
            self.last_dashboard_edit_time.pop(user_id, None)
            if user_id in self.waiting_files and self.waiting_files[user_id]:
                await self._start_new_collection(user_id, self.waiting_files.pop(user_id))

    async def file_processor_worker(self):
        logger.info("File Processor Worker started.")
        DASHBOARD_EDIT_THROTTLE_SECONDS = 5
        while True:
            try:
                message, user_id = await self.file_queue.get()
                
                # --- NEW: Duration Check ---
                media = getattr(message, message.media.value, None)
                if media and hasattr(media, 'duration') and media.duration and media.duration < 1200: # 20 minutes
                    logger.info(f"Skipping file '{media.file_name}' for user {user_id} due to short duration.")
                    if user_id in self.open_batches:
                        self.open_batches[user_id].setdefault('skipped_files', []).append(media.file_name)
                    # Don't create a new batch for a single skipped file, just ignore it.
                    self.file_queue.task_done()
                    continue

                self.stream_channel_id = await get_index_db_channel(user_id) or self.owner_db_channel
                if not self.stream_channel_id: continue

                copied_message = await self.send_with_protection(message.copy, self.owner_db_channel)
                if not copied_message: continue

                await save_file_data(user_id, message, copied_message, copied_message)
                
                if user_id in self.processing_users:
                    self.waiting_files.setdefault(user_id, []).append(copied_message)
                    continue
                
                loop = asyncio.get_event_loop()

                if user_id not in self.open_batches:
                    await self._start_new_collection(user_id, [copied_message])
                else:
                    collection_data = self.open_batches[user_id]
                    if collection_data.get('timer'): collection_data['timer'].cancel()
                    collection_data['messages'].append(copied_message)
                    
                    last_edit = self.last_dashboard_edit_time.get(user_id, 0)
                    if (time.time() - last_edit) > DASHBOARD_EDIT_THROTTLE_SECONDS:
                        dashboard_msg = collection_data.get('dashboard_message')
                        if dashboard_msg:
                           status_text = "⏳ **Status:** Collecting more files..."
                           new_text = await self._generate_dashboard_text(collection_data, status_text)
                           await self.send_with_protection(dashboard_msg.edit_text, new_text)
                           self.last_dashboard_edit_time[user_id] = time.time()
                    
                    collection_data['timer'] = loop.call_later(10, lambda u=user_id: asyncio.create_task(self._finalize_collection(u)))

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

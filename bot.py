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

        # For the new time-based collection model
        self.processing_users = set()
        self.waiting_files = {}
        
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

    async def _start_new_collection(self, user_id, initial_messages):
        """Helper function to create a new collection window and dashboard."""
        loop = asyncio.get_event_loop()
        dashboard_msg = await self.send_with_protection(
            self.send_message,
            chat_id=user_id,
            text=f"**File(s) Detected**\n\n"
                 f"📊 **Files Collected:** `{len(initial_messages)}`\n"
                 f"⏳ **Status:** Started a 30-second window to collect more files..."
        )
        self.open_batches[user_id] = {
            'messages': initial_messages,
            'timer': loop.call_later(30, lambda u=user_id: asyncio.create_task(self._finalize_collection(u))),
            'dashboard_message': dashboard_msg
        }

    async def _finalize_collection(self, user_id):
        self.processing_users.add(user_id)
        try:
            if user_id not in self.open_batches or not self.open_batches[user_id]:
                return

            collection_data = self.open_batches.pop(user_id)
            messages = collection_data.get('messages', [])
            dashboard_msg = collection_data.get('dashboard_message')

            if not messages:
                if dashboard_msg: await self.send_with_protection(dashboard_msg.delete)
                return

            logical_batches = {}
            if dashboard_msg:
                await self.send_with_protection(dashboard_msg.edit_text, f"⏳ Grouping `{len(messages)}` collected files into logical batches...")

            for msg in messages:
                filename = getattr(msg, msg.media.value).file_name
                batch_title = await get_title_key(filename)
                if not batch_title:
                    batch_title = "Uncategorized"
                
                if batch_title not in logical_batches:
                    logical_batches[batch_title] = []
                logical_batches[batch_title].append(msg)

            total_batches = len(logical_batches)
            if dashboard_msg:
                await self.send_with_protection(dashboard_msg.edit_text, f"✅ Found `{total_batches}` unique batch(es). Starting to process and post...")
            await asyncio.sleep(2)

            user = await get_user(user_id)
            if not user: return
            
            post_channel_id = await get_post_channel(user_id)
            if not post_channel_id:
                if dashboard_msg: await self.send_with_protection(dashboard_msg.edit_text, "❌ **Error!**\n\nNo Post Channel is configured. Cannot create post.")
                return

            if not await notify_and_remove_invalid_channel(self, user_id, post_channel_id, "Post"):
                 if dashboard_msg: await self.send_with_protection(dashboard_msg.edit_text, "❌ **Error!**\n\nCould not access the configured Post Channel.")
                 return

            processed_count = 0
            for batch_title, batch_messages in logical_batches.items():
                processed_count += 1
                if dashboard_msg:
                    await self.send_with_protection(dashboard_msg.edit_text,
                                                  f"**Processing Batch {processed_count}/{total_batches}**\n\n"
                                                  f"🎬 **Batch:** `{batch_title}`\n"
                                                  f"⏳ **Status:** Creating post for `{len(batch_messages)}` file(s)...")

                posts_to_send = await create_post(self, user_id, batch_messages)
                
                for post in posts_to_send:
                    poster, caption, footer = post
                    if poster:
                        await self.send_with_protection(self.send_photo, post_channel_id, photo=poster, caption=caption, reply_markup=footer)
                    else:
                        await self.send_with_protection(self.send_message, post_channel_id, caption, reply_markup=footer, disable_web_page_preview=True)
                    await asyncio.sleep(2)

            if dashboard_msg:
                await self.send_with_protection(dashboard_msg.edit_text, f"✅ **All Done!**\n\nSuccessfully processed and posted all `{total_batches}` batches.")
                await asyncio.sleep(10)
                await self.send_with_protection(dashboard_msg.delete)

        except Exception as e:
            logger.exception(f"CRITICAL Error finalizing collection for user {user_id}: {e}")
            if dashboard_msg: await self.send_with_protection(dashboard_msg.edit_text, f"❌ **Error!**\n\nAn unexpected error occurred while processing your files.")
        finally:
            self.processing_users.discard(user_id)
            if user_id in self.waiting_files and self.waiting_files[user_id]:
                logger.info(f"Starting new collection for user {user_id} with {len(self.waiting_files[user_id])} waiting files.")
                waiting_messages = self.waiting_files.pop(user_id)
                await self._start_new_collection(user_id, waiting_messages)

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
                
                if user_id in self.processing_users:
                    self.waiting_files.setdefault(user_id, []).append(copied_message)
                    logger.info(f"User {user_id} is processing. Added file to waiting list (total waiting: {len(self.waiting_files[user_id])}).")
                    continue
                
                loop = asyncio.get_event_loop()

                if user_id not in self.open_batches:
                    await self._start_new_collection(user_id, [copied_message])
                else:
                    collection_data = self.open_batches[user_id]
                    if collection_data.get('timer'): collection_data['timer'].cancel()
                    
                    collection_data['messages'].append(copied_message)
                    
                    dashboard_msg = collection_data.get('dashboard_message')
                    if dashboard_msg:
                        try:
                           await self.send_with_protection(
                               dashboard_msg.edit_text,
                               f"**File Detected**\n\n"
                               f"📊 **Files Collected:** `{len(collection_data['messages'])}`\n"
                               f"⏳ **Status:** Resetting 30-second window to collect more files..."
                           )
                        except MessageNotModified: pass
                    
                    collection_data['timer'] = loop.call_later(30, lambda u=user_id: asyncio.create_task(self._finalize_collection(u)))

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

# bot.py

import logging
import asyncio
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyromod import Client
from aiohttp import web
from config import Config
from database.db import (
    get_user, save_file_data, get_owner_db_channel, get_stream_channel
)
from utils.helpers import create_post, get_title_key, notify_and_remove_invalid_channel, calculate_title_similarity

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()])
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyromod").setLevel(logging.WARNING)
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
        
        self.owner_db_channel_id = None
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

    async def _finalize_batch(self, user_id, batch_key):
        try:
            if user_id not in self.open_batches or batch_key not in self.open_batches[user_id]:
                return
            
            batch_data = self.open_batches[user_id].pop(batch_key)
            messages = batch_data.get('messages', [])
            
            if not messages: return

            logger.info(f"Finalizing batch '{batch_key}' for user {user_id} with {len(messages)} files.")

            user = await get_user(user_id)
            if not user or not user.get('post_channels'): return

            valid_post_channels = [ch_id for ch_id in user.get('post_channels', []) if await notify_and_remove_invalid_channel(self, user_id, ch_id, "Post")]
            
            if not valid_post_channels: return

            posts_to_send = await create_post(self, user_id, messages)
            
            for channel_id in valid_post_channels:
                for post in posts_to_send:
                    poster, caption, footer = post
                    if poster:
                        await self.send_with_protection(self.send_photo, channel_id, photo=poster, caption=caption, reply_markup=footer)
                    else:
                        await self.send_with_protection(self.send_message, channel_id, caption, reply_markup=footer, disable_web_page_preview=True)
                    await asyncio.sleep(2) 

        except Exception as e:
            logger.exception(f"CRITICAL Error finalizing batch {batch_key} for user {user_id}: {e}")
        finally:
            if user_id in self.open_batches and not self.open_batches[user_id]:
                del self.open_batches[user_id]

    async def file_processor_worker(self):
        logger.info("File Processor Worker started.")
        while True:
            try:
                message, user_id = await self.file_queue.get()
                
                if not self.owner_db_channel_id: self.owner_db_channel_id = await get_owner_db_channel()
                if not self.stream_channel_id: self.stream_channel_id = await get_stream_channel()
                if not self.owner_db_channel_id:
                    logger.error("Owner DB Channel is not set. File processing skipped.")
                    self.file_queue.task_done()
                    continue

                copied_message = await self.send_with_protection(message.copy, self.owner_db_channel_id)
                if not copied_message:
                    logger.error(f"Failed to copy message to owner_db_channel for user {user_id}. Skipping file.")
                    self.file_queue.task_done()
                    continue

                stream_message = await self.send_with_protection(message.copy, self.stream_channel_id) if self.stream_channel_id and self.stream_channel_id != self.owner_db_channel_id else copied_message
                if not stream_message:
                    stream_message = copied_message

                await save_file_data(user_id, message, copied_message, stream_message)
                
                filename = getattr(copied_message, copied_message.media.value).file_name
                # --- THIS IS THE FIX ---
                # The get_title_key function (which calls clean_filename) now correctly returns one value.
                title_key = get_title_key(filename)
                
                if not title_key:
                    logger.warning(f"Could not generate a title key for filename: {filename}")
                    self.file_queue.task_done()
                    continue

                self.open_batches.setdefault(user_id, {})
                loop = asyncio.get_event_loop()
                
                found_batch = False
                for batch_key, batch_data in self.open_batches[user_id].items():
                    similarity = calculate_title_similarity(title_key, batch_key)
                    
                    # Using a high similarity threshold because titles are now very clean
                    if similarity > 95:
                        logger.info(f"File '{filename}' matches existing batch '{batch_key}' with {similarity}% similarity.")
                        batch_data['messages'].append(copied_message)
                        
                        if batch_data.get('timer'): batch_data['timer'].cancel()
                        batch_data['timer'] = loop.call_later(45, lambda bk=batch_key: asyncio.create_task(self._finalize_batch(user_id, bk)))
                        found_batch = True
                        break
                
                if not found_batch:
                    logger.info(f"No similar batch found. Creating new batch with key: '{title_key}'")
                    self.open_batches[user_id][title_key] = {
                        'messages': [copied_message],
                        'timer': loop.call_later(45, lambda key=title_key: asyncio.create_task(self._finalize_batch(user_id, key)))
                    }

            except Exception as e:
                logger.exception(f"CRITICAL Error in file_processor_worker's main loop: {e}")
            finally:
                await asyncio.sleep(0.5)
                self.file_queue.task_done()

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
        
        self.owner_db_channel_id = await get_owner_db_channel()
        self.stream_channel_id = await get_stream_channel()
        
        if self.owner_db_channel_id: logger.info(f"Loaded Owner DB ID [{self.owner_db_channel_id}]")
        else: logger.warning("Owner DB ID not set. Use 'Set Owner DB' as admin.")
        
        if self.stream_channel_id: logger.info(f"Loaded Stream Channel ID [{self.stream_channel_id}]")
        else: logger.info("Stream Channel not set. Will use Owner DB for streaming if needed.")
            
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

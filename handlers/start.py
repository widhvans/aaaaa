import logging
import re
from pyrogram import Client, filters, enums
from pyrogram.errors import UserNotParticipant, MessageNotModified, ChatAdminRequired, ChannelInvalid, PeerIdInvalid, ChannelPrivate
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from config import Config
from database.db import add_user, get_file_by_unique_id, get_user, is_user_verified, claim_verification_for_file
from utils.helpers import get_main_menu
from features.shortener import get_shortlink

logger = logging.getLogger(__name__)


@Client.on_message(filters.private & ~filters.command("start") & (filters.document | filters.video | filters.audio))
async def handle_private_file(client, message):
    if not client.owner_db_channel:
        return await message.reply_text("The bot is not yet configured by the admin. Please try again later.")
    
    processing_msg = await message.reply_text("⏳ Processing your file...", quote=True)
    try:
        copied_message = await message.copy(client.owner_db_channel)
        download_link = f"http://{client.vps_ip}:{client.vps_port}/download/{copied_message.id}"
        
        buttons = [[InlineKeyboardButton("📥 Fast Download", url=download_link)]]
        keyboard = InlineKeyboardMarkup(buttons)
        
        await client.send_cached_media(
            chat_id=message.chat.id,
            file_id=message.media.file_id,
            caption=f"`{message.media.file_name}`",
            reply_markup=keyboard,
            quote=True
        )
        await processing_msg.delete()
    except Exception as e:
        logger.exception("Error in handle_private_file")
        await processing_msg.edit_text(f"An error occurred: {e}")

async def send_file(client, requester_id, owner_id, file_unique_id):
    """Sends the correct, user-owned file to the person who requested it."""
    try:
        file_data = await get_file_by_unique_id(owner_id, file_unique_id)
        if not file_data:
            return await client.send_message(requester_id, "Sorry, this file is no longer available or the link is invalid.")
        
        owner_settings = await get_user(file_data['owner_id'])
        if not owner_settings:
             return await client.send_message(requester_id, "A configuration error occurred on the bot.")

        download_link = f"http://{client.vps_ip}:{client.vps_port}/download/{file_data['stream_id']}"
        
        buttons = [[InlineKeyboardButton("📥 Fast Download", url=download_link)]]
        keyboard = InlineKeyboardMarkup(buttons)
        
        file_name_raw = file_data.get('file_name', 'N/A')
        file_name_cleaned = re.sub(r'(@|\[@)\S+', '', file_name_raw).strip()
        
        filename_part = ""
        filename_url = owner_settings.get("filename_url") if owner_settings else None

        if filename_url:
            filename_part = f"[{file_name_cleaned}]({filename_url})"
        else:
            filename_part = f"`{file_name_cleaned}`"

        caption = f"✅ **Here is your file!**\n\n{filename_part}"

        await client.copy_message(
            chat_id=requester_id,
            from_chat_id=Config.OWNER_DB_CHANNEL,
            message_id=file_data['file_id'],
            caption=caption,
            reply_markup=keyboard,
            parse_mode=enums.ParseMode.MARKDOWN
        )
    except Exception:
        logger.exception("Error in send_file function")
        await client.send_message(requester_id, "Something went wrong while sending the file.")


@Client.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if message.from_user.is_bot: return
    requester_id = message.from_user.id
    await add_user(requester_id)
    
    if len(message.command) > 1:
        payload = message.command[1]
        try:
            if payload.startswith("finalget_"):
                _, owner_id_str, file_unique_id = payload.split("_", 2)
                owner_id = int(owner_id_str)
                
                file_data = await get_file_by_unique_id(owner_id, file_unique_id)
                if file_data:
                    owner_settings = await get_user(owner_id)
                    
                    if owner_settings and owner_settings.get('shortener_mode') == '12_hour':
                        was_already_verified = await is_user_verified(requester_id, owner_id)
                        claim_successful = await claim_verification_for_file(owner_id, file_unique_id, requester_id)
                        
                        if claim_successful and not was_already_verified:
                            await client.send_message(requester_id, "✅ **Verification Successful!**\n\nYou can now get direct links from this user's channels for the next 12 hours.")
                
                await send_file(client, requester_id, owner_id, file_unique_id)

            elif payload.startswith("ownerget_"):
                _, owner_id_str, file_unique_id = payload.split("_", 2)
                owner_id = int(owner_id_str)
                if requester_id == owner_id:
                    await send_file(client, requester_id, owner_id, file_unique_id)
                else:
                    await message.reply_text("This is a special link for the file owner only.")

            elif payload.startswith("get_"):
                await handle_public_file_request(client, message, requester_id, payload)

        except Exception:
            logger.exception("Error processing deep link in /start")
            await message.reply_text("Something went wrong or the link is invalid.")
    else:
        text = (
            f"Hello {message.from_user.mention}! 👋\n\n"
            "Welcome to your advanced **File Management Assistant**.\n\n"
            "I can help you store, manage, and share your files effortlessly. "
            "Whether you're looking for a quick streaming link or want to automate your channel posts, I have the tools for you.\n\n"
            "**Here's what I can do:**\n"
            "🗂️ **File Storage & Management**\n"
            "› Save unlimited files in your private channels.\n"
            "› Get fast direct download & streaming links.\n\n"
            "📢 **Powerful Auto-Posting**\n"
            "› Auto-post from storage channels to public channels.\n"
            "› Full customization of captions, posters, and buttons.\n\n"
            "Click **Let's Go 🚀** to open your settings menu and begin!"
        )
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Let's Go 🚀", callback_data=f"go_back_{requester_id}")],
            [InlineKeyboardButton("Tutorial 🎬", url=Config.TUTORIAL_URL)]
        ])
        
        await message.reply_text(text, reply_markup=keyboard)


async def handle_public_file_request(client, message, requester_id, payload):
    try:
        _, owner_id_str, file_unique_id = payload.split("_", 2)
        owner_id = int(owner_id_str)
    except (ValueError, IndexError):
        return await message.reply_text("The link is invalid or corrupted.")

    file_data = await get_file_by_unique_id(owner_id, file_unique_id)
    if not file_data: return await message.reply_text("File not found or link has expired.")
    
    owner_settings = await get_user(owner_id)
    
    fsub_channel = owner_settings.get('fsub_channel')
    if fsub_channel:
        try:
            await client.get_chat_member(chat_id=fsub_channel, user_id="me")
            try:
                await client.get_chat_member(chat_id=fsub_channel, user_id=requester_id)
            except UserNotParticipant:
                try:
                    invite_link = await client.export_chat_invite_link(fsub_channel)
                except Exception:
                    invite_link = None
                buttons = [
                    [InlineKeyboardButton("📢 Join Channel", url=invite_link)],
                    [InlineKeyboardButton("🔄 Retry", callback_data=f"retry_{payload}")]
                ]
                return await message.reply_text("You must join the channel to continue.", reply_markup=InlineKeyboardMarkup(buttons))
        except (ChatAdminRequired, ChannelInvalid, PeerIdInvalid, ChannelPrivate) as e:
            logger.error(f"FSub channel error for owner {owner_id} (Channel: {fsub_channel}): {e}")
            pass 
    
    try:
        with open(Config.BOT_USERNAME_FILE, 'r') as f:
            bot_username = f.read().strip().replace("@", "")
    except FileNotFoundError:
        logger.error(f"FATAL: Bot username file not found at {Config.BOT_USERNAME_FILE}. Cannot create fallback link.")
        return await message.reply_text("Bot is not configured correctly. Please contact the admin.")

    if not bot_username:
        logger.error("FATAL: Bot username is empty in the file. Cannot create fallback link.")
        return await message.reply_text("Bot is not configured correctly. Please contact the admin.")
        
    composite_id = f"{owner_id}_{file_unique_id}"
    final_delivery_link = f"https://t.me/{bot_username}?start=finalget_{composite_id}"
    
    shortener_enabled = owner_settings.get('shortener_enabled', True)
    shortener_mode = owner_settings.get('shortener_mode', 'each_time')
    text = ""
    buttons = []

    if not shortener_enabled:
        text = "✅ **Your link is ready!**\n\nClick the button below to get your file directly."
        buttons.append([InlineKeyboardButton("➡️ Get Your File ⬅️", url=final_delivery_link)])
    else:
        shortened_link = await get_shortlink(final_delivery_link, owner_id)
        if shortener_mode == 'each_time':
            text = "**Your file is almost ready!**\n\n1. Click the button below.\n2. You will be redirected back, and I will send you the file."
            buttons.append([InlineKeyboardButton("➡️ Click Here to Get Your File ⬅️", url=shortened_link)])
        elif shortener_mode == '12_hour':
            if await is_user_verified(requester_id, owner_id):
                text = "✅ **You are verified!**\n\nYour 12-hour verification is active. Click below to get your file directly."
                buttons.append([InlineKeyboardButton("➡️ Get Your File Directly ⬅️", url=final_delivery_link)])
            else:
                text = "**One-Time Verification Required**\n\nTo get direct access for 12 hours, please complete this one-time verification step."
                buttons.append([InlineKeyboardButton("➡️ Click to Verify (12 Hours) ⬅️", url=shortened_link)])

    if owner_settings.get("how_to_download_link"):
        buttons.append([InlineKeyboardButton("❓ How to Download", url=owner_settings["how_to_download_link"])])
    
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)


@Client.on_callback_query(filters.regex(r"^retry_"))
async def retry_handler(client, query):
    await query.message.delete()
    await handle_public_file_request(client, query.message, query.from_user.id, query.data.split("_", 1)[1])

@Client.on_callback_query(filters.regex(r"go_back_"))
async def go_back_callback(client, query):
    user_id = int(query.data.split("_")[-1])
    if query.from_user.id != user_id:
        return await query.answer("This is not for you!", show_alert=True)
    try:
        menu_text, menu_markup = await get_main_menu(user_id)
        await query.message.edit_text(text=menu_text, reply_markup=menu_markup, parse_mode=enums.ParseMode.MARKDOWN, disable_web_page_preview=True)
    except MessageNotModified:
        await query.answer()
    except Exception as e:
        logger.error(f"Error in go_back_callback: {e}")
        await query.answer("An error occurred while loading the menu.", show_alert=True)

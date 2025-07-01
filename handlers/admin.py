import logging
import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import Config
from database.db import (
    total_users_count, get_all_user_ids, get_storage_owners_count,
    get_storage_owner_ids, get_normal_user_ids, delete_all_files,
    set_owner_db_channel, set_stream_channel  # <-- Import set_stream_channel
)
from features.broadcaster import broadcast_message
from utils.helpers import go_back_button

logger = logging.getLogger(__name__)

# --- NEW: Handler for setting the stream channel ---
@Client.on_callback_query(filters.regex("set_stream_ch") & filters.user(Config.ADMIN_ID))
async def set_stream_channel_handler(client, query):
    try:
        prompt = await query.message.edit_text(
            "🔑 **Set Stream Channel**\n\n"
            "Please forward any message from the private channel where files will be copied for streaming. "
            "I must be an administrator in that channel.",
            reply_markup=go_back_button(query.from_user.id)
        )
        response = await client.listen(chat_id=query.from_user.id, filters=filters.forwarded, timeout=300)

        if response.forward_from_chat:
            channel_id = response.forward_from_chat.id
            await set_stream_channel(channel_id)
            # Update the client instance attribute immediately
            client.stream_channel_id = channel_id
            await response.reply_text(f"✅ **Stream Channel Set!**\n\nChannel: **{response.forward_from_chat.title}**\nID: `{channel_id}`\n\nFiles will now be copied here for streaming.", reply_markup=go_back_button(query.from_user.id))
        else:
            await response.reply_text("This is not a valid forwarded message from a channel.", reply_markup=go_back_button(query.from_user.id))
        
        await prompt.delete()

    except asyncio.TimeoutError:
        await query.message.edit_text("❗️ **Timeout:** Command cancelled.", reply_markup=go_back_button(query.from_user.id))
    except Exception as e:
        logger.exception("Error in set_stream_channel_handler")
        await query.message.edit_text(f"An error occurred: {e}", reply_markup=go_back_button(query.from_user.id))
# --- END NEW ---

@Client.on_callback_query(filters.regex("set_owner_db") & filters.user(Config.ADMIN_ID))
async def set_owner_db_handler(client, query):
    try:
        prompt = await query.message.edit_text(
            "🔑 **Set Owner Database Channel**\n\n"
            "Please forward any message from your main private channel where all files will be stored. "
            "I must be an administrator in that channel.",
            reply_markup=go_back_button(query.from_user.id)
        )
        response = await client.listen(chat_id=query.from_user.id, filters=filters.forwarded, timeout=300)

        if response.forward_from_chat:
            channel_id = response.forward_from_chat.id
            await set_owner_db_channel(channel_id)
            client.owner_db_channel_id = channel_id
            await response.reply_text(f"✅ **Owner Database Set!**\n\nChannel: **{response.forward_from_chat.title}**\nID: `{channel_id}`\n\nAll files will now be copied here.", reply_markup=go_back_button(query.from_user.id))
        else:
            await response.reply_text("This is not a valid forwarded message from a channel.", reply_markup=go_back_button(query.from_user.id))
        
        await prompt.delete()

    except asyncio.TimeoutError:
        await query.message.edit_text("❗️ **Timeout:** Command cancelled.", reply_markup=go_back_button(query.from_user.id))
    except Exception as e:
        logger.exception("Error in set_owner_db_handler")
        await query.message.edit_text(f"An error occurred: {e}", reply_markup=go_back_button(query.from_user.id))

# ... (Rest of the admin.py file remains unchanged) ...
@Client.on_message(filters.command("stats") & filters.user(Config.ADMIN_ID))
async def stats_handler(_, message):
    try:
        total = await total_users_count()
        storage_owners = await get_storage_owners_count()
        text = (
            "📊 **Bot Statistics**\n\n"
            f"**Total Users:** `{total}`\n"
            f"**Storage Owners:** `{storage_owners}`\n"
            f"_(Storage Owners are users who have set at least one channel)_"
        )
        await message.reply_text(text)
    except Exception:
        logger.exception("Error in /stats handler")
        await message.reply_text("An error occurred while fetching stats.")
@Client.on_message(filters.command("broadcast") & filters.user(Config.ADMIN_ID))
async def broadcast_prompt_handler(client, message):
    if not message.reply_to_message:
        return await message.reply_text("Please reply to a message to start a broadcast.")
    message_to_broadcast_id = message.reply_to_message.id
    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton("To All Users 📣", callback_data=f"bcast_all_{message_to_broadcast_id}")],
        [InlineKeyboardButton("To Storage Owners Only 🗃️", callback_data=f"bcast_storage_{message_to_broadcast_id}")],
        [InlineKeyboardButton("To Normal Users Only 👤", callback_data=f"bcast_normal_{message_to_broadcast_id}")]
    ])
    await message.reply_text("Who should receive this broadcast?", reply_markup=buttons)
@Client.on_callback_query(filters.regex(r"bcast_(all|storage|normal)_(\d+)") & filters.user(Config.ADMIN_ID))
async def broadcast_callback_handler(client, query):
    try:
        broadcast_type, message_id_str = query.data.split("_")[1:]
        message_id = int(message_id_str)
        await query.message.edit_text("Fetching user list... Please wait.")
        message_to_broadcast = await client.get_messages(chat_id=query.message.chat.id, message_ids=message_id)
        if not message_to_broadcast:
            return await query.message.edit_text("Error: Could not find the original message.")
        
        if broadcast_type == "all":
            user_ids = await get_all_user_ids()
        elif broadcast_type == "storage":
            user_ids = await get_storage_owner_ids()
        else: # broadcast_type == "normal"
            user_ids = await get_normal_user_ids()
            
        status_msg = await query.message.edit_text(f"Broadcasting to {len(user_ids)} users...")
        success, fail = await broadcast_message(client, user_ids, message_to_broadcast)
        await status_msg.edit_text(f"✅ **Broadcast Complete**\n\nSent to: `{success}` users.\nFailed for: `{fail}` users.")
    except Exception:
        logger.exception("Error in broadcast_callback_handler")
        await query.message.edit_text("An error occurred during broadcast.")
@Client.on_callback_query(filters.regex("reset_db_prompt") & filters.user(Config.ADMIN_ID))
async def reset_db_prompt(client, query):
    await query.message.edit_text(
        "**⚠️ DANGER ZONE ⚠️**\n\n"
        "Are you absolutely sure you want to delete **ALL** saved file links from the database? "
        "This action cannot be undone.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔴 Yes, I am sure. Reset everything.", callback_data="reset_db_confirm")],
            [InlineKeyboardButton("🟢 No, cancel.", callback_data=f"go_back_{query.from_user.id}")]
        ])
    )
@Client.on_callback_query(filters.regex("reset_db_confirm") & filters.user(Config.ADMIN_ID))
async def reset_db_confirm(client, query):
    await query.message.edit_text("⚙️ Resetting files database... Please wait.")
    deleted_count = await delete_all_files()
    await query.message.edit_text(f"✅ **Success!**\n\nDeleted **{deleted_count}** file entries from the database.")

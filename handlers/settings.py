import asyncio
import base64
import logging
import aiohttp
from pyrogram import Client, filters, enums
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from pyrogram.errors import MessageNotModified
from database.db import (
    get_user, update_user, add_to_list, remove_from_list,
    get_user_file_count, add_footer_button, remove_footer_button,
    get_all_user_files, get_paginated_files, search_user_files,
    add_user, set_post_channel, set_index_db_channel, get_index_db_channel
)
from utils.helpers import go_back_button, get_main_menu, create_post, clean_and_parse_filename, calculate_title_similarity, notify_and_remove_invalid_channel
from features.shortener import validate_shortener

logger = logging.getLogger(__name__)
ACTIVE_BACKUP_TASKS = set()


async def safe_edit_message(source, *args, **kwargs):
    try:
        if isinstance(source, CallbackQuery):
            message_to_edit = source.message
        elif isinstance(source, Message):
            message_to_edit = source
        else:
            logger.error(f"safe_edit_message called with invalid type: {type(source)}")
            return
        if 'parse_mode' not in kwargs:
            kwargs['parse_mode'] = ParseMode.MARKDOWN
        await message_to_edit.edit_text(*args, **kwargs)
    except MessageNotModified:
        try:
            if isinstance(source, CallbackQuery):
                await source.answer()
        except Exception:
            pass
    except Exception as e:
        logger.exception("Error while editing message")
        try:
            if isinstance(source, CallbackQuery):
                await source.answer("An error occurred. Please try again.", show_alert=True)
        except Exception:
            pass

async def get_shortener_menu_parts(user_id):
    user = await get_user(user_id)
    if not user: await add_user(user_id); user = await get_user(user_id)
    
    is_enabled = user.get('shortener_enabled', True)
    shortener_url = user.get('shortener_url')
    shortener_api = user.get('shortener_api')
    shortener_mode = user.get('shortener_mode', 'each_time')
    
    text = "**🔗 Shortener Settings**\n\nHere are your current settings:"
    if shortener_url and shortener_api:
        text += f"\n**Domain:** `{shortener_url}`"
        text += f"\n**API Key:** `{shortener_api}`"
    else:
        text += "\n`No shortener domain or API is set.`"
        
    status_text = 'ON 🟢' if is_enabled else 'OFF 🔴'
    mode_text = "Each Time" if shortener_mode == 'each_time' else "12 Hour Verify"
    text += f"\n\n**Status:** {status_text}"
    text += f"\n**Verification Mode:** {mode_text}"
    
    buttons = [
        [InlineKeyboardButton(f"Turn Shortener {'OFF' if is_enabled else 'ON'}", callback_data="toggle_shortener")]
    ]
    if shortener_mode == 'each_time':
        buttons.append([InlineKeyboardButton("🔄 Switch to 12 Hour Verify", callback_data="toggle_smode")])
    else:
        buttons.append([InlineKeyboardButton("🔄 Switch to Each Time", callback_data="toggle_smode")])
    
    buttons.append([InlineKeyboardButton("✏️ Set/Edit API & Domain", callback_data="set_shortener")])
    
    if shortener_url or shortener_api:
        buttons.append([InlineKeyboardButton("🗑️ Reset API & Domain", callback_data="reset_shortener")])
        
    buttons.append([go_back_button(user_id).inline_keyboard[0][0]])
    return text, InlineKeyboardMarkup(buttons)

@Client.on_callback_query(filters.regex("^reset_shortener$"))
async def reset_shortener_handler(client, query):
    user_id = query.from_user.id
    await update_user(user_id, "shortener_url", None)
    await update_user(user_id, "shortener_api", None)
    await query.answer("✅ Shortener settings have been reset.", show_alert=True)
    text, markup = await get_shortener_menu_parts(user_id)
    await safe_edit_message(query, text=text, reply_markup=markup)

async def get_poster_menu_parts(user_id):
    user = await get_user(user_id)
    if not user: await add_user(user_id); user = await get_user(user_id)
    
    is_enabled = user.get('show_poster', True)
    text = f"**🖼️ Poster Settings**\n\nIMDb Poster is currently **{'ON' if is_enabled else 'OFF'}**."
    return text, InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Turn Poster {'OFF 🔴' if is_enabled else 'ON 🟢'}", callback_data="toggle_poster")],
        [go_back_button(user_id).inline_keyboard[0][0]]
    ])

async def get_fsub_menu_parts(client, user_id):
    user = await get_user(user_id)
    if not user: await add_user(user_id); user = await get_user(user_id)
    
    fsub_ch = user.get('fsub_channel')
    text = "**📢 FSub Settings**\n\n"
    if fsub_ch:
        is_valid = await notify_and_remove_invalid_channel(client, user_id, fsub_ch, "FSub")
        if is_valid:
            try:
                chat = await client.get_chat(fsub_ch)
                text += f"Current FSub Channel: **{chat.title}**"
            except:
                text += f"Current FSub Channel ID: `{fsub_ch}`"
    else:
        text += "No FSub channel is set."
    return text, InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Set/Change FSub", callback_data="set_fsub")],
        [go_back_button(user_id).inline_keyboard[0][0]]
    ])

@Client.on_callback_query(filters.regex("^how_to_download_menu$"))
async def how_to_download_menu_handler(client, query):
    user_id = query.from_user.id
    user = await get_user(user_id)
    if not user: await add_user(user_id); user = await get_user(user_id)

    download_link = user.get("how_to_download_link")

    text = "**❓ How to Download Link Settings**\n\n"
    if download_link:
        text += f"Your current 'How to Download' tutorial link is:\n`{download_link}`"
    else:
        text += "You have not set a 'How to Download' link yet."

    buttons = [
        [InlineKeyboardButton("✏️ Set/Change Link", callback_data="set_download")],
        [go_back_button(user_id).inline_keyboard[0][0]]
    ]
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)


# --- Main Callback Handlers ---

@Client.on_callback_query(filters.regex("^manage_channels_menu$"))
async def manage_channels_submenu_handler(client, query):
    text = "🗂️ **Manage Channels**\n\nSelect which type of channel you want to manage."
    buttons = [
        [InlineKeyboardButton("➕ Manage Auto Post", callback_data="manage_post_ch")],
        [InlineKeyboardButton("🗃️ Manage Index DB", callback_data="manage_db_ch")],
        [go_back_button(query.from_user.id).inline_keyboard[0][0]]
    ]
    markup = InlineKeyboardMarkup(buttons)
    await safe_edit_message(query, text=text, reply_markup=markup)

@Client.on_callback_query(filters.regex("^filename_link_menu$"))
async def filename_link_menu_handler(client, query):
    user = await get_user(query.from_user.id)
    if not user: await add_user(query.from_user.id); user = await get_user(query.from_user.id)
    
    filename_url = user.get("filename_url")
    
    text = "**✍️ Filename Link Settings**\n\nThis URL will be used as a hyperlink for the filename when a user receives a file."
    if filename_url:
        text += f"\n\n**Current Link:**\n`{filename_url}`"
    else:
        text += "\n\n`You have not set a filename link yet.`"
    
    buttons = [
        [InlineKeyboardButton("✏️ Set/Change Link", callback_data="set_filename_link")],
        [go_back_button(query.from_user.id).inline_keyboard[0][0]]
    ]
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)


@Client.on_callback_query(filters.regex(r"^(shortener|poster|fsub)_menu$"))
async def settings_submenu_handler(client, query):
    user_id = query.from_user.id
    menu_type = query.data.split("_")[0]
    if menu_type == "shortener": text, markup = await get_shortener_menu_parts(user_id)
    elif menu_type == "poster": text, markup = await get_poster_menu_parts(user_id)
    elif menu_type == "fsub": text, markup = await get_fsub_menu_parts(client, user_id)
    else: return
    await safe_edit_message(query, text=text, reply_markup=markup)

@Client.on_callback_query(filters.regex(r"toggle_shortener$"))
async def toggle_shortener_handler(client, query):
    user_id = query.from_user.id
    user = await get_user(user_id)
    if not user: await add_user(user_id); user = await get_user(user_id)
    
    new_status = not user.get('shortener_enabled', True)
    await update_user(user_id, 'shortener_enabled', new_status)
    await query.answer(f"Shortener is now {'ON' if new_status else 'OFF'}", show_alert=True)
    text, markup = await get_shortener_menu_parts(user_id)
    await safe_edit_message(query, text=text, reply_markup=markup)

@Client.on_callback_query(filters.regex(r"toggle_smode$"))
async def toggle_shortener_mode_handler(client, query):
    user_id = query.from_user.id
    user = await get_user(user_id)
    if not user: await add_user(user_id); user = await get_user(user_id)
    
    current_mode = user.get('shortener_mode', 'each_time')
    if current_mode == 'each_time':
        new_mode = '12_hour'
        mode_text = "12 Hour Verify"
    else:
        new_mode = 'each_time'
        mode_text = "Each Time"
    await update_user(user_id, 'shortener_mode', new_mode)
    await query.answer(f"Shortener mode set to: {mode_text}", show_alert=True)
    text, markup = await get_shortener_menu_parts(user_id)
    await safe_edit_message(query, text=text, reply_markup=markup)

@Client.on_callback_query(filters.regex(r"toggle_poster$"))
async def toggle_poster_handler(client, query):
    user_id = query.from_user.id
    user = await get_user(user_id)
    if not user: await add_user(user_id); user = await get_user(user_id)
    
    new_status = not user.get('show_poster', True)
    await update_user(user_id, 'show_poster', new_status)
    await query.answer(f"Poster is now {'ON' if new_status else 'OFF'}", show_alert=True)
    text, markup = await get_poster_menu_parts(user_id)
    await safe_edit_message(query, text=text, reply_markup=markup)

@Client.on_callback_query(filters.regex(r"my_files_(\d+)"))
async def my_files_handler(client, query):
    try:
        user_id = query.from_user.id
        page = int(query.data.split("_")[-1])
        total_files = await get_user_file_count(user_id)
        files_per_page = 5
        text = f"**📂 Your Saved Files ({total_files} Total)**\n\n"
        if total_files == 0:
            text += "You have not saved any files yet."
        else:
            files_on_page = await get_paginated_files(user_id, page, files_per_page)
            if not files_on_page: text += "No more files found on this page."
            else:
                for file in files_on_page:
                    composite_id = f"{file['owner_id']}_{file['file_unique_id']}"
                    deep_link = f"https://t.me/{client.me.username}?start=ownerget_{composite_id}"
                    text += f"**File:** `{file['file_name']}`\n**Link:** [Click Here to Get File]({deep_link})\n\n"
        buttons, nav_row = [], []
        if page > 1: nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"my_files_{page-1}"))
        if total_files > page * files_per_page: nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"my_files_{page+1}"))
        if nav_row: buttons.append(nav_row)
        buttons.append([InlineKeyboardButton("🔍 Search My Files", callback_data="search_my_files")])
        buttons.append([InlineKeyboardButton("« Go Back", callback_data=f"go_back_{user_id}")])
        await safe_edit_message(query, text=text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)
    except Exception:
        logger.exception("Error in my_files_handler"); await query.answer("Something went wrong.", show_alert=True)

async def _format_and_send_search_results(client, query, user_id, search_query, page):
    files_per_page = 5
    files_list, total_files = await search_user_files(user_id, search_query, page, files_per_page)
    text = f"**🔎 Search Results for `{search_query}` ({total_files} Found)**\n\n"
    if not files_list: text += "No files found for your query."
    else:
        for file in files_list:
            composite_id = f"{file['owner_id']}_{file['file_unique_id']}"
            deep_link = f"https://t.me/{client.me.username}?start=ownerget_{composite_id}"
            text += f"**File:** `{file['file_name']}`\n**Link:** [Click Here to Get File]({deep_link})\n\n"
    buttons, nav_row = [], []
    encoded_query = base64.urlsafe_b64encode(search_query.encode()).decode().strip("=")
    if page > 1: nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"search_results_{page-1}_{encoded_query}"))
    if total_files > page * files_per_page: nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"search_results_{page+1}_{encoded_query}"))
    if nav_row: buttons.append(nav_row)
    buttons.append([InlineKeyboardButton("📚 Back to Full List", callback_data="my_files_1")])
    buttons.append([InlineKeyboardButton("« Go Back to Settings", callback_data=f"go_back_{user_id}")])
    await safe_edit_message(query, text=text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)

@Client.on_callback_query(filters.regex("search_my_files"))
async def search_my_files_prompt(client, query):
    user_id = query.from_user.id
    try:
        prompt = await query.message.edit_text("**🔍 Search Your Files**\n\nPlease send the name of the file you want to find.", reply_markup=go_back_button(user_id))
        response = await client.listen(chat_id=user_id, timeout=300, filters=filters.text)
        await response.delete()
        await _format_and_send_search_results(client, query, user_id, response.text, 1)
    except asyncio.TimeoutError: await safe_edit_message(query, text="❗️ **Timeout:** Search cancelled.", reply_markup=go_back_button(user_id))
    except Exception as e:
        logger.exception("Error in search_my_files_prompt"); await safe_edit_message(query, text=f"An error occurred: {e}", reply_markup=go_back_button(user_id))

@Client.on_callback_query(filters.regex(r"search_results_(\d+)_(.+)"))
async def search_results_paginator(client, query):
    try:
        page = int(query.matches[0].group(1))
        encoded_query = query.matches[0].group(2)
        padding = 4 - (len(encoded_query) % 4)
        search_query = base64.urlsafe_b64decode(encoded_query + "=" * padding).decode()
        await _format_and_send_search_results(client, query, query.from_user.id, search_query, page)
    except Exception:
        logger.exception("Error during search pagination"); await safe_edit_message(query, text="An error occurred during pagination.")

@Client.on_callback_query(filters.regex("backup_links"))
async def backup_links_handler(client, query):
    user = await get_user(query.from_user.id)
    if not user: await add_user(query.from_user.id); user = await get_user(query.from_user.id)
    
    post_channels = user.get('post_channels', [])
    if not post_channels: return await query.answer("You have not set any Post Channels yet.", show_alert=True)
    kb = []
    for ch_id in post_channels:
        try: kb.append([InlineKeyboardButton((await client.get_chat(ch_id)).title, callback_data=f"start_backup_{ch_id}")])
        except: continue
    if not kb: return await query.answer("Could not access any of your Post Channels.", show_alert=True)
    kb.append([InlineKeyboardButton("« Go Back", callback_data=f"go_back_{query.from_user.id}")])
    await safe_edit_message(query, text="**🔄 Smart Backup**\n\nSelect a channel to back up your posts to.", reply_markup=InlineKeyboardMarkup(kb))

@Client.on_callback_query(filters.regex(r"start_backup_-?\d+"))
async def start_backup_process(client, query):
    user_id = query.from_user.id
    if user_id in ACTIVE_BACKUP_TASKS: return await query.answer("A backup process is already running.", show_alert=True)
    channel_id = int(query.data.split("_")[-1])
    ACTIVE_BACKUP_TASKS.add(user_id)
    try:
        await query.message.edit_text("⏳ `Step 1/3:` Fetching all your file records...")
        all_file_docs = await (await get_all_user_files(user_id)).to_list(length=None)
        if not all_file_docs:
            return await safe_edit_message(query, text="You have no files to back up.", reply_markup=go_back_button(user_id))
        
        await query.message.edit_text("⏳ `Step 2/3:` Intelligently grouping files by similarity...")
        batches = []
        for doc in all_file_docs:
            if not doc.get('file_name'): continue
            
            parsed_info = clean_and_parse_filename(doc['file_name'])
            if not parsed_info: continue
            doc_title = parsed_info['batch_title']

            added_to_existing_batch = False
            for batch in batches:
                batch_info = clean_and_parse_filename(batch[0]['file_name'])
                if not batch_info: continue
                batch_title = batch_info['batch_title']
                
                if calculate_title_similarity(doc_title, batch_title) > 0.85:
                    batch.append(doc)
                    added_to_existing_batch = True
                    break
            
            if not added_to_existing_batch:
                batches.append([doc])

        total_batches = len(batches)
        await safe_edit_message(query, text=f"✅ `Step 2/3:` Found **{total_batches}** unique posts to create. Starting backup...", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Backup", callback_data=f"cancel_backup_{user_id}")]]))
        
        for i, file_docs_batch in enumerate(batches):
            if user_id not in ACTIVE_BACKUP_TASKS:
                await safe_edit_message(query, text="❌ Backup cancelled by user.", reply_markup=go_back_button(user_id)); return
            try:
                message_ids = [int(d['raw_link'].split('/')[-1]) for d in file_docs_batch]
                source_chat_id = int("-100" + file_docs_batch[0]['raw_link'].split('/')[-2])
                file_messages = await client.get_messages(source_chat_id, message_ids)
                posts_to_send = await create_post(client, user_id, file_messages)
                for post in posts_to_send:
                    poster, caption, footer = post
                    if poster: await client.send_photo(channel_id, photo=poster, caption=caption, reply_markup=footer)
                    else: await client.send_message(channel_id, caption, reply_markup=footer, disable_web_page_preview=True)
                    await asyncio.sleep(3)
                progress_text = f"🔄 `Step 3/3:` Progress: {i + 1} / {total_batches} batches processed."
                await safe_edit_message(query, text=progress_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Backup", callback_data=f"cancel_backup_{user_id}")]]))
            except Exception as e:
                logger.exception(f"Failed to post batch during backup for user {user_id}.")
                await client.send_message(user_id, f"Failed to back up a batch. Error: {e}")
        
        await query.message.delete()
        await client.send_message(user_id, "✅ **Backup Complete!**", reply_markup=go_back_button(user_id))
    except Exception as e:
        logger.exception("Major error in backup process"); await safe_edit_message(query, text=f"A major error occurred: {e}", reply_markup=go_back_button(user_id))
    finally:
        ACTIVE_BACKUP_TASKS.discard(user_id)

@Client.on_callback_query(filters.regex(r"cancel_backup_"))
async def cancel_backup_handler(client, query):
    user_id = int(query.data.split("_")[-1])
    if query.from_user.id != user_id: return await query.answer("This is not for you.", show_alert=True)
    if user_id in ACTIVE_BACKUP_TASKS:
        ACTIVE_BACKUP_TASKS.discard(user_id); await query.answer("Cancellation signal sent.", show_alert=True)
    else: await query.answer("No active backup process found.", show_alert=True)

@Client.on_callback_query(filters.regex("manage_footer"))
async def manage_footer_handler(client, query):
    user = await get_user(query.from_user.id)
    if not user: await add_user(query.from_user.id); user = await get_user(query.from_user.id)

    buttons = user.get('footer_buttons', [])
    text = "**👣 Manage Footer Buttons**\n\nYou can add up to 3 buttons."
    kb = [[InlineKeyboardButton(f"❌ {btn['name']}", callback_data=f"rm_footer_{btn['name']}")] for btn in buttons]
    if len(buttons) < 3: kb.append([InlineKeyboardButton("➕ Add New Button", callback_data="add_footer")])
    kb.append([InlineKeyboardButton("« Go Back", callback_data=f"go_back_{query.from_user.id}")])
    await safe_edit_message(query, text=text, reply_markup=InlineKeyboardMarkup(kb))

@Client.on_callback_query(filters.regex("add_footer"))
async def add_footer_handler(client, query):
    user_id = query.from_user.id
    try:
        prompt_msg = await query.message.edit_text("Send the name for your new button.", reply_markup=go_back_button(user_id))
        button_name_msg = await client.listen(chat_id=user_id, timeout=300)
        await prompt_msg.edit_text(f"OK. Now, send the URL for the '{button_name_msg.text}' button.", reply_markup=go_back_button(user_id))
        button_url_msg = await client.listen(chat_id=user_id, timeout=300)
        button_url = button_url_msg.text.strip()
        if not button_url.startswith(("http://", "https://")):
            button_url = "https://" + button_url
        await add_footer_button(user_id, button_name_msg.text, button_url)
        await button_name_msg.delete(); await button_url_msg.delete()
        await safe_edit_message(query, text="✅ New footer button added!", reply_markup=go_back_button(user_id))
    except asyncio.TimeoutError: await safe_edit_message(query, text="❗️ **Timeout:** Cancelled.", reply_markup=go_back_button(user_id))
    except Exception as e:
        logger.exception("Error in add_footer_handler"); await safe_edit_message(query, text=f"An error occurred: {e}", reply_markup=go_back_button(user_id))

@Client.on_callback_query(filters.regex(r"rm_footer_"))
async def remove_footer_handler(client, query):
    await remove_footer_button(query.from_user.id, query.data.split("_", 2)[2])
    await query.answer("Button removed!", show_alert=True)
    await manage_footer_handler(client, query)

@Client.on_callback_query(filters.regex(r"manage_(post|db)_ch"))
async def manage_channels_handler(client, query):
    user_id, ch_type = query.from_user.id, query.data.split("_")[1]
    
    if ch_type == 'post':
        ch_type_key, ch_type_name = "post_channels", "Post"
    else:
        ch_type_key, ch_type_name = "index_db_channel", "Index DB"
    
    user_data = await get_user(user_id)
    if not user_data:
        await add_user(user_id)
        user_data = await get_user(user_id)

    text = f"**Manage Your {ch_type_name} Channels**\n\n"
    buttons = []
    
    channels = user_data.get(ch_type_key, [])
    if not isinstance(channels, list):
        channels = [channels] if channels else []

    if channels:
        await query.answer("Checking channel status...")
        text += "Here are your connected channels. Click to remove.\n\n"
        for ch_id in channels:
            try:
                chat = await client.get_chat(ch_id)
                member = await client.get_chat_member(ch_id, "me")
                if member.status not in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]:
                    buttons.append([InlineKeyboardButton(f"⚠️ Admin rights needed in {chat.title}", callback_data=f"rm_{ch_type}_{ch_id}")])
                else:
                    buttons.append([InlineKeyboardButton(f"✅ {chat.title}", callback_data=f"rm_{ch_type}_{ch_id}")])
            except Exception as e:
                logger.warning(f"Could not access channel {ch_id} for user {user_id}. Error: {e}")
                buttons.append([InlineKeyboardButton(f"👻 Ghost Channel - Click to Remove", callback_data=f"rm_{ch_type}_{ch_id}")])
    else:
        text += "You haven't added any channels yet."
        
    buttons.append([InlineKeyboardButton("➕ Add New Channel", callback_data=f"add_{ch_type}_ch")])
    buttons.append([InlineKeyboardButton("« Go Back", callback_data="manage_channels_menu")])
    await safe_edit_message(query, text=text, reply_markup=InlineKeyboardMarkup(buttons))


@Client.on_callback_query(filters.regex(r"rm_(post|db)_-?\d+"))
async def remove_channel_handler(client, query):
    _, ch_type, ch_id_str = query.data.split("_")
    user_id = query.from_user.id
    ch_id = int(ch_id_str)
    
    if ch_type == 'post':
        await remove_from_list(user_id, "post_channels", ch_id)
    else:
        await update_user(user_id, "index_db_channel", None)
        
    await query.answer("Channel removed!", show_alert=True)
    query.data = f"manage_{ch_type}_ch"
    await manage_channels_handler(client, query)

@Client.on_callback_query(filters.regex(r"add_(post|db)_ch"))
async def add_channel_prompt(client, query):
    user_id, ch_type_short = query.from_user.id, query.data.split("_")[1]
    ch_type_name = "Post" if ch_type_short == "post" else "Index DB"
    
    try:
        prompt = await query.message.edit_text(f"Forward a message from your target **{ch_type_name} Channel**.\n\nI must be an admin there.", reply_markup=go_back_button(user_id))
        response = await client.listen(chat_id=user_id, filters=filters.forwarded, timeout=300)
        
        if response.forward_from_chat:
            channel_id = response.forward_from_chat.id
            if ch_type_short == 'post':
                await set_post_channel(user_id, channel_id)
            else:
                await set_index_db_channel(user_id, channel_id)

            await response.reply_text(f"✅ Connected to **{response.forward_from_chat.title}**.", reply_markup=go_back_button(user_id))
        else: 
            await response.reply_text("This is not a valid forwarded message from a channel.", reply_markup=go_back_button(user_id))
            
        await prompt.delete()
        if response: await response.delete()
        
    except asyncio.TimeoutError:
        if 'prompt' in locals() and prompt: await safe_edit_message(prompt, text="Command timed out.")
    except Exception as e:
        logger.exception("Error in add_channel_prompt")
        await query.message.reply_text(f"An error occurred: {e}", reply_markup=go_back_button(user_id))

@Client.on_callback_query(filters.regex("^set_filename_link$"))
async def set_filename_link_handler(client, query):
    user_id = query.from_user.id
    try:
        prompt = await query.message.edit_text("Please send the full URL you want your filenames to link to.", reply_markup=go_back_button(user_id))
        response = await client.listen(chat_id=user_id, timeout=300, filters=filters.text)
        
        url_text = response.text.strip()
        if not url_text.startswith(("http://", "https://")):
            url_text = "https://" + url_text
            
        await update_user(user_id, "filename_url", url_text)
        await response.reply_text("✅ Filename link updated!", reply_markup=go_back_button(user_id))
        await prompt.delete()
    except asyncio.TimeoutError: await safe_edit_message(query, text="❗️ **Timeout:** Cancelled.", reply_markup=go_back_button(user_id))
    except:
        logger.exception("Error in set_filename_link_handler"); await safe_edit_message(query, text="An error occurred.", reply_markup=go_back_button(user_id))

@Client.on_callback_query(filters.regex("^(set_fsub|set_download)$"))
async def set_other_links_handler(client, query):
    user_id, action = query.from_user.id, query.data.split("_")[1]
    prompts = {
        "fsub": ("📢 **Set FSub**\n\nForward a message from your FSub channel.", "fsub_channel"),
        "download": ("❓ **Set 'How to Download'**\n\nSend your tutorial URL.", "how_to_download_link")
    }
    prompt_text, key = prompts[action]
    
    prompt = None
    try:
        if action == "download":
            user = await get_user(user_id)
            if user:
                current_link = user.get(key)
                if current_link:
                    prompt_text += f"\n\n**Current Link:** `{current_link}`"

        prompt = await query.message.edit_text(prompt_text, reply_markup=go_back_button(user_id), disable_web_page_preview=True)
        
        listen_filters = filters.forwarded if action == "fsub" else filters.text
        response = await client.listen(chat_id=user_id, timeout=300, filters=listen_filters)
        
        value = None
        if action == "fsub":
            if not response.forward_from_chat:
                await response.reply("Not a valid forwarded message.", reply_markup=go_back_button(user_id))
                return
            value = response.forward_from_chat.id
            await update_user(user_id, key, value)
            await response.reply("✅ FSub channel updated!", reply_markup=go_back_button(user_id))

        else:
            url_to_check = response.text.strip()
            if not url_to_check.startswith(("http://", "https://")):
                url_to_check = "https://" + url_to_check

            await prompt.edit_text(f"⏳ **Validating URL...**\n`{url_to_check}`")
            
            is_valid = False
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(url_to_check, timeout=5, allow_redirects=True) as resp:
                        if resp.status in range(200, 400):
                            is_valid = True
            except Exception as e:
                logger.error(f"URL validation failed for {url_to_check}: {e}")
                is_valid = False

            if is_valid:
                await update_user(user_id, key, url_to_check)
                await prompt.edit_text("✅ **Success!**\n\nYour 'How to Download' link has been verified and saved.")
                await asyncio.sleep(3)
                await how_to_download_menu_handler(client, query)

            else:
                await prompt.edit_text(
                    "❌ **Validation Failed!**\n\n"
                    "The URL you provided appears to be invalid or inaccessible. "
                    "Your settings have **not** been saved.\n\n"
                    "Please check the link and try again.",
                    reply_markup=go_back_button(user_id)
                )

    except asyncio.TimeoutError:
        if prompt:
            await safe_edit_message(prompt, text="❗️ **Timeout:** Cancelled.", reply_markup=go_back_button(user_id))
    except Exception as e:
        logger.exception("Error in set_other_links_handler")
        if prompt:
            await safe_edit_message(prompt, text=f"An error occurred: {e}", reply_markup=go_back_button(user_id))
    finally:
        if 'response' in locals() and response:
            try:
                await response.delete()
            except:
                pass
        if action == 'fsub' and prompt:
             try:
                await prompt.delete()
             except:
                 pass


@Client.on_callback_query(filters.regex("^set_shortener$"))
async def set_shortener_handler(client, query):
    user_id = query.from_user.id
    
    try:
        prompt_msg = await query.message.edit_text(
            "**🔗 Step 1/2: Set Domain**\n\n"
            "Please send your shortener website's domain name (e.g., `example.com`).",
            reply_markup=go_back_button(user_id)
        )
        domain_msg = await client.listen(chat_id=user_id, timeout=300)
        domain = domain_msg.text.strip()
        await domain_msg.delete()

        await prompt_msg.edit_text(
            f"**🔗 Step 2/2: Set API Key**\n\n"
            f"Domain: `{domain}`\n"
            "Now, please send your API key.",
            reply_markup=go_back_button(user_id)
        )
        api_msg = await client.listen(chat_id=user_id, timeout=300)
        api_key = api_msg.text.strip()
        await api_msg.delete()
        
        await prompt_msg.edit_text("⏳ **Testing your credentials...**\nPlease wait a moment.")
        is_valid = await validate_shortener(domain, api_key)

        if is_valid:
            await update_user(user_id, "shortener_url", domain)
            await update_user(user_id, "shortener_api", api_key)
            await prompt_msg.edit_text("✅ **Success!**\n\nYour shortener has been verified and saved.")
            await asyncio.sleep(3)
        else:
            await prompt_msg.edit_text(
                "❌ **Validation Failed!**\n\n"
                "The domain or API key you provided appears to be incorrect. "
                "Your settings have **not** been saved.\n\n"
                "Please check your credentials and try again.",
                reply_markup=go_back_button(user_id)
            )
            return

        text, markup = await get_shortener_menu_parts(user_id)
        await safe_edit_message(prompt_msg, text=text, reply_markup=markup)

    except (asyncio.TimeoutError, Exception) as e:
        if isinstance(e, asyncio.TimeoutError):
            await safe_edit_message(query, text="❗️ **Timeout:** Command cancelled.", reply_markup=go_back_button(user_id))
        else:
            logger.exception("Error in set_shortener_handler")
            await safe_edit_message(query, text=f"An error occurred: {e}", reply_markup=go_back_button(user_id))

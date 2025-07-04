# utils/helpers.py 

import re
import base64
import logging
import PTN
import asyncio
from imdb import Cinemagoer
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant, ChatAdminRequired, ChannelInvalid, PeerIdInvalid, ChannelPrivate
from config import Config
from database.db import get_user, remove_from_list, update_user
from features.poster import get_poster
from thefuzz import fuzz

logger = logging.getLogger(__name__)

PHOTO_CAPTION_LIMIT = 1024
TEXT_MESSAGE_LIMIT = 4096

# Initialize the Cinemagoer instance
ia = Cinemagoer()

def format_bytes(size):
    """Converts bytes to a human-readable format with custom rounding."""
    if not isinstance(size, (int, float)) or size == 0:
        return ""
    power = 1024
    n = 0
    power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while size >= power and n < len(power_labels) - 1:
        size /= power
        n += 1
    if n >= 3: return f"{size:.1f} {power_labels[n]}"
    elif n == 2: return f"{round(size)} {power_labels[n]}"
    else: return f"{int(size)} {power_labels[n]}"

async def get_definitive_title_from_imdb(title_from_filename):
    """
    Uses the cinemagoer library to find the official title and year from IMDb,
    with added checks to prevent incorrect matches.
    """
    if not title_from_filename:
        return None, None
    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, lambda: ia.search_movie(title_from_filename))
        
        if not results:
            logger.warning(f"No IMDb results found for '{title_from_filename}'")
            return None, None
            
        movie = results[0]
        await loop.run_in_executor(None, lambda: ia.update(movie))
        
        imdb_title = movie.get('title')
        imdb_year = movie.get('year')
        
        # To prevent wildly inaccurate matches, we'll check the similarity
        # between the filename title and the IMDb title.
        similarity = fuzz.token_sort_ratio(title_from_filename.lower(), imdb_title.lower())
        
        if similarity < 50: # Adjust this threshold as needed
            logger.warning(f"IMDb result '{imdb_title}' has low similarity ({similarity}%) to '{title_from_filename}'. Ignoring.")
            return None, None
            
        logger.info(f"IMDb lookup successful for '{title_from_filename}': Found '{imdb_title} ({imdb_year})'")
        return imdb_title, imdb_year

    except Exception as e:
        logger.error(f"Error fetching data from IMDb for '{title_from_filename}': {e}")
        return None, None

async def clean_and_parse_filename(name: str):
    """
    A highly robust, multi-stage parsing engine for filenames.
    """
    # --- Stage 1: Initial Parsing with PTN ---
    parsed_info = PTN.parse(name)
    
    initial_title = parsed_info.get('title')
    year = parsed_info.get('year')
    season = parsed_info.get('season')
    
    # Advanced episode parsing to handle single episodes and ranges
    episode_info_str = ""
    episode_match = re.search(r'[Ee][Pp]?\s?(\d+)(?:\s?-\s?[Ee][Pp]?\s?(\d+))?', name)
    if episode_match:
        start_ep = int(episode_match.group(1))
        if episode_match.group(2):
            end_ep = int(episode_match.group(2))
            episode_info_str = f"E{start_ep:02d}-E{end_ep:02d}"
        else:
            episode_info_str = f"E{start_ep:02d}"
    elif parsed_info.get('episode'):
        # Fallback to PTN's episode parsing if our regex fails
        episode = parsed_info.get('episode')
        if isinstance(episode, list):
            episode_info_str = f"E{min(episode):02d}-E{max(episode):02d}"
        else:
            episode_info_str = f"E{episode:02d}"

    is_series = season is not None or episode_info_str != ""

    # --- Stage 2: IMDb Verification for Definitive Title ---
    definitive_title, definitive_year = await get_definitive_title_from_imdb(initial_title)

    # --- Stage 3: Assemble Final, Clean Data ---
    if definitive_title:
        final_title = definitive_title
        final_year = definitive_year
    else:
        final_title = initial_title
        final_year = year
        
    batch_title = f"{final_title}"
    if is_series and season:
        batch_title += f" S{season:02d}"

    # Re-assemble quality tags from the original filename for the post body
    quality_tags_parts = []
    if parsed_info.get('resolution'): quality_tags_parts.append(parsed_info.get('resolution'))
    if parsed_info.get('quality'): quality_tags_parts.append(parsed_info.get('quality'))
    if parsed_info.get('codec'): quality_tags_parts.append(parsed_info.get('codec'))
    if parsed_info.get('audio'): quality_tags_parts.append(parsed_info.get('audio'))
        
    media_info = {
        "batch_title": batch_title.strip(),
        "year": final_year,
        "is_series": is_series,
        "season_info": f"S{season:02d}" if season else "",
        "episode_info": episode_info_str,
        "quality_tags": " | ".join(filter(None, quality_tags_parts))
    }
    
    return media_info

async def create_post(client, user_id, messages):
    user = await get_user(user_id)
    if not user: return []

    media_info_list = []
    for m in messages:
        media = getattr(m, m.media.value, None)
        if not media: continue
        
        info = await clean_and_parse_filename(media.file_name)
        if info:
            info['file_size'] = media.file_size
            info['file_unique_id'] = media.file_unique_id
            media_info_list.append(info)

    if not media_info_list: return []

    media_info_list.sort(key=lambda x: natural_sort_key(x.get('episode_info', '')))

    first_info = media_info_list[0]
    primary_display_title, year = first_info['batch_title'], first_info['year']
    
    base_caption_header = f"🎬 **{primary_display_title} {f'({year})' if year else ''}**"
    post_poster = await get_poster(first_info['batch_title'], year) if user.get('show_poster', True) else None
    
    footer_buttons = user.get('footer_buttons', [])
    footer_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(btn['name'], btn['url'])] for btn in footer_buttons]) if footer_buttons else None
    
    header_line = "▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱"
    footer_line = "\n\n" + "•·•·•·•·•·•·•·•·•·••·•·•·•·•·•·•·•"
    CAPTION_LIMIT = PHOTO_CAPTION_LIMIT if post_poster else TEXT_MESSAGE_LIMIT
    
    all_link_entries = []
    for info in media_info_list:
        display_tags_parts = []
        if info['is_series'] and info['episode_info']:
            display_tags_parts.append(info['episode_info'])
        if info['quality_tags']:
            display_tags_parts.append(info['quality_tags'])
        
        display_tags = " | ".join(display_tags_parts)

        composite_id = f"{user_id}_{info['file_unique_id']}"
        link = f"http://{Config.VPS_IP}:{Config.VPS_PORT}/get/{composite_id}"
        file_size_str = format_bytes(info['file_size'])

        file_entry = f"📁 {display_tags}" if display_tags else "📁"
        file_entry += f"\n    ➤ [Click Here]({link}) ({file_size_str})" if file_size_str else f"\n    ➤ [Click Here]({link})"
        all_link_entries.append(file_entry)

    final_posts, current_links_part = [], []
    base_caption = f"{header_line}\n{base_caption_header}\n{header_line}"
    current_length = len(base_caption) + len(footer_line)

    for entry in all_link_entries:
        entry_length = len(entry) + 2
        if current_length + entry_length > CAPTION_LIMIT:
            if current_links_part:
                caption = f"{base_caption}\n\n" + "\n\n".join(current_links_part) + footer_line
                final_posts.append((post_poster if not final_posts else None, caption, footer_keyboard))
            current_links_part, current_length = [entry], len(base_caption) + len(footer_line) + entry_length
        else:
            current_links_part.append(entry)
            current_length += entry_length
            
    if current_links_part:
        caption = f"{base_caption}\n\n" + "\n\n".join(current_links_part) + footer_line
        final_posts.append((post_poster if not final_posts else None, caption, footer_keyboard))
        
    total_posts = len(final_posts)
    if total_posts > 1:
        for i, (poster, cap, foot) in enumerate(final_posts):
            new_header = f"{base_caption_header} (Part {i+1}/{total_posts})"
            final_posts[i] = (poster, cap.replace(base_caption_header, new_header), foot)
    elif total_posts == 1 and final_posts:
        _, cap, foot = final_posts[0]
        final_posts[0] = (post_poster, cap, foot)
        
    return final_posts

async def get_title_key(filename: str) -> str:
    media_info = await clean_and_parse_filename(filename)
    # Return the batch title, which now includes the season for series
    return media_info['batch_title'] if media_info else None

def calculate_title_similarity(title1: str, title2: str) -> float:
    return fuzz.token_sort_ratio(title1.lower(), title2.lower())

def go_back_button(user_id):
    return InlineKeyboardMarkup([[InlineKeyboardButton("« Go Back", callback_data=f"go_back_{user_id}")]])

async def get_file_raw_link(message):
    return f"https://t.me/c/{str(message.chat.id).replace('-100', '')}/{message.id}"

def natural_sort_key(s):
    if not isinstance(s, str):
        return [s]
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'([0-9]+)', s)]

async def get_main_menu(user_id):
    user_settings = await get_user(user_id) or {}
    post_channels = user_settings.get('post_channels', [])
    index_channel = user_settings.get('index_db_channel')
    
    menu_text = "✅ **Setup Complete!**\n\nYou can now forward files to your Index Channel." if index_channel and post_channels else "⚙️ **Bot Settings**\n\nChoose an option below to configure the bot."
    shortener_text = "⚙️ Shortener Settings" if user_settings.get('shortener_url') else "🔗 Set Shortener"
    fsub_text = "⚙️ Manage FSub" if user_settings.get('fsub_channel') else "📢 Set FSub"
    
    buttons = [
        [InlineKeyboardButton("🗂️ Manage Channels", callback_data="manage_channels_menu")],
        [InlineKeyboardButton(shortener_text, callback_data="shortener_menu"), InlineKeyboardButton("🔄 Backup Links", callback_data="backup_links")],
        [InlineKeyboardButton("✍️ Filename Link", callback_data="filename_link_menu"), InlineKeyboardButton("👣 Footer Buttons", callback_data="manage_footer")],
        [InlineKeyboardButton("🖼️ IMDb Poster", callback_data="poster_menu"), InlineKeyboardButton("📂 My Files", callback_data="my_files_1")],
        [InlineKeyboardButton(fsub_text, callback_data="fsub_menu"), InlineKeyboardButton("❓ How to Download", callback_data="how_to_download_menu")]
    ]
    
    if user_id == Config.ADMIN_ID:
        pass
        
    return menu_text, InlineKeyboardMarkup(buttons)

async def notify_and_remove_invalid_channel(client, user_id, channel_id, channel_type):
    user_settings = await get_user(user_id)
    if not user_settings: return False
    
    db_key = f"{channel_type.lower()}_channels"
    if channel_type.lower() == 'index db':
        db_key = 'index_db_channel'
        
    try:
        await client.get_chat_member(channel_id, "me")
        return True
    except Exception:
        error_text = f"⚠️ **Channel Inaccessible**\n\nYour {channel_type.title()} Channel (ID: `{channel_id}`) has been automatically removed."
        try:
            await client.send_message(user_id, error_text)
            if isinstance(user_settings.get(db_key), list):
                 await remove_from_list(user_id, db_key, channel_id)
            else:
                 await update_user(user_id, db_key, None)
        except Exception as e:
            logger.error(f"Failed to notify/remove channel for user {user_id}. Error: {e}")
        return False

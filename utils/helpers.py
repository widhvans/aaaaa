# utils/helpers.py

import re
import base64
import logging
import PTN
import aiohttp
from bs4 import BeautifulSoup
from googlesearch import search
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import UserNotParticipant, ChatAdminRequired, ChannelInvalid, PeerIdInvalid, ChannelPrivate
from config import Config
from database.db import get_user, remove_from_list, update_user
from features.poster import get_poster
from thefuzz import fuzz

logger = logging.getLogger(__name__)

PHOTO_CAPTION_LIMIT = 1024
TEXT_MESSAGE_LIMIT = 4096

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

async def get_definitive_title_from_search(refined_title, year):
    """
    Uses a web search to find the official title from IMDb.
    """
    if not refined_title:
        return refined_title
    try:
        query = f'"{refined_title}" {year if year else ""} movie imdb'
        
        # We'll run the search in a separate thread to avoid blocking the event loop
        import asyncio
        loop = asyncio.get_event_loop()
        
        def search_sync():
            return list(search(query, num=1, stop=1, pause=2))
            
        urls = await loop.run_in_executor(None, search_sync)

        for url in urls:
            if "imdb.com/title/" in url:
                try:
                    headers = {'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'en-US,en;q=0.5'}
                    async with aiohttp.ClientSession(headers=headers) as session:
                        async with session.get(url, timeout=10) as response:
                            if response.status == 200:
                                soup = BeautifulSoup(await response.text(), 'html.parser')
                                
                                title_element = soup.select_one('h1[data-testid="hero-title-block__title"]')
                                if title_element:
                                    title = title_element.get_text(strip=True)
                                    
                                    year_element = soup.select_one('a[href*="/releaseinfo"]')
                                    if year_element:
                                        year_text = year_element.get_text(strip=True)
                                        if year_text.isdigit() and len(year_text) == 4:
                                            logger.info(f"Web verification successful. Found definitive title: '{title} ({year_text})'")
                                            return f"{title} ({year_text})"
                                    
                                    logger.info(f"Web verification successful. Found definitive title: '{title}'")
                                    return title

                except Exception as e:
                    logger.error(f"Error fetching/parsing IMDb page: {e}")
                    continue
    except Exception as e:
        logger.error(f"Error during web search for definitive title: {e}")

    logger.warning(f"Web verification failed, falling back to refined title: '{refined_title}'")
    return refined_title

async def clean_and_parse_filename(name: str):
    """
    The definitive, final, intelligent parsing engine.
    This version uses a multi-stage regex process and web verification
    to perfectly differentiate titles, seasons, episodes, and every quality tag.
    """
    original_name = name.replace('.', ' ').replace('_', ' ')

    # --- Stage 1: High-Precision Extraction ---
    
    # Extract Year first, as it's a clear marker
    year_match = re.search(r'[\(\[]?(\d{4})[\)\]]?', original_name)
    year = year_match.group(1) if year_match else None
    
    # Extract Season and Episode with advanced logic
    is_series = False
    season_str = ""
    episode_str = ""
    # This regex is designed to find SXXEXX, SXX EXX, Season XX Episode XX, etc. and capture the full episode range
    series_match = re.search(r'(?:[Ss]eason|[Ss])\s?(\d+)(?:\s?(?:[Ee]pisode|[Ee][Pp]?\.?)\s?([\d-]+))?', original_name, re.IGNORECASE)
    if series_match:
        is_series = True
        season_num = int(series_match.group(1))
        season_str = f"S{season_num:02d}"
        
        if series_match.group(2):
            # Handle episode ranges like 01-06
            episode_part = series_match.group(2)
            episode_nums = re.findall(r'\d+', episode_part)
            if len(episode_nums) > 1:
                episode_str = f"E{episode_nums[0].zfill(2)}-E{episode_nums[-1].zfill(2)}"
            elif len(episode_nums) == 1:
                episode_str = f"E{episode_nums[0].zfill(2)}"
    
    # Expanded list of all possible quality, source, and audio tags
    tags_to_find = [
        '1080p', '720p', '480p', '540p', 'WEB-DL', 'WEBRip', 'BluRay', 'HDTC', 'HDRip',
        'x264', 'x265', 'AAC', 'Dual Audio', 'Multi Audio', 'Hindi', 'English', 'ESub', 'HEVC', 
        'DDP5 1', 'DDP2 0', 'AMZN', 'Dua'
    ]
    # Unnecessary tags to be filtered out from the final caption
    unnecessary_tags = ['x264', 'x265', 'AAC', 'HEVC']

    all_tags_regex = r'\b(' + '|'.join(re.escape(tag) for tag in tags_to_find) + r')\b'
    found_tags = re.findall(all_tags_regex, original_name, re.IGNORECASE)
    
    # Standardize tags and handle language logic
    standardized_tags = {tag.strip().upper().replace("DUA", "DUAL AUDIO") for tag in found_tags}
    if "DUAL AUDIO" in standardized_tags or "MULTI AUDIO" in standardized_tags:
        standardized_tags.discard("HINDI")
        standardized_tags.discard("ENGLISH")
    
    # Filter out unnecessary tags
    final_tags = {tag for tag in standardized_tags if tag.upper() not in [ut.upper() for ut in unnecessary_tags]}

    quality_tags = " | ".join(sorted(list(final_tags), key=lambda x: x.lower()))


    # --- Stage 2: Aggressive Title Cleaning ---
    
    # Start with the full name and carve away the junk
    refined_title = original_name

    # Remove all extracted information to isolate the title
    if year: refined_title = refined_title.replace(year_match.group(0), '')
    if is_series and series_match:
        refined_title = re.sub(re.escape(series_match.group(0)), '', refined_title, flags=re.IGNORECASE)
    
    # Remove all found tags and promotional junk
    promo_junk = ['SkymoviesHD', 'PMI', 'part002']
    full_junk_list = tags_to_find + promo_junk
    
    for junk in full_junk_list:
        refined_title = re.sub(r'\b' + re.escape(junk) + r'\b', '', refined_title, flags=re.IGNORECASE)
    
    # Remove remaining junk words and symbols
    more_junk = ['completed', 'web series', 'mkv', 'esubs', 'du', 'au', 'dual', 'audi', 'audiol']
    junk_regex = r'\b(' + '|'.join(re.escape(word) for word in more_junk) + r')\b'
    refined_title = re.sub(junk_regex, '', refined_title, flags=re.I)
    refined_title = re.sub(r'[\(\)\[\]\{\}\+@]', '', refined_title) # Remove symbols
    refined_title = re.sub(r'\s\d\s\d\s', '', refined_title) # Remove ' 5 1 '
    refined_title = ' '.join(refined_title.split()).strip() # Consolidate spaces

    # --- Stage 3: Web Verification for Definitive Title ---
    
    definitive_title = await get_definitive_title_from_search(refined_title, year)
    
    # --- Stage 4: Assemble Final Data ---
    
    batch_title = f"{definitive_title} {season_str}".strip() if is_series else definitive_title

    media_info = {
        "batch_title": batch_title,
        "year": year,
        "is_series": is_series,
        "season_info": season_str,
        "episode_info": episode_str,
        "quality_tags": quality_tags
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

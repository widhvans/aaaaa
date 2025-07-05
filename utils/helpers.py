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

ia = Cinemagoer()

def simple_clean_filename(name: str) -> str:
    """
    A simple, synchronous function to clean a filename for display purposes.
    Removes brackets, extensions, and extra whitespace.
    """
    # Remove extension
    clean_name = ".".join(name.split('.')[:-1]) if '.' in name else name
    # Remove all content within brackets: (), [], {}
    clean_name = re.sub(r'[\(\[\{].*?[\)\]\}]', '', clean_name)
    # Replace separators and clean up spaces
    clean_name = clean_name.replace('.', ' ').replace('_', ' ').strip()
    # Final cleanup of extra spaces
    clean_name = re.sub(r'\s+', ' ', clean_name).strip()
    return clean_name

def go_back_button(user_id):
    """Creates a standard 'Go Back' button to return to the main menu."""
    return InlineKeyboardMarkup([[InlineKeyboardButton("« Go Back", callback_data=f"go_back_{user_id}")]])

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
    Uses the cinemagoer library to find the official title and year from IMDb.
    """
    if not title_from_filename:
        return None, None
    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, lambda: ia.search_movie(title_from_filename, results=1))
        
        if not results:
            return None, None
            
        movie = results[0]
        await loop.run_in_executor(None, lambda: ia.update(movie, info=['main']))
        
        imdb_title = movie.get('title')
        imdb_year = movie.get('year')
        
        similarity = fuzz.token_sort_ratio(title_from_filename.lower(), imdb_title.lower())
        
        if similarity < 50:
            return None, None
            
        return imdb_title, imdb_year

    except Exception as e:
        logger.error(f"Error fetching data from IMDb for '{title_from_filename}': {e}")
        return None, None

async def clean_and_parse_filename(name: str, cache: dict = None):
    """
    A next-gen, multi-pass robust filename parser.
    """
    original_name = name

    # --- PASS 1: Pre-Normalization and Heavy Junk Removal ---
    # Work with name before extension
    name = ".".join(name.split('.')[:-1]) if '.' in name else name
    # Aggressively remove all content within brackets: (), [], {}
    name = re.sub(r'[\(\[\{].*?[\)\]\}]', '', name)
    # Replace common separators with spaces
    name = name.replace('.', ' ').replace('_', ' ').strip()
    # Remove common symbols and channel-like tags
    name = re.sub(r'[#@$%&~]', '', name)
    name = re.sub(r'「.*?」', '', name) # For special brackets

    # --- PASS 2: Regex-based Keyword Junk Removal ---
    junk_words = [
        'UNCUT', 'ORG', 'HQ', 'ESubs', 'MSubs', 'REMASTERED', 'REPACK', 'PROPER', 'iNTERNAL',
        'Sample', 'Video', 'Dual', 'Audio', 'Multi', 'Hollywood', 'Movie', 'New', 'Episode',
        'Combined', 'Complete', 'Chapter',
        'PSA', 'JC', 'DIDAR', 'StarBoy', 'ClipmateMovies', 'OTT_Downloader_Bot', 'OTT_WebdlBot', 'MAPOriginals',
        'Hindi', 'English', 'Tamil', 'Telugu', 'Kannada', 'Malayalam', 'Punjabi', 'Japanese', 'Korean',
        'NF', 'AMZN', 'MAX', 'DSNP', 'ZEE5',
        '1080p', '720p', '576p', '480p', '360p', '240p', '4k', '3D',
        'x264', 'x265', 'h264', 'h265', '10bit', 'HEVC',
        'HDCAM', 'HDTC', 'HDRip', 'BluRay', 'WEB-DL', 'Web-Rip', 'DVDRip', 'BDRip',
        'DTS', 'AAC', 'AC3', 'E-AC-3', 'E-AC3', 'DD', 'DDP', 'HE-AAC',
        'Kbps'
    ]
    # This regex ensures we only match whole words (case-insensitive)
    junk_pattern_re = r'\b(' + r'|'.join(junk_words) + r')\b'
    name = re.sub(junk_pattern_re, '', name, flags=re.IGNORECASE)

    # --- PASS 3: Date and Final Cleanup ---
    # Remove date patterns like "September 11, 2023" or "September 11 2023"
    date_pattern = r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b'
    name = re.sub(date_pattern, '', name, flags=re.IGNORECASE)
    # Final cleanup of extra spaces
    name = re.sub(r'\s+', ' ', name).strip()
    
    # --- PASS 4: Intelligent Parsing and De-duplication ---
    parsed_info = PTN.parse(name)
    initial_title = parsed_info.get('title', '').strip()
    
    if initial_title:
        # De-duplicate by splitting the string by the found title and taking the first part.
        # This handles cases like "Title S01E01 Title" -> "Title S01E01"
        if name.lower().count(initial_title.lower()) > 1:
            first_occurrence_index = name.lower().find(initial_title.lower())
            rest_of_string = name[first_occurrence_index + len(initial_title):]
            
            second_occurrence_index = rest_of_string.lower().find(initial_title.lower())
            if second_occurrence_index != -1:
                # Cut the string off before the second occurrence of the title
                name = name[:first_occurrence_index + len(initial_title) + second_occurrence_index]
                # Re-parse after deduplication
                parsed_info = PTN.parse(name)
                initial_title = parsed_info.get('title', '').strip()

    # --- PASS 5: Final Extraction and IMDb Verification ---
    initial_title = re.sub(r'^\W*\d+mm\W*', '', initial_title, flags=re.IGNORECASE).strip()
    all_years = re.findall(r'\b(19[89]\d|20[0-2]\d)\b', original_name)
    year = all_years[0] if all_years else parsed_info.get('year')

    if not initial_title:
        initial_title = PTN.parse(original_name).get('title', '')
        if not initial_title: return None

    definitive_title, definitive_year = None, None
    cache_key = f"{initial_title}_{year}" if year else initial_title
    if cache is not None and cache_key in cache:
        definitive_title, definitive_year = cache[cache_key]
        logger.info(f"IMDb CACHE HIT for '{cache_key}'")
    else:
        logger.info(f"IMDb CACHE MISS for '{cache_key}'. Fetching from network...")
        definitive_title, definitive_year = await get_definitive_title_from_imdb(f"{initial_title} {year}" if year else initial_title)
        if cache is not None:
            cache[cache_key] = (definitive_title, definitive_year)

    final_title = definitive_title if definitive_title else initial_title
    final_year = definitive_year if definitive_year else year
    season = parsed_info.get('season')
    
    episode_info_str = ""
    search_name = original_name.replace('.', ' ').replace('_', ' ')
    episode_match = re.search(
        r'[Ee](?:p(?:isode)?)?\.?\s*(\d+)(?:\s*(?:-|to)\s*[Ee]?(?:p(?:isode)?)?\.?\s*(\d+))?',
        search_name, re.IGNORECASE
    )
    if episode_match:
        start_ep = int(episode_match.group(1))
        if not (1900 < start_ep < 2100 and not season):
            episode_info_str = f"E{start_ep:02d}"
            if episode_match.group(2): episode_info_str += f"-E{int(episode_match.group(2)):02d}"
    elif parsed_info.get('episode'):
        episode = parsed_info.get('episode')
        if isinstance(episode, list): episode_info_str = f"E{min(episode):02d}-E{max(episode):02d}"
        elif not (1900 < episode < 2100 and not season): episode_info_str = f"E{episode:02d}"
        
    is_series = season is not None or episode_info_str != ""
    display_title = f"{final_title}" + (f" ({final_year})" if final_year else "")
    
    quality_tags_parts = [
        parsed_info.get('resolution'), parsed_info.get('quality'), 
        parsed_info.get('codec'), parsed_info.get('audio')
    ]
        
    return {
        "batch_title": f"{final_title} S{season:02d}" if is_series and season else final_title,
        "display_title": display_title.strip(),
        "year": final_year, "is_series": is_series,
        "season_info": f"S{season:02d}" if season else "", "episode_info": episode_info_str,
        "quality_tags": " | ".join(filter(None, quality_tags_parts))
    }

async def create_post(client, user_id, messages, cache: dict):
    user = await get_user(user_id)
    if not user: return []

    media_info_list = []
    parse_tasks = [clean_and_parse_filename(getattr(m, m.media.value, None).file_name, cache) for m in messages if getattr(m, m.media.value, None)]
    parsed_results = await asyncio.gather(*parse_tasks)

    for i, info in enumerate(parsed_results):
        if info:
            media = getattr(messages[i], messages[i].media.value)
            info['file_size'] = media.file_size
            info['file_unique_id'] = media.file_unique_id
            media_info_list.append(info)

    if not media_info_list: return []

    media_info_list.sort(key=lambda x: natural_sort_key(x.get('episode_info', '')))
    first_info = media_info_list[0]
    primary_display_title = first_info['display_title']
    
    base_caption_header = f"🎬 **{primary_display_title}**"
    post_poster = await get_poster(first_info['batch_title'], first_info['year']) if user.get('show_poster', True) else None
    
    footer_buttons = user.get('footer_buttons', [])
    footer_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(btn['name'], url=btn['url'])] for btn in footer_buttons]) if footer_buttons else None
    
    header_line, footer_line = "▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱", "\n\n" + "•·•·•·•·•·•·•·•·•·••·•·•·•·•·•·•·•"
    CAPTION_LIMIT = PHOTO_CAPTION_LIMIT if post_poster else TEXT_MESSAGE_LIMIT
    
    all_link_entries = []
    for info in media_info_list:
        display_tags_parts = []
        if info['is_series'] and info['episode_info']: display_tags_parts.append(info['episode_info'])
        if info['quality_tags']: display_tags_parts.append(info['quality_tags'])
        display_tags = " | ".join(filter(None, display_tags_parts))
        link = f"http://{Config.VPS_IP}:{Config.VPS_PORT}/get/{user_id}_{info['file_unique_id']}"
        file_size_str = format_bytes(info['file_size'])
        all_link_entries.append(f"📁 {display_tags or 'File'}\n    ➤ [Click Here]({link}) ({file_size_str})")

    final_posts, current_links_part = [], []
    base_caption = f"{header_line}\n{base_caption_header}\n{header_line}"
    current_length = len(base_caption) + len(footer_line)

    for entry in all_link_entries:
        if current_length + len(entry) + 2 > CAPTION_LIMIT and current_links_part:
            final_posts.append((post_poster if not final_posts else None, f"{base_caption}\n\n" + "\n\n".join(current_links_part) + footer_line, footer_keyboard))
            current_links_part = [entry]
            current_length = len(base_caption) + len(footer_line) + len(entry) + 2
        else:
            current_links_part.append(entry)
            current_length += len(entry) + 2
            
    if current_links_part:
        final_posts.append((post_poster if not final_posts else None, f"{base_caption}\n\n" + "\n\n".join(current_links_part) + footer_line, footer_keyboard))
        
    if len(final_posts) > 1:
        for i, (poster, cap, foot) in enumerate(final_posts):
            final_posts[i] = (poster, cap.replace(primary_display_title, f"{primary_display_title} (Part {i+1}/{len(final_posts)})"), foot)
            
    return final_posts

def calculate_title_similarity(title1: str, title2: str) -> float:
    """Calculates similarity between two titles."""
    return fuzz.token_sort_ratio(title1.lower(), title2.lower())

async def get_title_key(filename: str) -> str:
    media_info = await clean_and_parse_filename(filename)
    return media_info['batch_title'] if media_info else None

async def get_file_raw_link(message):
    """Creates the raw 't.me/c/...' link for a message in a private channel."""
    return f"https.t.me/c/{str(message.chat.id).replace('-100', '')}/{message.id}"

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r'([0-9]+)', s or '')]

async def get_main_menu(user_id):
    user_settings = await get_user(user_id) or {}
    text = "✅ **Setup Complete!**\n\nYou can now forward files to your Index Channel." if user_settings.get('index_db_channel') and user_settings.get('post_channels') else "⚙️ **Bot Settings**\n\nChoose an option below to configure the bot."
    buttons = [
        [InlineKeyboardButton("🗂️ Manage Channels", callback_data="manage_channels_menu")],
        [InlineKeyboardButton("🔗 Shortener", callback_data="shortener_menu"), InlineKeyboardButton("🔄 Backup", callback_data="backup_links")],
        [InlineKeyboardButton("✍️ Filename Link", callback_data="filename_link_menu"), InlineKeyboardButton("👣 Footer Buttons", callback_data="manage_footer")],
        [InlineKeyboardButton("🖼️ IMDb Poster", callback_data="poster_menu"), InlineKeyboardButton("📂 My Files", callback_data="my_files_1")],
        [InlineKeyboardButton("📢 FSub", callback_data="fsub_menu"), InlineKeyboardButton("❓ How to Download", callback_data="how_to_download_menu")]
    ]
    return text, InlineKeyboardMarkup(buttons)

async def notify_and_remove_invalid_channel(client, user_id, channel_id, channel_type):
    try:
        await client.get_chat_member(channel_id, "me")
        return True
    except Exception:
        db_key = 'index_db_channel' if channel_type == 'Index DB' else 'post_channels'
        user_settings = await get_user(user_id)
        if isinstance(user_settings.get(db_key), list):
             await remove_from_list(user_id, db_key, channel_id)
        else:
             await update_user(user_id, db_key, None)
        await client.send_message(user_id, f"⚠️ **Channel Inaccessible**\n\nYour {channel_type} Channel (ID: `{channel_id}`) has been automatically removed because I could not access it.")
        return False

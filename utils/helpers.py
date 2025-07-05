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
    clean_name = ".".join(name.split('.')[:-1]) if '.' in name else name
    clean_name = re.sub(r'[\(\[\{].*?[\)\]\}]', '', clean_name)
    clean_name = clean_name.replace('.', ' ').replace('_', ' ').strip()
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
    Uses the cinemagoer library to find the official title and year from IMDb,
    with an ultra-strict "reality check" to prevent mismatches.
    """
    if not title_from_filename:
        return None, None
    try:
        loop = asyncio.get_event_loop()
        logger.info(f"Querying IMDb with cleaned title: '{title_from_filename}'")
        results = await loop.run_in_executor(None, lambda: ia.search_movie(title_from_filename, results=1))
        
        if not results:
            logger.warning(f"IMDb returned no results for '{title_from_filename}'")
            return None, None
            
        movie = results[0]
        imdb_title_raw = movie.get('title')
        
        # Stricter similarity check
        normalized_original = title_from_filename.lower().strip()
        normalized_imdb = imdb_title_raw.lower().strip()
        
        similarity = fuzz.ratio(normalized_original, normalized_imdb)
        token_set_similarity = fuzz.token_set_ratio(normalized_original, normalized_imdb)

        logger.info(f"IMDb Check: Original='{normalized_original}', IMDb='{normalized_imdb}', Ratio Similarity={similarity}%, Token Set Similarity={token_set_similarity}%")

        # Use a high threshold, but token_set_ratio is good for handling extra words
        if token_set_similarity < 95 and similarity < 85:
            logger.warning(f"IMDb mismatch REJECTED! Original: '{title_from_filename}', IMDb: '{imdb_title_raw}', Similarity too low.")
            return None, None

        await loop.run_in_executor(None, lambda: ia.update(movie, info=['main']))
        
        imdb_title = movie.get('title')
        imdb_year = movie.get('year')
        logger.info(f"IMDb match ACCEPTED for '{title_from_filename}': '{imdb_title} ({imdb_year})'")
        return imdb_title, imdb_year

    except Exception as e:
        logger.error(f"Error fetching data from IMDb for '{title_from_filename}': {e}")
        return None, None

async def clean_and_parse_filename(name: str, cache: dict = None):
    """
    A next-gen, multi-pass robust filename parser that preserves all metadata.
    """
    original_name = name
    
    # --- PASS 0: RADICAL PRE-CLEANING ---
    name_for_parsing = name.replace('_', ' ').replace('.', ' ')
    name_for_parsing = re.sub(r'www\..*?\..*?(?=\s|$)', '', name_for_parsing, flags=re.IGNORECASE)
    
    # More aggressive bracket cleaning for PTN, but keep original for episode parsing
    name_for_ptn = re.sub(r'\[.*?\]', '', name_for_parsing).strip()
    name_for_ptn = re.sub(r'\(.*?\)', '', name_for_ptn).strip()


    season_info_str = ""
    episode_info_str = ""

    # --- NEW PASS 1: High-confidence numeric range episode detection ---
    # Looks for patterns like [09 To 12 Eps] or [01-12] etc.
    search_name_for_eps = name.replace('_', ' ').replace('.', ' ')
    range_patterns = [
        # Matches [09 To 12 Eps], [09 - 12]
        r'\[\s*(\d{1,4})\s*(?:to|-|–|—)\s*(\d{1,4})\s*(?:Eps?)?\s*\]',
        # Matches E01-E12, EP01-12, Episode 01 to 12
        r'\b(?:E|EP|Episode)s?[\s.]?(\d{1,4})\s*(?:to|-|–|—)\s*(\d{1,4})\b',
        # Matches a range directly after a season tag, e.g., S01 01-12
        r'\bS(\d{1,2})[\s.]?(\d{1,4})\s*(?:to|-|–|—)\s*(\d{1,4})\b'
    ]
    for pattern in range_patterns:
        match = re.search(pattern, search_name_for_eps, re.IGNORECASE)
        if match:
            groups = match.groups()
            if pattern.startswith(r'\bS'): # Handle the third pattern
                if not season_info_str: # Only set season if not already found
                    season_info_str = f"S{int(groups[0]):02d}"
                start_ep, end_ep = groups[1], groups[2]
            else:
                start_ep, end_ep = groups[0], groups[1]
            
            # Ensure it's an increasing range
            if int(start_ep) < int(end_ep):
                episode_info_str = f"E{int(start_ep):02d}-E{int(end_ep):02d}"
                name_for_parsing = name_for_parsing.replace(match.group(0), ' ', 1)
                name_for_ptn = name_for_ptn.replace(match.group(0), ' ', 1)
                break 

    # --- PASS 2: High-confidence combined Season/Episode patterns (if not found above) ---
    if not episode_info_str:
        combined_patterns = {
            r'\bS(\d{1,2})\s*EP?(\d{1,4})\s*[-–—\s]*EP?(\d{1,4})\b': ('season', 'start_ep', 'end_ep'),
            r'\bS(\d{1,2})\s*EP?(\d{1,4})\s+to\s+EP?(\d{1,4})\b': ('season', 'start_ep', 'end_ep'),
            r'\[\s*S(\d{1,2})\s*E?P?\s*(\d{1,4})\s*[-–—]\s*E?P?(\d{1,4})\s*\]': ('season', 'start_ep', 'end_ep'),
            r'\[\s*S(\d{1,2})\s*E?P?\s*(\d{1,4})\s+to\s+E?P?(\d{1,4})\s*\]': ('season', 'start_ep', 'end_ep'),
        }
        for pattern, groups in combined_patterns.items():
            match = re.search(pattern, search_name_for_eps, re.IGNORECASE)
            if match:
                if not season_info_str:
                    season_info_str = f"S{int(match.group(1)):02d}"
                episode_info_str = f"E{int(match.group(2)):02d}-E{int(match.group(3)):02d}"
                name_for_parsing = name_for_parsing.replace(match.group(0), ' ', 1)
                name_for_ptn = name_for_ptn.replace(match.group(0), ' ', 1)
                break

    # --- PASS 3: Independent Season and Single Episode patterns ---
    if not season_info_str:
        season_match = re.search(r'\b(S|Season)\s*(\d{1,2})\b', search_name_for_eps, re.IGNORECASE)
        if season_match:
            season_info_str = f"S{int(season_match.group(2)):02d}"
            name_for_parsing = name_for_parsing.replace(season_match.group(0), ' ', 1)
            name_for_ptn = name_for_ptn.replace(season_match.group(0), ' ', 1)

    # --- PASS 4: PTN as a fallback on the pre-cleaned name ---
    parsed_info = PTN.parse(name_for_ptn)
    
    initial_title = parsed_info.get('title', '').strip()
    if not season_info_str and parsed_info.get('season'):
        season_info_str = f"S{parsed_info.get('season'):02d}"
    if not episode_info_str and parsed_info.get('episode'):
        episode = parsed_info.get('episode')
        if isinstance(episode, list):
            if len(episode) > 1: episode_info_str = f"E{min(episode):02d}-E{max(episode):02d}"
            elif episode: episode_info_str = f"E{episode[0]:02d}"
        else: episode_info_str = f"E{episode:02d}"
    
    year = parsed_info.get('year')

    # --- PASS 5: Aggressive Title Cleaning on the result of PTN ---
    title_to_clean = initial_title
    
    if year:
        title_to_clean = re.sub(r'\b' + str(year) + r'\b', '', title_to_clean)

    title_to_clean = re.sub(r'\bS\d{1,2}\b|\bE\d{1,4}\b', '', title_to_clean, flags=re.IGNORECASE)
    
    junk_words = [
        'Ep', 'Eps', 'Episode', 'Episodes', 'Season', 'Series', 'South', 'Dubbed', 'Completed',
        'Web', r'\d+Kbps', 'UNCUT', 'ORG', 'HQ', 'ESubs', 'MSubs', 'REMASTERED', 'REPACK',
        'PROPER', 'iNTERNAL', 'Sample', 'Video', 'Dual', 'Audio', 'Multi', 'Hollywood',
        'New', 'Combined', 'Complete', 'Chapter', 'PSA', 'JC', 'DIDAR', 'StarBoy',
        'Hindi', 'English', 'Tamil', 'Telugu', 'Kannada', 'Malayalam', 'Punjabi', 'Japanese', 'Korean',
        'NF', 'AMZN', 'MAX', 'DSNP', 'ZEE5', 'WEB-DL', 'HDRip', 'WEBRip', 'HEVC', 'x265', 'x264', 'AAC'
    ]
    junk_pattern_re = r'\b(' + r'|'.join(junk_words) + r')\b'
    cleaned_title = re.sub(junk_pattern_re, '', title_to_clean, flags=re.IGNORECASE)
    
    cleaned_title = re.sub(r'\b\d{1,4}\b', '', cleaned_title) # remove any remaining year-like numbers
    cleaned_title = re.sub(r'[-_.]', ' ', cleaned_title) # replace separators with spaces
    cleaned_title = re.sub(r'\s+', ' ', cleaned_title).strip() # normalize whitespace
    
    # If cleaning results in an empty string, fall back to the original name's base
    if not cleaned_title: cleaned_title = " ".join(original_name.split('.')[:-1])

    # --- PASS 6: IMDb Verification (with Caching) ---
    if not year:
        found_years = re.findall(r'\b(19[89]\d|20[0-3]\d)\b', original_name)
        if found_years: year = found_years[0]
        
    definitive_title, definitive_year = None, None
    cache_key = f"{cleaned_title}_{year}" if year else cleaned_title
    if cache is not None and cache_key in cache:
        definitive_title, definitive_year = cache[cache_key]
        logger.info(f"IMDb CACHE HIT for '{cache_key}'")
    else:
        # Use the aggressively cleaned title for the IMDb search
        definitive_title, definitive_year = await get_definitive_title_from_imdb(cleaned_title)
        if cache is not None:
            cache[cache_key] = (definitive_title, definitive_year)

    # --- PASS 7: Reconstruct and Return ---
    # Use the definitive title from IMDb if found, otherwise use our cleaned title
    final_title = definitive_title if definitive_title else cleaned_title.title()
    final_year = definitive_year if definitive_year else year
    
    is_series = season_info_str != "" or episode_info_str != ""

    display_title = f"{final_title.strip()}" + (f" ({final_year})" if final_year else "")
        
    return {
        "batch_title": f"{final_title} {season_info_str}".strip(),
        "display_title": display_title.strip(),
        "year": final_year,
        "is_series": is_series,
        "season_info": season_info_str, 
        "episode_info": episode_info_str,
        "quality_tags": " | ".join(filter(None, [parsed_info.get('resolution'), parsed_info.get('quality'), parsed_info.get('codec'), parsed_info.get('audio')]))
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
    post_poster = await get_poster(primary_display_title, first_info['year']) if user.get('show_poster', True) else None
    
    footer_buttons = user.get('footer_buttons', [])
    footer_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(btn['name'], url=btn['url'])] for btn in footer_buttons]) if footer_buttons else None
    
    header_line, footer_line = "▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱", "\n\n" + "•·•·•·•·•·•·•·•·•·••·•·•·•·•·•·•·•"
    CAPTION_LIMIT = PHOTO_CAPTION_LIMIT if post_poster else TEXT_MESSAGE_LIMIT
    
    all_link_entries = []
    for info in media_info_list:
        display_tags_parts = []
        
        # New logic to format episode string as "EP 01-05"
        if info.get('episode_info'):
            numbers = re.findall(r'\d+', info['episode_info'])
            if numbers:
                ep_text = '-'.join(f"{int(n):02d}" for n in numbers)
                display_tags_parts.append(f"EP {ep_text}")

        if info.get('quality_tags'):
            display_tags_parts.append(info['quality_tags'])
        
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

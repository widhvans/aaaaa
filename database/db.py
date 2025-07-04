import datetime
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from config import Config

client = AsyncIOMotorClient(Config.MONGO_URI)
db = client[Config.DATABASE_NAME]
logger = logging.getLogger(__name__)

users = db['users']
files = db['files']
bot_settings = db['bot_settings']
verified_users = db['verified_users']

async def add_user(user_id):
    """Adds a new user to the database if they don't already exist."""
    user_data = {
        'user_id': user_id, 'post_channels': [], 'index_db_channel': None,
        'shortener_url': None, 'shortener_api': None, 'fsub_channel': None,
        'filename_url': None, 'footer_buttons': [], 'show_poster': True,
        'shortener_enabled': True, 'how_to_download_link': None,
        'shortener_mode': 'each_time'
    }
    await users.update_one({'user_id': user_id}, {"$setOnInsert": user_data}, upsert=True)

async def is_user_verified(requester_id: int, owner_id: int) -> bool:
    try:
        verification = await verified_users.find_one({'requester_id': requester_id, 'owner_id': owner_id})
        if not verification or 'verified_at' not in verification or not isinstance(verification['verified_at'], datetime.datetime):
            return False
        twelve_hours_ago = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
        return verification['verified_at'] > twelve_hours_ago
    except Exception as e:
        logger.error(f"An error occurred in is_user_verified check: {e}")
        return False

async def add_user_verification(requester_id: int, owner_id: int):
    await verified_users.update_one(
        {'requester_id': requester_id, 'owner_id': owner_id},
        {"$set": {'verified_at': datetime.datetime.utcnow()}},
        upsert=True
    )

async def claim_verification_for_file(owner_id: int, file_unique_id: str, requester_id: int) -> bool:
    """Marks a file as 'verification claimed' for a specific owner to prevent reuse."""
    unclaimed_file_query = {
        'owner_id': owner_id, 
        'file_unique_id': file_unique_id, 
        'verification_claimed': {'$ne': True}
    }
    result = await files.update_one(unclaimed_file_query, {'$set': {'verification_claimed': True}})
    if result.modified_count > 0:
        await add_user_verification(requester_id, owner_id)
        return True
    return False

async def set_post_channel(user_id: int, channel_id: int):
    """Saves the post channel ID for a specific user."""
    await users.update_one({'user_id': user_id}, {'$addToSet': {'post_channels': channel_id}})

async def get_post_channel(user_id: int):
    """Retrieves the post channel ID for a specific user."""
    user = await users.find_one({'user_id': user_id})
    # Assuming one post channel for now, can be modified for multiple
    return user.get('post_channels')[0] if user and user.get('post_channels') else None

async def set_index_db_channel(user_id: int, channel_id: int):
    """Saves the index DB channel ID for a specific user."""
    await users.update_one({'user_id': user_id}, {'$set': {'index_db_channel': channel_id}}, upsert=True)

async def get_index_db_channel(user_id: int):
    """Retrieves the index DB channel ID for a specific user."""
    user = await users.find_one({'user_id': user_id})
    return user.get('index_db_channel') if user else None

async def save_file_data(owner_id, original_message, copied_message, stream_message):
    """Saves file metadata, including the new stream_id."""
    from utils.helpers import get_file_raw_link
    original_media = getattr(original_message, original_message.media.value)
    raw_link = await get_file_raw_link(copied_message)
    file_data = {
        'owner_id': owner_id,
        'file_unique_id': original_media.file_unique_id,
        'file_id': copied_message.id,
        'stream_id': stream_message.id,
        'file_name': original_media.file_name,
        'file_size': original_media.file_size,
        'raw_link': raw_link
    }
    await files.update_one(
        {'owner_id': owner_id, 'file_unique_id': original_media.file_unique_id},
        {'$set': file_data}, upsert=True
    )

async def get_user(user_id):
    return await users.find_one({'user_id': user_id})

async def get_all_user_ids(storage_owners_only=False):
    query = {}
    if storage_owners_only:
        query = {"$or": [{"post_channels": {"$exists": True, "$ne": []}}, {"index_db_channel": {"$exists": True, "$ne": None}}]}
    cursor = users.find(query, {'user_id': 1})
    return [doc['user_id'] for doc in await cursor.to_list(length=None) if 'user_id' in doc]

async def get_storage_owner_ids():
    query = {"$or": [{"post_channels": {"$exists": True, "$ne": []}}, {"index_db_channel": {"$exists": True, "$ne": None}}]}
    cursor = users.find(query, {'user_id': 1})
    return [doc['user_id'] for doc in await cursor.to_list(length=None) if 'user_id' in doc]

async def get_normal_user_ids():
    all_users_cursor = users.find({}, {'user_id': 1})
    storage_owners_cursor = users.find({"$or": [{"post_channels": {"$exists": True, "$ne": []}}, {"index_db_channel": {"$exists": True, "$ne": None}}]}, {'user_id': 1})
    all_user_ids = {doc['user_id'] for doc in await all_users_cursor.to_list(length=None) if 'user_id' in doc}
    storage_owner_ids = {doc['user_id'] for doc in await storage_owners_cursor.to_list(length=None) if 'user_id' in doc}
    return list(all_user_ids - storage_owner_ids)

async def get_storage_owners_count():
    query = {"$or": [{"post_channels": {"$exists": True, "$ne": []}}, {"index_db_channel": {"$exists": True, "$ne": None}}]}
    return await users.count_documents(query)

async def update_user(user_id, key, value):
    await users.update_one({'user_id': user_id}, {'$set': {key: value}}, upsert=True)

async def add_to_list(user_id, list_name, item):
    await users.update_one({'user_id': user_id}, {'$addToSet': {list_name: item}})

async def remove_from_list(user_id, list_name, item):
    await users.update_one({'user_id': user_id}, {'$pull': {list_name: item}})

async def find_owner_by_index_channel(channel_id):
    user = await users.find_one({'index_db_channel': channel_id})
    return user['user_id'] if user else None

async def get_file_by_unique_id(owner_id: int, file_unique_id: str):
    """Fetches a file based on its owner and unique_id."""
    return await files.find_one({'owner_id': owner_id, 'file_unique_id': file_unique_id})

async def get_user_file_count(owner_id):
    return await files.count_documents({'owner_id': owner_id})

async def get_all_user_files(user_id):
    return files.find({'owner_id': user_id})

async def get_paginated_files(user_id, page: int, page_size: int = 5):
    skip = (page - 1) * page_size
    cursor = files.find({'owner_id': user_id}).sort('_id', -1).skip(skip).limit(page_size)
    return await cursor.to_list(length=page_size)

async def search_user_files(user_id, query: str, page: int, page_size: int = 5):
    search_filter = {'owner_id': user_id, 'file_name': {'$regex': query, '$options': 'i'}}
    skip = (page - 1) * page_size
    total_files = await files.count_documents(search_filter)
    cursor = files.find(search_filter).sort('_id', -1).skip(skip).limit(page_size)
    files_list = await cursor.to_list(length=page_size)
    return files_list, total_files

async def total_users_count():
    return await users.count_documents({})

async def add_footer_button(user_id, button_name, button_url):
    button = {'name': button_name, 'url': button_url}
    await users.update_one({'user_id': user_id}, {'$push': {'footer_buttons': button}})

async def remove_footer_button(user_id, button_name):
    await users.update_one({'user_id': user_id}, {'$pull': {'footer_buttons': {'name': button_name}}})

async def delete_all_files():
    result = await files.delete_many({})
    return result.deleted_count

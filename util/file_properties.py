# util/file_properties.py (NEW FILE)

from pyrogram import Client
from typing import Any, Optional
from pyrogram.types import Message
from pyrogram.file_id import FileId

class FileIdError(Exception):
    pass

async def parse_file_id(message: "Message") -> Optional[FileId]:
    media = get_media_from_message(message)
    if media:
        return FileId.decode(media.file_id)

async def get_file_properties(client: Client, message_id: int):
    # stream_channel ya owner_db_channel se message fetch karein
    stream_channel = client.stream_channel_id or client.owner_db_channel_id
    if not stream_channel:
        raise ValueError("Neither Stream Channel nor Owner DB Channel is configured.")
    
    message = await client.get_messages(chat_id=stream_channel, message_ids=message_id)
    
    if not message or not message.media:
        raise FileIdError("Message not found or has no media.")
        
    media = get_media_from_message(message)
    file_id = await parse_file_id(message)
    
    # File properties ko file_id object mein attach karein
    setattr(file_id, "file_size", getattr(media, "file_size", 0))
    setattr(file_id, "mime_type", getattr(media, "mime_type", "application/octet-stream"))
    setattr(file_id, "file_name", getattr(media, "file_name", "unknown"))
    
    return file_id

def get_media_from_message(message: "Message") -> Any:
    media_types = (
        "audio", "document", "photo", "sticker", "animation", 
        "video", "voice", "video_note",
    )
    for attr in media_types:
        media = getattr(message, attr, None)
        if media:
            return media
    return None

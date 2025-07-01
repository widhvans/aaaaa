import asyncio
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated

async def broadcast_message(client, user_ids, message):
    success_count = 0
    fail_count = 0
    
    for user_id in user_ids:
        try:
            await message.copy(chat_id=user_id)
            success_count += 1
            await asyncio.sleep(0.1) # Add a small delay to avoid flood waits
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await message.copy(chat_id=user_id)
            success_count += 1
        except (UserIsBlocked, InputUserDeactivated):
            fail_count += 1
        except Exception:
            fail_count += 1
            
    return success_count, fail_count

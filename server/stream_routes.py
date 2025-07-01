# server/stream_routes.py (FULL REPLACEMENT - Final Stable Version)

import logging
import asyncio
from aiohttp import web
from pyrogram.errors import FileIdInvalid

logger = logging.getLogger(__name__)
routes = web.RouteTableDef()


@routes.get("/", allow_head=True)
async def root_route_handler(request):
    bot_username = request.app['bot'].me.username
    return web.json_response({
        "server_status": "running",
        "bot_status": f"connected_as @{bot_username}"
    })


@routes.get("/favicon.ico", allow_head=True)
async def favicon_handler(request):
    return web.Response(status=204)


@routes.get("/watch/{message_id:\\d+}", allow_head=True)
async def watch_handler(request: web.Request):
    try:
        message_id = int(request.match_info["message_id"])
        bot = request.app['bot']
        from util.render_template import render_page
        return web.Response(
            text=await render_page(bot, message_id),
            content_type='text/html'
        )
    except Exception as e:
        logger.critical(f"Unexpected error in watch handler: {e}", exc_info=True)
        return web.Response(text="Internal Server Error", status=500)


async def stream_or_download(request: web.Request, disposition: str):
    """
    Handles streaming by piping data directly from Telegram to the client.
    This method is optimized for stability and speed, avoiding disk I/O and complex session handling.
    """
    try:
        message_id = int(request.match_info.get("message_id"))
        bot = request.app['bot']

        # Stream channel ya owner DB channel se chat_id lein
        chat_id = bot.stream_channel_id or bot.owner_db_channel_id
        if not chat_id:
            raise ValueError("Streaming channels not configured on the bot.")

        # Telegram se message object prapt karein
        message = await bot.get_messages(chat_id=chat_id, message_ids=message_id)

        if not message or not message.media:
            return web.Response(text="File not found or has no media.", status=404)

        media = getattr(message, message.media.value)
        file_name = getattr(media, "file_name", "unknown.dat")
        file_size = getattr(media, "file_size", 0)

        headers = {
            "Content-Type": getattr(media, "mime_type", "application/octet-stream"),
            "Content-Disposition": f'{disposition}; filename="{file_name}"',
            "Content-Length": str(file_size)
        }
        
        response = web.StreamResponse(status=200, headers=headers)
        await response.prepare(request)
        
        # Seedhe Telegram se user tak stream karein, bina disk aur manual sessions ke
        async for chunk in bot.stream_media(message):
            try:
                await response.write(chunk)
            except (ConnectionError, asyncio.CancelledError):
                logger.warning(f"Client disconnected for message {message_id}. Stopping stream.")
                # Client chala gaya, isliye stream band kar dein
                break
        
        return response

    except (FileIdInvalid, ValueError) as e:
        logger.error(f"File ID or configuration error for stream request: {e}")
        return web.Response(text="File not found, link may have expired, or bot is misconfigured.", status=404)
    except Exception:
        logger.critical("FATAL: Unexpected error in stream/download handler", exc_info=True)
        return web.Response(text="Internal Server Error", status=500)


@routes.get("/stream/{message_id:\\d+}", allow_head=True)
async def stream_handler(request: web.Request):
    """Handler for inline video playback."""
    return await stream_or_download(request, "inline")


@routes.get("/download/{message_id:\\d+}", allow_head=True)
async def download_handler(request: web.Request):
    """Handler for direct file downloads."""
    return await stream_or_download(request, "attachment")

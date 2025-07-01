import jinja2
import aiofiles
import logging  # <-- FIX 1: Missing import add kiya gaya
from pyrogram import Client
from util.custom_dl import ByteStreamer  # Naye streaming engine ko import karein

async def render_page(bot: Client, message_id: int):
    """
    Naye streaming engine ka istemal karke watch page ke liye HTML template render karta hai.
    """
    streamer = ByteStreamer(bot)
    file_name = "File"  # Default naam

    try:
        # --- FIX 2: Naye engine ke sahi function (get_file_properties) ka istemal karein ---
        file_id = await streamer.get_file_properties(message_id)
        # Ab file_name seedhe file_id object se mil jayega
        if file_id and file_id.file_name:
            file_name = file_id.file_name.replace("_", " ")

    except Exception as e:
        # Agar file properties nahi milti hai, to error log karein
        logging.error(f"Could not get file properties for watch page (message_id {message_id}): {e}")

    # Stream aur download URLs banayein
    stream_url = f"http://{bot.vps_ip}:{bot.vps_port}/stream/{message_id}"
    download_url = f"http://{bot.vps_ip}:{bot.vps_port}/download/{message_id}"
    
    # Jinja2 template ko read aur render karein
    try:
        async with aiofiles.open('template/watch_page.html', 'r') as f:
            template_content = await f.read()
        template = jinja2.Template(template_content)

        return template.render(
            heading=f"Watch {file_name}",
            file_name=file_name,
            stream_url=stream_url,
            download_url=download_url
        )
    except FileNotFoundError:
        logging.error("FATAL: watch_page.html template not found in /template directory!")
        return "<html><body><h1>500 Internal Server Error</h1><p>Template file not found.</p></body></html>"
    except Exception as e:
        logging.error(f"Error rendering template: {e}", exc_info=True)
        return "<html><body><h1>500 Internal Server Error</h1><p>Could not render template.</p></body></html>"

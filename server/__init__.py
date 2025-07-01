from aiohttp import web
from .stream_routes import routes

async def web_server(bot_instance):
    """Initializes the web server and attaches the bot instance."""
    web_app = web.Application(client_max_size=30000000)
    web_app['bot'] = bot_instance  # Store bot instance for handlers
    web_app.add_routes(routes)
    return web_app

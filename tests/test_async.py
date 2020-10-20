import asyncio

import asyncpg
from aiohttp import web

loop = asyncio.get_event_loop()
loop.create_server(["127.0.0.1", "localhost"], port=1234)


async def handle(request):
    """Handle incoming requests."""
    pool = request.app["pool"]
    power = int(request.match_info.get("power", 10))

    # Take a connection from the pool.
    async with pool.acquire() as connection:
        # Open a transaction.
        async with connection.transaction():
            # Run the query passing the request argument.
            result = await connection.fetchval("select 2 ^ $1", power)
            return web.Response(text="2 ^ {} is {}".format(power, result))


async def init_app():
    """Initialize the application server."""
    app = web.Application()
    # Create a database connection pool
    dsn = "postgres://troc-pgdata:z!7ru$7aNuy=za@localhost:5432/navigator"
    app["pool"] = await asyncpg.create_pool(
        dsn=dsn, database="navigator", max_size=50, min_size=5, loop=loop
    )
    # Configure service routes
    app.router.add_route("GET", "/{power:\d+}", handle)
    app.router.add_route("GET", "/", handle)
    return app


# loop = asyncio.get_event_loop()
app = loop.run_until_complete(init_app())
web.run_app(app)

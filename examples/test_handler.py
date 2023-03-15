import sys
import asyncio
import logging
from asyncdb.exceptions.handlers import default_exception_handler

# Set up the logger with a custom StreamHandler
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# Sample asynchronous function that raises an exception
async def raise_exception():
    await asyncio.sleep(1)
    raise ValueError("This is a sample exception")

async def main():
    # Set the custom exception handler as the default
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(default_exception_handler)
    # Schedule the raise_exception() coroutine
    task = asyncio.create_task(raise_exception())

    # Wait for the task to finish
    await task
    # # Schedule the raise_exception() coroutine using asyncio.gather()
    # results = await asyncio.gather(raise_exception(), return_exceptions=True)
    # for exc in [r for r in results if isinstance(r, BaseException)]:
    #     logger.debug("message", exc_info=exc)

# Run the main() coroutine
asyncio.run(main())

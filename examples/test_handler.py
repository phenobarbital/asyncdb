import sys
import asyncio
from navconfig.logging import logging, logger
from asyncdb.exceptions.handlers import default_exception_handler

# Set up the logger with a custom StreamHandler
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

loop = asyncio.new_event_loop()
# Set the custom exception handler as the default
loop.set_exception_handler(default_exception_handler)
loop.set_debug(True)

# Sample asynchronous function that raises an exception
async def raise_exception():
    await asyncio.sleep(1)
    logger.warning(':: Prevent to fail ::')
    raise ValueError("This is a sample exception")

async def test():
    logger.warning(':: Prevent to fail ::')
    raise Exception("Something goes wrong")

async def main():
    # Schedule the raise_exception() coroutine
    logger.info(':: Start running task :: ')
    asyncio.create_task(test())

    task = asyncio.create_task(raise_exception())
    # Wait for the task to finish
    await task
    # # Schedule the raise_exception() coroutine using asyncio.gather()
    # await asyncio.gather(*[task], return_exceptions=False)
    # for exc in [r for r in results if isinstance(r, BaseException)]:
    #     logger.debug("message", exc_info=exc)


if __name__ == "__main__":
    # Run the main() coroutine
    loop.run_until_complete(
        main()
    )

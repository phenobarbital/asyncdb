import asyncio
import pprint
import signal

from asyncdb.exceptions import (_handle_done_tasks, default_exception_handler,
                                shutdown)

loop = asyncio.new_event_loop()
loop.set_exception_handler(default_exception_handler)
asyncio.set_event_loop(loop)

signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
for s in signals:
    loop.add_signal_handler(s, lambda s=s: loop.create_task(shutdown(loop, s)))


async def bug():
    print("STARTING")
    await asyncio.sleep(2)
    raise RuntimeError("BOOM!")
    # raise Exception('Explosion in the sky!')
    # raise asyncio.exceptions.CancelledError
    # raise TypeError


async def test():
    raise RuntimeError("Explosion in the sky!")


async def main():
    # Un-comment either 1 of the following 3 lines
    # await test() # will not call exception_handler
    # await asyncio.gather(test()) # will not call exception_handler
    asyncio.create_task(test())  # will call exception_handler


if __name__ == "__main__":
    try:
        task = loop.create_task(main())
        # task.add_done_callback(_handle_done_tasks)
        # asyncio.run(bug(), debug=True)
        # result = loop.run_until_complete(bug())
        group = asyncio.gather(*[task], loop=loop, return_exceptions=False)
        try:
            results = loop.run_until_complete(group)
            print("RESULTS: ", results)
        except RuntimeError as err:
            # handle exception
            raise err
        loop.run_forever()
    except KeyboardInterrupt as e:
        print("Caught keyboard interrupt. Canceling tasks...")

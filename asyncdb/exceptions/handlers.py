from typing import (
    Any
)
import asyncio
import logging
import uvloop


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()


def handle_done_tasks(task: asyncio.Task, logger: logging.Logger, *args: tuple[Any, ...]) -> None:
    try:
        return task.result()
    except asyncio.CancelledError:
        return True # Task cancellation should not be logged as an error.
    except Exception as err:  # pylint: disable=broad-except
        logger.exception(
            f"Exception raised by Task {task}, error: {err}", *args
        )
        return None


async def shutdown(loop: asyncio.AbstractEventLoop, signal = None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logging.info(
            f"Received exit signal {signal.name}..."
        )
    else:
        logging.warning("Shutting NOT via signal")
    logging.info("Closing all connections")
    try:
        tasks = [
            task.cancel()
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        ]
        status = [task.cancel() for task in tasks]
        logging.warning(
            f"Cancelling {len(tasks)} outstanding tasks: {status}"
        )
        await asyncio.gather(*tasks, return_exceptions=True)
        logging.warning('Asyncio Shutdown: Done graceful shutdown of subtasks')
    except asyncio.CancelledError:
        pass
    except Exception as ex:
        logging.exception(ex, stack_info=True)
        raise RuntimeError(
            f"Asyncio Shutdown Error: {ex}"
        ) from ex
    finally:
        loop.close()


def default_exception_handler(loop: asyncio.AbstractEventLoop, context):
    logging.debug(
        f"Asyncio Exception Handler Caught: {context!s}"
    )
    # first, handle with default handler
    if isinstance(context, Exception):
        # is a basic exception
        logging.exception(f"Exception {context!s}", stack_info=True)
        raise type(context)
    exception = context.get('exception')
    msg = context.get("message", None)
    loop.default_exception_handler(context)
    if exception:
        logging.error(f"AsyncDB: Caught exception: {exception}")
        if not isinstance(exception, asyncio.CancelledError):
            task = context.get("task", context["future"])
            exc = type(task.exception())
            try:
                logging.error(
                    f"{exc.__name__!s}: *{msg}* over task {task}"
                )
            except Exception as ex:
                logging.exception(ex, stack_info=True)
                raise RuntimeError(
                    f"Handler Error: {ex}"
                ) from ex
    else:
        logging.error(f"AsyncDB: Caught an error: {context}")
        try:
            task = context.get("task", context["future"])
        except KeyError:
            task = None
        logging.exception(
            f"Exception raised by Task {task}, Error: {msg}"
        )
        raise RuntimeError(
            f"{msg}: task: {task}"
        )

import asyncio
import logging
from typing import (
    Any
)
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

def handle_done_tasks(task: asyncio.Task, logger: logging.Logger, *args) -> Any:
    try:
        return task.result()
    except asyncio.CancelledError:
        pass  # Task cancellation should not be logged as an error.
    except Exception as err:  # pylint: disable=broad-except
        logger.exception(
            f"Exception raised by Task {task}, error: {err}", *args
        )


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
    except Exception as e:
        logging.exception(e, stack_info=True)
        raise Exception(
            f"Asyncio Shutdown Error: {e}"
        ) from e
    finally:
        loop.stop()


def default_exception_handler(loop: asyncio.AbstractEventLoop, context):
    logging.info(
        f"Asyncio Exception Handler Caught: {context!s}"
    )
    # first, handle with default handler
    if isinstance(context, Exception):
        # is a basic exception
        logging.exception(f"Exception {context!s}", stack_info=True)
        raise type(context)
    else:
        loop.default_exception_handler(context)
        if "exception" not in context:
            try:
                task = context.get("task", context["future"])
            except KeyError:
                task = None
            msg = context.get("exception", context["message"])
            # is an error
            logging.exception(f"Exception raised by Task {task}, Error: {msg}")
            raise Exception(f"{msg}: task: {task}")
        if not isinstance(context["exception"], asyncio.CancelledError):
            try:
                task = context.get("task", context["future"])
                msg = context.get("exception", context["message"])
                exception = type(task.exception())
                try:
                    logging.exception(
                        f"{exception.__name__!s}*{msg}* over task {task}")
                    raise exception()
                except Exception as e:
                    logging.exception(e, stack_info=True)
                    raise Exception(
                        f"Handler Error: {e}"
                    ) from e
            except KeyError:
                logging.exception(context, stack_info=True)

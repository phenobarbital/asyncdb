from typing import Any
import asyncio
import logging


def handle_done_tasks(task: asyncio.Task, logger: logging.Logger, *args: tuple[Any, ...]) -> None:
    try:
        return task.result()
    except asyncio.CancelledError:
        return True  # Task cancellation should not be logged as an error.
    except Exception as err:  # pylint: disable=broad-except
        logger.exception(f"Exception raised by Task {task}, error: {err}")
        return None


async def shutdown(loop: asyncio.AbstractEventLoop, signal=None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logging.info(f"Received exit signal {signal.name}...")
    else:
        logging.warning("Shutting NOT via signal")
    logging.info("Closing all connections")
    try:
        tasks = [
            task.cancel() for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()
        ]
        status = [task.cancel() for task in tasks]
        logging.warning(f"Cancelling {len(tasks)} outstanding tasks: {status}")
        await asyncio.gather(*tasks, return_exceptions=True)
        logging.warning("Asyncio Shutdown: Done graceful shutdown of subtasks")
    except asyncio.CancelledError:
        pass
    except Exception as ex:
        logging.exception(ex, stack_info=True)
        raise RuntimeError(f"Asyncio Shutdown Error: {ex}") from ex
    finally:
        loop.close()


def default_exception_handler(loop: asyncio.AbstractEventLoop, context):
    logging.debug(f"Asyncio Exception Handler Caught: {context!s}")

    exception = context.get("exception")
    msg = context.get("message", "")
    task = context.get("task") or context.get("future")

    # Call the default exception handler
    loop.default_exception_handler(context)

    if not exception and not msg:
        logging.error("AsyncDB: Caught an error with no exception or message in context.")
        raise RuntimeError(f"Unknown error: task: {task}")

    if isinstance(exception, asyncio.CancelledError):
        return

    try:
        if exception:
            exc_name = type(exception).__name__
            task_info = f"Task {task}" if task else "Unknown task"
            logging.error(f"{exc_name}: *{msg}* over {task_info}")
        else:
            logging.error(f"AsyncDB: Caught an error: {context}")
            if task:
                logging.exception(f"Exception raised by Task {task}, Error: {msg}")
            raise RuntimeError(f"{msg}: task: {task}")
    except Exception as ex:
        logging.exception("Handler Error", exc_info=True)
        raise RuntimeError(f"Handler Error: {ex}") from ex

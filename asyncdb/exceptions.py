import asyncio
import logging
from typing import (
    Any, Tuple
)


def _handle_done_tasks(
            task: asyncio.Task,
            logger: logging.Logger,
            *args: Tuple[Any, ...]
) -> Any:
    try:
        return task.result()
    except asyncio.CancelledError:
        pass  # Task cancellation should not be logged as an error.
    except Exception as err:  # pylint: disable=broad-except
        logger.exception(f"Exception raised by Task {task}, error: {err}", *args)


async def shutdown(loop: asyncio.AbstractEventLoop, signal: Any = None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logging.info(f"Received exit signal {signal.name}...")
    else:
        logging.warning("Shutting NOT via signal")
    logging.info("Closing all connections")
    try:
        tasks = [
            task.cancel()
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        ]
        [task.cancel() for task in tasks]
        logging.warning(f"Cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks, loop=loop, return_exceptions=True)
        logging.warning('Asyncio Shutdown: Done graceful shutdown of subtasks')
    except asyncio.CancelledError:
        print("All Tasks has been canceled")
    except Exception as err:
        print("Asyncio Generic Error", err)
    finally:
        loop.stop()


def default_exception_handler(loop: asyncio.AbstractEventLoop, context: Any):
    logging.info(f"AsyncDB Exception Handler Caught: {context!s}")
    # first, handle with default handler
    if isinstance(context, Exception):
        # is a basic exception
        logging.exception(f"Exception {context!s}")
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
                except Exception as err:
                    logging.exception(err)
            except KeyError:
                logging.exception(context, stack_info=True)


class AsyncDBException(Exception):
    """Base class for other exceptions"""

    code: int = 0
    message: str = ''

    def __init__(self, *args: object, message: str = '', code: int = None) -> None:
        self.args = (
            message,
            code,
            *args
        )
        self.message = message
        self.code = code
        super(AsyncDBException, self).__init__(message)

    def __repr__(self):
        return f"{__name__}({self.args!r})"

    def __str__(self):
        return f"{__name__}: {self.message}"

    def get(self):
        return self.message


class ProviderError(AsyncDBException):
    """Database Provider Error"""


class DataError(AsyncDBException, ValueError):
    """An error caused by invalid query input."""


class NotSupported(AsyncDBException):
    """Not Supported functionality"""


class UninitializedError(ProviderError):
    """Exception when provider cannot be initialized"""


class ConnectionTimeout(ProviderError):
    """Connection Timeout Error"""


class NoDataFound(ProviderError):
    """Raise when no data was found"""

    message = "No Data was Found"


class TooManyConnections(ProviderError):
    """Too Many Connections"""


class EmptyStatement(AsyncDBException):
    """Raise when no Statement was found"""


class UnknownPropertyError(ProviderError):
    """Raise when invalid property was provided"""


class StatementError(ProviderError):
    """Raise when statement Error"""


class ConditionsError(ProviderError):
    """Raise when Failed Conditions"""

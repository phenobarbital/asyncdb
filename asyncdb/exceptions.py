import asyncio
import logging
from typing import Any


# from pprint import pprint
async def shutdown(loop, signal=None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logging.info(f"Received exit signal {signal.name}...")
    else:
        logging.warning(f"Shutting NOT via signal")
    logging.info("Closing all connections")
    try:
        tasks = [
            task.cancel()
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        ]
        # [task.cancel() for task in tasks]
        logging.info(f"Cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        print("All Tasks has been canceled")
    except Exception as err:
        print("Asyncio Generic Error", err)
    finally:
        loop.stop()
        # loop.close()


def default_exception_handler(loop, context: Any):
    logging.info(f"Exception Handler Caught: {context!s}")
    # first, handle with default handler
    if isinstance(context, Exception):
        # is a basic exception
        logging.exception(f"Exception {context!s}")
        raise type(context)
    else:
        loop.default_exception_handler(context)
        if not "exception" in context:
            try:
                task = context.get("task", context["future"])
            except KeyError:
                task = None
            msg = context.get("exception", context["message"])
            # is an error
            logging.exception(f"Exception raised by Task {task}, Error: {msg}")
            raise Exception(f"{msg}: task: {task}")
        if not isinstance(context["exception"], asyncio.CancelledError):
            task = context.get("task", context["future"])
            msg = context.get("exception", context["message"])
            exception = type(task.exception())
            try:
                logging.exception(f"{exception.__name__!s}*{msg}* over task {task}")
                raise exception(msg)
            except Exception as err:
                logging.exception(err)
            # finally:
            #     loop.stop()
            #     logging.info("Successfully shutdown the AsyncDB Loop.")
                # loop.close()


def _handle_done_tasks(task: asyncio.Task) -> Any:
    try:
        return task.result()
    except asyncio.CancelledError:
        pass  # Task cancellation should not be logged as an error.
    except Exception as err:
        logging.exception(f"Exception raised by Task {task}, error: {err}")


class asyncDBException(Exception):
    """Base class for other exceptions"""

    code: int = 0

    def __init__(self, message: str, *args, code: int = None, **kwargs):
        super(asyncDBException, self).__init__()
        self.args = (
            message,
            code,
        )
        self.message = message
        if code:
            self.code = code

    def __repr__(self):
        return f"{__name__}(message={self.message})"

    def __str__(self):
        return f"{self.message}"

    def get(self):
        return self.message


class ProviderError(asyncDBException):
    """Database Provider Error"""

    def __init__(self, message: str, *args, code: int = None, **kwargs):
        asyncDBException.__init__(self, message, code, *args, **kwargs)


class DataError(asyncDBException, ValueError):
    """An error caused by invalid query input."""


class NotSupported(asyncDBException):
    """Not Supported functionality"""


class UninitializedError(ProviderError):
    """Exception when provider cant be initialized"""


class ConnectionTimeout(ProviderError):
    """Connection Timeout Error"""


class NoDataFound(ProviderError):
    """Raise when no data was found"""

    message = "No Data was Found"


class TooManyConnections(ProviderError):
    """Too Many Connections"""


class EmptyStatement(asyncDBException):
    """Raise when no Statement was found"""


class UnknownPropertyError(ProviderError):
    """Raise when invalid property was provide"""


class StatementError(ProviderError):
    """Raise when an Statement Error"""


class ConditionsError(ProviderError):
    """Raise when Failed Conditions"""

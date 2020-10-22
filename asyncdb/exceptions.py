import logging
import asyncio
from typing import Any

async def shutdown(loop, signal=None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logging.info(f"Received exit signal {signal.name}...")
    logging.info("Closing all connections")
    try:
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
        [task.cancel() for task in tasks]
        logging.info(f"Cancelling {len(tasks)} outstanding tasks")
        await asyncio.gather(*tasks, return_exceptions=False)
    except asyncio.CancelledError:
        print('All Tasks has been canceled')
    except Exception as err:
        print('Asyncio Generic Error', err)

def default_exception_handler(loop, context: Any):
    # first, handle with default handler
    if context:
        loop.default_exception_handler(context)
        logging.debug("Handled exception {0}".format(data))
        print("Exception Handler Caught")
        try:
            exception = context.get('exception', None)
            if isinstance(exception, KeyboardInterrupt):
                try:
                    logger.info("Shutting down...")
                    loop.call_soon_threadsafe(shutdown(loop))
                except Exception as e:
                    print(e)
                finally:
                    loop.close()
                    logger.info("Successfully shutdown the AsyncDB Loop.")
            msg = context.get("message", context["message"])
            print(exception, msg)
            msg = context.get("exception", context["message"])
            print("Caught DB Exception: {}".format(str(msg)))
        except (TypeError, AttributeError, KeyError, IndexError) as err:
            logging.info("Caught Exception: {}, error: {}".format(str(context), err))

def _handle_done_tasks(task: asyncio.Task) -> Any:
    result = None
    try:
        result = task.result()
    except asyncio.CancelledError:
        pass # Task cancellation should not be logged as an error.
    except Exception as err:
        logging.exception(f'Exception raised by Task {task}, error {err}')
    finally:
        return result

class asyncDBException(Exception):
    """Base class for other exceptions"""
    def __init__(self, message:str = '', code:int = None, *args, **kwargs):
        super(asyncDBException, self).__init__(*args,**kwargs)
        self.args = (message, code, )
        self.message = message
        if code:
            self.code = code
        print(args)

    def __str__(self):
        if self.code:
            return f'{__name__} -> {self.message}, code {self.code}'
        else:
            return f'{__name__} -> {self.message}'

    def get(self):
        return self.message

class ProviderError(asyncDBException):
    """Database Provider Error"""
    def __init__(self, message:str = '', code:int = None, *args,**kwargs):
        asyncDBException.__init__(self, message, code, *args,**kwargs)

class DataError(asyncDBException, ValueError):
    """An error caused by invalid query input."""


class NotSupported(asyncDBException):
    """Not Supported functionality"""

class NotImplementedError(asyncDBException):
    """Exception for Not implementation"""


class UninitializedError(ProviderError):
    """Exception when provider cant be initialized"""


class ConnectionError(ProviderError):
    """Generic Connection Error"""


class ConnectionTimeout(ProviderError):
    """Connection Timeout Error"""


class NoDataFound(ProviderError):
    """Raise when no data was found"""


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

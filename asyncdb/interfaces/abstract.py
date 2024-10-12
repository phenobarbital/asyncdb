from typing import Union, Any
import asyncio
from collections.abc import Awaitable
from abc import ABC
import logging
from contextlib import AbstractAsyncContextManager
from ..exceptions import default_exception_handler


class AbstractDriver(ABC):
    """Base driver for all Database Drivers."""

    def __init__(self, **kwargs):
        self._connection: Awaitable = None
        self._pool: Awaitable = None
        self._connected: bool = False
        self._encoding = kwargs.get("encoding", "utf-8")
        self._timeout = kwargs.get("timeout", 600)
        self._logger = logging.getLogger(f"DB.{self.__class__.__name__}")

    @property
    def log(self):
        return self._logger

    async def connection(self) -> Any:
        raise NotImplementedError()  # pragma: no cover

    open = connection

    def set_connection(self, connection):
        self._connection = connection

    async def close(self, timeout: int = 10) -> None:
        raise NotImplementedError()  # pragma: no cover

    disconnect = close

    def is_closed(self):
        if not self._connected:
            self._logger.debug(f"Connection closed on: {self._pool}")
            return True
        return False

    def pool(self):
        return self._pool

    def is_connected(self):
        return bool(self._connected)

    def get_connection(self):
        return self._connection

    engine = get_connection

    @property
    def raw_connection(self) -> Any:
        return self._connection


class EventLoopManager:
    """Basic Interface for Managing the Event Loop inside of Drivers."""

    def __init__(self, loop: Union[asyncio.AbstractEventLoop, None] = None, **kwargs):
        self._loop: Awaitable = None
        if loop:
            self._loop = loop
            asyncio.set_event_loop(self._loop)
        else:
            try:
                self._loop = asyncio.get_event_loop()
                asyncio.set_event_loop(self._loop)
            except RuntimeError:
                try:
                    self._loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(self._loop)
                except RuntimeError as e:
                    raise RuntimeError(
                        "No Event Loop is running. Please, run this driver inside an asyncio loop."
                    ) from e
        if self._loop.is_closed():
            self._loop = asyncio.get_running_loop()
            asyncio.set_event_loop(self._loop)
        # exception handler
        self._loop.set_exception_handler(default_exception_handler)

    def get_loop(self):
        return self._loop

    event_loop = get_loop

    def event_loop_is_closed(self):
        return bool(self._loop.is_closed()) if self._loop else True


class PoolContextManager(Awaitable, AbstractAsyncContextManager):
    """Async Conext version for AsyncDB pool-based drivers."""

    def __init__(self, **kwargs):
        self._connection: Awaitable = None
        self._pool: Awaitable = None

    # Magic Methods:
    async def __aenter__(self):
        if not self._pool:
            await self.connect()
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        await self.release(connection=self._connection, timeout=5)
        self._connection = None

    ### Implementing __await__ method
    def __await__(self):
        # TODO: explain how to an awaitable behavior
        # driver = await DriverContextManager()
        return self._connection.__await__()


class DriverContextManager(Awaitable, AbstractAsyncContextManager):
    """Async Conext version for AsyncDB drivers."""

    def __init__(self, **kwargs):
        self._connection: Awaitable = None
        self._pool: Awaitable = None

    ### Async Context magic Methods
    async def __aenter__(self):
        if not self._connection:
            try:
                await self.connection()
            except Exception as err:
                logging.exception(f"Closing Error: {err}", stack_info=True, stacklevel=2)
                raise
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        try:
            await asyncio.wait_for(self.close(), timeout=10)
        except asyncio.TimeoutError as e:
            logging.warning(f"Close timed out: {e}")
        except RuntimeError as e:
            self._logger.error(str(e))
        except Exception as err:
            logging.exception(f"Closing Error: {err}", stack_info=True, stacklevel=2)
            raise

    ### Implementing __await__ method
    def __await__(self):
        # TODO: explain how to an awaitable behavior
        # driver = await DriverContextManager()
        return self._connection.__await__()

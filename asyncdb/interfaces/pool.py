from typing import Optional, Union, Any
from collections.abc import Callable, Awaitable
from abc import abstractmethod
import logging
from .abstract import (
    AbstractDriver,
    PoolContextManager,
    EventLoopManager
)


class PoolBackend(AbstractDriver, PoolContextManager, EventLoopManager):
    """
    Basic Interface for Pool-based Connectors.
    """

    _provider: str = "pool"
    _syntax: str = ""  # Used by QueryParser for parsing queries
    _init_func: Optional[Callable] = None

    def __init__(
        self,
        params: dict[Any] = None,
        **kwargs
    ) -> None:
        self._connected: bool = False
        self._encoding = kwargs.get('encoding', "utf-8")
        self._max_queries = kwargs.get('max_queries', 300)
        self._timeout = kwargs.get('timeout', 600)
        if 'credentials' in kwargs:
            params = kwargs.get('credentials', {})
        AbstractDriver.__init__(
            self,
            **kwargs
        )
        PoolContextManager.__init__(
            self,
            **kwargs,
        )
        EventLoopManager.__init__(
            self,
            **kwargs
        )
        try:
            self._debug = bool(params.get("DEBUG", False))
        except (TypeError, KeyError, AttributeError):
            self._debug = kwargs.get('debug', False)
        # set the logger:
        self._logger = logging.getLogger(name=__name__)

    async def connect(self) -> "PoolBackend":
        """connect.
        async database initialization.
        """
        raise NotImplementedError()  # pragma: no cover

    open = connect

    async def disconnect(self, timeout: int = 5) -> None:
        """close.
        Closing Pool Connection.
        """
        raise NotImplementedError()  # pragma: no cover

    close = disconnect

    @abstractmethod
    async def acquire(self):
        """acquire.
        Take a connection from the pool.
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    async def release(
        self,
        connection: Union[Callable, Awaitable, None] = None,
        timeout: int = 10
    ) -> None:
        """release.
        Relase the connection back to the pool.
        """
        raise NotImplementedError()  # pragma: no cover

    @property
    def log(self):
        return self._logger

    def pool(self):
        return self._pool

    def is_connected(self):
        return self._connected

    def get_connection(self):
        return self._pool

    engine = get_connection

    def is_closed(self):
        if not self._connected:
            logging.debug(f"Connection closed on: {self._pool}")
            return True
        return False

    @classmethod
    def driver(cls):
        return cls.__name__

    @classmethod
    def dialect(cls):
        return cls._syntax
# Copyright (C) 2018-present Jesus Lara
#
"""Pure Python exception hierarchy for AsyncDB.

This module replaces the former Cython-based implementation
(exceptions.pyx / exceptions.pxd) with an equivalent pure Python
module that preserves the exact same public API and class hierarchy.
"""
from typing import Any, Optional


class AsyncDBException(Exception):
    """Base class for all AsyncDB exceptions.

    Attributes:
        message: Human-readable error description.
        code: Numeric error code (default 0).
        stacktrace: Optional stack-trace string captured at raise time.
        kwargs: Extra keyword arguments passed to the constructor.
    """

    code: int
    message: str
    stacktrace: Optional[str]
    kwargs: "dict[str, Any]"

    def __init__(
        self,
        message: Any = "",
        *args: Any,
        code: int = 0,
        **kwargs: Any,
    ) -> None:
        """Initialise the exception.

        Args:
            message: Error message string (or object with a ``.message``
                attribute).
            *args: Additional positional arguments forwarded to
                :class:`Exception`.
            code: Numeric error code.
            **kwargs: Additional keyword arguments stored in ``self.kwargs``.
                A special key ``stacktrace`` is extracted and stored in
                ``self.stacktrace``.
        """
        super().__init__(message, *args)
        if hasattr(message, "message"):
            self.message = str(message.message)
        else:
            self.message = str(message)
        self.stacktrace = kwargs.pop("stacktrace", None)
        # Store remaining kwargs separately — do NOT assign to self.args to
        # avoid shadowing Exception.args (which is a tuple of positional args).
        self.kwargs = kwargs
        self.code = int(code)

    def __repr__(self) -> str:
        """Return ``'{message}, code: {code}'``."""
        return f"{self.message}, code: {self.code}"

    def __str__(self) -> str:
        """Return ``'{message}, code: {code}'``."""
        return f"{self.message}, code: {self.code}"

    def get(self) -> str:
        """Return the error message string.

        Returns:
            The ``message`` attribute of this exception.
        """
        return self.message


class ProviderError(AsyncDBException):
    """Database Provider Error."""


class DriverError(AsyncDBException):
    """Connection Driver Error."""


class ModelError(AsyncDBException):
    """An error caused by a Data Model."""


class ConnectionMissing(AsyncDBException):
    """Error when a Connection is missing or wrong."""


class DataError(AsyncDBException):
    """An error caused by invalid query input."""


class NotSupported(AsyncDBException):
    """Not Supported functionality."""


class EmptyStatement(AsyncDBException):
    """Raise when no Statement was found."""


class UninitializedError(ProviderError):
    """Exception when provider cannot be initialized."""


class ConnectionTimeout(ProviderError):
    """Connection Timeout Error."""


class NoDataFound(ProviderError):
    """Raise when no data was found."""

    def __init__(self, message: Optional[str] = None, **kwargs: Any) -> None:
        """Initialise with default message and code=404.

        Args:
            message: Optional override for the default ``"Data Not Found"``
                message.
            **kwargs: Additional keyword arguments forwarded to the base class.
        """
        super().__init__(message or "Data Not Found", code=404, **kwargs)


class TooManyConnections(ProviderError):
    """Too Many Connections."""


class UnknownPropertyError(ProviderError):
    """Raise when an invalid property was provided."""


class StatementError(ProviderError):
    """Raise when a Statement Error occurs."""


class ConditionsError(ProviderError):
    """Raise when Failed Conditions occur."""

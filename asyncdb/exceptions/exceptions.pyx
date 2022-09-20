# cython: language_level=3, embedsignature=True
# Copyright (C) 2018-present Jesus Lara
#
cdef class AsyncDBException(Exception):
    """Base class for other AsyncDB exceptions"""
    def __init__(self, str message, *args, int code = 0):
        if not message:
            message = f"{args!s}"
        self.args = (
            message,
            code,
            *args
        )
        self.message = message
        self.code = code
        super().__init__(message)

    def __repr__(self):
        return f"{__name__}({self.args!r})"

    def __str__(self):
        return f"{__name__}: {self.message}"

    def get(self):
        return self.message


class ProviderError(AsyncDBException):
    """Database Provider Error"""
    pass

class DriverError(AsyncDBException):
    """Connection Driver Error.
    """

class ConnectionMissing(AsyncDBException):
    """Error when a Connection is missing or wrong.
    """

class DataError(ValueError):
    """An error caused by invalid query input."""


class NotSupported(AsyncDBException):
    """Not Supported functionality"""

class EmptyStatement(AsyncDBException):
    """Raise when no Statement was found"""

class UninitializedError(ProviderError):
    """Exception when provider cannot be initialized"""


class ConnectionTimeout(ProviderError):
    """Connection Timeout Error"""

class NoDataFound(ProviderError):
    """Raise when no data was found"""
    message = "No Data was Found"


class TooManyConnections(ProviderError):
    """Too Many Connections"""

class UnknownPropertyError(ProviderError):
    """Raise when invalid property was provided"""


class StatementError(ProviderError):
    """Raise when statement Error"""
    pass

class ConditionsError(ProviderError):
    """Raise when Failed Conditions"""
    pass

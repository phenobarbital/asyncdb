# cython: language_level=3, embedsignature=True, boundscheck=False, wraparound=True, initializedcheck=False
# Copyright (C) 2018-present Jesus Lara
#
cdef class AsyncDBException(Exception):
    """Base class for other exceptions"""

    code: int = 0

    def __init__(self, object message, *args, int code = 0, **kwargs):
        super().__init__(message)
        if hasattr(message, 'message'):
            self.message = message.message
        else:
            self.message = str(message)
        self.stacktrace = None
        if 'stacktrace' in kwargs:
            self.stacktrace = kwargs['stacktrace']
        self.args = kwargs
        self.code = int(code)

    def __repr__(self):
        return f"{self.message}, code: {self.code}"

    def __str__(self):
        return f"{self.message}, code: {self.code}"

    def get(self):
        return self.message

cdef class ProviderError(AsyncDBException):
    """Database Provider Error"""

cdef class DriverError(AsyncDBException):
    """Connection Driver Error.
    """

cdef class ModelError(AsyncDBException):
    """An error caused by Data Model."""

cdef class ConnectionMissing(AsyncDBException):
    """Error when a Connection is missing or wrong.
    """

cdef class DataError(AsyncDBException):
    """An error caused by invalid query input."""


cdef class NotSupported(AsyncDBException):
    """Not Supported functionality"""


cdef class EmptyStatement(AsyncDBException):
    """Raise when no Statement was found"""


cdef class UninitializedError(ProviderError):
    """Exception when provider cannot be initialized"""


cdef class ConnectionTimeout(ProviderError):
    """Connection Timeout Error"""


cdef class NoDataFound(ProviderError):
    """Raise when no data was found"""
    def __init__(self, str message = None):
        super().__init__(message or f"Data Not Found", code=404)


cdef class TooManyConnections(ProviderError):
    """Too Many Connections"""


cdef class UnknownPropertyError(ProviderError):
    """Raise when invalid property was provided"""


cdef class StatementError(ProviderError):
    """Raise when statement Error"""


cdef class ConditionsError(ProviderError):
    """Raise when Failed Conditions"""

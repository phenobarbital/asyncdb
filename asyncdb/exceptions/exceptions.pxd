# cython: language_level=3, embedsignature=True, boundscheck=False, wraparound=True, initializedcheck=False
# Copyright (C) 2018-present Jesus Lara
#
cdef class AsyncDBException(Exception):
    """Base class for other exceptions"""

cdef class ProviderError(AsyncDBException):
    """Database Provider Error"""

cdef class DriverError(AsyncDBException):
    """Connection Driver Error.
    """

cdef class ConnectionMissing(AsyncDBException):
    """Error when a Connection is missing or wrong.
    """

cdef class ModelError(AsyncDBException):
    """An error caused by Data Model."""

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

cdef class TooManyConnections(ProviderError):
    """Too Many Connections"""

cdef class UnknownPropertyError(ProviderError):
    """Raise when invalid property was provided"""

cdef class StatementError(ProviderError):
    """Raise when statement Error"""

cdef class ConditionsError(ProviderError):
    """Raise when Failed Conditions"""

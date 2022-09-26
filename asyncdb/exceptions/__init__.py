"""Exception Handler for AsyncDB.
"""
from .exceptions import (
    ProviderError,
    DriverError,
    DataError,
    NotSupported,
    UninitializedError,
    ConnectionTimeout,
    ConnectionMissing,
    NoDataFound,
    TooManyConnections,
    EmptyStatement,
    UnknownPropertyError,
    StatementError,
    ConditionsError
)
from .handlers import default_exception_handler, handle_done_tasks, shutdown


__all__ = (
    'default_exception_handler',
    'handle_done_tasks',
    'shutdown',
    'ProviderError',
    'DriverError',
    'DataError',
    'NotSupported',
    'UninitializedError',
    'ConnectionTimeout',
    'ConnectionMissing',
    'NoDataFound',
    'TooManyConnections',
    'EmptyStatement',
    'UnknownPropertyError',
    'StatementError',
    'ConditionsError'
)

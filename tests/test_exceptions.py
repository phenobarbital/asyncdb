# Copyright (C) 2018-present Jesus Lara
#
"""Unit tests for the AsyncDB pure Python exception hierarchy.

These tests validate that the pure Python replacement module (FEAT-001)
preserves the exact same API, class hierarchy, and runtime behaviour as the
former Cython-based implementation.
"""
import pytest
from asyncdb.exceptions import (
    AsyncDBException,
    ProviderError,
    DriverError,
    ModelError,
    ConnectionMissing,
    DataError,
    NotSupported,
    EmptyStatement,
    UninitializedError,
    ConnectionTimeout,
    NoDataFound,
    TooManyConnections,
    UnknownPropertyError,
    StatementError,
    ConditionsError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ALL_EXCEPTIONS = [
    AsyncDBException,
    ProviderError,
    DriverError,
    ModelError,
    ConnectionMissing,
    DataError,
    NotSupported,
    EmptyStatement,
    UninitializedError,
    ConnectionTimeout,
    NoDataFound,
    TooManyConnections,
    UnknownPropertyError,
    StatementError,
    ConditionsError,
]

# ---------------------------------------------------------------------------
# Construction — all 15 classes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("exc_class", ALL_EXCEPTIONS)
def test_can_instantiate(exc_class):
    """Every exception class must accept a message string."""
    exc = exc_class("test")
    assert isinstance(exc, Exception)


@pytest.mark.parametrize("exc_class", ALL_EXCEPTIONS)
def test_can_raise_and_catch(exc_class):
    """Every exception class must be raisable and catchable by its own type."""
    with pytest.raises(exc_class):
        raise exc_class("test")


# ---------------------------------------------------------------------------
# Base-class behaviour
# ---------------------------------------------------------------------------


def test_base_exception_init():
    """AsyncDBException stores message and code correctly."""
    exc = AsyncDBException("test error", code=42)
    assert exc.message == "test error"
    assert exc.code == 42


def test_base_exception_repr():
    """`__repr__` returns '{message}, code: {code}'."""
    exc = AsyncDBException("boom", code=500)
    assert repr(exc) == "boom, code: 500"


def test_base_exception_str():
    """`__str__` returns '{message}, code: {code}'."""
    exc = AsyncDBException("boom", code=500)
    assert str(exc) == "boom, code: 500"


def test_base_exception_get():
    """`get()` returns the message string."""
    exc = AsyncDBException("hello")
    assert exc.get() == "hello"


def test_base_exception_defaults():
    """Default code is 0 and stacktrace is None."""
    exc = AsyncDBException("simple")
    assert exc.code == 0
    assert exc.stacktrace is None


# ---------------------------------------------------------------------------
# NoDataFound special case
# ---------------------------------------------------------------------------


def test_no_data_found_defaults():
    """NoDataFound() without arguments uses code=404 and 'Data Not Found'."""
    exc = NoDataFound()
    assert exc.code == 404
    assert "Data Not Found" in exc.message


def test_no_data_found_custom_message():
    """NoDataFound accepts a custom message while keeping code=404."""
    exc = NoDataFound("nothing here")
    assert exc.code == 404
    assert exc.message == "nothing here"


# ---------------------------------------------------------------------------
# Kwargs handling — Exception.args must NOT be shadowed
# ---------------------------------------------------------------------------


def test_exception_args_is_tuple():
    """Exception.args must remain a tuple of positional arguments."""
    exc = AsyncDBException("msg", "a", "b")
    assert isinstance(exc.args, tuple)


def test_exception_args_first_element():
    """Exception.args[0] must be the message string."""
    exc = AsyncDBException("msg", "extra1", "extra2")
    assert exc.args[0] == "msg"


def test_kwargs_stored_separately():
    """Extra kwargs must be stored in self.kwargs, not in self.args."""
    exc = AsyncDBException("msg", foo="bar")
    assert hasattr(exc, "kwargs")
    assert exc.kwargs.get("foo") == "bar"


# ---------------------------------------------------------------------------
# Stacktrace kwarg
# ---------------------------------------------------------------------------


def test_stacktrace_kwarg():
    """A 'stacktrace' kwarg is extracted and stored in self.stacktrace."""
    exc = AsyncDBException("err", stacktrace="traceback here")
    assert exc.stacktrace == "traceback here"


def test_stacktrace_not_in_kwargs():
    """'stacktrace' must not remain in self.kwargs after extraction."""
    exc = AsyncDBException("err", stacktrace="tb")
    assert "stacktrace" not in exc.kwargs


# ---------------------------------------------------------------------------
# Inheritance hierarchy
# ---------------------------------------------------------------------------


def test_provider_error_inherits_base():
    assert issubclass(ProviderError, AsyncDBException)


def test_driver_error_inherits_base():
    assert issubclass(DriverError, AsyncDBException)


def test_model_error_inherits_base():
    assert issubclass(ModelError, AsyncDBException)


def test_connection_missing_inherits_base():
    assert issubclass(ConnectionMissing, AsyncDBException)


def test_data_error_inherits_base():
    assert issubclass(DataError, AsyncDBException)


def test_not_supported_inherits_base():
    assert issubclass(NotSupported, AsyncDBException)


def test_empty_statement_inherits_base():
    assert issubclass(EmptyStatement, AsyncDBException)


def test_uninitialized_error_inherits_provider():
    assert issubclass(UninitializedError, ProviderError)
    assert issubclass(UninitializedError, AsyncDBException)


def test_connection_timeout_inherits_provider():
    assert issubclass(ConnectionTimeout, ProviderError)
    assert issubclass(ConnectionTimeout, AsyncDBException)


def test_no_data_found_inherits_provider():
    assert issubclass(NoDataFound, ProviderError)
    assert issubclass(NoDataFound, AsyncDBException)


def test_too_many_connections_inherits_provider():
    assert issubclass(TooManyConnections, ProviderError)


def test_unknown_property_error_inherits_provider():
    assert issubclass(UnknownPropertyError, ProviderError)


def test_statement_error_inherits_provider():
    assert issubclass(StatementError, ProviderError)


def test_conditions_error_inherits_provider():
    assert issubclass(ConditionsError, ProviderError)


# ---------------------------------------------------------------------------
# isinstance checks
# ---------------------------------------------------------------------------


def test_isinstance_chain():
    """A NoDataFound instance must satisfy isinstance checks up the chain."""
    exc = NoDataFound("missing")
    assert isinstance(exc, NoDataFound)
    assert isinstance(exc, ProviderError)
    assert isinstance(exc, AsyncDBException)
    assert isinstance(exc, Exception)


# ---------------------------------------------------------------------------
# Raise/catch with parent classes
# ---------------------------------------------------------------------------


def test_catch_no_data_found_as_provider_error():
    """NoDataFound can be caught as ProviderError."""
    with pytest.raises(ProviderError):
        raise NoDataFound("missing")


def test_catch_driver_error_as_async_db_exception():
    """DriverError can be caught as AsyncDBException."""
    with pytest.raises(AsyncDBException):
        raise DriverError("fail")


def test_catch_uninitialized_as_provider_and_base():
    """UninitializedError can be caught as both ProviderError and AsyncDBException."""
    with pytest.raises(ProviderError):
        raise UninitializedError("not ready")

    with pytest.raises(AsyncDBException):
        raise UninitializedError("not ready")


# ---------------------------------------------------------------------------
# Verify the module is pure Python (not a .so / Cython extension)
# ---------------------------------------------------------------------------


def test_exceptions_module_is_pure_python():
    """The exceptions module must be a .py file, not a compiled extension."""
    import asyncdb.exceptions.exceptions as mod
    assert mod.__file__ is not None
    assert mod.__file__.endswith(".py"), (
        f"Expected a .py module but got: {mod.__file__}"
    )


# ---------------------------------------------------------------------------
# All 15 classes are exported from asyncdb.exceptions
# ---------------------------------------------------------------------------


def test_all_exceptions_in_package_all():
    """Every exception class must appear in asyncdb.exceptions.__all__."""
    import asyncdb.exceptions as pkg

    assert len(ALL_EXCEPTIONS) == 15
    for cls in ALL_EXCEPTIONS:
        assert cls.__name__ in pkg.__all__, (
            f"{cls.__name__} missing from asyncdb.exceptions.__all__"
        )


# ---------------------------------------------------------------------------
# message-object branch (hasattr guard)
# ---------------------------------------------------------------------------


def test_message_from_object_with_message_attr():
    """Constructor must extract and str()-coerce .message from message objects."""

    class FakeError:
        message = "inner message"

    exc = AsyncDBException(FakeError())
    assert exc.message == "inner message"
    assert isinstance(exc.message, str)


def test_message_from_object_non_string_message_attr():
    """A non-string .message attribute must be coerced to str."""

    class WeirdError:
        message = 42  # int, not str

    exc = AsyncDBException(WeirdError())
    assert exc.message == "42"
    assert isinstance(exc.message, str)


# ---------------------------------------------------------------------------
# NoDataFound positional-args behaviour (intentional limitation)
# ---------------------------------------------------------------------------


def test_no_data_found_rejects_extra_positional_args():
    """NoDataFound does not accept extra positional args (matches Cython behaviour)."""
    with pytest.raises(TypeError):
        NoDataFound("msg", "unexpected_positional")  # type: ignore[call-arg]

"""Tests for the Windows-safe uvloop activation policy (FEAT-5 / TASK-23).

These tests verify that:

* ``install_uvloop()`` never raises, regardless of platform or whether
  ``uvloop`` is installed.
* ``uvloop`` is never imported or activated on Windows.
* ``uvloop`` activates automatically when it is importable on a supported
  (non-Windows) platform.
* The ``AsyncDB``/``AsyncPool``/``asyncdb`` factories keep their dynamic,
  driver-name-based module lookup behavior.
"""

import asyncio
import builtins
import sys

import pytest

import asyncdb.connections as connections_module
from asyncdb.connections import AsyncDB, AsyncPool, asyncdb
from asyncdb.drivers.dummy import dummy
from asyncdb.utils.uv import install_uvloop


def test_install_uvloop_is_noop_when_missing(monkeypatch):
    """``install_uvloop`` must not raise when ``uvloop`` is not installed."""
    monkeypatch.setattr(sys, "platform", "linux")

    real_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "uvloop":
            raise ImportError("No module named 'uvloop'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    # Must not raise.
    install_uvloop()


def test_install_uvloop_skips_windows(monkeypatch):
    """``install_uvloop`` must not attempt uvloop activation on Windows."""
    monkeypatch.setattr(sys, "platform", "win32")

    real_import = builtins.__import__
    attempted_uvloop_import = False

    def _fake_import(name, *args, **kwargs):
        nonlocal attempted_uvloop_import
        if name == "uvloop":
            attempted_uvloop_import = True
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    previous_policy = asyncio.get_event_loop_policy()
    install_uvloop()

    assert attempted_uvloop_import is False
    # The standard asyncio event loop policy remains usable/unchanged.
    assert isinstance(asyncio.get_event_loop_policy(), type(previous_policy))


def test_install_uvloop_activates_when_available_on_supported_platform(monkeypatch):
    """``uvloop`` activates automatically when present on a supported OS."""
    uvloop = pytest.importorskip("uvloop")
    monkeypatch.setattr(sys, "platform", "linux")

    install_uvloop()

    assert isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy)

    # Restore the default policy so other tests are not affected.
    asyncio.set_event_loop_policy(None)


def test_asyncdb_factories_keep_dynamic_lookup(monkeypatch):
    """``AsyncDB``, ``AsyncPool`` and ``asyncdb`` keep resolving
    ``asyncdb.drivers.<driver>`` dynamically by driver name via
    ``module_exists``."""
    instance = AsyncDB("dummy", params={"host": "127.0.0.1", "port": "0", "db": 0})
    assert isinstance(instance, dummy)

    context_manager = asyncdb(
        "dummy", params={"host": "127.0.0.1", "port": "0", "db": 0}
    )
    assert context_manager is not None

    # AsyncPool resolves "<driver>Pool" from "asyncdb.drivers.<driver>";
    # verify the dynamic lookup contract without requiring a live backend.
    calls = []

    def _fake_module_exists(module_name, classpath):
        calls.append((module_name, classpath))
        return dummy

    monkeypatch.setattr(connections_module, "module_exists", _fake_module_exists)
    pool_instance = AsyncPool(
        "dummy", params={"host": "127.0.0.1", "port": "0", "db": 0}
    )
    assert isinstance(pool_instance, dummy)
    assert calls == [("dummyPool", "asyncdb.drivers.dummy")]

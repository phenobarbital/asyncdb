"""Core Windows portability regression suite (FEAT-5 / TASK-27).

Assembles the cross-module contracts implemented in TASK-23 (uvloop
policy), TASK-24 (optional provider import isolation) and TASK-26 (release
wheel tag/extension validation) into a single regression suite proving:

* The core package imports on a platform without any optional/native
  provider dependency installed.
* `uvloop` absence and Windows behavior remain safe.
* A missing provider dependency does not poison the factory for another,
  already-portable provider.
* Release wheels carry the expected platform tag and compiled extension
  for their platform.

No external database service is required to run this suite.
"""

import asyncio
import builtins
import subprocess
import sys

import pytest

from asyncdb.connections import AsyncDB
from asyncdb.drivers.dummy import dummy
from asyncdb.exceptions import DriverError
from asyncdb.utils.uv import install_uvloop

from .test_release_wheel import (
    EXPECTED_WINDOWS_CPYTHON_TAGS,
    assert_wheel_is_valid_for_platform,
    parse_wheel_filename,
)
from .test_release_wheel import windows_wheel_path  # noqa: F401 (fixture re-export)
from .test_release_wheel import linux_wheel_path  # noqa: F401 (fixture re-export)


# ── Core import isolation ────────────────────────────────────────────────


def test_core_wheel_imports_without_optional_dependencies():
    """`import asyncdb` and loading the portable `dummy` driver must
    succeed in a fresh interpreter even when every optional/native provider
    dependency audited in TASK-24 is unavailable."""
    optional_provider_modules = (
        "pymssql",
        "MySQLdb",
        "aioodbc",
        "pyodbc",
        "cassandra",
        "acsylla",
        "uvloop",
    )
    script = (
        "import sys\n"
        f"for name in {optional_provider_modules!r}:\n"
        "    sys.modules[name] = None\n"
        "import asyncdb\n"
        "from asyncdb import AsyncDB\n"
        "instance = AsyncDB('dummy', params={'host': '127.0.0.1', 'port': '0', 'db': 0})\n"
        "assert instance is not None\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


# ── uvloop policy ─────────────────────────────────────────────────────────


def test_uvloop_absence_does_not_break_core_import(monkeypatch):
    """`install_uvloop()` must be a safe no-op when `uvloop` cannot be
    imported, regardless of platform."""
    monkeypatch.setattr(sys, "platform", "linux")
    real_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "uvloop":
            raise ImportError("No module named 'uvloop'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    install_uvloop()  # must not raise


def test_uvloop_windows_behavior_is_safe(monkeypatch):
    """On Windows, `install_uvloop()` must never attempt to import uvloop
    and the standard asyncio event loop policy must remain usable."""
    monkeypatch.setattr(sys, "platform", "win32")

    real_import = builtins.__import__
    attempted = False

    def _fake_import(name, *args, **kwargs):
        nonlocal attempted
        if name == "uvloop":
            attempted = True
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    install_uvloop()

    assert attempted is False
    # The standard asyncio policy must still be able to create a loop.
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    try:
        assert loop is not None
    finally:
        loop.close()


# ── Provider isolation at the factory level ──────────────────────────────


def test_portable_provider_factory_survives_missing_provider(monkeypatch):
    """Selecting a provider whose native dependency is missing must raise a
    focused `DriverError` without preventing a subsequent, unrelated
    portable provider (`dummy`) from loading successfully."""
    sys.modules.pop("asyncdb.drivers.sqlserver", None)
    monkeypatch.setitem(sys.modules, "pymssql", None)

    with pytest.raises(DriverError):
        AsyncDB("sqlserver", params={"host": "127.0.0.1", "port": "0", "db": 0})

    sys.modules.pop("asyncdb.drivers.sqlserver", None)

    # The portable "dummy" driver must still load without issue.
    instance = AsyncDB("dummy", params={"host": "127.0.0.1", "port": "0", "db": 0})
    assert isinstance(instance, dummy)


# ── Release wheel tag/extension checks ───────────────────────────────────


def test_wheel_extension_matches_platform(windows_wheel_path, linux_wheel_path):
    """Each wheel's compiled extension must match its own platform tag."""
    windows_member = assert_wheel_is_valid_for_platform(str(windows_wheel_path))
    assert windows_member.endswith(".pyd")

    linux_member = assert_wheel_is_valid_for_platform(str(linux_wheel_path))
    assert linux_member.endswith(".so")


def test_supported_windows_tag(windows_wheel_path):
    """A Windows release wheel must use a supported CPython tag and the
    `win_amd64` platform tag."""
    tags = parse_wheel_filename(str(windows_wheel_path))
    assert tags["platform_tag"] == "win_amd64"
    assert tags["python_tag"] in EXPECTED_WINDOWS_CPYTHON_TAGS

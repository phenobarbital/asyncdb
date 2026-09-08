"""Tests for optional/native provider import isolation (FEAT-5 / TASK-24).

These tests verify that:

* A missing optional/native provider dependency does not prevent
  ``import asyncdb`` or loading an unrelated, already-portable driver.
* Selecting a provider whose optional/native dependency is unavailable
  raises a focused ``DriverError`` naming the provider and its installation
  extra.
* Genuine (non-``ImportError``) provider import/runtime failures are not
  indiscriminately rewritten as missing-dependency errors.
"""

import builtins
import importlib
import subprocess
import sys

import pytest

from asyncdb.exceptions import DriverError


def test_core_import_without_unrelated_provider_dependency():
    """A missing/blocked provider dependency must not prevent `import
    asyncdb` or loading an unrelated, already-portable driver (dummy) in a
    fresh interpreter."""
    script = (
        "import sys\n"
        "for name in ('pymssql', 'MySQLdb', 'aioodbc', 'pyodbc', "
        "'cassandra', 'acsylla'):\n"
        "    sys.modules[name] = None\n"
        "import asyncdb\n"
        "from asyncdb.drivers.dummy import dummy\n"
        "assert dummy is not None\n"
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


@pytest.mark.parametrize(
    "module_path,missing_name,extra_hint",
    [
        ("asyncdb.drivers.mssql", "pymssql", "msqlserver"),
        ("asyncdb.drivers.sqlserver", "pymssql", "msqlserver"),
        ("asyncdb.drivers.mysqlclient", "MySQLdb", "mysql"),
        ("asyncdb.drivers.odbc", "aioodbc", "odbc"),
        ("asyncdb.drivers.cassandra", "cassandra", "cassandra"),
        ("asyncdb.drivers.scylladb", "acsylla", "scylla"),
    ],
)
def test_selected_provider_missing_extra_has_actionable_error(
    monkeypatch, module_path, missing_name, extra_hint
):
    """Selecting a provider whose native/optional dependency is unavailable
    raises a focused DriverError naming the provider and install extra."""
    sys.modules.pop(module_path, None)
    monkeypatch.setitem(sys.modules, missing_name, None)

    try:
        with pytest.raises(DriverError) as excinfo:
            importlib.import_module(module_path)

        message = str(excinfo.value)
        assert extra_hint in message
        assert "pip install asyncdb[" in message
    finally:
        # Do not leave a half-loaded module cached for later tests.
        sys.modules.pop(module_path, None)


def test_provider_runtime_import_error_is_not_rewritten(monkeypatch):
    """A non-``ImportError`` raised while a provider module is loading
    (e.g. a genuine runtime bug) must propagate unmodified and must not be
    reinterpreted as a missing-dependency DriverError."""
    module_path = "asyncdb.drivers.mysqlclient"
    sys.modules.pop(module_path, None)

    real_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "MySQLdb":
            raise RuntimeError("simulated genuine provider bug")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    try:
        with pytest.raises(RuntimeError, match="simulated genuine provider bug"):
            importlib.import_module(module_path)
    finally:
        sys.modules.pop(module_path, None)

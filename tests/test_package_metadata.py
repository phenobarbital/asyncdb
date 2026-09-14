"""Tests for Windows-aligned dependency metadata (FEAT-5 / TASK-25).

These tests validate the `pyproject.toml` dependency/extras boundary
without importing any provider package: `uvloop` stays an optional extra,
the approved `default` provider dependencies are declared, and the core
`dependencies` list is free of the native/optional provider packages
audited in TASK-24 (which must remain in their explicit extras).
"""

import sys
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - repository targets Python >= 3.10
    import tomli as tomllib

PYPROJECT_PATH = Path(__file__).resolve().parent.parent / "pyproject.toml"

# Native/optional provider packages audited in TASK-24 that must remain in
# their explicit extras and never leak into the core dependency contract.
_NATIVE_PROVIDER_PACKAGES = {
    "pymssql",
    "mysqlclient",
    "asyncmy",
    "aioodbc",
    "pyodbc",
    "cassandra-driver",
    "scylla_driver",
    "acsylla",
    "cqlsh",
    "jpype1",
    "jaydebeapi",
    "oracledb",
    "pylibmc",
    "uvloop",
}


def _load_pyproject() -> dict[str, Any]:
    """Load and parse the project's `pyproject.toml`.

    Returns:
        The parsed TOML document as a dictionary.
    """
    with open(PYPROJECT_PATH, "rb") as fh:
        return tomllib.load(fh)


def _package_name(requirement: str) -> str:
    """Extract the bare (lowercased) package name from a requirement string.

    Args:
        requirement: A PEP 508-style requirement string, e.g.
            ``"pymssql==2.3.1"`` or ``"influxdb-client[async]==1.45.0"``.

    Returns:
        The lowercased package name without version specifiers or extras.
    """
    name = requirement.strip()
    for sep in ("[", "==", ">=", "<=", ">", "<", "~=", "!="):
        name = name.split(sep, 1)[0]
    return name.strip().lower()


def test_uvloop_is_optional():
    """`uvloop` must only be declared in its own optional extra, never as a
    core dependency."""
    data = _load_pyproject()
    dependencies = data["project"]["dependencies"]
    optional = data["project"]["optional-dependencies"]

    core_names = {_package_name(dep) for dep in dependencies}
    assert "uvloop" not in core_names

    assert "uvloop" in optional
    uvloop_extra_names = {_package_name(dep) for dep in optional["uvloop"]}
    assert "uvloop" in uvloop_extra_names


def test_default_extra_contains_approved_provider_dependencies():
    """The `default` extra must declare dependencies for every approved
    default-scope provider (SQLite, RethinkDB, InfluxDB, SQL Server, Redis,
    Delta Lake, DuckDB)."""
    data = _load_pyproject()
    default_extra = data["project"]["optional-dependencies"]["default"]
    names = {_package_name(dep) for dep in default_extra}

    expected = {
        "aiosqlite",
        "rethinkdb",
        "influxdb",
        "influxdb-client",
        "pymssql",
        "redis",
        "deltalake",
        "duckdb",
    }
    assert expected.issubset(names)


def test_core_dependency_contract_is_portable():
    """Core dependencies must not include the native/optional provider
    packages audited in TASK-24; those remain in explicit extras."""
    data = _load_pyproject()
    dependencies = data["project"]["dependencies"]
    names = {_package_name(dep) for dep in dependencies}

    assert not (names & _NATIVE_PROVIDER_PACKAGES)

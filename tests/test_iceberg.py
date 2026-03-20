"""Unit and integration tests for the AsyncDB Apache Iceberg driver.

All tests use a local SQLite-backed Iceberg catalog so no external
infrastructure is required.
"""
import asyncio
from pathlib import Path

import pyarrow as pa
import pandas as pd
import polars as pl
import pytest
import pytest_asyncio

from asyncdb.drivers.iceberg import iceberg
from asyncdb.exceptions import DriverError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _sqlite_params(tmp_path: Path) -> dict:
    """Build params dict for a local SQLite-backed Iceberg catalog.

    Args:
        tmp_path: Pytest ``tmp_path`` fixture providing a temporary directory.

    Returns:
        Params dict ready for the ``iceberg`` driver constructor.
    """
    return {
        "catalog_name": "test_catalog",
        "catalog_type": "sql",
        "catalog_properties": {
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse": str(tmp_path / "warehouse"),
        },
        "namespace": "test_ns",
    }


@pytest.fixture
def arrow_schema() -> pa.Schema:
    """Return a simple PyArrow schema for tests.

    Returns:
        A three-column PyArrow schema.
    """
    return pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("value", pa.float64(), nullable=True),
        ]
    )


@pytest.fixture
def sample_arrow_table(arrow_schema: pa.Schema) -> pa.Table:
    """Return a small PyArrow Table for testing writes.

    Args:
        arrow_schema: The schema fixture.

    Returns:
        A ``pa.Table`` with three rows.
    """
    return pa.Table.from_pylist(
        [
            {"id": 1, "name": "Alice", "value": 1.1},
            {"id": 2, "name": "Bob", "value": 2.2},
            {"id": 3, "name": "Charlie", "value": 3.3},
        ],
        schema=arrow_schema,
    )


@pytest.fixture
def sample_pandas_df() -> pd.DataFrame:
    """Return a small Pandas DataFrame for write tests.

    Returns:
        A ``pd.DataFrame`` with three rows.
    """
    return pd.DataFrame(
        {
            "id": [4, 5],
            "name": ["Dave", "Eve"],
            "value": [4.4, 5.5],
        }
    )


# ---------------------------------------------------------------------------
# Driver instantiation & factory
# ---------------------------------------------------------------------------


def test_driver_instantiation(tmp_path: Path) -> None:
    """Test that the iceberg driver can be instantiated directly.

    Args:
        tmp_path: Pytest temporary directory.
    """
    params = _sqlite_params(tmp_path)
    driver = iceberg(params=params)
    assert driver._provider == "iceberg"
    assert driver._syntax == "nosql"
    assert driver._catalog_name == "test_catalog"
    assert driver._catalog_type == "sql"
    assert driver._namespace == "test_ns"


def test_driver_factory_auto_discovery(tmp_path: Path) -> None:
    """Test that AsyncDB factory can discover the iceberg driver.

    Args:
        tmp_path: Pytest temporary directory.
    """
    try:
        from asyncdb import AsyncDB  # noqa: PLC0415

        params = _sqlite_params(tmp_path)
        db = AsyncDB("iceberg", params=params)
        assert db is not None
    except ImportError:
        pytest.skip("AsyncDB factory not available in this test environment")


# ---------------------------------------------------------------------------
# Connection & context manager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connection(tmp_path: Path) -> None:
    """Test connecting and disconnecting from a local catalog.

    Args:
        tmp_path: Pytest temporary directory.
    """
    params = _sqlite_params(tmp_path)
    driver = iceberg(params=params)
    result = await driver.connection()
    assert driver._connected is True
    assert driver._connection is not None
    assert result is driver
    await driver.close()
    assert driver._connected is False
    assert driver._connection is None


@pytest.mark.asyncio
async def test_context_manager(tmp_path: Path) -> None:
    """Test the async context manager connects and disconnects properly.

    Args:
        tmp_path: Pytest temporary directory.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        assert driver._connected is True
    assert driver._connected is False


@pytest.mark.asyncio
async def test_not_connected_raises(tmp_path: Path) -> None:
    """Test that calling a method without connecting raises DriverError.

    Args:
        tmp_path: Pytest temporary directory.
    """
    params = _sqlite_params(tmp_path)
    driver = iceberg(params=params)
    with pytest.raises(DriverError, match="not connected"):
        await driver.list_namespaces()


# ---------------------------------------------------------------------------
# Namespace CRUD
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_and_list_namespace(tmp_path: Path) -> None:
    """Test creating a namespace and verifying it appears in the list.

    Args:
        tmp_path: Pytest temporary directory.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("my_ns")
        namespaces = await driver.list_namespaces()
        assert "my_ns" in namespaces


@pytest.mark.asyncio
async def test_namespace_properties(tmp_path: Path) -> None:
    """Test retrieving namespace metadata/properties.

    Args:
        tmp_path: Pytest temporary directory.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("props_ns", properties={"owner": "test"})
        props = await driver.namespace_properties("props_ns")
        assert isinstance(props, dict)
        assert props.get("owner") == "test"


@pytest.mark.asyncio
async def test_drop_namespace(tmp_path: Path) -> None:
    """Test dropping a namespace removes it from the list.

    Args:
        tmp_path: Pytest temporary directory.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("drop_ns")
        await driver.drop_namespace("drop_ns")
        namespaces = await driver.list_namespaces()
        assert "drop_ns" not in namespaces


@pytest.mark.asyncio
async def test_drop_nonexistent_namespace_raises(tmp_path: Path) -> None:
    """Test that dropping a non-existent namespace raises DriverError.

    Args:
        tmp_path: Pytest temporary directory.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        with pytest.raises(DriverError):
            await driver.drop_namespace("does_not_exist")


@pytest.mark.asyncio
async def test_use_namespace(tmp_path: Path) -> None:
    """Test the use() method sets the default namespace.

    Args:
        tmp_path: Pytest temporary directory.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.use("another_ns")
        assert driver._namespace == "another_ns"


# ---------------------------------------------------------------------------
# Table lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_and_load_table(tmp_path: Path, arrow_schema: pa.Schema) -> None:
    """Test creating a table and loading it back.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        tbl = await driver.create_table("test_ns.my_table", schema=arrow_schema)
        assert tbl is not None
        loaded = await driver.load_table("test_ns.my_table")
        assert loaded is not None


@pytest.mark.asyncio
async def test_table_exists(tmp_path: Path, arrow_schema: pa.Schema) -> None:
    """Test table_exists returns True/False correctly.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        assert await driver.table_exists("test_ns.no_such_table") is False
        await driver.create_table("test_ns.exists_table", schema=arrow_schema)
        assert await driver.table_exists("test_ns.exists_table") is True


@pytest.mark.asyncio
async def test_rename_table(tmp_path: Path, arrow_schema: pa.Schema) -> None:
    """Test renaming a table.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.original", schema=arrow_schema)
        await driver.rename_table("test_ns.original", "test_ns.renamed")
        assert await driver.table_exists("test_ns.renamed") is True
        assert await driver.table_exists("test_ns.original") is False


@pytest.mark.asyncio
async def test_drop_table(tmp_path: Path, arrow_schema: pa.Schema) -> None:
    """Test dropping a table removes it from the catalog.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.drop_me", schema=arrow_schema)
        await driver.drop_table("test_ns.drop_me")
        assert await driver.table_exists("test_ns.drop_me") is False


@pytest.mark.asyncio
async def test_tables_list(tmp_path: Path, arrow_schema: pa.Schema) -> None:
    """Test the synchronous tables() method lists tables in a namespace.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.table_a", schema=arrow_schema)
        await driver.create_table("test_ns.table_b", schema=arrow_schema)
        table_list = driver.tables("test_ns")
        assert any("table_a" in t for t in table_list)
        assert any("table_b" in t for t in table_list)


@pytest.mark.asyncio
async def test_table_schema(tmp_path: Path, arrow_schema: pa.Schema) -> None:
    """Test schema() returns a PyArrow schema.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.schema_table", schema=arrow_schema)
        await driver.load_table("test_ns.schema_table")
        returned_schema = driver.schema()
        assert isinstance(returned_schema, pa.Schema)
        assert "id" in returned_schema.names


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_write_append_arrow(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test appending a PyArrow Table to an Iceberg table.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.write_arrow", schema=arrow_schema)
        result = await driver.write(sample_arrow_table, "test_ns.write_arrow", mode="append")
        assert result is True

        arrow_result = await driver.scan("test_ns.write_arrow")
        assert arrow_result.num_rows == 3


@pytest.mark.asyncio
async def test_write_append_pandas(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_pandas_df: pd.DataFrame,
) -> None:
    """Test appending a Pandas DataFrame to an Iceberg table.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_pandas_df: Sample Pandas data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.write_pandas", schema=arrow_schema)
        result = await driver.write(sample_pandas_df, "test_ns.write_pandas", mode="append")
        assert result is True

        arrow_result = await driver.scan("test_ns.write_pandas")
        assert arrow_result.num_rows == 2


@pytest.mark.asyncio
async def test_write_overwrite(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
    sample_pandas_df: pd.DataFrame,
) -> None:
    """Test overwriting an Iceberg table replaces all rows.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Initial data (3 rows).
        sample_pandas_df: Replacement data (2 rows).
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.overwrite_table", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.overwrite_table", mode="append")
        await driver.write(sample_pandas_df, "test_ns.overwrite_table", mode="overwrite")

        result = await driver.scan("test_ns.overwrite_table")
        assert result.num_rows == 2


@pytest.mark.asyncio
async def test_write_polars(
    tmp_path: Path,
    arrow_schema: pa.Schema,
) -> None:
    """Test writing a Polars DataFrame to an Iceberg table.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
    """
    params = _sqlite_params(tmp_path)
    polars_df = pl.DataFrame(
        {
            "id": [10, 20],
            "name": ["Polars1", "Polars2"],
            "value": [10.0, 20.0],
        }
    )
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.polars_table", schema=arrow_schema)
        result = await driver.write(polars_df, "test_ns.polars_table", mode="append")
        assert result is True

        arrow_result = await driver.scan("test_ns.polars_table")
        assert arrow_result.num_rows == 2


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scan_returns_arrow(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test scan() returns a PyArrow Table.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.scan_table", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.scan_table")
        result = await driver.scan("test_ns.scan_table")
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3


@pytest.mark.asyncio
async def test_get_pandas(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test get() with factory='pandas' returns a Pandas DataFrame.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.get_pandas", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.get_pandas")
        result = await driver.get("test_ns.get_pandas", factory="pandas")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3


@pytest.mark.asyncio
async def test_get_polars(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test get() with factory='polars' returns a Polars DataFrame.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.get_polars", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.get_polars")
        result = await driver.get("test_ns.get_polars", factory="polars")
        assert isinstance(result, pl.DataFrame)
        assert result.shape[0] == 3


@pytest.mark.asyncio
async def test_query_duckdb_sql(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test query() executes SQL via DuckDB and returns results.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.query_table", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.query_table")

        result, error = await driver.query(
            "SELECT id, name FROM iceberg_table WHERE id > 1",
            table_id="test_ns.query_table",
            factory="arrow",
        )
        assert error is None
        assert isinstance(result, pa.Table)
        assert result.num_rows == 2


@pytest.mark.asyncio
async def test_queryrow(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test queryrow() returns exactly one row.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.queryrow_table", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.queryrow_table")

        result, error = await driver.queryrow(
            "SELECT * FROM iceberg_table",
            table_id="test_ns.queryrow_table",
            factory="arrow",
        )
        assert error is None
        assert isinstance(result, pa.Table)
        assert result.num_rows == 1


@pytest.mark.asyncio
async def test_to_df(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test to_df() returns a Pandas DataFrame with all rows.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.to_df_table", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.to_df_table")
        result = await driver.to_df("test_ns.to_df_table", factory="pandas")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Metadata & snapshot history
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_metadata(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test metadata() returns expected keys.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.meta_table", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.meta_table")
        meta = await driver.metadata("test_ns.meta_table")
        assert "location" in meta
        assert "properties" in meta
        assert "schema" in meta
        assert "snapshot_count" in meta
        assert meta["snapshot_count"] >= 1


@pytest.mark.asyncio
async def test_history(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test history() returns snapshot entries after a write.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.hist_table", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.hist_table")
        hist = await driver.history("test_ns.hist_table")
        assert isinstance(hist, list)
        assert len(hist) >= 1
        assert "snapshot_id" in hist[0]
        assert "timestamp_ms" in hist[0]


@pytest.mark.asyncio
async def test_current_snapshot(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test current_snapshot() returns the latest snapshot info.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.snap_table", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.snap_table")
        snap = await driver.current_snapshot("test_ns.snap_table")
        assert isinstance(snap, dict)
        assert "snapshot_id" in snap


@pytest.mark.asyncio
async def test_snapshots(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test snapshots() returns a list of snapshot objects.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.snaps_table", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.snaps_table")
        snaps = await driver.snapshots("test_ns.snaps_table")
        assert isinstance(snaps, list)
        assert len(snaps) >= 1


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_nonexistent_table_raises(tmp_path: Path) -> None:
    """Test that loading a non-existent table raises DriverError.

    Args:
        tmp_path: Pytest temporary directory.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        with pytest.raises(DriverError):
            await driver.load_table("test_ns.ghost_table")


@pytest.mark.asyncio
async def test_write_unsupported_mode_raises(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test that write() with an invalid mode raises DriverError.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.mode_table", schema=arrow_schema)
        with pytest.raises(DriverError, match="Unsupported write mode"):
            await driver.write(sample_arrow_table, "test_ns.mode_table", mode="upsert_wrong")


@pytest.mark.asyncio
async def test_delete_without_filter_raises(
    tmp_path: Path,
    arrow_schema: pa.Schema,
) -> None:
    """Test that delete() without a filter raises DriverError.

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.del_table", schema=arrow_schema)
        with pytest.raises(DriverError, match="delete_filter is required"):
            await driver.delete("test_ns.del_table", delete_filter=None)


# ---------------------------------------------------------------------------
# Fetch aliases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_all_alias(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test that fetch_all is an alias for query().

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.alias_table", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.alias_table")
        result, error = await driver.fetch_all(
            table_id="test_ns.alias_table", factory="arrow"
        )
        assert error is None
        assert isinstance(result, pa.Table)


@pytest.mark.asyncio
async def test_fetch_one_alias(
    tmp_path: Path,
    arrow_schema: pa.Schema,
    sample_arrow_table: pa.Table,
) -> None:
    """Test that fetch_one is an alias for queryrow().

    Args:
        tmp_path: Pytest temporary directory.
        arrow_schema: PyArrow schema fixture.
        sample_arrow_table: Sample data fixture.
    """
    params = _sqlite_params(tmp_path)
    async with iceberg(params=params) as driver:
        await driver.create_namespace("test_ns")
        await driver.create_table("test_ns.one_alias", schema=arrow_schema)
        await driver.write(sample_arrow_table, "test_ns.one_alias")
        result, error = await driver.fetch_one(
            table_id="test_ns.one_alias", factory="arrow"
        )
        assert error is None
        assert result is not None

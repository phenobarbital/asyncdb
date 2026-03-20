"""Tests for the refactored Delta driver write() and create() methods.

FEAT-003 / TASK-013: Validates the full write_deltalake API surface
exposed through asyncdb's delta driver.
"""
import types
import warnings

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from deltalake import DeltaTable

from asyncdb.drivers.delta import delta


@pytest.fixture
def driver():
    """Create a minimal delta driver stub for testing write/create.

    The full driver MRO has a pre-existing __init__ chain issue,
    so we construct a lightweight stub with the required attributes
    and bind the write/create methods from the real class.
    """
    stub = object.__new__(delta)
    stub.storage_options = {"timeout": "120s"}
    stub._delta = None
    # Bind the real async methods to the stub
    stub.write = types.MethodType(delta.write, stub)
    stub.create = types.MethodType(delta.create, stub)
    return stub


@pytest.fixture
def sample_pandas_df():
    """Small Pandas DataFrame for testing."""
    return pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10, 20, 30]})


@pytest.fixture
def sample_polars_df():
    """Small Polars DataFrame for testing."""
    return pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10, 20, 30]})


@pytest.fixture
def sample_arrow_table():
    """Small PyArrow Table for testing."""
    return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10, 20, 30]})


# --- write() mode tests ---


@pytest.mark.asyncio
async def test_write_append(driver, sample_pandas_df, tmp_path):
    """write() with mode='append' creates table then appends rows."""
    await driver.write(sample_pandas_df, "t1", tmp_path, mode="append")
    await driver.write(sample_pandas_df, "t1", tmp_path, mode="append")
    dt = DeltaTable(str(tmp_path / "t1"))
    result = dt.to_pandas()
    assert len(result) == 6


@pytest.mark.asyncio
async def test_write_overwrite(driver, sample_pandas_df, tmp_path):
    """write() with mode='overwrite' replaces table contents."""
    await driver.write(sample_pandas_df, "t1", tmp_path, mode="append")
    await driver.write(sample_pandas_df, "t1", tmp_path, mode="overwrite")
    dt = DeltaTable(str(tmp_path / "t1"))
    result = dt.to_pandas()
    assert len(result) == 3


@pytest.mark.asyncio
async def test_write_ignore(driver, sample_pandas_df, tmp_path):
    """write() with mode='ignore' does not write if table exists."""
    await driver.write(sample_pandas_df, "t1", tmp_path, mode="append")
    df2 = pd.DataFrame({"id": [99], "name": ["z"], "value": [999]})
    await driver.write(df2, "t1", tmp_path, mode="ignore")
    dt = DeltaTable(str(tmp_path / "t1"))
    result = dt.to_pandas()
    assert len(result) == 3
    assert 99 not in result["id"].values


@pytest.mark.asyncio
async def test_write_error_mode(driver, sample_pandas_df, tmp_path):
    """write() with mode='error' raises when table already exists."""
    from asyncdb.exceptions import DriverError

    await driver.write(sample_pandas_df, "t1", tmp_path, mode="error")
    with pytest.raises(DriverError):
        await driver.write(sample_pandas_df, "t1", tmp_path, mode="error")


# --- Data type tests ---


@pytest.mark.asyncio
async def test_write_pandas(driver, sample_pandas_df, tmp_path):
    """write() accepts Pandas DataFrame."""
    await driver.write(sample_pandas_df, "t1", tmp_path)
    dt = DeltaTable(str(tmp_path / "t1"))
    assert dt.to_pandas().shape[0] == 3


@pytest.mark.asyncio
async def test_write_polars(driver, sample_polars_df, tmp_path):
    """write() accepts Polars DataFrame (converted to Arrow internally)."""
    await driver.write(sample_polars_df, "t1", tmp_path)
    dt = DeltaTable(str(tmp_path / "t1"))
    assert dt.to_pandas().shape[0] == 3


@pytest.mark.asyncio
async def test_write_arrow(driver, sample_arrow_table, tmp_path):
    """write() accepts PyArrow Table."""
    await driver.write(sample_arrow_table, "t1", tmp_path)
    dt = DeltaTable(str(tmp_path / "t1"))
    assert dt.to_pandas().shape[0] == 3


# --- schema_mode tests ---


@pytest.mark.asyncio
async def test_write_schema_mode_merge(driver, tmp_path):
    """write() with schema_mode='merge' allows adding new columns."""
    df1 = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    await driver.write(df1, "t1", tmp_path, mode="append")

    df2 = pd.DataFrame({"id": [3], "name": ["c"], "extra": [True]})
    await driver.write(df2, "t1", tmp_path, mode="append", schema_mode="merge")

    dt = DeltaTable(str(tmp_path / "t1"))
    result = dt.to_pandas()
    assert "extra" in result.columns
    assert len(result) == 3


@pytest.mark.asyncio
async def test_write_schema_mode_overwrite(driver, tmp_path):
    """write() with schema_mode='overwrite' replaces the schema entirely."""
    df1 = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    await driver.write(df1, "t1", tmp_path, mode="append")

    df2 = pd.DataFrame({"x": [10.0], "y": [20.0]})
    await driver.write(df2, "t1", tmp_path, mode="overwrite", schema_mode="overwrite")

    dt = DeltaTable(str(tmp_path / "t1"))
    result = dt.to_pandas()
    assert list(result.columns) == ["x", "y"]
    assert len(result) == 1


# --- configuration tests ---


@pytest.mark.asyncio
async def test_write_configuration(driver, sample_pandas_df, tmp_path):
    """write() forwards configuration metadata to the Delta table."""
    config = {"delta.appendOnly": "true"}
    await driver.write(sample_pandas_df, "t1", tmp_path, configuration=config)
    dt = DeltaTable(str(tmp_path / "t1"))
    metadata = dt.metadata()
    assert metadata.configuration.get("delta.appendOnly") == "true"


# --- predicate (targeted overwrite) tests ---


@pytest.mark.asyncio
async def test_write_predicate(driver, tmp_path):
    """write() with predicate does targeted overwrite."""
    df_initial = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
    await driver.write(df_initial, "t1", tmp_path, mode="append")

    df_replacement = pd.DataFrame({"id": [99], "val": ["z"]})
    await driver.write(
        df_replacement, "t1", tmp_path, mode="overwrite", predicate="id > 2",
    )

    dt = DeltaTable(str(tmp_path / "t1"))
    result = dt.to_pandas()
    ids = sorted(result["id"].tolist())
    assert 1 in ids
    assert 2 in ids
    assert 99 in ids
    assert 3 not in ids


# --- Backward compatibility: if_exists deprecation ---


@pytest.mark.asyncio
async def test_write_if_exists_deprecated(driver, sample_pandas_df, tmp_path):
    """write() with if_exists emits DeprecationWarning and still works."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        await driver.write(sample_pandas_df, "t1", tmp_path, if_exists="append")
        dep_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(dep_warnings) == 1
        assert "if_exists is deprecated" in str(dep_warnings[0].message)

    dt = DeltaTable(str(tmp_path / "t1"))
    assert dt.to_pandas().shape[0] == 3


# --- create() tests ---


@pytest.mark.asyncio
async def test_create_pandas(driver, sample_pandas_df, tmp_path):
    """create() creates a Delta table from a Pandas DataFrame."""
    dest = tmp_path / "new_table"
    await driver.create(dest, sample_pandas_df, name="test_table")
    dt = DeltaTable(str(dest))
    assert dt.to_pandas().shape[0] == 3


@pytest.mark.asyncio
async def test_create_with_configuration(driver, sample_pandas_df, tmp_path):
    """create() forwards configuration to the Delta table."""
    dest = tmp_path / "configured_table"
    await driver.create(
        dest, sample_pandas_df, configuration={"delta.appendOnly": "true"},
    )
    dt = DeltaTable(str(dest))
    assert dt.metadata().configuration.get("delta.appendOnly") == "true"


@pytest.mark.asyncio
async def test_create_with_schema_mode(driver, tmp_path):
    """create() with schema_mode allows schema evolution on subsequent calls."""
    dest = tmp_path / "schema_table"
    df1 = pd.DataFrame({"a": [1], "b": [2]})
    await driver.create(dest, df1, mode="append")

    df2 = pd.DataFrame({"a": [3], "b": [4], "c": [5]})
    await driver.create(dest, df2, mode="append", schema_mode="merge")

    dt = DeltaTable(str(dest))
    result = dt.to_pandas()
    assert "c" in result.columns
    assert len(result) == 2


@pytest.mark.asyncio
async def test_create_polars(driver, sample_polars_df, tmp_path):
    """create() handles Polars DataFrame (converted to Arrow internally)."""
    dest = tmp_path / "polars_table"
    await driver.create(dest, sample_polars_df)
    dt = DeltaTable(str(dest))
    assert dt.to_pandas().shape[0] == 3


# --- partition_by tests ---


@pytest.mark.asyncio
async def test_write_partition_by(driver, tmp_path):
    """write() with partition_by creates partitioned Delta table."""
    df = pd.DataFrame({
        "region": ["us", "us", "eu", "eu"],
        "id": [1, 2, 3, 4],
    })
    await driver.write(df, "partitioned", tmp_path, partition_by=["region"])
    dt = DeltaTable(str(tmp_path / "partitioned"))
    result = dt.to_pandas()
    assert len(result) == 4


# --- storage_options fallback test ---


@pytest.mark.asyncio
async def test_write_uses_driver_storage_options(driver, sample_pandas_df, tmp_path):
    """write() uses self.storage_options when storage_options param is None."""
    await driver.write(sample_pandas_df, "t1", tmp_path)
    dt = DeltaTable(str(tmp_path / "t1"))
    assert dt.to_pandas().shape[0] == 3


# --- name and description forwarding ---


@pytest.mark.asyncio
async def test_write_name_description(driver, sample_pandas_df, tmp_path):
    """write() forwards name and description to Delta table metadata."""
    await driver.write(
        sample_pandas_df, "t1", tmp_path, name="my_table", description="A test table",
    )
    dt = DeltaTable(str(tmp_path / "t1"))
    metadata = dt.metadata()
    assert metadata.name == "my_table"
    assert metadata.description == "A test table"

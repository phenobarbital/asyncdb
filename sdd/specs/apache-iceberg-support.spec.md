# FEAT-002: apache-iceberg-support

**Title**: Apache Iceberg Driver for AsyncDB
**Status**: draft
**Created**: 2026-03-19
**Author**: Jesus Lara

---

## Problem Statement

AsyncDB currently supports a wide range of database and data-lake drivers (PostgreSQL, BigQuery, DuckDB, Delta Lake, Cassandra, etc.), but lacks support for **Apache Iceberg** — the open table format increasingly adopted for large-scale analytics, data lake architectures, and lakehouse patterns.

Users working with Iceberg-based data lakes (on S3, GCS, or HDFS) must resort to external tools or manual integrations. A native asyncdb driver using **PyIceberg** (the pure-Python Iceberg implementation, no JVM required) would enable:

1. Unified async access to Iceberg catalogs (Hive, BigQuery, SQL-Postgres, REST, Glue).
2. Full table lifecycle management (create, load, rename, drop).
3. Namespace (database/schema) management.
4. Read/write operations returning PyArrow Tables, Pandas DataFrames, Polars DataFrames, or DuckDB results.
5. Advanced write modes: partial overwrites, upserts, and file-level ingestion from Parquet.
6. Consistency with the existing asyncdb driver interface (`InitDriver`).

---

## Proposed Solution

Implement a new `iceberg` driver at `asyncdb/drivers/iceberg.py` that wraps the **PyIceberg** library, following the same architectural pattern as the existing `delta` driver (extends `InitDriver`, wraps synchronous calls with `asyncio.to_thread` where needed).

### Key Design Decisions

1. **Base class**: `InitDriver` (not `SQLDriver`) — Iceberg is a table format, not a SQL database. SQL queries over Iceberg data will be delegated to DuckDB (same pattern as `delta.py`).
2. **Catalog-centric connection model**: The `connection()` method loads a PyIceberg `Catalog` instance. Catalog type (hive, glue, rest, sql, bigquery, dynamodb) is determined from `params`.
3. **Async wrapping**: PyIceberg is synchronous. All blocking I/O operations (catalog RPCs, S3/GCS reads, file writes) will be wrapped in `asyncio.to_thread()` to avoid blocking the event loop.
4. **Output formats**: Query results are natively PyArrow Tables; conversion to Pandas, Polars, and DuckDB is done on-demand via factory parameter (consistent with `delta` driver).
5. **Optional dependency group**: PyIceberg and its extras will be declared as an optional dependency group `iceberg` in `pyproject.toml`.

---

## Detailed Design

### Driver Class: `iceberg`

```
asyncdb/drivers/iceberg.py
```

**Class hierarchy:**
```
InitDriver
  └── iceberg
```

**Provider metadata:**
```python
_provider = "iceberg"
_syntax = "nosql"
```

### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `params["catalog_name"]` | `str` | Catalog identifier (default: `"default"`) |
| `params["catalog_type"]` | `str` | One of: `rest`, `hive`, `glue`, `sql`, `bigquery`, `dynamodb` |
| `params["catalog_properties"]` | `dict` | PyIceberg catalog properties (URI, credentials, warehouse, etc.) |
| `params["namespace"]` | `str` | Default namespace to operate in (optional) |
| `params["storage_options"]` | `dict` | S3/GCS/ADLS credentials and config (optional) |

### Connection Lifecycle

```python
async def connection(self, **kwargs) -> Self:
    """Load the PyIceberg catalog."""
    # Wraps pyiceberg.catalog.load_catalog() in asyncio.to_thread()

async def close(self) -> None:
    """Release catalog reference and clean up."""
```

### Namespace Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_namespace` | `async def create_namespace(self, namespace: str, properties: dict = None) -> None` | Create a new namespace |
| `list_namespaces` | `async def list_namespaces(self) -> list[str]` | List all namespaces |
| `drop_namespace` | `async def drop_namespace(self, namespace: str) -> None` | Drop a namespace |
| `namespace_properties` | `async def namespace_properties(self, namespace: str) -> dict` | Get namespace metadata |

### Table Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `create_table` | `async def create_table(self, table_id: str, schema, partition_spec=None, **kwargs) -> Any` | Create table from PyArrow schema or Iceberg schema |
| `register_table` | `async def register_table(self, table_id: str, metadata_location: str) -> Any` | Register an existing Iceberg table |
| `load_table` | `async def load_table(self, table_id: str) -> Any` | Load a table reference |
| `table_exists` | `async def table_exists(self, table_id: str) -> bool` | Check if table exists |
| `rename_table` | `async def rename_table(self, from_id: str, to_id: str) -> None` | Rename a table |
| `drop_table` | `async def drop_table(self, table_id: str, purge: bool = False) -> None` | Drop a table |
| `tables` | `def tables(self, namespace: str = "") -> list[str]` | List tables in namespace |
| `table` | `def table(self, tablename: str = "") -> dict` | Get table schema/metadata |

### Read Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `query` | `async def query(self, sentence: str = None, table_id: str = None, factory: str = "arrow", **kwargs)` | Query via DuckDB SQL or Iceberg scan expressions |
| `queryrow` | `async def queryrow(self, sentence: str = None, table_id: str = None, factory: str = "arrow", **kwargs)` | Fetch single row |
| `get` | `async def get(self, table_id: str, columns: list = None, row_filter: str = None, factory: str = "arrow")` | Scan table with optional column/row pruning |
| `scan` | `async def scan(self, table_id: str, row_filter=None, selected_fields=None, snapshot_id=None, **kwargs)` | Low-level scan returning PyArrow Table |
| `to_df` | `async def to_df(self, table_id: str, factory: str = "pandas", **kwargs)` | Convert table data to DataFrame |
| `fetch_all` | alias for `query` | |
| `fetch_one` | alias for `queryrow` | |

**Factory parameter values:** `"arrow"`, `"pandas"`, `"polars"`, `"duckdb"`

### Write Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `write` | `async def write(self, data, table_id: str, mode: str = "append", **kwargs) -> bool` | Write data (PyArrow Table, Pandas DF, or Polars DF) |
| `overwrite` | `async def overwrite(self, data, table_id: str, overwrite_filter: str = None, **kwargs) -> bool` | Partial overwrite with filter expression |
| `upsert` | `async def upsert(self, data, table_id: str, join_cols: list[str], **kwargs) -> bool` | Merge/upsert based on join columns |
| `add_files` | `async def add_files(self, table_id: str, file_paths: list[str], **kwargs) -> bool` | Register existing Parquet files into table |
| `delete` | `async def delete(self, table_id: str, delete_filter: str, **kwargs) -> None` | Delete rows matching filter |

### Metadata & Utility

| Method | Signature | Description |
|--------|-----------|-------------|
| `schema` | `def schema(self, table_id: str) -> Any` | Get table schema (PyArrow or Iceberg) |
| `metadata` | `async def metadata(self, table_id: str) -> dict` | Get table metadata (snapshots, properties) |
| `history` | `async def history(self, table_id: str) -> list[dict]` | Get snapshot history |
| `snapshots` | `async def snapshots(self, table_id: str) -> list` | List table snapshots |
| `current_snapshot` | `async def current_snapshot(self, table_id: str) -> dict` | Get current snapshot info |

### Query via DuckDB (same pattern as Delta driver)

For SQL queries, the driver will:
1. Load the Iceberg table as a PyArrow dataset.
2. Register it in an in-memory DuckDB connection.
3. Execute the SQL query.
4. Return results in the requested factory format (arrow, pandas, polars).

---

## Dependencies

### pyproject.toml optional dependency group

```toml
[project.optional-dependencies]
iceberg = [
    "pyiceberg[pyarrow,pandas,duckdb,polars,s3fs,gcsfs,sql-postgres,hive,ray]>=0.9.0",
]
```

Individual extras from pyiceberg:
- `pyarrow` — core data format
- `pyiceberg-core` — Rust-optimized core (optional, improves performance)
- `pandas` — DataFrame support
- `duckdb` — SQL query engine
- `polars` — Polars DataFrame support
- `s3fs` — S3 storage backend
- `gcsfs` — GCS storage backend
- `sql-postgres` — PostgreSQL-backed catalog
- `hive` — Hive metastore catalog
- `ray` — Ray integration for distributed processing

---

## Acceptance Criteria

1. **Driver registration**: `AsyncDB("iceberg", params={...})` loads the `iceberg` driver via the factory.
2. **Catalog connection**: Successfully connects to at least REST, SQL (PostgreSQL), and file-system catalogs.
3. **Namespace CRUD**: Create, list, and drop namespaces.
4. **Table lifecycle**: Create table from PyArrow schema, load, check existence, rename, drop.
5. **Read data**: Scan table and return as PyArrow Table, Pandas DataFrame, Polars DataFrame, or via DuckDB.
6. **Write data**: Append and overwrite data from PyArrow Table and Pandas DataFrame.
7. **Partial overwrite**: Overwrite rows matching a filter expression.
8. **Upsert**: Merge data using join columns.
9. **Add files**: Register existing Parquet files into an Iceberg table.
10. **Query via DuckDB**: Execute SQL queries over Iceberg tables using DuckDB.
11. **Async compliance**: All I/O methods are async; no blocking of the event loop.
12. **Error handling**: All PyIceberg exceptions are wrapped in `DriverError`.
13. **Context manager**: Supports `async with` pattern.
14. **Tests**: Unit tests covering connection, namespace ops, table ops, read, and write.
15. **Example script**: `examples/test_iceberg.py` demonstrating typical usage.

---

## Architectural Design

### File Layout

```
asyncdb/
  drivers/
    iceberg.py           # Main driver implementation
examples/
  test_iceberg.py        # Usage example
tests/
  test_iceberg.py        # Unit/integration tests
```

### Integration Points

- **Factory** (`asyncdb/connections.py`): Auto-discovered by module name — no changes needed.
- **Output formats** (`asyncdb/drivers/outputs/`): Reuse existing `OutputFactory` for arrow/pandas serialization where applicable.
- **Exceptions** (`asyncdb/exceptions/`): Wrap all PyIceberg errors in `DriverError`.

### Concurrency Model

```
User code (async) ──► iceberg driver (async methods)
                          │
                          ├── asyncio.to_thread(catalog.load_table(...))
                          ├── asyncio.to_thread(table.scan().to_arrow())
                          └── asyncio.to_thread(table.append(arrow_table))
```

All PyIceberg blocking calls are offloaded to the default thread pool executor via `asyncio.to_thread()`, preserving the async-first contract of asyncdb.

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| PyIceberg is synchronous | Could block event loop if not wrapped | All I/O wrapped in `asyncio.to_thread()` |
| PyIceberg API changes (pre-1.0) | Breaking changes in minor versions | Pin minimum version `>=0.9.0`, test against latest |
| Heavy dependency tree (S3, GCS, Hive) | Large install size | Use optional extras; only install what's needed |
| Upsert not natively supported in all PyIceberg versions | Feature gap | Check version at runtime; raise `NotImplementedError` if unavailable |
| Catalog-specific behaviors differ | Inconsistent behavior across backends | Document supported catalogs; test with REST + SQL at minimum |

---

## Out of Scope

- **Schema evolution** (add/rename/drop columns on existing tables) — future enhancement.
- **Time-travel queries** (query at specific snapshot ID) — future enhancement, though `scan()` accepts `snapshot_id`.
- **Compaction / maintenance** operations — defer to PyIceberg CLI or external tools.
- **Distributed writes via Ray** — initial implementation focuses on single-node; Ray integration is a future enhancement.
- **Custom Iceberg expressions DSL** — use string-based filter expressions initially.

---

## Worktree Strategy

- **Isolation unit**: `per-spec` (sequential tasks in a single worktree)
- **Rationale**: All tasks build on each other (base class → namespace ops → table ops → read → write → tests). No parallelizable tasks.
- **Cross-feature dependencies**: None. This spec is independent of FEAT-001 (exception-migration).

---

## References

- [PyIceberg documentation](https://py.iceberg.apache.org/)
- [PyIceberg API reference](https://py.iceberg.apache.org/api/)
- [Apache Iceberg spec](https://iceberg.apache.org/spec/)
- Existing driver reference: `asyncdb/drivers/delta.py` (closest architectural pattern)
- Existing driver reference: `asyncdb/drivers/bigquery.py` (cloud catalog pattern)

#!/usr/bin/env python3
"""Apache Iceberg driver for AsyncDB.

Notes on Iceberg Provider
--------------------------
This provider wraps the PyIceberg library to provide async access to
Apache Iceberg tables via any supported catalog (REST, Hive, Glue,
SQL, BigQuery, DynamoDB).

All blocking PyIceberg calls are offloaded to the default thread-pool
executor via ``asyncio.to_thread()`` so the event loop is never blocked.
"""
import asyncio
import gc
import time
from typing import Any, Optional, Union

import duckdb

try:
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pandas as pd
    import polars as pl
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import (
        NamespaceAlreadyExistsError,
        NamespaceNotEmptyError,
        NoSuchNamespaceError,
        NoSuchTableError,
        TableAlreadyExistsError,
    )
    # IcebergError is not a stable public base class across versions;
    # use Exception as the catch-all for PyIceberg errors.
    IcebergError = Exception
    _PYICEBERG_AVAILABLE = True
except ImportError:
    _PYICEBERG_AVAILABLE = False
    IcebergError = Exception  # type: ignore[assignment,misc]

from ..exceptions import DriverError
from .base import InitDriver


class _KwargsTerminator:
    """Terminate the cooperative __init__ MRO chain.

    Drivers that extend ``InitDriver`` (but not ``BaseDriver``) lack the
    ``ConnectionDSNBackend`` class in their MRO.  ``ConnectionDSNBackend``
    happens to call ``super().__init__()`` without args, which stops residual
    kwargs (e.g. ``params``) from propagating to ``object.__init__`` and
    raising ``TypeError``.

    This class provides the same termination for ``InitDriver``-only drivers.
    """

    def __init__(self, **kwargs):  # noqa: ANN001
        # Intentionally swallow remaining kwargs and call object.__init__()
        # without arguments to satisfy cooperative MI requirements.
        super().__init__()


def _require_pyiceberg() -> None:
    """Raise DriverError if PyIceberg is not installed.

    Raises:
        DriverError: When pyiceberg is not available.
    """
    if not _PYICEBERG_AVAILABLE:
        raise DriverError(
            "PyIceberg is not installed. "
            "Install it with: pip install 'asyncdb[iceberg]'"
        )


def _parse_table_id(table_id: Union[str, tuple]) -> tuple:
    """Parse a table identifier into a (namespace, table) tuple.

    Supports both ``"namespace.table"`` dot-notation and
    ``("namespace", "table")`` tuple form.

    Args:
        table_id: Table identifier as a dot-separated string or a 2-tuple.

    Returns:
        A 2-tuple ``(namespace, table_name)``.

    Raises:
        DriverError: When the table_id format is invalid.
    """
    if isinstance(table_id, tuple):
        return table_id
    if isinstance(table_id, str) and "." in table_id:
        parts = table_id.split(".", 1)
        return (parts[0], parts[1])
    return (table_id,)


class iceberg(InitDriver, _KwargsTerminator):
    """Async Apache Iceberg driver backed by PyIceberg.

    Extends ``InitDriver`` to provide catalog management, namespace CRUD,
    full table lifecycle, read/write operations, and metadata/history
    access against any PyIceberg-supported catalog.

    Attributes:
        _provider: Driver provider name (``"iceberg"``).
        _syntax: Query syntax style (``"nosql"``).
    """

    _provider = "iceberg"
    _syntax = "nosql"
    _dsn_template = ""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop = None,
        params: dict = None,
        **kwargs,
    ) -> None:
        """Initialise the Iceberg driver.

        Args:
            loop: Optional event loop. Uses the running loop if not provided.
            params: Connection parameters dict with keys:
                - ``catalog_name`` (str): Catalog identifier. Default: ``"default"``.
                - ``catalog_type`` (str): One of ``rest``, ``hive``, ``glue``,
                  ``sql``, ``bigquery``, ``dynamodb``.
                - ``catalog_properties`` (dict): PyIceberg catalog properties
                  (URI, warehouse path, credentials, etc.).
                - ``namespace`` (str): Default namespace for operations.
                - ``storage_options`` (dict): S3/GCS/ADLS credentials.
            **kwargs: Additional keyword arguments forwarded to ``InitDriver``.
        """
        _require_pyiceberg()
        if params is None:
            params = {}
        self._catalog_name: str = params.pop("catalog_name", "default")
        self._catalog_type: str = params.pop("catalog_type", "sql")
        self._catalog_properties: dict = params.pop("catalog_properties", {})
        self._namespace: str = params.pop("namespace", "")
        self._storage_options: dict = params.pop("storage_options", {})
        self._current_table: Any = None
        super().__init__(loop=loop, params=params, **kwargs)

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "iceberg":
        """Connect to the catalog on context manager entry.

        Returns:
            The driver instance after successful connection.
        """
        await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the catalog connection on context manager exit.

        Args:
            exc_type: Exception type, if any.
            exc_val: Exception value, if any.
            exc_tb: Exception traceback, if any.
        """
        await self.close()

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connection(self, **kwargs) -> "iceberg":
        """Load the PyIceberg catalog and mark the driver as connected.

        Calls ``pyiceberg.catalog.load_catalog()`` inside a thread to
        avoid blocking the event loop.

        Returns:
            Self, to allow chaining with ``async with`` syntax.

        Raises:
            DriverError: On any PyIceberg or unexpected error.
        """
        _require_pyiceberg()
        self._logger.info(
            "Iceberg: connecting to catalog '%s' (type=%s)",
            self._catalog_name,
            self._catalog_type,
        )
        try:
            props = {
                "type": self._catalog_type,
                **self._catalog_properties,
            }
            self._connection = await asyncio.to_thread(
                load_catalog,
                self._catalog_name,
                **props,
            )
        except IcebergError as exc:
            raise DriverError(f"Iceberg catalog error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unknown connection error: {exc}") from exc

        if self._connection:
            self._connected = True
            self._initialized_on = time.time()
        return self

    async def close(self) -> None:
        """Release the catalog reference and clean up resources.

        Raises:
            DriverError: On any unexpected error during cleanup.
        """
        try:
            self._connection = None
            self._current_table = None
            self._connected = False
            gc.collect()
        except Exception as exc:
            raise DriverError(f"Iceberg close error: {exc}") from exc

    disconnect = close

    def _require_connection(self) -> None:
        """Assert that the driver is connected.

        Raises:
            DriverError: When the driver is not connected.
        """
        if not self._connected or self._connection is None:
            raise DriverError(
                "Iceberg driver is not connected. Call connection() first."
            )

    # ------------------------------------------------------------------
    # Required abstract method stubs
    # ------------------------------------------------------------------

    def execute(self, sentence: Any):  # noqa: D102
        raise NotImplementedError("Use query() for Iceberg read operations.")

    async def execute_many(self, sentence: str = ""):  # noqa: D102
        raise NotImplementedError

    async def prepare(self, sentence: str = ""):  # noqa: D102
        raise NotImplementedError

    def test_connection(self, **kwargs):  # noqa: D102
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Namespace operations  (TASK-005)
    # ------------------------------------------------------------------

    async def use(self, namespace: str = "") -> None:
        """Set the default namespace for subsequent table operations.

        Args:
            namespace: Namespace identifier to activate.
        """
        self._namespace = namespace

    async def create_namespace(
        self,
        namespace: str,
        properties: Optional[dict] = None,
    ) -> None:
        """Create a new namespace (database/schema) in the catalog.

        Args:
            namespace: Name of the namespace to create.
            properties: Optional dict of namespace-level metadata properties.

        Raises:
            DriverError: On PyIceberg error or if the namespace already exists.
        """
        self._require_connection()
        try:
            await asyncio.to_thread(
                self._connection.create_namespace,
                namespace,
                properties or {},
            )
        except NamespaceAlreadyExistsError as exc:
            raise DriverError(f"Namespace '{namespace}' already exists: {exc}") from exc
        except IcebergError as exc:
            raise DriverError(f"Iceberg create_namespace error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    async def list_namespaces(self) -> list[str]:
        """List all namespaces in the catalog.

        Returns:
            A list of namespace name strings.

        Raises:
            DriverError: On PyIceberg error.
        """
        self._require_connection()
        try:
            raw = await asyncio.to_thread(self._connection.list_namespaces)
            # PyIceberg returns a list of tuples, e.g. [("ns1",), ("ns2",)]
            return [".".join(ns) for ns in raw]
        except IcebergError as exc:
            raise DriverError(f"Iceberg list_namespaces error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    async def drop_namespace(self, namespace: str) -> None:
        """Drop an existing namespace from the catalog.

        Args:
            namespace: Name of the namespace to drop.

        Raises:
            DriverError: If the namespace does not exist or is not empty.
        """
        self._require_connection()
        try:
            await asyncio.to_thread(self._connection.drop_namespace, namespace)
        except NoSuchNamespaceError as exc:
            raise DriverError(f"Namespace '{namespace}' does not exist: {exc}") from exc
        except NamespaceNotEmptyError as exc:
            raise DriverError(f"Namespace '{namespace}' is not empty: {exc}") from exc
        except IcebergError as exc:
            raise DriverError(f"Iceberg drop_namespace error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    async def namespace_properties(self, namespace: str) -> dict:
        """Return namespace metadata as a dictionary.

        Args:
            namespace: Name of the namespace to inspect.

        Returns:
            Dictionary of namespace properties.

        Raises:
            DriverError: If the namespace does not exist.
        """
        self._require_connection()
        try:
            result = await asyncio.to_thread(
                self._connection.load_namespace_properties,
                namespace,
            )
            return dict(result)
        except NoSuchNamespaceError as exc:
            raise DriverError(f"Namespace '{namespace}' does not exist: {exc}") from exc
        except IcebergError as exc:
            raise DriverError(f"Iceberg namespace_properties error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    # ------------------------------------------------------------------
    # Table lifecycle  (TASK-006)
    # ------------------------------------------------------------------

    def _resolve_table_id(self, table_id: Union[str, tuple]) -> tuple:
        """Resolve a table_id, applying the default namespace if needed.

        Args:
            table_id: Table identifier as dot-string or tuple.

        Returns:
            A tuple suitable for PyIceberg catalog calls.
        """
        parsed = _parse_table_id(table_id)
        if len(parsed) == 1 and self._namespace:
            return (self._namespace, parsed[0])
        return parsed

    async def create_table(
        self,
        table_id: Union[str, tuple],
        schema: Any,
        partition_spec: Any = None,
        **kwargs,
    ) -> Any:
        """Create a new Iceberg table from a PyArrow or Iceberg schema.

        Args:
            table_id: Table identifier (dot-notation string or tuple).
            schema: PyArrow schema or PyIceberg ``Schema`` object.
            partition_spec: Optional PyIceberg ``PartitionSpec``.
            **kwargs: Additional keyword arguments passed to ``create_table``.

        Returns:
            The newly created PyIceberg ``Table`` object.

        Raises:
            DriverError: If the table already exists or on PyIceberg error.
        """
        self._require_connection()
        tid = self._resolve_table_id(table_id)
        try:
            def _create():
                args = {**kwargs}
                if partition_spec is not None:
                    args["partition_spec"] = partition_spec
                return self._connection.create_table(tid, schema=schema, **args)

            table = await asyncio.to_thread(_create)
            self._current_table = table
            return table
        except TableAlreadyExistsError as exc:
            raise DriverError(f"Table '{table_id}' already exists: {exc}") from exc
        except IcebergError as exc:
            raise DriverError(f"Iceberg create_table error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    async def register_table(
        self,
        table_id: Union[str, tuple],
        metadata_location: str,
    ) -> Any:
        """Register an existing Iceberg table by metadata file location.

        Args:
            table_id: Table identifier.
            metadata_location: Path/URI to the Iceberg metadata JSON file.

        Returns:
            The registered PyIceberg ``Table`` object.

        Raises:
            DriverError: On PyIceberg error.
        """
        self._require_connection()
        tid = self._resolve_table_id(table_id)
        try:
            table = await asyncio.to_thread(
                self._connection.register_table,
                tid,
                metadata_location,
            )
            self._current_table = table
            return table
        except IcebergError as exc:
            raise DriverError(f"Iceberg register_table error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    async def load_table(self, table_id: Union[str, tuple]) -> Any:
        """Load a table reference from the catalog.

        Stores the loaded table in ``self._current_table`` for subsequent
        read/write operations.

        Args:
            table_id: Table identifier.

        Returns:
            The PyIceberg ``Table`` object.

        Raises:
            DriverError: If the table does not exist.
        """
        self._require_connection()
        tid = self._resolve_table_id(table_id)
        try:
            table = await asyncio.to_thread(self._connection.load_table, tid)
            self._current_table = table
            return table
        except NoSuchTableError as exc:
            raise DriverError(f"Table '{table_id}' does not exist: {exc}") from exc
        except IcebergError as exc:
            raise DriverError(f"Iceberg load_table error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    async def table_exists(self, table_id: Union[str, tuple]) -> bool:
        """Check whether a table exists in the catalog.

        Args:
            table_id: Table identifier.

        Returns:
            ``True`` if the table exists, ``False`` otherwise.

        Raises:
            DriverError: On unexpected PyIceberg error.
        """
        self._require_connection()
        tid = self._resolve_table_id(table_id)
        try:
            return await asyncio.to_thread(self._connection.table_exists, tid)
        except IcebergError as exc:
            raise DriverError(f"Iceberg table_exists error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    async def rename_table(
        self,
        from_id: Union[str, tuple],
        to_id: Union[str, tuple],
    ) -> None:
        """Rename a table in the catalog.

        Args:
            from_id: Current table identifier.
            to_id: New table identifier.

        Raises:
            DriverError: On PyIceberg error.
        """
        self._require_connection()
        from_tid = self._resolve_table_id(from_id)
        to_tid = self._resolve_table_id(to_id)
        try:
            await asyncio.to_thread(self._connection.rename_table, from_tid, to_tid)
        except NoSuchTableError as exc:
            raise DriverError(f"Table '{from_id}' does not exist: {exc}") from exc
        except IcebergError as exc:
            raise DriverError(f"Iceberg rename_table error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    async def drop_table(
        self,
        table_id: Union[str, tuple],
        purge: bool = False,
    ) -> None:
        """Drop a table from the catalog.

        Args:
            table_id: Table identifier.
            purge: When ``True``, also purge underlying data files.

        Raises:
            DriverError: If the table does not exist or on PyIceberg error.
        """
        self._require_connection()
        tid = self._resolve_table_id(table_id)
        try:
            # Note: the purge parameter is not supported by all catalog
            # implementations (e.g. SqlCatalog). We attempt it and fall back
            # to a plain drop if the kwarg is rejected.
            def _drop() -> None:
                try:
                    self._connection.drop_table(tid, purge_requested=purge)
                except TypeError:
                    # Catalog does not support purge_requested – drop without it.
                    self._connection.drop_table(tid)

            await asyncio.to_thread(_drop)
            if self._current_table is not None:
                self._current_table = None
        except NoSuchTableError as exc:
            raise DriverError(f"Table '{table_id}' does not exist: {exc}") from exc
        except IcebergError as exc:
            raise DriverError(f"Iceberg drop_table error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    def tables(self, namespace: str = "") -> list[str]:
        """List table identifiers in the given namespace (synchronous).

        Args:
            namespace: Namespace to list tables in. Uses the default
                namespace when empty.

        Returns:
            List of fully-qualified table identifier strings.

        Raises:
            DriverError: When not connected or on PyIceberg error.
        """
        self._require_connection()
        ns = namespace or self._namespace
        try:
            raw = self._connection.list_tables(ns)
            return [".".join(t) for t in raw]
        except NoSuchNamespaceError as exc:
            raise DriverError(f"Namespace '{ns}' does not exist: {exc}") from exc
        except IcebergError as exc:
            raise DriverError(f"Iceberg list_tables error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected error: {exc}") from exc

    def table(self, tablename: str = "") -> dict:
        """Return schema and metadata for a table (synchronous).

        Args:
            tablename: Table identifier. Uses ``_current_table`` when empty.

        Returns:
            Dictionary with keys ``table_id``, ``schema``, ``location``,
            ``properties``, and ``snapshot_count``.

        Raises:
            DriverError: When no table is specified/loaded or on error.
        """
        self._require_connection()
        if tablename:
            tid = self._resolve_table_id(tablename)
            tbl = self._connection.load_table(tid)
        elif self._current_table is not None:
            tbl = self._current_table
        else:
            raise DriverError("No table specified and no current table is loaded.")
        meta = tbl.metadata
        return {
            "table_id": str(tbl.identifier),
            "schema": str(tbl.schema()),
            "location": meta.location,
            "properties": dict(meta.properties),
            "snapshot_count": len(meta.snapshots),
        }

    def schema(self, table_id: Union[str, tuple] = "") -> Any:
        """Return the PyArrow schema for a table (synchronous).

        Args:
            table_id: Table identifier. Uses ``_current_table`` when empty.

        Returns:
            The PyArrow ``Schema`` for the table.

        Raises:
            DriverError: When the table is not found or not loaded.
        """
        self._require_connection()
        if table_id:
            tid = self._resolve_table_id(table_id)
            tbl = self._connection.load_table(tid)
        elif self._current_table is not None:
            tbl = self._current_table
        else:
            raise DriverError("No table specified and no current table is loaded.")
        return tbl.schema().as_arrow()

    # ------------------------------------------------------------------
    # Read operations  (TASK-007)
    # ------------------------------------------------------------------

    def _to_factory(self, arrow_table: Any, factory: str) -> Any:
        """Convert a PyArrow Table to the requested output format.

        Args:
            arrow_table: The source ``pa.Table``.
            factory: One of ``"arrow"``, ``"pandas"``, ``"polars"``,
                ``"duckdb"``.

        Returns:
            Data in the requested format.

        Raises:
            DriverError: When an unsupported factory value is given.
        """
        if factory == "arrow":
            return arrow_table
        if factory == "pandas":
            return arrow_table.to_pandas()
        if factory == "polars":
            return pl.from_arrow(arrow_table)
        if factory == "duckdb":
            con = duckdb.connect()
            con.register("_iceberg_result", arrow_table)
            return con.execute("SELECT * FROM _iceberg_result").arrow()
        raise DriverError(f"Unsupported factory format: '{factory}'")

    async def _load_table_ref(self, table_id: Optional[Union[str, tuple]]) -> Any:
        """Load a table reference or return the current one.

        Args:
            table_id: Table identifier, or ``None`` to use ``_current_table``.

        Returns:
            A PyIceberg ``Table`` object.

        Raises:
            DriverError: When no table is specified or loaded.
        """
        if table_id:
            return await self.load_table(table_id)
        if self._current_table is not None:
            return self._current_table
        raise DriverError("No table_id specified and no current table is loaded.")

    async def scan(
        self,
        table_id: Optional[Union[str, tuple]] = None,
        row_filter: Any = None,
        selected_fields: Optional[tuple] = None,
        snapshot_id: Optional[int] = None,
        **kwargs,
    ) -> Any:
        """Low-level scan returning a PyArrow Table.

        Args:
            table_id: Table identifier. Uses ``_current_table`` when ``None``.
            row_filter: PyIceberg filter expression (string or expression).
            selected_fields: Tuple of column names to project.
            snapshot_id: Snapshot ID for historical reads.
            **kwargs: Extra arguments forwarded to ``table.scan()``.

        Returns:
            A ``pa.Table`` with the scan results.

        Raises:
            DriverError: On PyIceberg or scan error.
        """
        self._require_connection()
        tbl = await self._load_table_ref(table_id)
        try:
            def _scan() -> Any:
                scan_args: dict = {**kwargs}
                if row_filter is not None:
                    scan_args["row_filter"] = row_filter
                if selected_fields is not None:
                    scan_args["selected_fields"] = selected_fields
                if snapshot_id is not None:
                    scan_args["snapshot_id"] = snapshot_id
                return tbl.scan(**scan_args).to_arrow()

            return await asyncio.to_thread(_scan)
        except IcebergError as exc:
            raise DriverError(f"Iceberg scan error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected scan error: {exc}") from exc

    async def get(
        self,
        table_id: Optional[Union[str, tuple]] = None,
        columns: Optional[list] = None,
        row_filter: Any = None,
        factory: str = "arrow",
    ) -> Any:
        """Scan a table with optional column and row pruning.

        Args:
            table_id: Table identifier. Uses ``_current_table`` when ``None``.
            columns: List of column names to select.
            row_filter: PyIceberg row-filter expression.
            factory: Output format — ``"arrow"``, ``"pandas"``, ``"polars"``,
                or ``"duckdb"``.

        Returns:
            Scan results in the requested factory format.

        Raises:
            DriverError: On scan or conversion error.
        """
        selected_fields = tuple(columns) if columns else None
        arrow_table = await self.scan(
            table_id=table_id,
            row_filter=row_filter,
            selected_fields=selected_fields,
        )
        return self._to_factory(arrow_table, factory)

    async def query(
        self,
        sentence: Optional[str] = None,
        table_id: Optional[Union[str, tuple]] = None,
        factory: str = "arrow",
        **kwargs,
    ) -> list:
        """Execute a SQL query over an Iceberg table using DuckDB.

        Loads the table as a PyArrow dataset, registers it in an in-memory
        DuckDB connection, executes the SQL sentence, and returns results
        in the requested factory format.

        Args:
            sentence: SQL SELECT statement. When ``None``, returns all rows.
            table_id: Table identifier. Uses ``_current_table`` when ``None``.
            factory: Output format — ``"arrow"``, ``"pandas"``, ``"polars"``.
            **kwargs: Extra arguments forwarded to ``scan()``.

        Returns:
            ``[result, error]`` — compatible with the AsyncDB driver contract.
        """
        result = None
        error = None
        try:
            arrow_table = await self.scan(table_id=table_id, **kwargs)
            tablename = "iceberg_table"
            with duckdb.connect() as con:
                con.register(tablename, arrow_table)
                if sentence and sentence.strip().upper().startswith("SELECT"):
                    rst = con.execute(sentence)
                else:
                    rst = con.execute(f"SELECT * FROM {tablename}")
                if factory == "pandas":
                    result = rst.df()
                elif factory == "polars":
                    result = rst.pl()
                elif factory == "arrow":
                    result = rst.arrow()
                else:
                    result = rst.arrow()
        except DriverError:
            raise
        except Exception as exc:
            error = exc
        return [result, error]

    fetch_all = query

    async def queryrow(
        self,
        sentence: Optional[str] = None,
        table_id: Optional[Union[str, tuple]] = None,
        factory: str = "arrow",
        **kwargs,
    ) -> list:
        """Execute a SQL query and return a single row.

        Args:
            sentence: SQL SELECT statement. Automatically adds ``LIMIT 1``
                if not already present.
            table_id: Table identifier. Uses ``_current_table`` when ``None``.
            factory: Output format — ``"arrow"``, ``"pandas"``, ``"polars"``.
            **kwargs: Extra arguments forwarded to ``scan()``.

        Returns:
            ``[result, error]`` where ``result`` is a single-row object in
            the requested factory format.
        """
        result = None
        error = None
        try:
            if sentence is None:
                sentence = "SELECT * FROM iceberg_table LIMIT 1"
            elif "LIMIT" not in sentence.upper():
                sentence = f"{sentence.strip()} LIMIT 1"

            arrow_table = await self.scan(table_id=table_id, **kwargs)
            tablename = "iceberg_table"
            with duckdb.connect() as con:
                con.register(tablename, arrow_table)
                rst = con.execute(sentence)
                if factory == "pandas":
                    df = rst.df()
                    result = df.iloc[0] if not df.empty else None
                elif factory == "polars":
                    pl_df = rst.pl()
                    result = pl_df.row(0) if pl_df.shape[0] > 0 else None
                elif factory == "arrow":
                    at = rst.arrow()
                    result = at.slice(0, 1) if at.num_rows > 0 else None
                else:
                    result = rst.arrow()
        except DriverError:
            raise
        except Exception as exc:
            error = exc
        return [result, error]

    fetch_one = queryrow

    async def to_df(
        self,
        table_id: Optional[Union[str, tuple]] = None,
        factory: str = "pandas",
        **kwargs,
    ) -> Any:
        """Convert a full Iceberg table to a DataFrame.

        Args:
            table_id: Table identifier. Uses ``_current_table`` when ``None``.
            factory: Output format — ``"pandas"`` (default), ``"polars"``,
                or ``"arrow"``.
            **kwargs: Extra arguments forwarded to ``scan()``.

        Returns:
            A DataFrame in the requested factory format.

        Raises:
            DriverError: On scan or conversion error.
        """
        arrow_table = await self.scan(table_id=table_id, **kwargs)
        return self._to_factory(arrow_table, factory)

    # ------------------------------------------------------------------
    # Write operations  (TASK-008)
    # ------------------------------------------------------------------

    def _to_arrow(self, data: Any, target_schema: Optional[Any] = None) -> Any:
        """Convert data to a PyArrow Table, optionally casting to a target schema.

        Accepts ``pa.Table``, ``pd.DataFrame``, or ``pl.DataFrame``.
        When ``target_schema`` is provided (a ``pa.Schema``), the resulting
        Arrow table is cast to match that schema so that type mismatches
        (e.g. int64 vs int32) are resolved automatically.

        Args:
            data: Input data in one of the supported formats.
            target_schema: Optional PyArrow schema to cast the output to.

        Returns:
            A ``pa.Table`` (cast to ``target_schema`` when provided).

        Raises:
            DriverError: When the data type is not supported or casting fails.
        """
        if isinstance(data, pa.Table):
            arrow_table = data
        elif isinstance(data, pd.DataFrame):
            arrow_table = pa.Table.from_pandas(data, preserve_index=False)
        elif isinstance(data, pl.DataFrame):
            arrow_table = data.to_arrow()
        else:
            raise DriverError(
                f"Unsupported data type for write: {type(data).__name__}. "
                "Expected pa.Table, pd.DataFrame, or pl.DataFrame."
            )

        if target_schema is not None:
            try:
                arrow_table = arrow_table.cast(target_schema)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                # If direct cast fails, try field-by-field safe cast.
                arrays = []
                for field in target_schema:
                    col = arrow_table.column(field.name)
                    arrays.append(col.cast(field.type, safe=False))
                arrow_table = pa.table(
                    {field.name: arr for field, arr in zip(target_schema, arrays)},
                    schema=target_schema,
                )
        return arrow_table

    async def write(
        self,
        data: Any,
        table_id: Optional[Union[str, tuple]] = None,
        mode: str = "append",
        **kwargs,
    ) -> bool:
        """Write data to an Iceberg table.

        Args:
            data: Data to write (``pa.Table``, ``pd.DataFrame``, or
                ``pl.DataFrame``).
            table_id: Table identifier. Uses ``_current_table`` when ``None``.
            mode: Write mode — ``"append"`` (default) or ``"overwrite"``.
            **kwargs: Extra arguments (ignored; reserved for future use).

        Returns:
            ``True`` on success.

        Raises:
            DriverError: On unsupported mode, conversion error, or
                PyIceberg write error.
        """
        self._require_connection()
        tbl = await self._load_table_ref(table_id)
        target_schema = tbl.schema().as_arrow()
        arrow_table = self._to_arrow(data, target_schema=target_schema)
        try:
            if mode == "append":
                await asyncio.to_thread(tbl.append, arrow_table)
            elif mode == "overwrite":
                await asyncio.to_thread(tbl.overwrite, arrow_table)
            else:
                raise DriverError(f"Unsupported write mode: '{mode}'")
            return True
        except DriverError:
            raise
        except IcebergError as exc:
            raise DriverError(f"Iceberg write error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected write error: {exc}") from exc

    async def overwrite(
        self,
        data: Any,
        table_id: Optional[Union[str, tuple]] = None,
        overwrite_filter: Optional[Any] = None,
        **kwargs,
    ) -> bool:
        """Perform a partial overwrite of an Iceberg table with a filter.

        Args:
            data: Replacement data (``pa.Table``, ``pd.DataFrame``, or
                ``pl.DataFrame``).
            table_id: Table identifier. Uses ``_current_table`` when ``None``.
            overwrite_filter: PyIceberg filter expression selecting rows to
                replace. When ``None``, replaces all rows.
            **kwargs: Extra arguments (ignored; reserved for future use).

        Returns:
            ``True`` on success.

        Raises:
            DriverError: On conversion or PyIceberg error.
        """
        self._require_connection()
        tbl = await self._load_table_ref(table_id)
        target_schema = tbl.schema().as_arrow()
        arrow_table = self._to_arrow(data, target_schema=target_schema)
        try:
            def _overwrite():
                if overwrite_filter is not None:
                    tbl.overwrite(arrow_table, overwrite_filter=overwrite_filter)
                else:
                    tbl.overwrite(arrow_table)

            await asyncio.to_thread(_overwrite)
            return True
        except IcebergError as exc:
            raise DriverError(f"Iceberg overwrite error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected overwrite error: {exc}") from exc

    async def upsert(
        self,
        data: Any,
        table_id: Optional[Union[str, tuple]] = None,
        join_cols: Optional[list[str]] = None,
        **kwargs,
    ) -> Any:
        """Merge data into an Iceberg table using identifier fields (upsert).

        Rows are matched using ``identifier_field_ids`` defined on the
        Iceberg schema. Matched rows are updated; unmatched rows are inserted.
        The ``join_cols`` parameter documents which columns serve as the
        identifier — they must already be set on the schema.

        Args:
            data: Data to upsert (``pa.Table``, ``pd.DataFrame``, or
                ``pl.DataFrame``).
            table_id: Table identifier. Uses ``_current_table`` when ``None``.
            join_cols: Column names acting as the join/identifier key.
                Must already be configured on the Iceberg schema as
                ``identifier_field_ids``.
            **kwargs: Extra arguments (ignored; reserved for future use).

        Returns:
            The PyIceberg ``UpsertResult`` object with ``rows_updated`` and
            ``rows_inserted`` attributes.

        Raises:
            DriverError: When upsert is not supported, or on write error.
        """
        self._require_connection()
        tbl = await self._load_table_ref(table_id)
        target_schema = tbl.schema().as_arrow()
        arrow_table = self._to_arrow(data, target_schema=target_schema)
        if not hasattr(tbl, "upsert"):
            raise DriverError(
                "The installed PyIceberg version does not support upsert. "
                "Upgrade to pyiceberg>=0.11.0."
            )
        try:
            return await asyncio.to_thread(tbl.upsert, arrow_table)
        except IcebergError as exc:
            raise DriverError(f"Iceberg upsert error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected upsert error: {exc}") from exc

    async def add_files(
        self,
        table_id: Optional[Union[str, tuple]] = None,
        file_paths: Optional[list[str]] = None,
        **kwargs,
    ) -> bool:
        """Register existing Parquet files into an Iceberg table.

        The Parquet files must be compatible with the table's schema.

        Args:
            table_id: Table identifier. Uses ``_current_table`` when ``None``.
            file_paths: List of Parquet file paths/URIs to register.
            **kwargs: Extra arguments forwarded to ``table.add_files()``.

        Returns:
            ``True`` on success.

        Raises:
            DriverError: On PyIceberg error.
        """
        self._require_connection()
        tbl = await self._load_table_ref(table_id)
        if not file_paths:
            raise DriverError("file_paths must be a non-empty list of Parquet file paths.")
        try:
            await asyncio.to_thread(tbl.add_files, file_paths, **kwargs)
            return True
        except IcebergError as exc:
            raise DriverError(f"Iceberg add_files error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected add_files error: {exc}") from exc

    async def delete(
        self,
        table_id: Optional[Union[str, tuple]] = None,
        delete_filter: Optional[Any] = None,
        **kwargs,
    ) -> None:
        """Delete rows from an Iceberg table matching a filter expression.

        Args:
            table_id: Table identifier. Uses ``_current_table`` when ``None``.
            delete_filter: PyIceberg filter expression selecting rows to
                delete.
            **kwargs: Extra arguments (ignored; reserved for future use).

        Raises:
            DriverError: When ``delete_filter`` is missing or on write error.
        """
        self._require_connection()
        if delete_filter is None:
            raise DriverError("delete_filter is required for the delete operation.")
        tbl = await self._load_table_ref(table_id)
        try:
            await asyncio.to_thread(tbl.delete, delete_filter)
        except IcebergError as exc:
            raise DriverError(f"Iceberg delete error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected delete error: {exc}") from exc

    # ------------------------------------------------------------------
    # Metadata, snapshots & history  (TASK-009)
    # ------------------------------------------------------------------

    async def metadata(self, table_id: Optional[Union[str, tuple]] = None) -> dict:
        """Return comprehensive table metadata.

        Args:
            table_id: Table identifier. Uses ``_current_table`` when ``None``.

        Returns:
            Dictionary with keys: ``location``, ``properties``, ``schema``,
            ``partition_spec``, ``snapshot_count``, ``current_snapshot_id``.

        Raises:
            DriverError: On PyIceberg error.
        """
        self._require_connection()
        tbl = await self._load_table_ref(table_id)
        try:
            def _metadata() -> dict:
                meta = tbl.metadata
                current = tbl.current_snapshot()
                return {
                    "location": meta.location,
                    "properties": dict(meta.properties),
                    "schema": str(tbl.schema()),
                    "partition_spec": str(tbl.spec()),
                    "snapshot_count": len(meta.snapshots),
                    "current_snapshot_id": current.snapshot_id if current else None,
                }

            return await asyncio.to_thread(_metadata)
        except IcebergError as exc:
            raise DriverError(f"Iceberg metadata error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected metadata error: {exc}") from exc

    async def history(self, table_id: Optional[Union[str, tuple]] = None) -> list[dict]:
        """Return the snapshot history of a table.

        Args:
            table_id: Table identifier. Uses ``_current_table`` when ``None``.

        Returns:
            A list of dicts, each with keys: ``snapshot_id``,
            ``timestamp_ms``, ``parent_id``, ``summary``.

        Raises:
            DriverError: On PyIceberg error.
        """
        self._require_connection()
        tbl = await self._load_table_ref(table_id)
        try:
            def _history() -> list[dict]:
                snapshots = tbl.metadata.snapshots
                if not snapshots:
                    return []

                def _summary_dict(summary) -> dict:
                    """Safely convert a PyIceberg Summary to a plain dict."""
                    if summary is None:
                        return {}
                    try:
                        return summary.model_dump()
                    except AttributeError:
                        pass
                    try:
                        return {k: v for k, v in summary.additional_properties.items()}
                    except AttributeError:
                        return {}

                return [
                    {
                        "snapshot_id": s.snapshot_id,
                        "timestamp_ms": s.timestamp_ms,
                        "parent_id": s.parent_snapshot_id,
                        "summary": _summary_dict(s.summary),
                    }
                    for s in snapshots
                ]

            return await asyncio.to_thread(_history)
        except IcebergError as exc:
            raise DriverError(f"Iceberg history error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected history error: {exc}") from exc

    async def snapshots(self, table_id: Optional[Union[str, tuple]] = None) -> list:
        """Return raw snapshot objects for a table.

        Args:
            table_id: Table identifier. Uses ``_current_table`` when ``None``.

        Returns:
            A list of PyIceberg ``Snapshot`` objects, or an empty list if
            the table has no snapshots.

        Raises:
            DriverError: On PyIceberg error.
        """
        self._require_connection()
        tbl = await self._load_table_ref(table_id)
        try:
            def _snapshots() -> list:
                return list(tbl.metadata.snapshots) if tbl.metadata.snapshots else []

            return await asyncio.to_thread(_snapshots)
        except IcebergError as exc:
            raise DriverError(f"Iceberg snapshots error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected snapshots error: {exc}") from exc

    async def current_snapshot(
        self, table_id: Optional[Union[str, tuple]] = None
    ) -> dict:
        """Return information about the current (latest) snapshot.

        Args:
            table_id: Table identifier. Uses ``_current_table`` when ``None``.

        Returns:
            A dict with keys ``snapshot_id``, ``timestamp_ms``, ``parent_id``,
            and ``summary``, or an empty dict if the table has no snapshots.

        Raises:
            DriverError: On PyIceberg error.
        """
        self._require_connection()
        tbl = await self._load_table_ref(table_id)
        try:
            def _current() -> dict:
                snap = tbl.current_snapshot()
                if snap is None:
                    return {}

                def _summary_dict(summary) -> dict:
                    """Safely convert a PyIceberg Summary to a plain dict."""
                    if summary is None:
                        return {}
                    try:
                        return summary.model_dump()
                    except AttributeError:
                        pass
                    try:
                        return {k: v for k, v in summary.additional_properties.items()}
                    except AttributeError:
                        return {}

                return {
                    "snapshot_id": snap.snapshot_id,
                    "timestamp_ms": snap.timestamp_ms,
                    "parent_id": snap.parent_snapshot_id,
                    "summary": _summary_dict(snap.summary),
                }

            return await asyncio.to_thread(_current)
        except IcebergError as exc:
            raise DriverError(f"Iceberg current_snapshot error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Iceberg unexpected current_snapshot error: {exc}") from exc

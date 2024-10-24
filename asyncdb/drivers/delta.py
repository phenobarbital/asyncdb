#!/usr/bin/env python3
""" DeltaLake no-async Provider.
Notes on memcache Provider
--------------------
This provider implements a simple subset of funcionalities
over DeltaLake DeltaTable Protocol.
TODO: add Thread Pool Support.
"""
import asyncio
from collections.abc import Iterable
import time
import gc
import duckdb
from typing import Any, Union, Optional
from datetime import datetime
from pathlib import Path, PurePath
import polars as pl
from pyarrow import Table
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
import pyarrow.dataset as ds
from pyarrow import fs
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import datatable as dt
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, DeltaProtocolError
from ..exceptions import DriverError
from .base import (
    InitDriver,
)


class delta(InitDriver):
    _provider = "delta"
    _syntax = "nosql"

    def __init__(self, loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs) -> None:

        storage_options = params.pop("storage_options", {})
        self.storage_options = {"timeout": "120s", **storage_options}
        self._delta = params.pop("path", None)
        super().__init__(loop=loop, params=params, **kwargs)
        self.kwargs = params

    ### Context magic Methods
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    async def connection(self, path: Union[str, Path] = None, version: int = None):  # pylint: disable=W0236
        """
        __init DeltaLake initialization.
        """
        if path:
            self._delta = path
        if not self._delta:
            raise DriverError("Missing Path to DeltaTable.")
        self._logger.info(f"DeltaTable: Connecting to {self._delta}")
        if not isinstance(self._delta, str):
            self._delta = str(self._delta)
        try:
            if version is not None:
                self.kwargs["version"] = version
            if self._delta.startswith("s3:"):
                raw_fs, normalized_path = fs.FileSystem.from_uri(self._delta)
                self._filesystem = fs.SubTreeFileSystem(normalized_path, raw_fs)
                self._connection = DeltaTable(self._delta)
                self._storage = self._connection.to_pyarrow_dataset(filesystem=self._filesystem)
            else:
                self._filesystem = None
                self._connection = DeltaTable(self._delta, storage_options=self.storage_options, **self.kwargs)
                self._storage = self._connection.to_pyarrow_dataset()
        except DeltaError as exc:
            raise DriverError(message=f"{exc}") from exc
        except Exception as err:
            raise DriverError(message=f"Unknown DataTable Error: {err}") from err
        # is connected
        if self._connection:
            self._connected = True
            self._initialized_on = time.time()
        return self

    async def close(self):  # pylint: disable=W0221,W0236
        """
        Closing DeltaTable Connection
        """
        try:
            if self._connection:
                # Close any open file systems or resources
                if hasattr(self._connection, "filesystem"):
                    self._connection.filesystem.close()
                self._connection = None
                # Close PyArrow file system if connected to S3
                if self._filesystem:
                    self._filesystem.close()
                    self._filesystem = None
            self._connected = False
            self._storage = None
            self._delta = None
            # Optionally force garbage collection
            gc.collect()
        except Exception as err:
            raise DriverError(f"Unknown Closing Error: {err}") from err

    disconnect = close

    def load_version(self, version: Union[int, datetime]):
        if isinstance(version, int):
            self._connection.load_version(version)
        elif isinstance(version, datetime):
            self._connection.load_with_datetime(version)
        return self

    def metadata(self):
        return self._connection.metadata()

    def schema(self):
        return self._connection.schema()

    def test_connection(self, key: str = "test_123", optional: int = 1):  # pylint: disable=W0221,W0236
        result = None
        error = None
        try:
            self.set(key, optional)
            result = self.get(key)
        except Exception as err:  # pylint: disable=W0703
            error = err
        finally:
            self.delete(key)
            return [result, error]  # pylint: disable=W0150

    async def create(
        self, path: Union[str, Path], data: Any, name: Optional[str] = None, mode: str = "append", **kwargs
    ):
        if isinstance(path, str):
            path = Path(str).resolve()
        if isinstance(data, str):
            data = Path(str).resolve()
        if isinstance(data, Path):
            # open this file with Pandas or Arrow
            ext = data.suffix
            if ext == ".csv":
                read_options = pcsv.ReadOptions()
                parse_options = pcsv.ParseOptions()
                convert_options = pcsv.ConvertOptions()
                data = pcsv.read_csv(
                    data, read_options=read_options, parse_options=parse_options, convert_options=convert_options
                )
            elif ext in [".xls", ".xlsx"]:
                if ext == ".xls":
                    engine = "xlrd"
                else:
                    engine = "openpyxl"
                data = pd.read_excel(data, engine=engine)
            elif ext == ".parquet":
                data = pq.read_table(data)
        try:
            write_deltalake(path, data, name=name, mode=mode, **kwargs)
        except DeltaError as exc:
            raise DriverError(f"Delta: can't create a table in path {path}, error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Delta Error: {exc}") from exc

    def execute(self, sentence: Any):  # pylint: disable=W0221,W0236
        raise NotImplementedError

    async def execute_many(self, sentence=""):  # pylint: disable=W0221,W0236
        raise NotImplementedError

    async def prepare(self, sentence=""):
        raise NotImplementedError

    async def use(self, database=""):
        raise NotImplementedError

    async def get(
        self,
        partitions: Optional[list] = None,
        columns: Optional[list] = None,
        factory: Optional[str] = "pandas",
        **kwargs,
    ):  # pylint: disable=W0221,W0236
        """get.
        Getting Data from Delta using columns and
        partitions.
        """
        result = None
        args = {}
        if partitions:
            args = {"partitions": partitions}
        if columns:
            args["columns"] = columns
        try:
            if factory == "pandas":
                result = self._connection.to_pandas(**args)
            elif factory == "arrow":
                result = self._connection.to_pyarrow_table(**args)
            elif factory == "arrow_dataset":
                result = self._connection.to_pyarrow_dataset(**args, **kwargs)
            return result
        except (DeltaError, DeltaProtocolError) as exc:
            raise DriverError(f"DeltaTable Error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Delta Get Error: {exc}") from exc

    async def query(
        self,
        sentence: Optional[str] = None,
        partitions: Optional[list] = None,
        tablename: Optional[str] = "arrow_dataset",
        factory: Optional[str] = "pandas",
        **kwargs,
    ):  # pylint: disable=W0221,W0236
        """query.
        Getting Data from Delta using a query (with DuckDB)
        """
        result = None
        error = None
        args = {}
        if partitions:
            args = {"partitions": partitions}
        try:
            # connect to an in-memory database
            with duckdb.connect() as con:
                dataset = self._connection.to_pyarrow_dataset(**args, **kwargs)
                ex_data = duckdb.arrow(dataset)
                if sentence and sentence.strip().upper().startswith("SELECT"):
                    # Register the Arrow dataset as a table
                    con.register(tablename, dataset)
                    rst = con.execute(sentence)
                    if factory == "pandas":
                        result = rst.df()
                    elif factory == "polars":
                        result = rst.df_polars()
                    elif factory == "arrow":
                        result = rst.arrow()
                else:
                    result = ex_data.filter(sentence)
                    if factory == "pandas":
                        result = result.to_df()
                    elif factory == "polars":
                        result = result.pl()
                    elif factory == "arrow":
                        result = result.to_arrow_table()
        except (DeltaError, DeltaProtocolError) as exc:
            error = exc
        except Exception as exc:
            error = exc
        finally:
            return [result, error]  # pylint: disable=W0150

    fetch_all = query

    async def queryrow(
        self,
        sentence: Optional[str] = None,
        partitions: Optional[list] = None,
        tablename: Optional[str] = "arrow_dataset",
        factory: Optional[str] = "pandas",
        **kwargs,
    ):
        """queryrow.
        Get a single row from Delta using a query (with DuckDB).
        """
        result = None
        dataset = None
        error = None
        args = {}
        if partitions:
            args = {"partitions": partitions}

        # Modify the SQL query to return only one row using LIMIT 1
        if sentence and not sentence.strip().upper().startswith("SELECT"):
            raise ValueError("Only SELECT queries are allowed")

        if "LIMIT" not in sentence.upper():
            sentence = f"{sentence.strip()} LIMIT 1"

        try:
            rst = None
            with duckdb.connect() as con:
                dataset = self._connection.to_pyarrow_dataset(**args, **kwargs)
                # Register the dataset as a table in DuckDB
                con.register(tablename, dataset)
                rst = con.execute(sentence)
                # Return the result based on the factory
                if rst:
                    if factory == "pandas":
                        df = rst.df()
                        if not df.empty:
                            result = df.iloc[0]
                    elif factory == "polars":
                        pl_df = rst.df_polars()
                        if pl_df.shape[0] > 0:
                            result = pl_df.row(0)  # Get the first row
                    elif factory == "arrow":
                        arrow_table = rst.arrow()
                        if arrow_table.num_rows > 0:
                            result = arrow_table.slice(0, 1)  # Return the first row
                    else:
                        raise ValueError(f"Unsupported factory type: {factory}")
        except (DeltaError, DeltaProtocolError) as exc:
            error = exc
        except Exception as exc:
            error = exc
        finally:
            rst = None
            dataset = None
            return [result, error]  # pylint: disable=W0150

    fetch_one = queryrow

    async def file_to_parquet(
        self, filename: Union[str, Path], parquet: str, factory: str = "pandas", chunksize: int = 100000, **kwargs
    ):
        """file_to_parquet.

        Creating a parquet file from a File (CSV/XLSX) object.
        """
        if isinstance(filename, str):
            filename = Path(filename).resolve()
        ext = filename.suffix
        arguments = kwargs.get("pd_args", {})
        df = None
        if ext in (".csv", ".CSV", ".txt", ".TXT"):
            if factory == "pandas":
                csv_chunks = pd.read_csv(
                    filename,
                    quotechar='"',
                    decimal=",",
                    engine="c",
                    keep_default_na=False,
                    na_values=["NULL", "TBD"],
                    na_filter=True,
                    skipinitialspace=True,
                    chunksize=chunksize,  # Process in chunks
                    **arguments,
                )
                # Write Parquet using schema derived from the first chunk
                parquet_writer = None
                try:
                    for _, chunk in enumerate(csv_chunks):
                        # Convert chunk to Arrow Table
                        table = Table.from_pandas(chunk)
                        if parquet_writer is None:
                            # Define the schema from the first chunk
                            schema = table.schema
                            # Create the ParquetWriter with the schema
                            parquet_writer = pq.ParquetWriter(parquet, schema, compression="snappy")
                        # Write the chunk to the Parquet file
                        parquet_writer.write_table(table)
                finally:
                    if parquet_writer:
                        parquet_writer.close()
            elif factory == "datatable":
                frame = dt.fread(filename, **arguments)
                df = frame.to_pandas()
            elif factory == "arrow":
                atable = pcsv.read_csv(filename, **arguments)
        elif ext in [".xls", ".xlsx"]:
            if ext == ".xls":
                engine = "xlrd"
            else:
                engine = "openpyxl"
            df = pd.read_excel(
                filename, na_values=["NULL", "TBD"], na_filter=True, engine=engine, keep_default_na=False, **arguments
            )
        try:
            if df is not None:
                df.to_parquet(parquet, engine="pyarrow", compression="snappy")
            elif atable is not None:
                pq.write_table(atable, parquet, compression="snappy")
        except Exception as exc:
            raise DriverError(f"Delta File To Parquet Error: {exc}") from exc

    async def write(
        self,
        data: Union[pd.DataFrame, dt.Frame, pl.DataFrame, Iterable],
        table_id: str,
        path: PurePath,
        if_exists: str = "append",
        partition_by: list = None,
        **kwargs,
    ):
        """write.
        Writing Data into Delta Table.

        Args:
        - data: Data to be written,
          it can be a Pandas DataFrame, a Polars DataFrame, a DataTable Frame or a list.
        - table_id: Table Identifier
        - path: Path to the Delta Table.
        - if_exists: if_exists mode, default is "append", can be "error", "overwrite" or "ignore".
        """
        args = {"mode": if_exists, "engine": "rust", **kwargs}
        if partition_by is not None:
            args["partition_by"] = partition_by
        try:
            destination = path.joinpath(table_id)
            if isinstance(data, pd.DataFrame):
                write_deltalake(destination, data, **args)
            elif isinstance(data, (dt.Frame, pl.DataFrame)):
                if isinstance(data, dt.Frame):
                    data = pl.DataFrame(data.to_pandas())
                data.write_delta(destination, **args)
            else:
                # assuming a pyarrow:
                write_deltalake(destination, data, **args)
            # Destination will be the new file path:
            self._delta = destination
        except (DeltaError, DeltaProtocolError) as exc:
            raise DriverError(f"DeltaTable Error: {exc}") from exc
        except Exception as exc:
            raise DriverError(f"Delta Write Error: {exc}") from exc

    async def to_df(
        self,
        partitions: Optional[list] = None,
        columns: Optional[list] = None,
        factory: Optional[str] = "pandas",
        **kwargs,
    ):  # pylint: disable=W0221,W0236
        """query.
        Getting Delta Table into a Dataframe.

        Args:
        - partitions: List of Partitions.
        - columns: List of Columns.
        - factory: Factory to be used, default is "pandas",
            can be "arrow", "polars" or "datatable".
        """
        result = None
        error = None
        args = {}
        if partitions:
            args = {"partitions": partitions}
        if columns:
            args["columns"] = columns
        try:
            if factory == "pandas":
                result = self._connection.to_pandas(**args)
            elif factory == "arrow":
                result = self._connection.to_pyarrow_table(**args)
            elif factory == "arrow_dataset":
                result = self._connection.to_pyarrow_dataset(**args, **kwargs)
            elif factory == "polars":
                table = self._connection.to_pyarrow_table(**args)
                result = pl.from_arrow(table)
        except (DeltaError, DeltaProtocolError) as exc:
            error = exc
        except Exception as exc:
            error = exc
        finally:
            return [result, error]  # pylint: disable=W0150

    async def copy_to(
        self,
        source: Union[str, Path],
        destination: Union[str, Path],
        columns: list[str],
        separator: str = ",",
        has_header: bool = True,
        lazy: bool = True,
        replace_destination: bool = False,
        compression: str = "zstd",
        **kwargs,
    ) -> Path:
        """
        Copy a CSV file efficiently to Parquet using Polars.

        Returns:
        - Path: Path to the created Parquet file.
        """
        if isinstance(source, str):
            source = Path(source)
        if isinstance(destination, str):
            destination = Path(destination)

        if not source.exists():
            raise FileNotFoundError(f"Parquet: File {source} not found")

        if destination.exists():
            if replace_destination is True:
                destination.unlink()
            else:
                raise FileExistsError(f"Parquet: File {destination} already exists")
        else:
            if destination.parent.exists() is False:
                destination.parent.mkdir(parents=True, exist_ok=True)

        if lazy is True:
            mtd = pl.scan_csv
        else:
            mtd = pl.read_csv
        try:
            df = mtd(
                source,
                separator=separator,
                has_header=has_header,
                new_columns=columns,  # Pass the column names
                infer_schema_length=0,  # Infer schema from the first batch of rows
                low_memory=True,  # Enable low memory mode for large files
                **kwargs,
            )
            if lazy is True:
                df.sink_parquet(destination, compression=compression, row_group_size=100_000)
            else:
                df.write_parquet(destination, compression=compression, row_group_size=100_000)
            # Read the Parquet file metadata
            pq_file = pq.ParquetFile(destination)
            metadata = pq_file.metadata
            return destination, metadata
        except FileExistsError:
            raise
        except Exception as err:
            raise DriverError(f"Delta: Error on COPY to Parquet: {err!s}") from err

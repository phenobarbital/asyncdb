#!/usr/bin/env python3
""" DeltaLake no-async Provider.
Notes on memcache Provider
--------------------
This provider implements a simple subset of funcionalities
over DeltaLake DeltaTable Protocol.
TODO: add Thread Pool Support.
"""
import asyncio
import time
from typing import Any, Union, Optional
from datetime import datetime
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow.csv as pcsv
from pyarrow import fs
import pandas as pd
import datatable as dt
from deltalake import DeltaTable
from deltalake import PyDeltaTableError
from deltalake.table import DeltaTableProtocolError
from deltalake.writer import write_deltalake
from asyncdb.exceptions import (
    DriverError
)
from .abstract import (
    InitDriver,
)


class delta(InitDriver):
    _provider = "delta"
    _syntax = "nosql"

    def __init__(
            self,
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        try:
            self.storage_options = params["storage_options"]
            del params["storage_options"]
        except KeyError:
            self.storage_options = {}
        try:
            self.filename = params["filename"]
            del params["filename"]
        except KeyError as ex:
            raise DriverError(
                "Delta: Missing Filename on Parameters"
            ) from ex
        super().__init__(
            loop=loop, params=params, **kwargs
        )
        self.kwargs = params

### Context magic Methods
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # Create a memcache Connection
    async def connection(self, version: int = None): # pylint: disable=W0236
        """
        __init Memcache initialization.
        """
        self._logger.info(
            f"DeltaTable: Connecting to {self.filename}"
        )
        try:
            if version is not None:
                self.kwargs["version"] = version
            if self.filename.startswith('s3:'):
                raw_fs, normalized_path = fs.FileSystem.from_uri(self.filename)
                filesystem = fs.SubTreeFileSystem(normalized_path, raw_fs)
                self._connection = DeltaTable(self.filename)
                self._storage = self._connection.to_pyarrow_dataset(filesystem=filesystem)
            # filesystem = fs.SubTreeFileSystem(self.filename, fs.LocalFileSystem())
            else:
                self._connection = DeltaTable(
                    self.filename, storage_options=self.storage_options, **self.kwargs
                )
        except PyDeltaTableError as exc:
            raise DriverError(
                message=f"{exc}"
            ) from exc
        except Exception as err:
            raise DriverError(
                message=f"Unknown DataTable Error: {err}"
            ) from err
        # is connected
        if self._connection:
            self._connected = True
            self._initialized_on = time.time()
        return self

    async def close(self): # pylint: disable=W0221,W0236
        """
        Closing DeltaTable Connection
        """
        try:
            pass # TODO
        except Exception as err:
            raise DriverError(
               f"Unknown Closing Error: {err}"
            ) from err

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

    def test_connection(self, key: str = 'test_123', optional: int = 1): # pylint: disable=W0221,W0236
        result = None
        error = None
        try:
            self.set(key, optional)
            result = self.get(key)
        except Exception as err: # pylint: disable=W0703
            error = err
        finally:
            self.delete(key)
            return [result, error] # pylint: disable=W0150

    async def create(self, path: Union[str, Path], data: Any, name: Optional[str] = None, mode: str = 'append', **kwargs):
        if isinstance(path, str):
            path = Path(str).resolve()
        if isinstance(data, str):
            data = Path(str).resolve()
        if isinstance(data, Path):
            # open this file with Pandas or Arrow
            ext = data.suffix
            if ext == '.csv':
                read_options = pcsv.ReadOptions()
                parse_options = pcsv.ParseOptions()
                convert_options = pcsv.ConvertOptions()
                data = pcsv.read_csv(
                    data,
                    read_options=read_options,
                    parse_options=parse_options,
                    convert_options=convert_options
                )
            elif ext in ['.xls', '.xlsx']:
                if ext == '.xls':
                    engine = 'xlrd'
                else:
                    engine = 'openpyxl'
                data = pd.read_excel(
                    data,
                    engine=engine
                )
            elif ext == '.parquet':
                data = pq.read_table(
                    data
                )
        try:
            write_deltalake(
                path, data, name=name, mode=mode, **kwargs
            )
        except PyDeltaTableError as exc:
            raise DriverError(
                f"Delta: can't create a table in path {path}, error: {exc}"
            ) from exc
        except Exception as exc:
            raise DriverError(
                f"Delta Error: {exc}"
            ) from exc


    def execute(self, sentence: Any): # pylint: disable=W0221,W0236
        raise NotImplementedError

    async def execute_many(self, sentence=""): # pylint: disable=W0221,W0236
        raise NotImplementedError

    async def prepare(self, sentence=""):
        raise NotImplementedError

    async def use(self, database=""):
        raise NotImplementedError

    async def get(
        self,
        partitions: Optional[list] = None,
        columns: Optional[list] = None,
        factory: Optional[str] = 'pandas',
        **kwargs
    ): # pylint: disable=W0221,W0236
        """get.
        Getting Data from Delta using columns and
        partitions.
        """
        result = None
        args = {}
        if partitions:
            args = {
                "partitions": partitions
            }
        if columns:
            args['columns'] = columns
        try:
            if factory == 'pandas':
                result = self._connection.to_pandas(
                    **args
                )
            elif factory == 'arrow':
                result = self._connection.to_pyarrow_table(
                    **args
                )
            elif factory == 'arrow_dataset':
                result = self._connection.to_pyarrow_dataset(
                    **args, **kwargs
                )
            return result
        except (PyDeltaTableError, DeltaTableProtocolError) as exc:
            raise DriverError(
                f"DeltaTable Error: {exc}"
            ) from exc
        except Exception as exc:
            raise DriverError(
                f"Query Error: {exc}"
            ) from exc

    async def query(
            self,
            sentence: Optional[str] = None,
            partitions: Optional[list] = None,
            columns: Optional[list] = None,
            factory: Optional[str] = 'pandas',
            **kwargs
    ): # pylint: disable=W0221,W0236
        """query.
        Getting Data from Delta using a query (with DuckDB) or via columns and
        partitions.
        """
        result = None
        error = None
        args = {}
        if partitions:
            args = {
                "partitions": partitions
            }
        if columns:
            args['columns'] = columns
        try:
            if factory == 'pandas':
                result = self._connection.to_pandas(
                    **args
                )
            elif factory == 'arrow':
                result = self._connection.to_pyarrow_table(
                    **args
                )
            elif factory == 'arrow_dataset':
                result = self._connection.to_pyarrow_dataset(
                    **args, **kwargs
                )
        except (PyDeltaTableError, DeltaTableProtocolError) as exc:
            error = exc
            raise DriverError(
                f"DeltaTable Error: {exc}"
            ) from exc
        except Exception as exc:
            error = exc
            raise DriverError(
                f"Query Error: {exc}"
            ) from exc
        finally:
            return [result, error]  # pylint: disable=W0150

    fetch_all = query

    def queryrow(self, key: str, *args): # pylint: disable=W0221,W0236
        return self.get(key, *args)

    fetch_one = queryrow

    async def file_to_parquet(
            self, filename: Union[str, Path],
            parquet: str,
            factory: str = 'pandas',
            **kwargs
    ):
        """csv_to_parquet.

        Creating a parquet file from a CSV object.
        """
        if isinstance(filename, str):
            filename = Path(filename).resolve()
        ext = filename.suffix
        arguments = kwargs.get('pd_args', {})
        df = None
        if ext in ('.csv', '.txt', '.TXT', '.CSV'):
            if factory == 'pandas':
                df = pd.read_csv(
                    filename,
                    quotechar='"',
                    decimal=',',
                    engine='c',
                    keep_default_na=False,
                    na_values=['NULL', 'TBD'],
                    na_filter=True,
                    skipinitialspace=True,
                    **arguments
                )
            elif factory == 'datatable':
                frame = dt.fread(filename, **arguments)
                df = frame.to_pandas()
            elif factory == 'arrow':
                atable = pcsv.read_csv(filename, **arguments)
        elif ext in ['.xls', '.xlsx']:
            if ext == '.xls':
                engine = 'xlrd'
            else:
                engine = 'openpyxl'
            df = pd.read_excel(
                filename,
                na_values=['NULL', 'TBD'],
                na_filter=True,
                engine=engine,
                keep_default_na=False,
                **arguments
            )
        try:
            if df is not None:
                df.to_parquet(parquet, engine='pyarrow', compression='snappy')
            elif atable is not None:
                pq.write_table(atable, parquet, compression='snappy')
        except Exception as exc:
            raise DriverError(
                f"Query Error: {exc}"
            ) from exc

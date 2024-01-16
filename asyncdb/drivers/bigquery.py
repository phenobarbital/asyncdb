import os
from typing import Any
from collections.abc import Iterable
from pathlib import Path
import asyncio
import pandas_gbq
import pandas as pd
from google.cloud import bigquery as bq
from google.cloud.exceptions import Conflict
from google.oauth2 import service_account

from asyncdb.exceptions import DriverError
from .sql import SQLDriver


class bigquery(SQLDriver):
    _provider = "bigquery"
    _syntax = "sql"
    _test_query = "SELECT 1"

    def __init__(self, dsn: str = "", loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs) -> None:
        self._credentials = params.get("credentials", None)
        if self._credentials:
            self._credentials = Path(self._credentials).expanduser().resolve()
        self._account = None
        self._dsn = ""
        self._project_id = params.get("project_id", None)
        super().__init__(dsn=dsn, loop=loop, params=params, **kwargs)
        if not self._credentials:
            self._account = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", None)
        if self._account is None and self._credentials is None:
            raise DriverError("BigQuery: Missing account Credentials")
        self._connection = None  # BigQuery does not use traditional connections

    async def connection(self):
        """Initialize BigQuery client.
        # Assuming that authentication is handled outside (via environment variables or similar)
        """
        try:
            if self._credentials:  # usage of explicit credentials
                self.credentials = service_account.Credentials.from_service_account_file(self._credentials)
                if not self._project_id:
                    self._project_id = self.credentials.project_id
                self._connection = bq.Client(credentials=self.credentials, project=self._project_id)
                self._connected = True
            else:
                self.credentials = self._account
                self._connection = bq.Client(project=self._project_id)
        except Exception as e:
            raise DriverError(f"BigQuery: Error initializing client: {e}")
        return self

    async def close(self):
        # BigQuery client does not maintain persistent connections, so nothing to close here.
        self._connected = False

    disconnect = close

    async def execute(self, query, **kwargs):
        """
        Execute a BigQuery query
        """
        if not self._connection:
            await self.connection()
        try:
            job = self._connection.query(query, **kwargs)
            result = job.result()  # Waits for the query to finish
            return result
        except Exception as e:
            raise DriverError(f"BigQuery: Error executing query: {e}")

    async def execute_many(self, query, **kwargs):
        """
        Execute a BigQuery query
        """
        if not self._connection:
            await self.connection()
        try:
            job = self._connection.query(query, **kwargs)
            result = job.result()  # Waits for the query to finish
            return result
        except Exception as e:
            raise DriverError(f"BigQuery: Error executing query: {e}")

    async def prepare(self, sentence: str, **kwargs):
        pass

    def get_query_config(self, **kwargs):
        return bq.QueryJobConfig(**kwargs)

    def get_load_config(self, **kwargs):
        args = {}
        _type = kwargs.pop("type", "json")
        if _type == "json":
            args = {"source_format": bq.SourceFormat.NEWLINE_DELIMITED_JSON, "autodetect": True}
        args = {**kwargs, **args}
        return bq.LoadJobConfig(**args)

    async def create_dataset(self, dataset_id: str):
        try:
            dataset_ref = bq.DatasetReference(self._connection.project, dataset_id)
            dataset_obj = bq.Dataset(dataset_ref)
            dataset_obj = self._connection.create_dataset(dataset_obj)
            return dataset_obj
        except Conflict:
            self._logger.warning(f"Dataset {self._connection.project}.{dataset_obj.dataset_id} already exists")
            return dataset_obj
        except Exception as exc:
            self._logger.error(f"Error creating Dataset: {exc}")
            raise DriverError(f"Error creating Dataset: {exc}")

    async def create_table(self, dataset_id, table_id, schema):
        """
        Create a new table in the specified BigQuery dataset.
        :param dataset_id: The ID of the dataset
        :param table_id: The ID of the table to create
        :param schema: A list of google.cloud.bigquery.SchemaField objects
        """
        if not self._connection:
            await self.connection()

        dataset_ref = bq.DatasetReference(self._connection.project, dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = bq.Table(table_ref, schema=schema)
        try:
            table = self._connection.create_table(table)  # API request
            self._logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
            return table
        except Conflict:
            self._logger.warning(f"Table {table.project}.{table.dataset_id}.{table.table_id} already exists")
            return table
        except Exception as e:
            raise DriverError(f"BigQuery: Error creating table: {e}")

    async def truncate_table(self, table_id: str, dataset_id: str):
        """
        Truncate a BigQuery table by overwriting with an empty table.
        """
        if not self._connection:
            await self.connection()

        # Construct a reference to the dataset
        dataset_ref = bq.DatasetReference(self._connection.project, dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = self._connection.get_table(table_ref)  # API request to fetch the table schema

        # Create an empty table with the same schema
        job_config = bq.QueryJobConfig(destination=table_ref)
        job_config.write_disposition = bq.WriteDisposition.WRITE_TRUNCATE

        try:
            job = self._connection.query(f"SELECT * FROM `{table_ref}` WHERE FALSE", job_config=job_config)
            job.result()  # Wait for the job to finish
            self._logger.info(f"Truncated table {dataset_id}.{table_id}")
        except Exception as e:
            raise DriverError(f"BigQuery: Error truncating table: {e}")

    async def query(self, sentence: str, **kwargs):
        if not self._connection:
            await self.connection()
        await self.valid_operation(sentence)
        self.start_timing()
        error = None
        try:
            job = self._connection.query(sentence, **kwargs)
            result = job.result()  # Waits for the query to finish
        except Exception as e:
            error = f"BigQuery: Error executing query: {e}"
        finally:
            self.generated_at()
            if error:
                return [None, error]
            return await self._serializer(result, error)  # pylint: disable=W0150

    async def queryrow(self, sentence: str):
        pass

    async def fetch(self, sentence: str, use_pandas: bool = False, **kwargs):
        """fetch.

        Get a Query directly into a Pandas Dataframe.
        Args:
            sentence (str): Query to be executed.
        """
        if not self._connection:
            await self.connection()
        await self.valid_operation(sentence)
        self.start_timing()
        error = None
        try:
            if use_pandas is True:
                result = pandas_gbq.read_gbq(
                    sentence,
                    project_id=self._project_id,
                    credentials=self.credentials,
                    dialect="standard",
                    use_bqstorage_api=True,
                    **kwargs,
                )
            else:
                result = self._connection.query(sentence, **kwargs).to_dataframe()
        except Exception as e:
            error = f"BigQuery: Error executing Fetch: {e}"
        finally:
            self.generated_at()
            if error:
                return [None, error]
            return (result, error)  # pylint: disable=W0150

    async def fetch_all(self, query, *args):
        """
        Fetch all results from a BigQuery query
        """
        results = await self.execute(query, *args)
        return results

    async def fetch_one(self, query, *args):
        """
        Fetch all results from a BigQuery query
        """
        results = await self.execute(query, *args)
        return [dict(row) for row in results]

    async def write(
        self,
        table_id: str,
        data,
        dataset_id: str = None,
        use_streams: bool = False,
        use_pandas: bool = False,
        if_exists: str = "append",
        **kwargs,
    ):
        """
        Write data to a BigQuery table
        """
        if not self._connection:
            await self.connection()
        try:
            if isinstance(data, pd.DataFrame):
                if use_pandas is True:
                    job = await self._thread_func(
                        self._connection.load_table_from_dataframe, data, table_id, if_exists=if_exists, **kwargs
                    )
                else:
                    job = await self._thread_func(
                        data.to_gbq, table_id, project_id=self._project_id, if_exists=if_exists
                    )
            elif isinstance(data, list):
                dataset_ref = self._connection.dataset(dataset_id)
                table_ref = dataset_ref.table(table_id)
                table = bq.Table(table_ref)
                if use_streams is True:
                    errors = await self._thread_func(self._connection.insert_rows_json, table, data, **kwargs)
                    if errors:
                        raise RuntimeError(f"Errors occurred while inserting rows: {errors}")
                else:
                    job = await self._thread_func(self._connection.load_table_from_json, table, data, **kwargs)
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, job.result)
                    if job.errors and len(job.errors) > 0:
                        raise RuntimeError(f"Job failed with errors: {job.errors}")
                    else:
                        self._logger.info(f"Loaded {len(data)} rows into {table_id}")

            self._logger.info(f"Inserted rows into {table_id}")
        except Exception as e:
            raise DriverError(f"BigQuery: Error writing to table: {e}")

    async def load_table_from_uri(
        self,
        source_uri: str,
        table: Any = None,
        job_config=None,
        dataset_id: str = None,
        table_id: str = None,
    ):
        """
        Load a BigQuery table from a Google Cloud Storage URI
        """
        if not self._connection:
            await self.connection()
        if not table:
            dataset_ref = self._connection.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)
            table = bq.Table(table_ref)
        try:
            job = await self._thread_func(
                self._connection.load_table_from_uri, source_uri, table, job_config=job_config
            )
            job.result()  # Waits for table load to complete.
            self._logger.info(f"Loaded {job.output_rows} rows into {table.project}.{table.dataset_id}.{table.table_id}")
            return job
        except Exception as e:
            raise DriverError(f"BigQuery: Error loading table from URI: {e}")

    @property
    def connected(self):
        return self._connection is not None

    def is_connected(self):
        return self._connected

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError

    async def use(self, database: str):
        raise NotImplementedError  # pragma: no cover

import os
from typing import Any, Optional, Union
from collections.abc import Iterable
import uuid
from enum import Enum
import re
from pathlib import Path, PurePath
from dataclasses import is_dataclass
import asyncio
import aiofiles
import pandas_gbq
import pandas as pd
import pyarrow
import datetime
from google.cloud import storage
from google.cloud import bigquery as bq
from google.cloud.exceptions import Conflict, NotFound
from google.cloud.bigquery import LoadJobConfig, SourceFormat
from google.oauth2 import service_account
from .sql import SQLDriver
from ..exceptions import DriverError
from ..interfaces.model import ModelBackend
from ..models import Model
from ..utils.types import Entity


class bigquery(SQLDriver, ModelBackend):
    _provider = "bigquery"
    _syntax = "sql"
    _test_query = "SELECT 1"
    _dsn_template: str = ""

    def __init__(self, dsn: str = "", loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs) -> None:
        self._credentials = params.get("credentials")
        if self._credentials:
            if isinstance(self._credentials, str):
                self._credentials = Path(self._credentials).expanduser().resolve()
            elif isinstance(self._credentials, PurePath):
                self._credentials = self._credentials.resolve()
        self._account = None
        self._dsn = ""
        self._project_id = params.get("project_id")
        super().__init__(dsn=dsn, loop=loop, params=params, **kwargs)
        if not self._credentials:
            self._account = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", None)
        if self._account is None and self._credentials is None:
            raise DriverError("BigQuery: Missing account Credentials")
        # BigQuery does not use traditional connections
        self._connection = None

    async def connection(self):
        """Initialize BigQuery client.
        Assuming that authentication is handled outside
        (via environment variables or similar)
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
        self._connection = None

    async def execute(self, query, **kwargs):
        """
        Execute a BigQuery query
        """
        result = None
        error = None
        if not self._connection:
            await self.connection()
        try:
            job = self._connection.query(query, **kwargs)
            result = job.result()  # Waits for the query to finish
        except Exception as e:
            error = e
        return result, error

    async def execute_many(self, query, **kwargs):
        """
        Execute a BigQuery query
        """
        result = None
        error = None
        if not self._connection:
            await self.connection()
        try:
            job = self._connection.query(query, **kwargs)
            result = job.result()  # Waits for the query to finish
        except Exception as e:
            error = e
        return result, error

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

    create_keyspace = create_dataset

    async def drop_dataset(self, dataset_id: str):
        try:
            dataset_ref = bq.DatasetReference(self._connection.project, dataset_id)
            self._connection.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
            return True
        except Exception as exc:
            self._logger.error(f"Error deleting Dataset: {exc}")
            raise DriverError(f"Error deleting Dataset: {exc}")

    drop_keyspace = drop_dataset

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
            self._logger.warning(
                f"Table {table.project}.{table.dataset_id}.{table.table_id} already exists"
            )
            return table
        except Exception as e:
            raise DriverError(f"BigQuery: Error creating table: {e}") from e

    async def truncate_table(self, table_id: str, dataset_id: str):
        """
        Truncate a BigQuery table by overwriting it with an empty table.

        Parameters:
            dataset_id (str): The ID of the dataset containing the table.
            table_id (str): The ID of the table to truncate.

        Raises:
            DriverError: If there is an issue truncating the table.
        """
        if not self._connection:
            await self.connection()

        try:
            # Reference to the dataset and table
            dataset_ref = self._connection.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)

            # Ensure the table exists
            try:
                table = self._connection.get_table(table_ref)
            except NotFound:
                raise DriverError(
                    f"BigQuery: Table `{dataset_id}.{table_id}` does not exist."
                )

            # Configure the query job to overwrite the table
            job_config = bq.QueryJobConfig(
                destination=table_ref,
                write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
                allow_large_results=True
            )

            # Execute a query that selects no rows, effectively truncating the table
            query = f"SELECT * FROM `{self._project_id}.{dataset_id}.{table_id}` WHERE FALSE"

            self._logger.debug(f"Truncating table with query: {query}")
            job = self._connection.query(query, job_config=job_config)

            # Wait for the job to complete
            await asyncio.get_event_loop().run_in_executor(None, job.result)

            self._logger.info(f"Successfully truncated table `{dataset_id}.{table_id}`.")
            return True
        except DriverError:
            raise
        except Exception as e:
            self._logger.error(
                f"BigQuery: Error truncating table `{dataset_id}.{table_id}`: {e}"
            )
            raise DriverError(
                f"BigQuery: Error truncating table `{dataset_id}.{table_id}`: {e}"
            ) from e

    async def query(self, sentence: str, factory: Optional[str] = None, **kwargs):
        if not self._connection:
            await self.connection()
        await self.valid_operation(sentence)
        self.start_timing()
        if factory:
            self.output_format(factory)
        error = None
        result = None
        try:
            job = self._connection.query(sentence, **kwargs)
            if factory == 'pandas':
                result = job.to_dataframe()
            elif factory == 'tuple':
                result = [tuple(row.values()) for row in job]
            else:
                result = job.result()
        except Exception as e:
            error = f"BigQuery: Error executing query: {e}"
        finally:
            self.generated_at()
            return [None, error] if error else await self._serializer(result, error)

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
        result = None
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
        return await self.execute(query, *args)

    async def fetch_one(self, query, *args):
        """
        Fetch all results from a BigQuery query
        """
        results = await self.execute(query, *args)
        return [dict(row) for row in results]

    async def write(
        self,
        data,
        table_id: str = None,
        dataset_id: str = None,
        use_streams: bool = False,
        use_pandas: bool = True,  # by default using BigQuery
        use_schema: Optional[Any] = None,
        if_exists: str = "append",
        **kwargs,
    ):
        """
        Write data to a BigQuery table.
        """
        if not self._connection:
            await self.connection()
        job = None
        table = f"{self._project_id}.{dataset_id}.{table_id}"
        try:
            if isinstance(data, pd.DataFrame):
                if use_pandas:
                    try:
                        if if_exists == 'replace':
                            disposition = 'WRITE_TRUNCATE',
                        elif if_exists == 'append':
                            disposition = 'WRITE_APPEND',
                        else:
                            disposition = 'WRITE_EMPTY',
                        job_config = bq.LoadJobConfig(
                            schema=use_schema,
                            write_disposition=disposition,
                        )
                        job = await self._thread_func(
                            self._connection.load_table_from_dataframe,
                            data,
                            table,
                            # if_exists=if_exists,
                            job_config=job_config,
                            **kwargs
                        )
                    except pyarrow.lib.ArrowTypeError as err:
                        err_msg = str(err)
                        if col_match := re.search(
                            r'column with name:\s*"([^"]+)"\s+and datatype:\s*"([^"]+)"', err_msg
                        ):
                            column_name = col_match[1]
                            received_dtype = col_match[2]
                        else:
                            column_name = "Unknown"
                            received_dtype = "Unknown"
                        # Try to extract expected datatype details
                        if expected_match := re.search(
                            r'to an appropriate pyarrow datatype:\s*(.+)$', err_msg):
                            expected_dtype = expected_match[1].strip()
                        else:
                            expected_dtype = "Unknown"
                        raise DriverError(
                            f"BigQuery: Error writing to table: "
                            f"Column '{column_name}' has datatype '{received_dtype}', "
                            f"but expected '{expected_dtype}'"
                        ) from err
                else:
                    object_cols = data.select_dtypes(include=["object"]).columns
                    for column in object_cols:
                        dtype = str(type(data[column].values[0]))
                        if dtype == "<class 'datetime.date'>":
                            data[column] = pd.to_datetime(data[column], infer_datetime_format=True)
                    table = f"{dataset_id}.{table_id}"
                    job = await self._thread_func(
                        data.to_gbq,
                        table,
                        project_id=self._project_id,
                        credentials=self.credentials,
                        if_exists=if_exists,
                    )
            elif isinstance(data, list):
                dataset_ref = self._connection.dataset(dataset_id)
                table_ref = dataset_ref.table(table_id)
                table = bq.Table(table_ref)
                if use_streams:
                    errors = await self._thread_func(self._connection.insert_rows_json, table, data, **kwargs)
                    if errors:
                        raise RuntimeError(f"Errors occurred while inserting rows: {errors}")
                else:
                    job_config = bq.LoadJobConfig(
                        source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
                    )
                    job = await self._thread_func(
                        self._connection.load_table_from_json, data, table, job_config=job_config, **kwargs
                    )
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, job.result)
                    if job.errors and len(job.errors) > 0:
                        raise RuntimeError(f"Job failed with errors: {job.errors}")
                    else:
                        self._logger.info(f"Loaded {len(data)} rows into {table_id}")
            self._logger.info(f"Inserted rows into {dataset_id}.{table_id}: {len(data)} rows")
            # return Job object
            return job
        except Exception as e:
            raise DriverError(f"BigQuery: Error writing to table: {e}") from e

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

    async def create_gcs_from_csv(
        self,
        bucket_name: str,
        object_name: str,
        csv_data: Union[bytes, PurePath, pd.DataFrame],
        overwrite: bool = False,
        **kwargs,
    ) -> tuple:
        """Creates a GCS object from CSV data."""
        # we cannot import directly at the top level
        credentials = service_account.Credentials.from_service_account_file(self._credentials)
        if isinstance(csv_data, PurePath) and csv_data.is_file():
            async with aiofiles.open(csv_data, mode="rb") as file:
                csv_data = await file.read()
        elif isinstance(csv_data, pd.DataFrame):
            csv_data = csv_data.to_csv(index=False)
        elif not isinstance(csv_data, bytes):
            raise DriverError("BigQuery: Invalid file object")
        try:
            storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            if blob.exists():
                if not overwrite:
                    return f"gs://{bucket_name}/{object_name}", "Object already exists and overwrite is set to False."
                else:
                    self._logger.info(f"Object {object_name} exists in {bucket_name} and will be overwritten.")
            # Upload from a string
            blob.upload_from_string(csv_data, content_type="text/csv")
            # If successful, return the GCS URI
            gcs_uri = f"gs://{bucket_name}/{object_name}"
            return gcs_uri, None
        except Exception as e:
            raise DriverError(f"BigQuery: Error creating GCS object: {e}")

    async def read_csv_from_gcs(
        self,
        table_id: str,
        dataset_id: str,
        bucket_uri: str = None,
        bucket_name: str = None,
        object_name: str = None,
        **kwargs,
    ):
        """Load data into a BigQuery table from a CSV file in GCS."""
        try:
            if not bucket_uri:
                gcs_uri = f"gs://{bucket_name}/{object_name}"
            else:
                gcs_uri = bucket_uri
            job_config = LoadJobConfig(source_format=SourceFormat.CSV, autodetect=True, **kwargs)
            table = f"{self._project_id}.{dataset_id}.{table_id}"
            job = self._connection.load_table_from_uri(gcs_uri, table, job_config=job_config)
            job.result()  # Wait for the job to complete
            return job
        except Exception as e:
            raise DriverError(f"BigQuery: Error loading from CSV in GCS: {e}")

    async def read_csv(self, table_id, dataset_id, file_obj: Union[bytes, PurePath], **kwargs):
        """Load data into a BigQuery table from a CSV file object."""
        job_config = LoadJobConfig(source_format=SourceFormat.CSV, autodetect=True, **kwargs)
        if isinstance(file_obj, PurePath) and file_obj.is_file():
            async with aiofiles.open(file_obj, mode="rb") as file:
                file_obj = await file.read()
        elif not isinstance(file_obj, bytes):
            raise DriverError("BigQuery: Invalid file object")
        try:
            job = self._connection.load_table_from_file(file_obj, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete
            return job
        except Exception as e:
            raise DriverError(f"BigQuery: Error loading from CSV: {e}")

    async def read_excel(self, table_id, dataset_id, file_obj, **kwargs):
        """Load data into a BigQuery table from an Excel file object."""
        try:
            df = pd.read_excel(file_obj)
            return await self.write(table_id, df, dataset_id, use_pandas=True, **kwargs)
        except Exception as e:
            raise DriverError(f"BigQuery: Error loading from Excel: {e}")

    async def multi_query(self, queries: list):
        """Execute multiple BigQuery queries in parallel."""
        tasks = []
        for query in queries:
            tasks.append(asyncio.create_task(self.execute(query)))  # Create async tasks
        results = await asyncio.gather(*tasks)  # Execute tasks concurrently and gather results
        return results

    ##############################
    ## Model Logic:
    ##############################
    def get_table_ref(self, schema: str, table: str):
        """Returns the referencie of a BQ Table."""
        dataset_ref = bq.DatasetReference(self._project_id, schema)
        table_ref = dataset_ref.table(table)
        return self._connection.get_table(table_ref)
        # dataset_ref = self._connection.dataset(schema)
        # table_ref = dataset_ref.table(table)
        # return bq.Table(table_ref)

    def _get_param_type(self, value):
        """
        Determine the BigQuery parameter type based on Python value.
        """
        import datetime

        if value is None:
            return 'STRING'
        elif isinstance(value, bool):
            return 'BOOL'
        elif isinstance(value, int):
            return 'INT64'
        elif isinstance(value, float):
            return 'FLOAT64'
        elif isinstance(value, str):
            return 'STRING'
        elif isinstance(value, bytes):
            return 'BYTES'
        elif isinstance(value, datetime.datetime):
            return 'TIMESTAMP'
        elif isinstance(value, datetime.date):
            return 'DATE'
        elif isinstance(value, datetime.time):
            return 'TIME'
        # Add more type mappings as needed
        else:
            # Default to string for complex types
            return 'STRING'

    async def _insert_(self, _model: Model, **kwargs):  # pylint: disable=W0613
        """
        insert a row from model.
        """
        cols = []
        columns = []
        source = {}
        _filter = {}
        n = 1
        fields = _model.columns()
        for name, field in fields.items():
            try:
                val = getattr(_model, field.name)
            except AttributeError:
                continue
            ## getting the value of column:
            value = self._get_value(field, val)
            column = field.name
            columns.append(column)
            # validating required field
            try:
                required = field.required()
            except AttributeError:
                required = False
            pk = self._get_attribute(field, value, attr="primary_key")
            if pk is True and value is None and "db_default" in field.metadata:
                continue
            if required is False and value is None or value == "None":
                if "db_default" in field.metadata:
                    continue
                else:
                    # get default value
                    default = field.default
                    if callable(default):
                        value = default()
                    else:
                        continue
            elif required is True and value is None or value == "None":
                if "db_default" in field.metadata:
                    # field get a default value from database
                    continue
                else:
                    raise ValueError(f"Field {name} is required and value is null over {_model.Meta.name}")
            elif is_dataclass(value):
                if isinstance(value, Model):
                    ### get value for primary key associated with.
                    try:
                        value = getattr(value, name)
                    except AttributeError:
                        value = None
            elif isinstance(value, uuid.UUID):
                value = str(value)  # convert to string, for now
            elif isinstance(value, Enum):
                value = value.value
            source[column] = val
            cols.append(column)
            n += 1
            if pk := self._get_attribute(field, value, attr="primary_key"):
                _filter[column] = pk
        try:
            table = self.get_table_ref(_model.Meta.schema, _model.Meta.name)
            # dataset_ref = self._connection.dataset(_model.Meta.schema)
            # table_ref = dataset_ref.table(_model.Meta.name)
            # table = bq.Table(table_ref)
            job_config = bq.LoadJobConfig(
                source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            job = await self._thread_func(self._connection.load_table_from_json, [source], table, job_config=job_config)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, job.result)
            if job.errors and len(job.errors) > 0:
                raise RuntimeError(f"Error Inserting Data: {job.errors}")
            # get the row inserted again:
            condition = self._where(fields, **_filter)
            _select_stmt = f"SELECT * FROM {table} {condition}"
            self._logger.debug(f"SELECT: {_select_stmt}")
            job = self._connection.query(_select_stmt)
            result = job.result()
            result = next(iter(result))
            if result:
                _model.reset_values()
                for f, val in result.items():
                    setattr(_model, f, val)
                return _model
        except Exception as err:
            raise DriverError(message=f"Error on Insert over table {_model.Meta.name}: {err!s}") from err

    async def _delete_(self, _model: Model, _filter: dict = None, **kwargs):  # pylint: disable=W0613
        """
        delete a row from model.
        """
        table = f"{self._project_id}.{_model.Meta.schema}.{_model.Meta.name}"
        source = []
        if not _filter:
            _filter = {}
        n = 1
        fields = _model.columns()
        for _, field in fields.items():
            try:
                val = getattr(_model, field.name)
            except AttributeError:
                continue
            ## getting the value of column:
            value = self._get_value(field, val)
            column = field.name
            source.append(value)
            n += 1
            curval = _model.old_value(column)
            if pk := self._get_attribute(field, curval, attr="primary_key"):
                if column in _filter:
                    # already this value on delete:
                    continue
                _filter[column] = pk
        try:
            condition = self._where(fields, **_filter)
            if not condition:
                raise DriverError(f"Avoid DELETE without WHERE conditions: {_filter}")
            _delete = f"DELETE FROM {table} {condition};"
            self._logger.debug(f"DELETE: {_delete}")
            job = self._connection.query(_delete)
            job.result()  # Waits for the query to finish
            num_deleted_rows = job.num_dml_affected_rows
            return f"DELETE {num_deleted_rows} rows: {_filter!s}"
        except Exception as err:
            raise DriverError(message=f"Error on DELETE {_model.Meta.name}: {err!s}") from err

    async def _update_(self, _model: Model, **kwargs):  # pylint: disable=W0613
        """
        Updating a row in a Model.
        TODO: How to update when if primary key changed.
        Alternatives: Saving *dirty* status and previous value on dict
        """
        table = self.get_table_ref(_model.Meta.schema, _model.Meta.name)
        cols = []
        source = {}
        _filter = {}
        _updated = {}
        _primary = []
        param_values = {}
        n = 1
        fields = _model.columns()
        for name, field in fields.items():
            try:
                val = getattr(_model, field.name)
            except AttributeError:
                continue
            ## getting the value of column:
            value = self._get_value(field, val)
            column = field.name
            # validating required field
            try:
                required = field.required()
            except AttributeError:
                required = False
            if required is False and value is None or value == "None":
                default = field.default
                if callable(default):
                    value = default()
                else:
                    continue
            elif required is True and value is None or value == "None":
                if "db_default" in field.metadata:
                    # field get a default value from database
                    continue
                raise ValueError(f"Field {name} is required and value is null over {_model.Meta.name}")
            elif is_dataclass(value):
                if isinstance(value, Model):
                    ### get value for primary key associated with.
                    try:
                        value = getattr(value, name)
                    except AttributeError:
                        value = None
            curval = _model.old_value(name)
            if pk := self._get_attribute(field, curval, attr="primary_key"):
                _filter[column] = pk
                param_values[f"where_{column}"] = curval
                _primary.append(column)
            if pk := self._get_attribute(field, value, attr="primary_key"):
                _updated[column] = pk
                _primary.append(column)
            if curval == value:
                continue  # no changes
            cols.append(name)  # pylint: disable=C0209
            source[name] = value
            param_values[f"set_{column}"] = value  # For parameterized query
            n += 1
        try:
            # Obtain the table to access its schema
            table_ref = self.get_table_ref(_model.Meta.schema, _model.Meta.name)
            # Create a dictionary of column types
            column_types = {}
            for field in table_ref.schema:
                field_type = field.field_type
                # Detect arrays based on REPEATED mode
                if field.mode == 'REPEATED':
                    column_types[field.name] = f"ARRAY<{field_type}>"
                elif field_type.startswith('ARRAY'):
                    # Save both the array type and element type
                    column_types[field.name] = field_type
                elif field_type == 'INTEGER':
                    column_types[field.name] = 'INT64'
                elif field_type == 'FLOAT':
                    column_types[field.name] = 'FLOAT64'
                elif field_type == 'BOOLEAN':
                    column_types[field.name] = 'BOOL'
                else:
                    column_types[field.name] = field_type

            # Build parameterized SET clause
            set_clauses = []
            for key in source:
                set_clauses.append(f"{key} = @set_{key}")
            set_clause = ", ".join(set_clauses)

            # Build parameterized WHERE clause
            where_conditions = []
            for key in _filter:
                param_values[f"where_{key}"] = _filter[key]
                where_conditions.append(f"{key} = @where_{key}")

            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""

            # Create the parameterized query
            _update = f"UPDATE {table} SET {set_clause}{where_clause}"
            self._logger.debug(f"UPDATE: {_update}")

            # Create query job configuration with parameters
            query_params = []
            for param_name, param_value in param_values.items():
                # Extract the column name from the parameter name (remove prefix)
                if param_name.startswith('set_'):
                    column_name = param_name[4:]  # Remove 'set_'
                elif param_name.startswith('where_'):
                    column_name = param_name[6:]  # Remove 'where_'
                elif param_name.startswith('select_'):
                    column_name = param_name[7:]  # Remove 'select_'
                else:
                    column_name = param_name

                # Determine the type based on column type
                if column_name in column_types:
                    param_type = column_types[column_name]
                    # For array types, we need special handling
                    if param_type.startswith('ARRAY'):
                        try:
                            # Extract the internal type of the array (between < and >)
                            inner_type = param_type[param_type.find('<')+1:param_type.find('>')]
                            array_param = bq.ArrayQueryParameter(
                                param_name,
                                inner_type,  # The internal type of the array (STRING, INT64, etc.)
                                []  # Empty array for NULL
                            )
                            query_params.append(array_param)
                            continue  # Skip to next parameter
                        except Exception:
                            # Fallback to scalar if there's a problem
                            param_type = 'STRING'
                else:
                    param_type = self._get_param_type(param_value)

                query_params.append(bq.ScalarQueryParameter(param_name, param_type, param_value))

            job_config = bq.QueryJobConfig(query_parameters=query_params)

            # Execute the query with parameters
            job = self._connection.query(_update, job_config=job_config)
            job.result()  # Wait for completion
            num_affected_rows = job.num_dml_affected_rows
            self._logger.info(f"UPDATED rows: {num_affected_rows}")

            # Retrieve updated records
            new_conditions = {**_filter, **_updated}
            # Create new parameterized query for selection
            select_params = []
            select_conditions = []
            select_param_values = {}

            for key, value in new_conditions.items():
                select_param_values[f"select_{key}"] = value
                select_conditions.append(f"{key} = @select_{key}")

            select_condition = " WHERE " + " AND ".join(select_conditions) if select_conditions else ""

            for param_name, param_value in select_param_values.items():
                if param_name.startswith('select_'):
                    column_name = param_name[7:]  # Remove 'select_'
                else:
                    column_name = param_name

                if param_value is None and column_name in column_types:
                    param_type = column_types[column_name]
                    # For array types, we need special handling
                    if param_type.startswith('ARRAY'):
                        # Create an empty array parameter with the correct type
                        try:
                            # Extract the internal type of the array (between < and >)
                            inner_type = param_type[param_type.find('<')+1:param_type.find('>')]
                            array_param = bq.ArrayQueryParameter(
                                param_name,
                                inner_type,  # The internal type of the array (STRING, INT64, etc.)
                                []  # Empty array for NULL
                            )
                            select_params.append(array_param)
                            continue  # Skip to next parameter
                        except Exception:
                            # Fallback to scalar if there's a problem
                            param_type = 'STRING'
                else:
                    param_type = self._get_param_type(param_value)

                select_params.append(bq.ScalarQueryParameter(param_name, param_type, param_value))

            select_config = bq.QueryJobConfig(query_parameters=select_params)

            _all = f"SELECT * FROM {table}{select_condition}"
            job = self._connection.query(_all, job_config=select_config)
            result = job.result()
            data = dict(next(iter(result))) if result.total_rows > 0 else {}
            _model.reset_values()
            for f, val in data.items():
                setattr(_model, f, val)
            return _model
        except Exception as err:
            raise DriverError(message=f"Error on Update over table {_model.Meta.name}: {err!s}") from err

    async def _save_(self, _model: Model, *args, **kwargs):
        """
        Save a row in a Model, using Insert-or-Update methodology.
        """
        raise NotImplementedError("Method not implemented")

    async def _fetch_(self, _model: Model, *args, **kwargs):
        """
        Returns one single Row using Model.
        """
        try:
            schema = ""
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = f"{_model.Meta.schema}.{_model.Meta.name}"
        fields = _model.columns()
        _filter = {}
        for name, field in fields.items():
            if name in kwargs:
                try:
                    val = kwargs[name]
                except AttributeError:
                    continue
                ## getting the value of column:
                datatype = field.type
                value = Entity.toSQL(val, datatype)
                _filter[name] = value
        condition = self._where(fields, **_filter)
        _get = f"SELECT * FROM {table} {condition}"
        try:
            job = self._connection.query(_get)
            result = job.result()  # Waits for the query to finish
            return next(iter(result))
        except Exception as e:
            raise DriverError(f"Error: Model Fetch over {table}: {e}") from e

    async def _filter_(self, _model: Model, *args, **kwargs):
        """
        Filter a Model using Fields.
        """
        try:
            schema = ""
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = f"{_model.Meta.schema}.{_model.Meta.name}"
        fields = _model.columns(_model)
        _filter = {}
        if args:
            columns = ",".join(args)
        else:
            columns = "*"
        for name, field in fields.items():
            if name in kwargs:
                try:
                    val = kwargs[name]
                except AttributeError:
                    continue
                ## getting the value of column:
                datatype = field.type
                value = Entity.toSQL(val, datatype)
                _filter[name] = value
        condition = self._where(fields, **_filter)
        _get = f"SELECT {columns} FROM {table} {condition}"
        try:
            job = self._connection.query(_get)
            result = job.result()  # Waits for the query to finish
            return [row for row in result]
        except Exception as e:
            raise DriverError(f"Error: Model GET over {table}: {e}") from e

    async def _select_(self, *args, **kwargs):
        """
        Get a query from Model.
        """
        try:
            model = kwargs["_model"]
        except KeyError as e:
            raise DriverError(f"Missing Model for SELECT {kwargs!s}") from e
        try:
            schema = ""
            sc = model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{model.Meta.name}"
        except AttributeError:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        if args:
            condition = "{}".join(args)
        else:
            condition = None
        if "fields" in kwargs:
            columns = ",".join(kwargs["fields"])
        else:
            columns = "*"
        _get = f"SELECT {columns} FROM {table} {condition}"
        try:
            job = self._connection.query(_get)
            result = job.result()  # Waits for the query to finish
            return [row for row in result]
        except Exception as e:
            raise DriverError(f"Error: Model GET over {table}: {e}") from e

    async def _get_(self, _model: Model, *args, **kwargs):
        """
        Get one row from model.
        """
        try:
            schema = ""
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = f"{_model.Meta.schema}.{_model.Meta.name}"
        fields = _model.columns(_model)
        _filter = {}
        if args:
            columns = ",".join(args)
        else:
            columns = ",".join(fields)  # getting only selected fields
        for name, field in fields.items():
            if name in kwargs:
                try:
                    val = kwargs[name]
                except AttributeError:
                    continue
                ## getting the value of column:
                datatype = field.type
                value = Entity.toSQL(val, datatype)
                _filter[name] = value
        condition = self._where(fields, **_filter)
        _get = f"SELECT {columns} FROM {table} {condition}"
        print("SELECT :: ", _get)
        try:
            job = self._connection.query(_get)
            result = job.result()  # Waits for the query to finish
            return next(iter(result))
        except Exception as e:
            raise DriverError(f"Error: Model GET over {table}: {e}") from e

    async def _all_(self, _model: Model, *args, **kwargs):  # pylint: disable=W0613
        """
        Get all rows on a Model.
        """
        try:
            schema = ""
            if sc := _model.Meta.schema:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = f"{_model.Meta.schema}.{_model.Meta.name}"
        if "fields" in kwargs:
            columns = ",".join(kwargs["fields"])
        else:
            columns = "*"
        _all = f"SELECT {columns} FROM {table}"
        try:
            job = self._connection.query(_all, **kwargs)
            result = job.result()  # Waits for the query to finish
            return [row for row in result]
        except Exception as e:
            raise DriverError(f"Error: Model All over {table}: {e}") from e

    async def _remove_(self, _model: Model, **kwargs):
        """
        Deleting some records using Model.
        """
        table = f"{_model.Meta.schema}.{_model.Meta.name}"
        fields = _model.columns(_model)
        _filter = {}
        for name, field in fields.items():
            datatype = field.type
            if name in kwargs:
                val = kwargs[name]
                value = Entity.toSQL(val, datatype)
                _filter[name] = value
        condition = self._where(fields, **_filter)
        if not condition:
            raise ValueError("Avoid DELETE without WHERE conditions")
        _delete = f"DELETE FROM {table} {condition}"
        try:
            self._logger.debug(f"DELETE: {_delete}")
            job = self._connection.query(_delete)
            job.result()  # Waits for the query to finish
            num_deleted_rows = job.num_dml_affected_rows
            return f"DELETE {num_deleted_rows} rows: {_filter!s}"
        except Exception as err:
            raise DriverError(message=f"Error on DELETE {_model.Meta.name}: {err!s}") from err

    async def _updating_(self, *args, _filter: dict = None, **kwargs):
        """
        Updating records using Model.
        """
        param_values = {}
        try:
            model = kwargs["_model"]
        except KeyError as e:
            raise DriverError(f"Missing Model for SELECT {kwargs!s}") from e
        try:
            schema = ""
            sc = model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{model.Meta.name}"
        except AttributeError:
            table = model.__name__
        fields = model.columns(model)
        if _filter is None and args:
            _filter = args[0]
        source = {}
        new_cond = {}
        for name, field in fields.items():
            try:
                val = kwargs[name]
            except (KeyError, AttributeError):
                continue
            ## getting the value of column:
            value = self._get_value(field, val)
            column = name
            if name in _filter:
                new_cond[name] = value
            source[name] = value
            param_values[f"set_{name}"] = value  # For parameterized query

        try:
            # Obtain the table to access its schema
            table_ref = self.get_table_ref(model.Meta.schema, model.Meta.name)
            # Create a dictionary of column types
            column_types = {}
            for field in table_ref.schema:
                field_type = field.field_type
                # Detect arrays based on REPEATED mode
                if field.mode == 'REPEATED':
                    column_types[field.name] = f"ARRAY<{field_type}>"
                elif field_type.startswith('ARRAY'):
                    # Save both the array type and element type
                    column_types[field.name] = field_type
                elif field_type == 'INTEGER':
                    column_types[field.name] = 'INT64'
                elif field_type == 'FLOAT':
                    column_types[field.name] = 'FLOAT64'
                elif field_type == 'BOOLEAN':
                    column_types[field.name] = 'BOOL'
                else:
                    column_types[field.name] = field_type

            # Build parameterized SET clause
            set_clauses = []
            for key in source:
                set_clauses.append(f"{key} = @set_{key}")
            set_clause = ", ".join(set_clauses)

            # Build parameterized WHERE clause
            where_conditions = []
            for key in _filter:
                param_values[f"where_{key}"] = _filter[key]
                where_conditions.append(f"{key} = @where_{key}")

            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""

            # Create the parameterized query
            _update = f"UPDATE {table} SET {set_clause}{where_clause}"
            self._logger.debug(f"UPDATE: {_update}")

            # Create query job configuration with parameters
            query_params = []
            for param_name, param_value in param_values.items():
                # Extract the column name from the parameter name (remove prefix)
                if param_name.startswith('set_'):
                    column_name = param_name[4:]  # Remove 'set_'
                elif param_name.startswith('where_'):
                    column_name = param_name[6:]  # Remove 'where_'
                elif param_name.startswith('select_'):
                    column_name = param_name[7:]  # Remove 'select_'
                else:
                    column_name = param_name

                # Determine the type based on column type
                if column_name in column_types:
                    param_type = column_types[column_name]
                    # For array types, we need special handling
                    if param_type.startswith('ARRAY'):
                        try:
                            # Extract the internal type of the array (between < and >)
                            inner_type = param_type[param_type.find('<')+1:param_type.find('>')]
                            array_param = bq.ArrayQueryParameter(
                                param_name,
                                inner_type,  # The internal type of the array (STRING, INT64, etc.)
                                []  # Empty array for NULL
                            )
                            query_params.append(array_param)
                            continue  # Skip to next parameter
                        except Exception:
                            # Fallback to scalar if there's a problem
                            param_type = 'STRING'
                else:
                    param_type = self._get_param_type(param_value)

                query_params.append(bq.ScalarQueryParameter(param_name, param_type, param_value))

            job_config = bq.QueryJobConfig(query_parameters=query_params)

            # Execute the query with parameters
            job = self._connection.query(_update, job_config=job_config)
            job.result()  # Wait for completion
            num_affected_rows = job.num_dml_affected_rows
            self._logger.info(f"UPDATED rows: {num_affected_rows}")

            # Retrieve updated records
            new_conditions = {**_filter, **new_cond}
            # Create new parameterized query for selection
            select_params = []
            select_conditions = []
            select_param_values = {}

            for key, value in new_conditions.items():
                select_param_values[f"select_{key}"] = value
                select_conditions.append(f"{key} = @select_{key}")

            select_condition = " WHERE " + " AND ".join(select_conditions) if select_conditions else ""

            for param_name, param_value in select_param_values.items():
                if param_name.startswith('select_'):
                    column_name = param_name[7:]  # Remove 'select_'
                else:
                    column_name = param_name

                if param_value is None and column_name in column_types:
                    param_type = column_types[column_name]
                    # For array types, we need special handling
                    if param_type.startswith('ARRAY'):
                        # Create an empty array parameter with the correct type
                        try:
                            # Extract the internal type of the array (between < and >)
                            inner_type = param_type[param_type.find('<')+1:param_type.find('>')]
                            array_param = bq.ArrayQueryParameter(
                                param_name,
                                inner_type,  # The internal type of the array (STRING, INT64, etc.)
                                []  # Empty array for NULL
                            )
                            select_params.append(array_param)
                            continue  # Skip to next parameter
                        except Exception:
                            # Fallback to scalar if there's a problem
                            param_type = 'STRING'
                else:
                    param_type = self._get_param_type(param_value)

                select_params.append(bq.ScalarQueryParameter(param_name, param_type, param_value))

            select_config = bq.QueryJobConfig(query_parameters=select_params)

            _all = f"SELECT * FROM {table}{select_condition}"
            job = self._connection.query(_all, job_config=select_config)
            result = job.result()
            return [model(**dict(r)) for r in result]
        except Exception as err:
            raise DriverError(message=f"Error on UPDATE over table {model.Meta.name}: {err!s}") from err

    async def _deleting_(self, *args, _filter: dict = None, **kwargs):
        """
        Deleting records using Model.
        """
        try:
            model = kwargs["_model"]
        except KeyError as e:
            raise DriverError(f"Missing Model for SELECT {kwargs!s}") from e
        try:
            schema = ""
            sc = model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{model.Meta.name}"
        except AttributeError:
            table = model.__name__
        fields = model.columns(model)
        if _filter is None and args:
            _filter = args[0]
        cols = []
        source = []
        new_cond = {}
        n = 1
        for name, field in fields.items():
            try:
                val = kwargs[name]
            except (KeyError, AttributeError):
                continue
            ## getting the value of column:
            value = self._get_value(field, val)
            source.append(value)
            if name in _filter:
                new_cond[name] = value
            cols.append("{} = {}".format(name, "?".format(n)))  # pylint: disable=C0209
            n += 1
        try:
            condition = self._where(fields, **_filter)
            _delete = f"DELETE FROM {table} {condition}"
            self._logger.debug(f"DELETE: {_delete}")
            stmt = await self.get_sentence(_delete)
            result = self._connection.excute(stmt, source)
            print(f"DELETE {result}: {_filter!s}")
            return f"DELETED: {_filter}"
        except Exception as err:
            raise DriverError(message=f"Error on DELETE over table {model.Meta.name}: {err!s}") from err

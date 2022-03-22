#!/usr/bin/env python3
import asyncio
import json
import time
import logging
from datetime import datetime
import pandas
from influxdb_client import InfluxDBClient, ApiClient, DBRPsService, Point, Dialect, WriteOptions, BucketRetentionRules
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS, PointSettings
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.flux_table import FluxStructureEncoder
from influxdb_client.rest import RESTResponse
from dataclasses import is_dataclass, asdict
from functools import partial
from typing import (
    Any,
    List,
    Dict,
    Optional,
    Union,
    Tuple
)
from asyncdb.exceptions import (
    ConnectionTimeout,
    DataError,
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
    TooManyConnections,
)
from asyncdb.providers.base import InitProvider
from asyncdb.utils import (
    EnumEncoder,
    SafeDict,
)
from .interfaces import (
    ConnectionDSNBackend
)


class WriteCallback(object):
    def success(self, conf: Tuple[str, str, str], data: str):
        """Successfully written batch."""
        logging.debug(f"Written batch: {conf}, data: {data}")

    def error(self, conf: Tuple[str, str, str], data: str, exception: InfluxDBError):
        """Unsuccessfully writen batch."""
        logging.error(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf: Tuple[str, str, str], data: str, exception: InfluxDBError):
        """Retryable error."""
        logging.error(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


class influx(InitProvider, ConnectionDSNBackend):
    _provider = "influxdb"
    _syntax = "sql"

    def __init__(
            self,
            dsn: str = '',
            loop: asyncio.AbstractEventLoop = None,
            params: Dict[Any, Any] = None,
            **kwargs
    ) -> None:
        self._test_query = "SELECT 1"
        self._query_raw = "SELECT {fields} FROM {table} {where_cond}"
        self._version: str = None
        self._dsn = "{protocol}://{host}:{port}"
        self._client = None
        try:
            self._debug = kwargs['debug']
        except KeyError:
            self._debug = False
        if not params:
            params: dict = {
                "host": "localhost",
                "port": 8086
            }
        try:
            params['protocol'] = kwargs['protocol']
        except KeyError:
            params['protocol'] = 'http'
        InitProvider.__init__(
            self,
            loop=loop,
            params=params,
            **kwargs
        )
        ConnectionDSNBackend.__init__(
            self,
            dsn=dsn,
            params=params,
            **kwargs
        )
        try:
            self._config_file: str = kwargs['config_file']
        except KeyError:
            self._config_file = None
        if self._config_file is None:
            # authentication:
            try:
                self._token = self.params["token"]
            except KeyError:
                try:
                    self._token = self.params["password"]
                except KeyError:
                    raise Exception('InfluxDB: Missing Token Authentication.')
                self._token = None
            try:
                self._org = self.params['org'] if self.params['org'] else self.params['organization']
            except KeyError:
                try:
                    self._org = kwargs['user']
                except KeyError:
                    raise Exception('InfluxDB: Missing Organization on Connection Info.')
        # callback
        self._callback = WriteCallback
        # dialect for export to csv
        self._dialect = Dialect(header=True, delimiter=",", comment_prefix="#", annotations=[], date_time_format="RFC3339")
        

    async def connection(self):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            if self._config_file:
                self._connection = InfluxDBClient.from_config_file(self._config_file)
            else:
                params = {
                    "timeout": self._timeout,
                    "connection_pool_maxsize": 5,
                    "enable_gzip": True,
                    "debug": self._debug,
                    "org": self._org
                }    
                if self._dsn:
                    print("URL ", self._dsn)
                    params['url'] = self._dsn
                else:
                    # fallback to host
                    params['url'] = self.params["host"]
                if self._token:
                    params['token'] = self._token
                self._connection = InfluxDBClient(
                    **params
                )
            # checking if works:
            self._version = self._connection.version()
            try:
                if self._connection.ready():
                    self._client = self._connection.api_client
            except Exception as err:
                logging.exception(f'Error creating REST client: {err}')
            settings = {
                "app_name": "${env.APP_NAME}",
                "customer": self._org
            }
            self._settings = PointSettings(
                **settings
            )
            if self._version:
                self._connected = True
                self._initialized_on = time.time()
        except Exception as err:
            self._connection = None
            self._cursor = None
            logging.exception(err)
            raise ProviderError(
                message="InfluxDB connection Error: {}".format(str(err))
            )
        finally:
            return self
        
    async def close(self):
        """
        Closing a Connection
        """
        try:
            if self._connection:
                self._logger.debug("InfluxDB: Closing Connection")
                try:
                    self._connection.close()
                except Exception as err:
                    self._connection = None
                    raise ProviderError(
                        message="InfluxDB: Connection Error, Terminated: {}".format(str(err))
                    )
        except Exception as err:
            raise ProviderError(
                message="InfluxDB: Close Error: {}".format(str(err))
            )
        finally:
            self._connection = None
            self._connected = False

    async def test_connection(self):
        error = None
        result = None
        if self._connection:
            try:
                result = await self.query("SHOW databases")
            except Exception as err:
                error = err
            finally:
                return [result, error]
            
    def api_client(self):
        return self._client

    async def ping(self):
        """ping.

            Check if the influx instance is active.
        Returns:
            bool: a boolean with the response of the instance.
        """
        # return self._connection.get_list_database()
        return self._connection.ping()
    
    async def health(self):
        """health.

        Returns:
            HealthCheck: a class with Health information of the instance
        """
        return self._connection.health()
    
    @property
    def organization(self):
        """Organization Name.
        """
        return self._org
    
    @organization.setter
    def organization(self, org):
        self._org = org
        
    def settings(self, config: Dict):
        """settings.
            Set Default Tags for every measurement.
        Args:
            config (Dict): list of variable values to be used as settings.
        """
        self._settings = PointSettings(
            **config
        )
        
    def set_callback(self, callback: WriteCallback):
        """SetCallback.
        
        Set the current Callback for Writes.

        Args:
            callback (function): an extension class from WriteCallback.
        """
        self._callback = callback

    def version(self):
        """version.
        Get Version information about InfluxDB instance.
        Returns:
            dict: version information.
        """
        return self._version if self._version is not None else self._connection.version()
    
    async def list_buckets(self):
        buckets_api = self._connection.buckets_api()
        return buckets_api.find_buckets().buckets

    async def create_bucket(self, bucket: str, btype: str = 'expire', expiration: int = 0, **kwgars):
        try:
            buckets_api = self._connection.buckets_api()
            rules = BucketRetentionRules(type=btype, every_seconds=expiration, **kwgars)
            created = buckets_api.create_bucket(bucket_name=bucket, retention_rules=rules, org=self._org)
        except Exception as err:
            raise ProviderError(
                message="Error creating Bucket {}".format(err)
            )
            
    create_database = create_bucket

    async def use(self, database: str):
        pass

    async def write(self, data: List, bucket: str, **kwargs):
        try:
            result = None
            with self._connection.write_api(
                    write_options=ASYNCHRONOUS,
                    success_callback=self._callback.success,
                    error_callback=self._callback.error,
                    retry_callback=self._callback.retry,
                    point_settings=self._settings) as writer:
                """
                Write data into InfluxDB
                """
                if isinstance(data, pandas.core.frame.DataFrame):
                # need the index and the name of the measurement
                    rst = writer.write(
                        bucket=bucket,
                        org=self._org,
                        data_frame_measurement_name = kwargs['name'],
                        data_frame_tag_columns = kwargs['index'],
                        record=data
                    )
                elif is_dataclass(data):
                    name = kwargs['name']
                    tag_keys = list(asdict(data).keys())
                    field_keys = kwargs['fields']
                    try:
                        time_keys = kwargs['time']
                    except KeyError:
                        time_keys = {}
                    rst = writer.write(
                        bucket=bucket,
                        org=self._org,
                        record_measurement_name = name,
                        record_tag_keys = tag_keys,
                        record_field_keys = field_keys,
                        **time_keys
                    )  
                else:
                    rst = writer.write(bucket=bucket, org=self._org, record=data)
                result = rst.get()
            return result
        except RuntimeError as err:
            error = "InfluxDB: Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "InfluxDB: Error on Write: {}".format(str(err))
            raise Exception(error)

    save = write

    async def query(self, sentence: str, format: str = 'native', params: Dict = None):
        self._result = None
        error = None
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            query_api = self._connection.query_api()
            if format == 'pandas':
                reader = partial(query_api.query_data_frame, query=sentence, params=params)
                # self._result = query_api.query_data_frame(sentence, params=params)
            elif format == 'csv':
                reader = partial(query_api.query_csv, query=sentence, params=params, dialect=self._dialect)
                # self._result = query_api.query_csv(sentence, params=params, dialect=self._dialect)
            else:
                reader = partial(query_api.query, query=sentence, params=params)
                # self._result = query_api.query(sentence, params=params)
            self._result = await self._loop.run_in_executor(None, reader)
            if not self._result:
                raise NoDataFound("InfluxDB: No Data was Found")
            if format == 'json':
                self._result = json.dumps(self._result, cls=FluxStructureEncoder)
        except NoDataFound:
            raise
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)
        
    queryrow = query
        
    async def fetch_all(self, sentence: str, *args, params: Dict = None):
        await self.valid_operation(sentence)
        try:
            self.start_timing()
            query_api = self._connection.query_api()
            if format == 'pandas':
                result = query_api.query_data_frame(sentence, params=params)
            elif format == 'csv':
                result = query_api.query_csv(sentence, params=params, dialect=self._dialect)
            else:
                result = query_api.query(sentence, params=params)
            if not result:
                raise NoDataFound("InfluxDB: No Data was Found")
            if format == 'json':
                result = json.dumps(self._result, cls=FluxStructureEncoder)
            self.generated_at()
            return result
        except NoDataFound:
            raise
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        
    fetch_one = fetch_all

    async def execute(self, sentence: str, method: str = "GET", **kwargs):
        """Execute a transaction

        returns: results of the execution
        """
        error = None
        result = None
        await self.valid_operation(sentence)
        try:
            rst = self._client.call_api(sentence, method, **kwargs)
            print(rst, type(rst), str(rst))
            rst = self._client.request(url=self._dsn + sentence, method=method, **kwargs)
            print(rst, type(rst), str(rst))
            if isinstance(rst, RESTResponse):
                try:
                    result = json.loads(rst.data)
                except ValueError:
                    result = rst.data
            else:
                result = rst
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
            raise [None, error]
        finally:
            return [result, error]
        
    async def execute_many(self, sentence: Union[str, Any], method: str = "GET", **kwargs):
        """Execute many transactions at once.

        returns: results of the execution
        """
        raise NotImplementedError

    def column_info(self, table):
        """
        column_info
          get column information about a table
        """
        raise NotImplementedError
    
    def prepare(self, sentence: str, *args, **kwargs):
        raise NotImplementedError

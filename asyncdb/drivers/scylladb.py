import os
from typing import Union, Any
import asyncio
import time
from enum import Enum
import uuid
from dataclasses import is_dataclass, astuple, fields
from ssl import PROTOCOL_TLSv1
import logging
from pathlib import PurePath
import aiofiles
import pandas as pd

# async driver:
import acsylla as c

# Cassandra:
from cassandra import ReadTimeout
from cassandra.io.asyncorereactor import AsyncoreConnection

# from cassandra.io.asyncioreactor import AsyncioConnection
try:
    from cassandra.io.libevreactor import LibevConnection

    LIBEV = True
except ImportError:
    LIBEV = False
from cassandra.concurrent import execute_concurrent
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    WhiteListRoundRobinPolicy,
    DowngradingConsistencyRetryPolicy,
    ConstantReconnectionPolicy,
    TokenAwarePolicy,
    RoundRobinPolicy,
)
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile, NoHostAvailable, ResultSet
from cassandra.query import (
    tuple_factory,
    dict_factory,
    ordered_dict_factory,
    named_tuple_factory,
    ConsistencyLevel,
    PreparedStatement,
    BatchStatement,
    SimpleStatement,
    BatchType,
)
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from .base import InitDriver
from ..meta.recordset import Recordset
from ..exceptions import NoDataFound, DriverError
from ..interfaces.model import ModelBackend
from ..models import Model
from ..utils.types import Entity


logging.getLogger("cassandra").setLevel(logging.INFO)
BATCH_SIZE = 1000


def pandas_factory(colnames, rows):
    df = pd.DataFrame(rows, columns=colnames)
    return df


def record_factory(colnames, rows):
    return Recordset(result=[dict(zip(colnames, values)) for values in rows], columns=colnames)


class scylladb(InitDriver, ModelBackend):
    _provider = "scylladb"
    _syntax = "cql"

    def __init__(self, loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs):
        self.hosts: list = []
        self.application_name = os.getenv("APP_NAME", "NAV")
        self._enable_shard_awareness = kwargs.pop("shard_awareness", False)
        self._test_query = "SELECT release_version FROM system.local"
        self._query_raw = "SELECT {fields} FROM {table} {where_cond}"
        self._cluster = None
        self._timeout: int = 120
        self._protocol: int = kwargs.pop("protocol", 4)
        self._driver: str = kwargs.pop("driver", "cassandra")
        self.heartbeat_interval: int = kwargs.pop("heartbeat_interval", 0)
        self._row_factory = kwargs.pop("row_factory", "dict_factory")
        self._force_closing: bool = kwargs.pop("force_closing", False)
        super(scylladb, self).__init__(loop=loop, params=params, **kwargs)
        try:
            if "host" in self.params:
                self._hosts = self.params["host"].split(",")
        except KeyError:
            self._hosts = ["127.0.0.1"]
        try:
            self.whitelist = kwargs["whitelist"]
        except KeyError:
            self.whitelist = None
        try:
            self._auth = {
                "username": self.params["username"],
                "password": self.params["password"],
            }
        except KeyError:
            self._auth = None

    def sync_close(self):
        # gracefully closing underlying connection
        if self._force_closing is False:
            # if not forced, then, only declared null connection
            self._connection = None
            return
        if self._connection:
            try:
                self._connection.shutdown()
            except Exception as err:
                self._connection = None
                raise DriverError(message=f"Connection Error, Terminated: {err}") from err
        if self._cluster:
            self._logger.debug("Closing Cluster")
            try:
                self._cluster.shutdown()
            except Exception as err:
                raise DriverError(f"Cluster Shutdown Error: {err}") from err

    async def async_close(self):
        if self._connection:
            try:
                await self._connection.close()
            except Exception as err:
                self._connection = None
                raise DriverError(message=f"Connection Error, Terminated: {err}") from err
        if self._cluster:
            self._logger.debug("Closing Cluster")
            try:
                await self._cluster.close()
            except Exception as err:
                raise DriverError(f"Cluster Shutdown Error: {err}") from err

    async def close(self):
        """close.
        Closing a Connection
        """
        try:
            if self._driver == "async":
                await self.async_close()
            else:
                self.sync_close()
        finally:
            self._cluster = None
            self._connection = None
            self._connected = False

    async def async_connect(self, keyspace: str = None):
        """
        Getting a Connection using async driver:
        """
        self._connection = None
        self._connected = False
        self._cluster = None
        ssl_opts = {}
        try:
            if self.params["ssl"] is not None:
                ssl_opts = {
                    "ssl_enable": True,
                    "ssl_trusted_cert": self.params["ssl"]["certfile"],
                    "ssl_version": PROTOCOL_TLSv1,
                    "ssl_private_key": self.params["ssl"]["userkey"],
                    "ssl_cert": self.params["ssl"]["usercert"],
                }
        except KeyError:
            pass
        if not self._auth:
            self._auth = {}
        params = {
            "port": self.params["port"],
            "application_name": "Navigator",
            "protocol_version": self._protocol,
            "connect_timeout": self._timeout,
            "heartbeat_interval_sec": self.heartbeat_interval,
            "num_threads_io": 4,
            **ssl_opts,
            **self._auth,
        }
        try:
            self._cluster = c.create_cluster(self._hosts, **params)
            self._connection = await self._cluster.connect(keyspace=keyspace)
            self._driver = "async"
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
            if "database" in self.params:
                await self.use(self.params["database"])
            else:
                self._keyspace = keyspace
        except DriverError:
            raise
        except Exception as err:
            self._logger.exception(f"Scylla Connection Error: {err}")
            self._connection = None
            self._cursor = None
            raise DriverError(message=f"Scylla Connection Error: {err}") from err

    async def connect(self, keyspace=None):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        self._cluster = None
        try:
            try:
                if self.params["ssl"] is not None:
                    ssl_opts = {
                        "ca_certs": self.params["ssl"]["certfile"],
                        "ssl_version": PROTOCOL_TLSv1,
                        "keyfile": self.params["ssl"]["userkey"],
                        "certfile": self.params["ssl"]["usercert"],
                    }
            except KeyError:
                ssl_opts = {}
            if self._enable_shard_awareness:
                policy = TokenAwarePolicy(RoundRobinPolicy())
            if self.whitelist:
                policy = WhiteListRoundRobinPolicy(self.whitelist)
            else:
                policy = DCAwareRoundRobinPolicy()
            # defining row factory:
            if self._row_factory == "dict_factory":
                row_factory = dict_factory
            elif self._row_factory == "pandas_factory":
                row_factory = pandas_factory
            elif self._row_factory == "tuple_factory":
                row_factory = tuple_factory
            elif self._row_factory == "named_tuple_factory":
                row_factory = named_tuple_factory
            elif self._row_factory == "ordered_dict_factory":
                row_factory = ordered_dict_factory
            elif self._row_factory == "record_factory":
                row_factory = record_factory
            else:
                # Set Dict Factory by default
                row_factory = named_tuple_factory
            defaultprofile = ExecutionProfile(
                load_balancing_policy=policy,
                retry_policy=DowngradingConsistencyRetryPolicy(),
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
                request_timeout=self._timeout,
                row_factory=row_factory,
            )
            # Long-term execution profile:
            longprofile = ExecutionProfile(
                load_balancing_policy=policy,
                retry_policy=DowngradingConsistencyRetryPolicy(),
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
                request_timeout=180,
                row_factory=row_factory,
            )
            # Pandas Profile
            pandasprofile = ExecutionProfile(
                load_balancing_policy=policy,
                retry_policy=DowngradingConsistencyRetryPolicy(),
                request_timeout=self._timeout,
                row_factory=pandas_factory,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            )
            tupleprofile = ExecutionProfile(
                load_balancing_policy=policy,
                retry_policy=DowngradingConsistencyRetryPolicy(),
                request_timeout=self._timeout,
                row_factory=named_tuple_factory,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            )
            orderedprofile = ExecutionProfile(
                load_balancing_policy=policy,
                retry_policy=DowngradingConsistencyRetryPolicy(),
                request_timeout=self._timeout,
                row_factory=ordered_dict_factory,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            )
            recordprofile = ExecutionProfile(
                load_balancing_policy=policy,
                retry_policy=DowngradingConsistencyRetryPolicy(),
                request_timeout=self._timeout,
                row_factory=record_factory,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            )
            profiles = {
                EXEC_PROFILE_DEFAULT: defaultprofile,
                "pandas": pandasprofile,
                "ordered": orderedprofile,
                "default": tupleprofile,
                "recordset": recordprofile,
                "long": longprofile,
            }
            # TODO: migrate to asyncio when available.
            if LIBEV is True:
                conn_class = LibevConnection
            else:
                conn_class = AsyncoreConnection
            params = {
                "application_name": self.application_name,
                "port": self.params["port"],
                "compression": True,
                "connection_class": conn_class,
                "protocol_version": self._protocol,
                "idle_heartbeat_interval": self.heartbeat_interval,
                "ssl_options": ssl_opts,
                "executor_threads": 4,
                "reconnection_policy": ConstantReconnectionPolicy(delay=5.0, max_attempts=100),
                "connect_timeout": 10,
            }
            auth_provider = None
            if self._auth:
                auth_provider = PlainTextAuthProvider(**self._auth)
            self._cluster = Cluster(
                self._hosts,
                auth_provider=auth_provider,
                execution_profiles=profiles,
                **params,
            )
            try:
                self._connection = self._cluster.connect(keyspace=keyspace)
            except NoHostAvailable as ex:
                raise DriverError(message=f"Not able to connect to any of the Scylla contact points: {ex}") from ex
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
            if "database" in self.params:
                await self.use(self.params["database"])
            else:
                self._keyspace = keyspace
        except DriverError:
            raise
        except Exception as err:
            self._logger.exception(f"Scylla Connection Error: {err}")
            self._connection = None
            self._cursor = None
            raise DriverError(message=f"Scylla Connection Error: {err}") from err

    async def connection(self, keyspace: str = None):
        if self._driver == "async":
            await self.async_connect(keyspace)
        else:
            await self.connect(keyspace)
        return self

    async def table_exists(self, table: str, keyspace: str = None, schema: str = None) -> bool:
        """
        Ensure the table exists. Optional If not, create it.

        Args:
            table_name (str): Name of the table.
            schema (str): CQL statement to create the table.
        """
        if not keyspace:
            keyspace = self._keyspace
        # Check if table exists
        tables, error = await self.execute(
            f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}'"
        )
        if table not in [row["table_name"] for row in tables]:
            # If table doesn't exist, create it
            if schema is not None:
                result, error = await self.execute(schema)
                if error:
                    self._logger.error(error)
                    return False
                self._logger.debug(f"Table was created: {table}")
            else:
                return False
        return True

    async def execute(  # pylint: disable=W0221
        self, sentence: Union[str, SimpleStatement, PreparedStatement], params: list = None, **kwargs
    ) -> Any:
        """Execute a transaction
        get a CQL sentence and execute
        returns: results of the execution
        """
        error = None
        self._result = None
        try:
            await self.valid_operation(sentence)
            if isinstance(sentence, PreparedStatement):
                smt = sentence
            elif isinstance(sentence, SimpleStatement):
                smt = sentence
            else:
                smt = self._connection.prepare(sentence)
            if self._driver == "async":
                statement = self._connection.create_statement(smt)
                self._result = await self._connection.execute(statement)
            else:
                fut = self._connection.execute_async(smt, params)
                self._result = fut.result()
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Execute: {err}"
        finally:
            return [self._result, error]  # pylint: disable=W0150

    async def execute_batch(
        self,
        sentences: list,
        params: list = None,
    ) -> Any:
        """execute_batch.

        Execute a transaction using Batch prepared statements.

        Args:
            sentences (List): List of parametrized CQL sentences.
            params (List, optional): List of dicts with parameters.

        Returns:
            Any: Resultset of execution.
        """
        result = None
        error = None
        await self.valid_operation(sentences)
        try:
            if self._driver == "async":
                batch = self._connection.create_batch_unlogged()
            else:
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            for idx, sentence in enumerate(sentences):
                args = ()
                if params:
                    args = params[idx]
                if isinstance(sentence, PreparedStatement):
                    bound_statement = sentence.bind(args)
                    batch.add(bound_statement)
                else:
                    smt = SimpleStatement(sentence)
                    batch.add(smt, args)
            if self._driver == "async":
                result = await self._connection.execute(batch)
            else:
                fut = self._connection.execute_async(batch)
                result = fut.result()
        except ReadTimeout:
            error = "Timeout executing sentences"
        except Exception as err:
            error = f"Error on Execute: {err}"
        finally:
            return [result, error]

    async def execute_many(  # pylint: disable=W0221
        self, sentence: Union[str, SimpleStatement, PreparedStatement], params: list = None
    ) -> Any:
        """execute_many.

        Execute a transaction many times using Batch prepared statements.

        Args:
            sentence (str): a parametrized CQL sentence.
            params (List, optional): List of dicts with parameters.

        Returns:
            Any: Resultset of execution.
        """
        result = None
        error = None
        await self.valid_operation(sentence)
        try:
            if self._driver == "async":
                batch = self._connection.create_batch_unlogged()
            else:
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            for p in params:
                args = ()
                if isinstance(p, dict):
                    args = tuple(p.values())
                elif isinstance(p, tuple):
                    args = p
                else:
                    args = tuple(p)
                if isinstance(sentence, PreparedStatement):
                    bound_statement = sentence.bind(args)
                    batch.add(bound_statement)
                else:
                    smt = SimpleStatement(sentence)
                    batch.add(smt, args)
                if len(batch) >= BATCH_SIZE:
                    if self._driver == "async":
                        await self._connection.execute(batch)
                        batch = self._connection.create_batch_unlogged()
                    else:
                        fut = self._connection.execute_async(batch)
                        result = fut.result()
                        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            if len(batch) > 0:
                if self._driver == "async":
                    result = await self._connection.execute(batch)
                else:
                    fut = self._connection.execute_async(batch)
                    result = fut.result()
        except ReadTimeout:
            error = "Timeout executing sentences"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Execute: {err}"
        finally:
            return [result, error]  # pylint: disable=W0150

    async def test_connection(self):  # pylint: disable=W0221
        result = None
        error = None
        try:
            result, error = await self.execute(self._test_query)
            result = [row for row in result]
        except Exception as err:  # pylint: disable=W0703
            error = err
        finally:
            return [result, error]  # pylint: disable=W0150

    async def use(self, database: str):
        try:
            self._connection.set_keyspace(database)
            self._keyspace = database
            self._logger.debug(f"Using Keyspace: {database}")
        except Exception as err:
            self._logger.error(err)
            raise
        return self

    async def drop_keyspace(self, keyspace: str):
        db = f"DROP KEYSPACE IF EXISTS {keyspace};"
        try:
            if self._driver == "async":
                result = await self._connection.execute(db)
            else:
                result = self._connection.execute(db)
            self._logger.debug(f"DROP {db}: {result!r}")
        except Exception as err:
            raise DriverError(f"Error: {err}") from err

    async def create_keyspace(self, keyspace: str, use: bool = True):
        db = "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}};"
        db = db.format(keyspace=keyspace)
        try:
            if self._driver == "async":
                result = await self._connection.execute(db)
            else:
                result = self._connection.execute(db)
            self._logger.debug(f"CREATE {db}: {result!r}")
        except Exception as err:
            raise DriverError(f"Error: {err}") from err
        if use is True:
            await self.use(keyspace)

    create_database = create_keyspace

    async def drop_table(self, table: str):
        db = f"DROP TABLE IF EXISTS {table};"
        try:
            if self._driver == "async":
                result = await self._connection.execute(db)
            else:
                result = self._connection.execute(db)
            self._logger.debug(f"Table Dropped: {db}: {result!r}")
        except Exception as err:
            raise DriverError(f"Error: {err}") from err

    async def create_table(
        self, table: str, schema: str = None, data: Any = None, pk: str = None, optionals: dict = None
    ):
        if schema:
            await self.use(schema)

        # Generate CREATE TABLE statement
        create_stmt = f"CREATE TABLE IF NOT EXISTS {table} ("

        # If data is a DataFrame, generate column definitions
        if isinstance(data, pd.DataFrame):
            dtype_mapping = {
                "int64": "int",
                "float64": "float",
                "object": "text",  # assuming object type is string
                "datetime64[ns]": "timestamp",
                # Add more type mappings as needed
            }
            columns = []
            for col, dtype in data.dtypes.items():
                scylla_type = dtype_mapping.get(str(dtype), "text")
                columns.append(f"{col} {scylla_type}")
            # Assuming the first column is the primary key for simplicity
            # Adjust as needed
            if pk is None:
                columns.append(f"PRIMARY KEY ({data.columns[0]})")
            else:
                columns.append(f"PRIMARY KEY ({pk})")

            create_stmt += ", ".join(columns) + ")"
        elif isinstance(data, dict):
            columns = []
            for col, dtype in data.items():
                columns.append(f"{col} {dtype}")
            if pk is None:
                columns.append(f"PRIMARY KEY ({list(data.keys())[0]})")
            else:
                columns.append(f"PRIMARY KEY ({pk})")
            create_stmt += ", ".join(columns) + ")"
        if optionals:
            if isinstance(optionals, dict):
                for k, v in optionals.items():
                    create_stmt += f" {k}={v}"
            elif isinstance(optionals, list):
                for item in optionals:
                    create_stmt += f" {item}"
            elif isinstance(optionals, str):
                create_stmt += f" {optionals}"
            else:
                raise ValueError("Optional WITH must be a list, dict or string")
        create_stmt += ";"
        # Execute the CREATE TABLE statement
        self._logger.debug(f"CREATE TABLE: {create_stmt}")
        if self._driver == "async":
            await self._connection.execute(create_stmt)
        else:
            self._connection.execute(create_stmt)

    async def prepare(self, sentence: str, consistency: str = "quorum"):
        await self.valid_operation(sentence)
        try:
            self._prepared = self._connection.prepare(sentence)
            if consistency == "quorum":
                self._prepared.consistency_level = ConsistencyLevel.QUORUM
            else:
                self._prepared.consistency_level = ConsistencyLevel.ALL
            return self._prepared
        except RuntimeError as ex:
            raise DriverError(message=f"Runtime Error: {ex}") from ex
        except Exception as ex:
            raise DriverError(f"Error on Query: {ex}") from ex

    def create_query(self, sentence: str, consistency: str = "quorum"):
        if consistency == "quorum":
            cl = ConsistencyLevel.QUORUM
        else:
            cl = ConsistencyLevel.ALL
        return SimpleStatement(sentence, consistency_level=cl)

    async def get_sentence(
        self, sentence: Union[str, SimpleStatement, PreparedStatement], prepared: bool = False, params: list = None
    ):
        if isinstance(sentence, PreparedStatement):
            if params:
                smt = sentence.bind(*params)
            else:
                smt = sentence
        elif isinstance(sentence, SimpleStatement):
            smt = sentence
        elif prepared is True:
            if self._driver == "async":
                st = await self._connection.prepare(sentence)
                smt = st.bind(*params)
            else:
                prepared = self._connection.prepare(sentence)
                smt = prepared.bind(*params)
        else:
            if self._driver == "async":
                smt = c.Statement(sentence, params)  # pylint: disable=E0110
            else:
                smt = SimpleStatement(sentence)
        return smt

    async def query(
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        prepared: bool = False,
        params: list = None,
        factory: str = EXEC_PROFILE_DEFAULT,
        **kwargs,
    ) -> Union[ResultSet, None]:
        error = None
        self._result = None
        try:
            await self.valid_operation(sentence)
            self.start_timing()
            smt = await self.get_sentence(sentence, prepared=prepared, params=params)
            if self._driver == "async":
                self._result = await self._connection.execute(smt)
            else:
                self._connection.fetch_size = None
                fut = self._connection.execute_async(smt, execution_profile=factory)
                self._result = fut.result()
            try:
                if factory in ("pandas", "record", "recordset"):
                    self._result.result = self._result._current_rows
            except ReadTimeout:
                error = f"Timeout reading Data from {sentence}"
            if not self._result:
                raise NoDataFound("Scylla: No Data was Found")
        except NoDataFound:
            raise
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def fetch_all(
        self, sentence: Union[str, SimpleStatement, PreparedStatement], params: list = None, **kwargs
    ) -> ResultSet:
        self._result = None
        try:
            await self.valid_operation(sentence)
            self.start_timing()
            self._result = self._connection.execute(sentence, params)
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
            self.generated_at()
            return self._result
        except NoDataFound:
            raise
        except RuntimeError as err:
            raise DriverError(message=f"Runtime Error: {err}") from err
        except Exception as err:
            raise Exception(f"Error on Query: {err}") from err

    async def fetch(self, sentence, params: list = None):
        if not params:
            params = []
        return self.fetch_all(sentence, params)

    async def queryrow(self, sentence: Union[str, SimpleStatement, PreparedStatement], params: list = None):
        error = None
        self._result = None
        try:
            await self.valid_operation(sentence)
            if self._driver == "async":
                smt = c.Statement(sentence, params)  # pylint: disable=E0110
                self._result = await self._connection.execute(smt)
            else:
                smt = SimpleStatement(sentence)
                self._result = self._connection.execute(sentence, params).one()
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
        except RuntimeError as err:
            error = f"Runtime on Query Row Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Error on Query Row: {err}"
        return [self._result, error]  # pylint: disable=W0150

    async def fetch_one(  # pylint: disable=W0221
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = None,
    ) -> ResultSet:
        self._result = None
        try:
            await self.valid_operation(sentence)
            self._result = self._connection.execute(sentence, params).one()
            if not self._result:
                raise NoDataFound("Scylla: No Data was Found")
        except NoDataFound:
            raise
        except RuntimeError as err:
            raise DriverError(message=f"Runtime on Query Row Error: {err}") from err
        except Exception as err:
            raise Exception(f"Error on Query Row: {err}") from err
        return self._result

    async def fetchrow(self, sentence, params: list = None):
        if not params:
            params = []
        return self.fetch_one(sentence=sentence, params=params)

    ### Model Logic:
    async def column_info(self, table: str, schema: str = None):
        """Column Info.

        Get Meta information about a table (column name, data type and PK).
        Useful to build a DataModel from Querying database.
        Parameters:
        @tablename: str The name of the table (including schema).
        """
        if not schema:
            schema = self._keyspace
        cql = f"select column_name as name, type, type as format_type, \
            kind from system_schema.columns where \
                keyspace_name = '{schema}' and table_name = '{table}';"
        if not self._connection:
            await self.connection()
        try:
            colinfo, error = await self.execute(cql)
            if error:
                return []
            return [d for d in colinfo]
        except Exception as err:
            self._logger.exception(f"Wrong Table information {table!s}: {err}")
            raise DriverError(f"Wrong Table information {table!s}: {err}") from err

    async def run_cqlsh_copy(self, keyspace, table, columns, data_file, sep: str = ","):
        # Construct the COPY command
        columns_str = ", ".join(columns)
        command_str = (
            f"COPY {keyspace}.{table} ({columns_str}) FROM '{data_file}' WITH DELIMITER='{sep}' AND HEADER=true"
        )
        self._logger.debug(f"COMMAND > {command_str}")
        # Create subprocess
        process = await asyncio.create_subprocess_exec(
            "cqlsh", "-e", command_str, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        # Wait for the subprocess to finish
        stdout, stderr = await process.communicate()

        # Return stdout, stderr, and the return code
        return stdout, stderr, process.returncode

    async def write(
        self,
        data: Union[list, dict, Any],
        sentence: str = None,
        table: str = None,
        keyspace: str = None,
        batch_size: int = 100,
        **kwargs,
    ):
        """
        Write data into ScyllaDB.
        """
        _data = None
        columns = None
        if not keyspace:
            keyspace = self._keyspace
        if isinstance(data, PurePath):
            # is a CSV file:
            if not data.exists():
                raise ValueError(f"CSV File {data} does not exist")
            self._logger.debug(f":: Loading CSV File {data.name} into {table}")
            sep = kwargs.get("separator", ",")
            columns = kwargs.get("columns", [])
            if not columns:
                async with aiofiles.open(data, mode="r") as file:
                    header = await file.readline()
                columns = header.strip().split(sep)
            _data = str(data)
            stdout, stderr, _ = await self.run_cqlsh_copy(keyspace, table, columns, _data, sep=sep)
            self._logger.debug(f"COPY: {stdout.decode()}")
            if stderr:
                print("Error: ", stderr.decode())
            return True
        elif isinstance(data, pd.core.frame.DataFrame):
            # Convert DataFrame to a list of tuples
            _data = data.itertuples(index=False, name=None)
            columns = data.columns.tolist()
        elif is_dataclass(data):
            _data = [astuple(data)]  # Wrap the tuple in a list
            columns = [f.name for f in fields(data)]
        elif isinstance(data, dict):
            _data = [tuple(data.values())]
            columns = list(data.keys())
        elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
            _data = [tuple(item.values()) for item in data]
            columns = list(data[0].keys())
        else:
            _data = [data]
            columns = None
        if _data is None:
            raise ValueError("Write Error: Unsupported data type")
        # Construct the INSERT statement if not provided
        if sentence is None and table:
            col_names = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            sentence = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

        if self._driver == "async":
            stmt = await self._connection.prepare(sentence)
            # List to hold all the tasks
            tasks = []
            # Create tasks for each insert batch
            for i in range(0, len(_data), batch_size):
                batch = _data[i : i + batch_size]
                tasks.append(self._execute_batch(stmt, batch))
            await asyncio.gather(*tasks)
        else:
            concurrency = kwargs.get("concurrency", 50)
            stmt = SimpleStatement(sentence)
            execute_concurrent(self._connection, ((stmt, row) for row in _data), concurrency=concurrency)

    copy = write

    async def _execute_batch(self, stmt, batch):
        tasks = []
        for row in batch:
            bound_stmt = stmt.bind(*row)
            tasks.append(self._connection.execute(bound_stmt))
        await asyncio.gather(*tasks)

    ## Model Logic:
    async def _insert_(self, _model: Model, **kwargs):  # pylint: disable=W0613
        """
        insert a row from model.
        """
        try:
            schema = ""
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
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
            # if pk is True and value is None:
            #     if "db_default" in field.metadata:
            #         continue
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
            source[column] = value
            cols.append(column)
            n += 1
            if pk := self._get_attribute(field, value, attr="primary_key"):
                _filter[column] = pk
        try:
            values = ", ".join([f":{a}" for a in cols])  # pylint: disable=C0209
            cols = ",".join(cols)
            insert = f"INSERT INTO {table}({cols}) VALUES({values}) IF NOT EXISTS;"
            self._logger.debug(f"INSERT: {insert}")
            stmt = self._connection.prepare(insert)
            result = self._connection.execute(stmt, source)
            if result.was_applied:
                # get the row inserted again:
                condition = " AND ".join([f"{key} = :{key}" for key in _filter])
                _select_stmt = f"SELECT * FROM {table} WHERE {condition}"
                self._logger.debug(f"SELECT: {_select_stmt}")
                stmt = self._connection.prepare(_select_stmt)
                result = self._connection.execute(stmt, _filter).one()
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
        try:
            schema = ""
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
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
            result = self._connection.execute(_delete)
            return f"DELETE {result}: {_filter!s}"
        except Exception as err:
            raise DriverError(message=f"Error on Insert over table {_model.Meta.name}: {err!s}") from err

    async def _update_(self, _model: Model, **kwargs):  # pylint: disable=W0613
        """
        Updating a row in a Model.
        TODO: How to update when if primary key changed.
        Alternatives: Saving *dirty* status and previous value on dict
        """
        try:
            schema = ""
            sc = _model.Meta.schema
            if sc:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        cols = []
        source = []
        _filter = {}
        _updated = {}
        _primary = []
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
                _primary.append(column)
            if pk := self._get_attribute(field, value, attr="primary_key"):
                _updated[column] = pk
                _primary.append(column)
            if curval == value:
                continue  # no changes
            cols.append(name)  # pylint: disable=C0209
            source.append(value)
            n += 1
        try:
            if any(col in _primary for col in cols):
                # in Cassandra we need to delete and insert again
                condition = self._where(fields, **_filter)
                _delete = f"DELETE FROM {table} {condition}"
                result = self._connection.execute(SimpleStatement(_delete))
                return await self._insert_(_model, **kwargs)
            set_fields = ", ".join(cols)
            condition = self._where(fields, **_filter)
            _update = f"UPDATE {table} SET {set_fields} {condition}"
            self._logger.debug(f"UPDATE: {_update}")
            stmt = await self.get_sentence(_update, prepared=True)
            self._connection.execute(stmt, source)
            condition = self._where(fields, **_updated)
            stmt = SimpleStatement(f"SELECT * FROM {table} {condition}")
            result = self._connection.execute(stmt).one()
            if result:
                _model.reset_values()
                for f, val in result.items():
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
            table = _model.__name__
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
            smt = SimpleStatement(_get)
            return self._connection.execute(smt).one()
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
            table = _model.__name__
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
            stmt = SimpleStatement(_get)
            fut = self._connection.execute_async(stmt)
            result = fut.result()
            return result
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
            table = model.__name__
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
            smt = SimpleStatement(_get)
            return self._connection.execute(smt)
        except Exception as e:
            raise DriverError(f"Error: Model SELECT over {table}: {e}") from e

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
            table = _model.__name__
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
        print("SELECT ", _get)
        try:
            smt = SimpleStatement(_get)
            return self._connection.execute(smt).one()
        except Exception as e:
            raise DriverError(f"Error: Model GET over {table}: {e}") from e

    async def _all_(self, _model: Model, *args, **kwargs):  # pylint: disable=W0613
        """
        Get all rows on a Model.
        """
        try:
            schema = ""
            # sc = _model.Meta.schema
            if sc := _model.Meta.schema:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        if "fields" in kwargs:
            columns = ",".join(kwargs["fields"])
        else:
            columns = "*"
        _all = f"SELECT {columns} FROM {table}"
        try:
            smt = SimpleStatement(_all)
            return self._connection.execute(smt)
        except Exception as e:
            raise DriverError(f"Error: Model All over {table}: {e}") from e

    async def _remove_(self, _model: Model, **kwargs):
        """
        Deleting some records using Model.
        """
        try:
            schema = ""
            if sc := _model.Meta.schema:
                schema = f"{sc}."
            table = f"{schema}{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
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
            smt = SimpleStatement(_delete)
            result = self._connection.execute(smt)
            return f"DELETE {result}: {_filter!s}"
        except Exception as err:
            raise DriverError(message=f"Error on DELETE {_model.Meta.name}: {err!s}") from err

    async def _updating_(self, *args, _filter: dict = None, **kwargs):
        """
        Updating records using Model.
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
            set_fields = ", ".join(cols)
            condition = self._where(fields, **_filter)
            _update = f"UPDATE {table} SET {set_fields} {condition}"
            self._logger.debug(f"UPDATE: {_update}")
            stmt = await self.get_sentence(_update, prepared=True)
            result = self._connection.execute(stmt, source)
            print(f"UPDATE {result}: {_filter!s}")

            new_conditions = {**_filter, **new_cond}
            condition = self._where(fields, **new_conditions)

            _all = f"SELECT * FROM {table} {condition}"
            stmt = await self.get_sentence(_all)
            result = self._connection.execute(stmt)
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

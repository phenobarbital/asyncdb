#!/usr/bin/env python3
"""Cassandra.

Cassandra Driver for asyncDB.

TODO: migrate to Thread Executors or Asyncio version.
"""
import time
import logging
import asyncio
from typing import (
    Any,
    List,
    Union
)
from ssl import PROTOCOL_TLSv1
import pandas as pd
from cassandra import ReadTimeout
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile, NoHostAvailable, ResultSet
from cassandra.io.asyncorereactor import AsyncoreConnection
try:
    from cassandra.io.libevreactor import LibevConnection
    LIBEV = True
except ImportError:
    LIBEV = False

from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    WhiteListRoundRobinPolicy,
    DowngradingConsistencyRetryPolicy,
    # RetryPolicy
)
from cassandra.query import (
    dict_factory,
    ordered_dict_factory,
    named_tuple_factory,
    ConsistencyLevel,
    PreparedStatement,
    BatchStatement,
    SimpleStatement,
    BatchType
)
from asyncdb.meta import Recordset
from asyncdb.exceptions import (
    NoDataFound,
    ProviderError,
    DriverError
)


from .abstract import InitDriver

def pandas_factory(colnames, rows):
    df = pd.DataFrame(rows, columns=colnames)
    return df

def record_factory(colnames, rows):
    return Recordset(result=[dict(zip(colnames, values)) for values in rows], columns=colnames)


class cassandra(InitDriver):
    _provider = "cassandra"
    _syntax = "cql"

    def __init__(self, loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs):
        self.hosts: list = []
        self._test_query = "SELECT release_version FROM system.local"
        self._query_raw = "SELECT {fields} FROM {table} {where_cond}"
        self._cluster = None
        self._timeout: int = 120
        super(cassandra, self).__init__(loop=loop, params=params, **kwargs)
        try:
            if "host" in self.params:
                self._hosts = self.params["host"].split(",")
        except KeyError:
            self._hosts = ["127.0.0.1"]
        try:
            self.whitelist = kwargs['whitelist']
        except KeyError:
            self.whitelist = None
        try:
            self._auth = {
                "username": self.params["username"],
                "password": self.params["password"],
            }
        except KeyError:
            self._auth = None

    async def close(self, timeout: int = 10):
        """close.
        Closing a Connection
        """
        try:
            # gracefully closing underlying connection
            if self._connection:
                self._logger.debug("Closing Connection")
                try:
                    self._connection.shutdown()
                except Exception as err:
                    self._cluster.shutdown()
                    self._connection = None
                    raise ProviderError(
                        message=f"Connection Error, Terminated: {err}"
                    ) from err
        except Exception as err:
            raise ProviderError(
                f"Close Error: {err}"
            ) from err
        finally:
            self._connection = None
            self._connected = False

    async def connection(self, keyspace=None):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        self._cluster = None
        try:
            try:
                if self.params['ssl'] is not None:
                    ssl_opts = {
                        'ca_certs': self.params['ssl']['certfile'],
                        'ssl_version': PROTOCOL_TLSv1,
                        'keyfile': self.params['ssl']['userkey'],
                        'certfile': self.params['ssl']['usercert']
                    }
            except KeyError:
                ssl_opts = {}
            if self.whitelist:
                policy = WhiteListRoundRobinPolicy(self.whitelist)
            else:
                policy = DCAwareRoundRobinPolicy()
            defaultprofile = ExecutionProfile(
                load_balancing_policy=policy,
                retry_policy=DowngradingConsistencyRetryPolicy(),
                request_timeout=self._timeout,
                row_factory=dict_factory,
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            )
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
                'pandas': pandasprofile,
                'ordered': orderedprofile,
                'default': tupleprofile,
                'recordset': recordprofile
            }
            params = {
                "port": self.params["port"],
                "compression": True,
                "connection_class": AsyncoreConnection,
                "protocol_version": 4,
                "connect_timeout": 60,
                "idle_heartbeat_interval": 0,
                "ssl_options": ssl_opts
            }
            if LIBEV is True:
                params["connection_class"] = LibevConnection
            auth_provider = None
            if self._auth:
                auth_provider = PlainTextAuthProvider(**self._auth)
            self._cluster = Cluster(
                self._hosts,
                auth_provider=auth_provider,
                execution_profiles=profiles,
                **params,
            )
            print(self._cluster)
            try:
                self._connection = self._cluster.connect(keyspace=keyspace)
            except NoHostAvailable as ex:
                raise ProviderError(
                    message=f'Not able to connect to any of the Cassandra contact points: {ex}'
                ) from ex
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
            if 'database' in self.params:
                self.use(self.params["database"])
            else:
                self._keyspace = keyspace
            return self
        except ProviderError:
            raise
        except Exception as err:
            logging.exception(f"connection Error, Terminated: {err}")
            self._connection = None
            self._cursor = None
            raise ProviderError(
                message=f"connection Error, Terminated: {err}"
            ) from err

    async def test_connection(self): # pylint: disable=W0221
        result = None
        error = None
        try:
            response = self._connection.execute(self._test_query)
            result = [row for row in response]
        except Exception as err: # pylint: disable=W0703
            error = err
        finally:
            return [result, error] # pylint: disable=W0150

    async def use(self, database: str):
        try:
            self._connection.set_keyspace(database)
            self._keyspace = database
        except Exception as err:
            logging.exception(err)
            raise
        return self

### Preparing a sentence
    def prepared_statement(self):
        return self._prepared

    def prepared_smt(self):
        return self._prepared

    async def prepare(self, sentence: str, consistency: str = 'quorum'):
        await self.valid_operation(sentence)
        try:
            self._prepared = self._connection.prepare(sentence)
            if consistency == 'quorum':
                self._prepared.consistency_level = ConsistencyLevel.QUORUM
            else:
                self._prepared.consistency_level = ConsistencyLevel.ALL
            return self._prepared
        except RuntimeError as ex:
            raise ProviderError(message=f"Runtime Error: {ex}") from ex
        except Exception as ex:
            raise DriverError(f"Error on Query: {ex}") from ex

    def create_query(self, sentence: str, consistency: str = 'quorum'):
        if consistency == 'quorum':
            cl = ConsistencyLevel.QUORUM
        else:
            cl = ConsistencyLevel.ALL
        return SimpleStatement(sentence, consistency_level=cl)

    async def query(
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = None,
        factory: str = EXEC_PROFILE_DEFAULT,
        **kwargs
    ) -> Union[ResultSet, None]:
        error = None
        self._result = None
        try:
            await self.valid_operation(sentence)
            self.start_timing()
            if isinstance(sentence, PreparedStatement):
                smt = sentence
            elif isinstance(sentence, SimpleStatement):
                smt = sentence
            else:
                smt = self._connection.prepare(sentence)
            self._connection.fetch_size = None
            fut = self._connection.execute_async(smt, params, execution_profile=factory)
            try:
                self._result = fut.result()
                if factory in ('pandas', 'record', 'recordset'):
                    self._result.result = self._result._current_rows
            except ReadTimeout:
                error = f'Timeout reading Data from {sentence}'
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
        except NoDataFound:
            raise
        except RuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err: # pylint: disable=W0703
            error = f"Error on Query: {err}"
        finally:
            self.generated_at()
            return await self._serializer(self._result, error) # pylint: disable=W0150

    async def fetch_all(
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = None,
        **kwargs
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
            raise ProviderError(message=f"Runtime Error: {err}") from err
        except Exception as err:
            raise Exception(f"Error on Query: {err}") from err

    async def fetch(self, sentence, params: list = None):
        if not params:
            params = []
        return self.fetch_all(sentence, params)

    async def queryrow(
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = None
    ):
        error = None
        self._result = None
        try:
            await self.valid_operation(sentence)
            self._result = self._connection.execute(sentence, params).one()
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
        except RuntimeError as err:
            error = f"Runtime on Query Row Error: {err}"
        except Exception as err: # pylint: disable=W0703
            error = f"Error on Query Row: {err}"
        return [self._result, error] # pylint: disable=W0150

    async def fetch_one( # pylint: disable=W0221
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = None,
    ) -> ResultSet:
        self._result = None
        try:
            await self.valid_operation(sentence)
            self._result = self._connection.execute(sentence, params).one()
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
        except RuntimeError as err:
            raise ProviderError (
                message=f"Runtime on Query Row Error: {err}"
            ) from err
        except Exception as err:
            raise Exception(
                f"Error on Query Row: {err}"
            ) from err
        return self._result

    async def fetchrow(self, sentence, params: list = None):
        if not params:
            params = []
        return self.fetch_one(sentence=sentence, params=params)

    async def execute( # pylint: disable=W0221
            self,
            sentence: Union[str, SimpleStatement, PreparedStatement],
            params: list = None,
            **kwargs
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
            fut = self._connection.execute_async(smt, params)
            try:
                self._result = fut.result()
            except ReadTimeout:
                error = 'Timeout executing sentences'
            if not self._result:
                error = NoDataFound("Cassandra: No Data was Found")
        except Exception as err: # pylint: disable=W0703
            error = f"Error on Execute: {err}"
        finally:
            return [self._result, error] # pylint: disable=W0150

    async def execute_many( # pylint: disable=W0221
            self,
            sentence: Union[str, SimpleStatement, PreparedStatement],
            params: list = None
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
        try:
            await self.valid_operation(sentence)
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            for p in params:
                args = ()
                if isinstance(p, dict):
                    args = tuple(p.values())
                else:
                    args = tuple(p)
                if isinstance(sentence, PreparedStatement):
                    smt = sentence
                else:
                    smt = SimpleStatement(sentence)
                batch.add(smt, p)
            fut = self._connection.execute_async(batch)
            result = fut.result()
        except ReadTimeout:
            error = 'Timeout executing sentences'
        except Exception as err: # pylint: disable=W0703
            error = f"Error on Execute: {err}"
        finally:
            return [result, error] # pylint: disable=W0150


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
            colinfo = self._connection.execute(cql)
            return [d for d in colinfo]
        except Exception as err:
            self._logger.exception(
                f"Wrong Table information {table!s}: {err}"
            )
            raise DriverError(
                f"Wrong Table information {table!s}: {err}"
            ) from err

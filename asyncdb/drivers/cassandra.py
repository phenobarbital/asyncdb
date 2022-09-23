#!/usr/bin/env python3
import time
import logging
from datetime import datetime
from typing import Any, Dict, List, Union, Tuple
from ssl import PROTOCOL_TLSv1
from asyncdb.exceptions import (
    NoDataFound,
    ProviderError,
)
import pandas as pd
from asyncdb.meta import Recordset
from asyncdb.providers import InitProvider
from cassandra import ReadTimeout
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile, NoHostAvailable, ResultSet
# from cassandra.io.asyncioreactor import AsyncioConnection
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
    RetryPolicy
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


def pandas_factory(colnames, rows):
    df = pd.DataFrame(rows, columns=colnames)
    return df

def record_factory(colnames, rows):
    return Recordset(result=[dict(zip(colnames, values)) for values in rows], columns=colnames)
    

class cassandra(InitProvider):
    _provider = "cassandra"
    _syntax = "cql"

    def __init__(
            self,
            loop=None,
            params: Dict = None,
            **kwargs
    ):
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
                        message="Connection Error, Terminated: {}".format(str(err))
                    )
        except Exception as err:
            raise ProviderError("Close Error: {}".format(str(err)))
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
            except NoHostAvailable:
                raise ProviderError(message='Not able to connect to any of the Cassandra contact points')
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
            if 'database' in self.params:
                self.use(self.params["database"])
            else:
                self._keyspace = keyspace
        except ProviderError:
            raise
        except Exception as err:
            logging.exception(f"connection Error, Terminated: {err}")
            self._connection = None
            self._cursor = None
            raise ProviderError(message="connection Error, Terminated: {}".format(str(err)))
        finally:
            return self

    async def test_connection(self):
        result = None
        error = None
        try:
            response = self._connection.execute(self._test_query)
            result = [row for row in response]
        except Exception as err:
            error = err
        finally:
            return [result, error]

    def use(self, dbname: str):
        try:
            self._connection.set_keyspace(dbname)
            self._keyspace = dbname
            # self._connection.execute(f"USE {dbname!s}")
        except Exception as err:
            logging.exception(err)
            raise
        return self

    """
    Preparing a sentence
    """

    def prepared_statement(self):
        return self._prepared

    def prepared_smt(self):
        return self._prepared

    async def prepare(self, sentence: str, consistency: str = 'quorum'):
        error = None
        await self.valid_operation(sentence)
        try:
            self._prepared = self._connection.prepare(sentence)
            if consistency == 'quorum':
                self._prepared.consistency_level = ConsistencyLevel.QUORUM
            else:
                self._prepared.consistency_level = ConsistencyLevel.ALL
        except RuntimeError as err:
            error = "Runtime Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query: {}".format(str(err))
            raise Exception(error)
        finally:
            return [self._prepared, error]

    def create_query(self, sentence: str, consistency: str = 'quorum'):
        if consistency == 'quorum':
            cl = ConsistencyLevel.QUORUM
        else:
            cl = ConsistencyLevel.ALL
        return SimpleStatement(sentence, consistency_level=cl)

    async def query(
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = [],
        factory: str = EXEC_PROFILE_DEFAULT
    ) -> Tuple[Union[ResultSet, None], Any]:
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
                    self._result.result = df = self._result._current_rows
            except ReadTimeout:
                error = f'Timeout reading Data from {sentence}'
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
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

    async def fetch_all(
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = [],
    ) -> ResultSet:
        self._result = None
        try:
            await self.valid_operation(sentence)
            self.start_timing()
            self._result = self._connection.execute(sentence, params)
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
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
            return self._result

    async def fetch(self, sentence, params: List = []):
        return self.fetch_all(sentence, params)

    async def queryrow(
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = [],
    ):
        error = None
        self._result = None
        try:
            await self.valid_operation(sentence)
            self._result = self._connection.execute(sentence, params).one()
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
        except NoDataFound:
            raise
        except RuntimeError as err:
            error = "Runtime on Query Row Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query Row: {}".format(str(err))
            raise Exception(error)
        return [self._result, error]

    async def fetch_one(
        self,
        sentence: Union[str, SimpleStatement, PreparedStatement],
        params: list = [],
    ) -> ResultSet:
        error = None
        self._result = None
        try:
            await self.valid_operation(sentence)
            self._result = self._connection.execute(sentence, params).one()
            if not self._result:
                raise NoDataFound("Cassandra: No Data was Found")
        except NoDataFound:
            raise
        except RuntimeError as err:
            error = "Runtime on Query Row Error: {}".format(str(err))
            raise ProviderError(message=error)
        except Exception as err:
            error = "Error on Query Row: {}".format(str(err))
            raise Exception(error)
        return self._result

    async def fetchrow(self, sentence, params: List = []):
        return self.fetch_one(sentence=sentence, params=params)

    async def execute(self, sentence: Union[str, SimpleStatement, PreparedStatement], params: List = None) -> Any:
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
                raise NoDataFound("Cassandra: No Data was Found")
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
            raise [None, error]
        finally:
            return [self._result, error]

    async def execute_many(self, sentence: Union[str, SimpleStatement, PreparedStatement], params: List = None) -> Any:
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
        except Exception as err:
            error = "Error on Execute: {}".format(str(err))
            raise [None, error]
        finally:
            return [result, error]
        
    """
    Model Logic:
    """

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
            self._logger.exception(f"Wrong Table information {table!s}: {err}")

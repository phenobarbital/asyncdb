""" RethinkDB async Provider.
Notes on RethinkDB async Provider
--------------------
TODO:
 * Index Manipulation
 * Limits (r.table('marvel').order_by('belovedness').limit(10).run(conn))
 * map reductions
 * slice (.slice(3,6).run(conn)) for pagination
 * Group, aggregation, ungroup and reduce
 * to_json_string, to_json

"""
from typing import (
    List,
    Dict,
    Any,
    Optional,
    Iterable
)
from .interfaces import (
    ConnectionDSNBackend,
    DBCursorBackend
)
from .base import (
    InitProvider,
    BaseCursor
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
from rethinkdb.errors import (
    ReqlDriverError,
    ReqlError,
    ReqlNonExistenceError,
    ReqlOpFailedError,
    ReqlOpIndeterminateError,
    ReqlResourceLimitError,
    ReqlRuntimeError,
)
import rethinkdb
from rethinkdb import RethinkDB
import ast
import asyncio
import uvloop
import logging
import time
from datetime import datetime


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()
# RT = RethinkDB()


class rethinkCursor(BaseCursor):
    """
    Cursor Object for RethinkDB.
    """
    _provider: "rethink"
    _connection: Any = None

    def __init__(
        self,
        provider: Any,
        sentence: Any
    ):
        self._provider = provider
        self._sentence = sentence
        self._connection = self._provider.get_connection()

    async def __aenter__(self) -> "rethinkCursor":
        print('ENTER HERE: ')
        return self

    async def __aenter__(self) -> "CursorBackend":
        try:
            self._cursor = await self._sentence.run(self._connection)
        except Exception as err:
            logging.exception(err)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            return await self._cursor.close()
        except Exception as err:
            logging.exception(err)

    async def __anext__(self):
        """Use `cursor.fetchrow()` to provide an async iterable."""
        try:
            row = await self._cursor.next()
        except rethinkdb.errors.ReqlCursorEmpty:
            row = None
        if row is not None:
            return row
        else:
            raise StopAsyncIteration

    async def fetch_one(self) -> Optional[Dict]:
        return await self._cursor.next()

    async def fetch_many(self, size: int = None) -> Iterable[List]:
        pass

    async def fetch_all(self) -> Iterable[List]:
        return list(self._cursor)

    """
    Cursor Methods.
    """

    async def fetch_one(self) -> Optional[Dict]:
        return await self._cursor.fetchone()

    async def fetch_many(self, size: int = None) -> Iterable[List]:
        return await self._cursor.fetch(size)

    async def fetch_all(self) -> Iterable[List]:
        return await self._cursor.fetchall()


class rethink(InitProvider, DBCursorBackend):
    _provider = "rethink"
    _syntax = "rql"

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        self.conditions = {}
        self.fields = []
        self.conditions = {}
        self.cond_definition = None
        self.refresh = False
        self.where = None
        self.ordering = None
        self.qry_options = None
        self._group = None
        self.distinct = None
        InitProvider.__init__(
            self,
            loop=loop,
            params=params,
            **kwargs
        )
        DBCursorBackend.__init__(self, params, **kwargs)
        # set rt object
        self._engine = RethinkDB()
        # set asyncio type
        self._engine.set_loop_type("asyncio")
        asyncio.set_event_loop(self._loop)
        # rethink understand "database" as db
        try:
            self.params["db"] = self.params["database"]
            del self.params["database"]
        except KeyError:
            pass

    async def connection(self):
        self._logger.debug(
            "RT Connection to host {} on port {} to database {}".format(
                self.params["host"], self.params["port"], self.params["db"]
            )
        )
        self.params["timeout"] = self._timeout
        try:
            self._connection = await self._engine.connect(
                **self.params
            )
            if self.params["db"]:
                await self.db(self.params["db"])
        except ReqlRuntimeError as err:
            error = f"No database connection could be established: {err!s}"
            raise ProviderError(message=error, code=503)
        except ReqlDriverError as err:
            error = f"No database connection could be established: {err!s}"
            raise ProviderError(message=error, code=503)
        except Exception as err:
            error = f"Exception on RethinkDB: {err!s}"
            raise ProviderError(message=error, code=503)
        finally:
            if self._connection:
                self._connected = True
        return self

    def engine(self):
        return self._engine

    async def close(self, wait=True):
        try:
            if self._connection:
                await self._connection.close(noreply_wait=wait)
        finally:
            self._connection = None
            self._connected = False

    disconnect = close

    async def release(self):
        await self.close(wait=10)

    """
    Basic Methods
    """

    async def use(self, db: str):
        self._db = db
        try:
            self._connection.use(self._db)
        except ReqlError as err:
            raise ProviderError(message=err, code=503)
        return self

    db = use

    async def createdb(self, dbname):
        """
        CreateDB
              create (if not exists) a new Database
        ------
        """
        try:
            if dbname not in await self._engine.db_list().run(self._connection):
                self._db = dbname
                return await self._engine.db_create(self._db).run(self._connection)
        finally:
            return self

    async def dropdb(self, dbname):
        """
        Drop a database
        """
        try:
            await self._engine.db_drop(dbname).run(self._connection)
        finally:
            if dbname == self._db:
                self._connection.use("test")
            return self

    async def sync(self, table):
        """
        sync
            ensures that writes on a given table are written to permanent storage
        """
        if self._connection:
            if table in await self._engine.db(self._db).table_list().run(
                self._connection
            ):
                return await self._engine.table(table).sync().run(self._connection)

    async def createindex(self, table, field="", name="", fields=[], multi=True):
        """
        CreateIndex
              create and index into a field or multiple fields
        """
        if self._connection:
            if table in await self._engine.db(self._db).table_list().run(
                self._connection
            ):
                # check for a single index
                if field:
                    try:
                        return (
                            await self._engine.table(table)
                            .index_create(field, multi=multi)
                            .run(self._connection)
                        )
                    except ReqlOpFailedError as err:
                        raise ProviderError(message=err, code=503)
                    # except (ReqlDriverError, ReqlRuntimeError):
                    #    return False
                elif type(fields) == list and len(fields) > 0:
                    idx = []
                    for field in fields:
                        idx.append(self._engine.row(field))
                    try:
                        return (
                            await self._engine.table(table)
                            .index_create(name, idx)
                            .run(self._connection)
                        )
                    except (ReqlDriverError, ReqlRuntimeError):
                        return False
            else:
                return False

    async def create_table(self, table, pk=None):
        """
        create_table
           Create a new table with optional primary key
        """
        try:
            if pk:
                return (
                    await self._engine.db(self._db)
                    .table_create(table, primary_key=pk)
                    .run(self._connection)
                )
            else:
                return (
                    await self._engine.db(self._db)
                    .table_create(table)
                    .run(self._connection)
                )
        finally:
            return self

    async def clean(self, table, conditions: dict = {}):
        """
        clean
           Clean a Table
        """
        result = []
        if self.conditions:
            conditions = {**conditions, **self.conditions}

        conditions.update((x, None)
                          for (x, y) in conditions.items() if y == "null")
        self._logger.debug("Conditions for clean {}".format(conditions))
        try:
            if conditions["filterdate"] == "CURRENT_DATE":
                conditions["filterdate"] = today(mask="%Y-%m-%d")
        except (KeyError, ValueError):
            conditions["filterdate"] = today(mask="%Y-%m-%d")
        result = await self.delete(table, filter=conditions, changes=False)
        if result:
            return result
        else:
            return []

    async def listdb(self):
        if self._connection:
            list = await self._engine.db_list().run(self._connection)
            return [x for x in list]
        else:
            return []

    async def list_databases(self):
        return await self.listdb()

    async def list_tables(self):
        if self._connection:
            future = asyncio.Future()
            lists = await self._engine.db(self._db).table_list().run(self._connection)
            future.set_result(lists)
            return [x for x in lists]
        else:
            return []

    async def drop_table(self, table):
        try:
            future = asyncio.Future()
            lists = await self._engine.db(self._db).table_list().run(self._connection)
            future.set_result(list)
            if not table in [x for x in lists]:
                error = "Table {} not exists in database".format(table)
                raise ProviderError(error)
            return (
                await self._engine.db(self._db).table_drop(table).run(self._connection)
            )
        except (ReqlDriverError, ReqlRuntimeError) as err:
            raise ProviderError(str(err))

    """
    Derived Methods (mandatory)
    """
    async def test_connection(self):
        result = None
        error = None
        try:
            result = await self._engine.db_list().run(self._connection)
        except Exception as err:
            return [None, err]
        finally:
            return [result, error]

    def execute(self):
        raise NotImplementedError

    def execute_many(self):
        raise NotImplementedError

    def prepare(self):
        pass

    async def query(self, tablename, filter=None):
        """
        query
            get all rows from a table
        -----
        """
        error = None
        self._result = None
        await self.valid_operation(tablename)
        data = []
        try:
            self.start_timing()
            self._columns = (
                await self._engine.table(tablename)
                .nth(0)
                .default(None)
                .keys()
                .run(self._connection)
            )
            if not filter:
                cursor = (
                    await self._engine.db(self._db)
                    .table(tablename)
                    .run(self._connection)
                )
            else:
                cursor = (
                    await self._engine.db(self._db)
                    .table(tablename)
                    .filter(filter)
                    .run(self._connection)
                )
            while await cursor.fetch_next():
                row = await cursor.next()
                data.append(row)
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Result on {tablename!s}",
                    code=404
                )
        except (ReqlNonExistenceError, ReqlRuntimeError) as err:
            error = f"Query Runtime Error: {err!s}"
            raise NoDataFound(error)
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
            raise ProviderError(message=error)
        except ReqlOpIndeterminateError as err:
            error = f"Query Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except (rethinkdb.errors.ReqlPermissionError, Exception) as err:
            raise ProviderError(message=err)
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)

    async def fetch_all(self, tablename: str, filter: Any = None):
        """
        fetch_all
            get all rows from a table (native)
        -----
        """
        error = None
        self._result = None
        await self.valid_operation(tablename)
        data = []
        try:
            self.start_timing()
            if not filter:
                cursor = (
                    await self._engine.db(self._db)
                    .table(tablename)
                    .run(self._connection)
                )
            else:
                cursor = (
                    await self._engine.db(self._db)
                    .table(tablename)
                    .filter(filter)
                    .run(self._connection)
                )
            while await cursor.fetch_next():
                row = await cursor.next()
                data.append(row)
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Result on {tablename!s}",
                    code=404
                )
        except (ReqlNonExistenceError, ReqlRuntimeError) as err:
            error = f"Query Runtime Error: {err!s}"
            raise NoDataFound(error)
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
            raise ProviderError(message=error)
        except ReqlOpIndeterminateError as err:
            error = f"Query Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except (rethinkdb.errors.ReqlPermissionError, Exception) as err:
            raise ProviderError(message=err)
        finally:
            self.generated_at()
            return self._result

    async def queryrow(self, tablename: str, filter: Dict = {}, id=0):
        """
        queryrow
            get only one row
        """
        error = None
        self._result = None
        await self.valid_operation(tablename)
        try:
            self.start_timing()
            data = (
                await self._engine.table(tablename)
                .filter(filter)
                .nth(id)
                .run(self._connection)
            )
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Row Result on {tablename!s}",
                    code=404
                )
        except (ReqlNonExistenceError, ReqlRuntimeError) as err:
            error = f"Query Runtime Error: {err!s}"
            raise NoDataFound(error)
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
            raise ProviderError(message=error)
        except ReqlOpIndeterminateError as err:
            error = f"Query Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except (rethinkdb.errors.ReqlPermissionError, Exception) as err:
            raise ProviderError(message=err)
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)

    async def fetch_one(self, tablename: str, filter: Dict = {}, id=0):
        """
        queryrow
            get only one row
        """
        error = None
        self._result = None
        await self.valid_operation(tablename)
        try:
            self.start_timing()
            data = (
                await self._engine.table(tablename)
                .filter(filter)
                .nth(id)
                .run(self._connection)
            )
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Row Result on {tablename!s}",
                    code=404
                )
        except (ReqlNonExistenceError, ReqlRuntimeError) as err:
            error = f"Query Runtime Error: {err!s}"
            raise NoDataFound(error)
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
            raise ProviderError(message=error)
        except ReqlOpIndeterminateError as err:
            error = f"Query Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except (rethinkdb.errors.ReqlPermissionError, Exception) as err:
            raise ProviderError(message=err)
        finally:
            self.generated_at()
            return self._result

    """
    New Methods
    """
    async def get(self, tablename: str, id: int = 0):
        """
        get
           get only one row based on primary key or filtering,
           Get a document by primary key.
        -----
        """
        error = None
        await self.valid_operation(tablename)
        try:
            data = await self._engine.table(
                tablename
                ).get(id).run(self._connection)
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Row Result on {tablename!s}",
                    code=404
                )
        except ReqlNonExistenceError as err:
            error = f"Missing (non existence) on {tablename}: {err!s}"
            raise NoDataFound(error)
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            error = f"RethinkDB Runtime Error: {err!s}"
            raise ProviderError(error)
        except Exception as err:
            error = f'Unexpected RethinkDB Error: {err!s}'
            raise ProviderError(error)
        finally:
            return await self._serializer(self._result, error)

    async def get_all(self, tablename, filter=[], index=""):
        """
        get_all.
           get all rows where the given value matches the value of
           the requested index.
        -----
        """
        error = None
        self._result = None
        await self.valid_operation(tablename)
        try:
            if index:
                cursor = (
                    await self._engine.table(tablename)
                    .get_all(filter, index=index)
                    .run(self._connection)
                )
            else:
                cursor = (
                    await self._engine.table(tablename)
                    .get_all(filter)
                    .run(self._connection)
                )
            data = []
            while await cursor.fetch_next():
                item = await cursor.next()
                data.append(item)
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Row Result on {tablename!s}",
                    code=404
                )
        except ReqlNonExistenceError as err:
            error = f"Missing (non existence) on {tablename}: {err!s}"
            raise NoDataFound(error)
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            error = f"RethinkDB Runtime Error: {err!s}"
            raise ProviderError(error)
        except Exception as err:
            error = f'Unexpected RethinkDB Error: {err!s}'
            raise ProviderError(error)
        finally:
            return await self._serializer(self._result, error)

    async def match(self, tablename, field="id", regexp="(?i)^[a-z]+$"):
        """
        match
           get all rows where the given value matches with a regular expression
        -----
        """
        self._result = None
        await self.valid_operation(tablename)
        try:
            data = (
                await self._engine.table(tablename)
                .filter(lambda doc: doc[field].match(regexp))
                .run(self._connection)
            )
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Row Result on {tablename!s}",
                    code=404
                )
        except ReqlNonExistenceError as err:
            error = f"Missing (non existence) on {tablename}: {err!s}"
            raise NoDataFound(error)
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            error = f"RethinkDB Runtime Error: {err!s}"
            raise ProviderError(error)
        except Exception as err:
            error = f'Unexpected RethinkDB Error: {err!s}'
            raise ProviderError(error)
        finally:
            return await self._serializer(self._result, error)

    async def insert(self, tablename, data):
        """
        insert
             create a record (insert)
        -----
        """
        try:
            inserted = (
                await self._engine.table(tablename)
                .insert(data, conflict="replace")
                .run(self._connection)
            )
            if inserted["errors"] > 0:
                raise ProviderError(
                    "INSERT Runtime Error: {}".format(
                        inserted["first_error"])
                )
                return False
            return inserted
        except ReqlNonExistenceError as err:
            raise NoDataFound(
                f"RethinkDB: Object {tablename} doesn't exists : {err!s}"
            )
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            raise ProviderError(
                f"RethinkDB: INSERT Runtime Error: {err!s}"
            )
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB INSERT Error: {err!s}'
            )

    async def replace(self, tablename, id, data):
        """
        replace
             replace a record (insert, update or delete)
        -----
        """
        try:
            replaced = (
                await self._engine.table(tablename)
                .get(id)
                .replace(data)
                .run(self._connection)
            )
            if replaced["errors"] > 0:
                raise ProviderError(
                    "REPLACE Runtime Error: {}".format(
                        self._result["first_error"])
                )
                return False
            return replaced
        except ReqlNonExistenceError as err:
            raise NoDataFound(
                f"RethinkDB: Object {tablename} doesn't exists : {err!s}"
            )
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            raise ProviderError(
                f"RethinkDB: REPLACE Runtime Error: {err!s}"
            )
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB REPLACE Error: {err!s}'
            )

    async def update(self, tablename, data, id=None, filter={}):
        """
        update
             update a record based on filter match
        -----
        """
        if id:
            sentence = self._engine.table(tablename).get(id).update(data)
        elif type(filter) == dict and len(filter) > 0:
            sentence = self._engine.table(tablename).filter(
                filter).update(data, return_changes=False)
        else:
            # update all documents in table
            sentence = self._engine.table(
                tablename
            ).update(data)
        try:
            self._result = (await sentence.run(self._connection))
            return self._result
        except ReqlRuntimeError as err:
            raise ProviderError(f"UPDATE Runtime Error: {err!s}")
        except ReqlNonExistenceError:
            raise ProviderError(
                f"Object {tablename} doesn't exists"
            )
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB REPLACE Error: {err!s}'
            )

    async def literal(self, tablename, id, data):
        """
        literal
            replace a field with another
        """
        try:
            self._result = (
                await self._engine.table(tablename)
                .get(id)
                .update({field: r.literal(data).run(self._connection)})
            )
            return self._result
        except ReqlRuntimeError as err:
            raise ProviderError(f"LITERAL Runtime Error: {err!s}")
        except ReqlNonExistenceError:
            raise ProviderError(
                f"LITERAL: Object {tablename} doesn't exists"
            )
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB LITERAL Error: {err!s}'
            )

    async def update_conditions(
                self,
                tablename,
                data,
                filter={},
                fieldname="filterdate"
            ):
        """
        update_conditions
             update a record based on a fieldname
        -----
        """
        try:
            self._result = (
                await self._engine.table(tablename)
                .filter(~self._engine.row.has_fields(fieldname))
                .filter(filter)
                .update(data)
                .run(self._connection)
            )
            return self._result
        except ReqlRuntimeError as err:
            raise ProviderError(f"UPDATE Runtime Error: {err!s}")
        except ReqlNonExistenceError:
            raise ProviderError(
                f"UPDATE: Object {tablename} doesn't exists"
            )
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB UPDATE Error: {err!s}'
            )

    async def delete(self, table, id=None, filter={}, changes=True):
        """
        delete
             delete a record based on id or filter search
        -----
        """
        if id:
            sentence = self._engine.table(table).get(
                id).delete(return_changes=changes)
        elif isinstance(filter, dict):
            sentence = self._engine.table(table).filter(
                filter).delete(return_changes=changes)
        else:
            sentence = self._engine.table(table).delete(return_changes=changes)
        try:
            self._result = (await sentence.run(self._connection))
            return self._result
        except ReqlRuntimeError as err:
            raise ProviderError(f"DELETE Runtime Error: {err!s}")
        except ReqlNonExistenceError:
            raise ProviderError(
                f"DELETE: Object {tablename} doesn't exists"
            )
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB DELETE Error: {err!s}'
            )

    async def between(self, tablename, min=None, max=None, idx=""):
        """
        between
             Get all documents between two keys
        -----
        """
        self._result = None
        await self.valid_operation(tablename)
        error = None
        if min:
            m = min
        else:
            m = self._engine.minval
        if max:
            mx = max
        else:
            mx = self._engine.maxval
        try:
            if idx:
                cursor = (
                    await self._engine.table(tablename)
                    .order_by(index=idx)
                    .between(m, mx, index=idx)
                    .run(self._connection)
                )
            else:
                cursor = (
                    await self._engine.table(tablename)
                    .between(m, mx)
                    .run(self._connection)
                )
            data = []
            while await cursor.fetch_next():
                item = await cursor.next()
                data.append(item)
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Row Result on {tablename!s}",
                    code=404
                )
        except ReqlNonExistenceError as err:
            error = f"Missing (non existence) on {tablename}: {err!s}"
            raise NoDataFound(error)
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            error = f"RethinkDB Runtime Error: {err!s}"
            raise ProviderError(error)
        except Exception as err:
            error = f'Unexpected RethinkDB Error: {err!s}'
            raise ProviderError(error)
        finally:
            return await self._serializer(self._result, error)

    """
    Cursors:
    """

    def cursor(self, tablename, filter=None):
        """
        cursor
            get all rows from a table, returning a Cursor.
        -----
        """
        error = None
        self._result = None
        try:
            if not filter:
                cursor = self._engine.db(self._db).table(tablename)
            else:
                cursor = self._engine.db(self._db).table(
                    tablename).filter(filter)
            return self.__cursor__(
                provider=self,
                sentence=cursor
            )
        except (ReqlNonExistenceError, ReqlRuntimeError) as err:
            raise NoDataFound(f"Query Runtime Error: {err!s}")
        except ReqlResourceLimitError as err:
            raise ProviderError(message=f"Query Limit Error: {err!s}")
        except ReqlOpIndeterminateError as err:
            raise ProviderError(message=f"Query Runtime Error: {err!s}")
        except (rethinkdb.errors.ReqlPermissionError, Exception) as err:
            raise ProviderError(message=err)

    """
    Infraestructure for Functions, creating filter conditions
    """

    def set_definition(self, options):
        self.cond_definition = options
        return self

    def set_options(self, params):
        print(" =========== PARAMS IS: %s" % params)
        # get fields and where_cond
        try:
            self.refresh = bool(params["refresh"])
            del params["refresh"]
        except (KeyError, IndexError):
            self.refresh = False
        # get conditions and query source
        try:
            self.fields = params["fields"]
            del params["fields"]
        except KeyError:
            self.fields = None
        print("FIELDS: {}".format(self.fields))
        # group by
        try:
            del params["group_by"]
        except KeyError:
            pass
        # where condition
        try:
            self.where = params["where_cond"]
            del params["where_cond"]
        except KeyError:
            self.where = None
        # ordering condition
        try:
            self.ordering = params["ordering"]
            del params["ordering"]
        except KeyError:
            pass
        # support for distinct
        try:
            self.distinct = params["distinct"]
            del params["distinct"]
        except KeyError:
            pass
        try:
            self.qry_options = params["qry_options"]
            del params["qry_options"]
        except (KeyError, ValueError):
            pass
        try:
            self.querylimit = params["querylimit"]
            del params["querylimit"]
        except KeyError:
            self.querylimit = None
        try:
            self._program = params["program_slug"]
            del params["program_slug"]
        except KeyError:
            pass
        # other options are set conditions
        self.set_conditions(conditions=params)

    def set_conditions(self, conditions={}):
        if conditions and len(conditions) > 0:
            for key, value in conditions.items():
                print("set conditions: key is %s, value is %s" % (key, value))
                if type(value) == list:
                    self.conditions[key] = value
                else:
                    val = is_udf(value)
                    if val:
                        self.conditions[key] = "{}".format(val)
                    # elif is_program_date(value):
                    #    val = get_program_date(value)
                    #    self.conditions[key] = "{}".format(val)
                    elif self.cond_definition and self.cond_definition[key] == "field":
                        self.conditions[key] = "{}".format(value)
                    elif value == "null" or value == "NULL":
                        self.conditions[key] = "null"
                    elif (
                        self.cond_definition and self.cond_definition[key] == "boolean"
                    ):
                        self.conditions[key] = value
                    elif isinteger(value) or isnumber(value):
                        self.conditions[key] = value
                    elif self.cond_definition and self.cond_definition[key] == "date":
                        self.conditions[key] = value.replace("'", "")
                    else:
                        print("RT condition %s for value %s" % (key, value))
                        self.conditions[key] = "{}".format(value)
            print("set_conditions: {}".format(self.conditions))
        return self

    """
    Query Options
    """

    def query_options(self, result, conditions):
        print(
            "PROGRAM FOR QUERY IS {} for option {}".format(
                self._program, self.qry_options
            )
        )
        if self.qry_options:
            hierarchy = get_hierarchy(self._program)
            if hierarchy:
                try:
                    get_filter = [
                        k.replace("!", "")
                        for k in conditions
                        if k.replace("!", "") in hierarchy
                    ]
                    filter_sorted = sorted(get_filter, key=hierarchy.index)
                except (TypeError, ValueError, KeyError):
                    return result
                ## processing different types of query option
                try:
                    get_index = hierarchy.index(filter_sorted.pop())
                    # print(get_index)
                    selected = hierarchy[get_index + 1:]
                except (KeyError, IndexError):
                    selected = []
                try:
                    if self.qry_options["null_rolldown"] == "true":
                        # print('null all conditions below selection')
                        if selected:
                            for n in selected:
                                result = result.and_(
                                    self._engine.row[n].eq(None))
                        else:
                            if get_filter:
                                last = get_filter.pop(0)
                                if last != hierarchy[-1]:
                                    result = result.and_(
                                        self._engine.row[last].ne(None)
                                    )
                                else:
                                    first = hierarchy.pop(0)
                                    # _where[first] = 'null'
                                    result = result.and_(
                                        self._engine.row[first].eq(None)
                                    )
                            else:
                                last = hierarchy.pop(0)
                                result = result.and_(
                                    self._engine.row[last].eq(None))
                except (KeyError, ValueError):
                    pass
                try:
                    if self.qry_options["select_child"] == "true":
                        try:
                            child = selected.pop(0)
                            result = result.and_(
                                self._engine.row[child].ne(None))
                            # _where[child] = '!null'
                            for n in selected:
                                result = result.and_(
                                    self._engine.row[n].eq(None))
                            return result
                        except (ValueError, IndexError):
                            if get_filter:
                                pass
                            else:
                                child = hierarchy.pop(0)
                                result = result.and_(
                                    self._engine.row[child].ne(None))
                                # _where[child] = '!null'
                                for n in hierarchy:
                                    # _where[n] = 'null'
                                    result = result.and_(
                                        self._engine.row[n].eq(None))
                except (KeyError, ValueError):
                    pass
                try:
                    if self.qry_options["select_stores"] == "true":
                        try:
                            last = selected.pop()
                            result = result.and_(
                                self._engine.row[last].ne(None))
                            return result
                        except (ValueError, IndexError):
                            last = hierarchy.pop()
                            result = result.and_(
                                self._engine.row[last].ne(None))
                except (KeyError, ValueError):
                    pass
        return result

    async def get_one(self, table, filter={}, id=0):

        if self._connection:
            try:
                self._result = (
                    await self._engine.table(table)
                    .filter(filter)
                    .nth(id)
                    .run(self._connection)
                )
                return self._result
            except ReqlNonExistenceError as err:
                raise ReqlNonExistenceError(
                    "Empty Result: {}".format(str(err)))
            except ReqlRuntimeError as err:
                raise ReqlRuntimeError(str(err))
            except Exception as err:
                raise Exception(err)

    async def run_one(self, table):
        """
        Functions for Query API
        """
        conditions = {}
        result = []
        if self.conditions:
            conditions = {**self.conditions}
        if self.where:
            conditions.update(self.where)
        conditions.update((x, None)
                          for (x, y) in conditions.items() if y == "null")
        try:
            if conditions["filterdate"] == "CURRENT_DATE":
                conditions["filterdate"] = today(mask="%Y-%m-%d")
        except (KeyError, ValueError):
            pass
        try:
            result = await self.get_one(table, filter=conditions)
        except (ReqlRuntimeError, ReqlNonExistenceError) as err:
            raise Exception("Error on Query One: {}".format(str(err)))
        if result:
            return result
        else:
            return []

    """
    run
       Run a filter based on where_cond and conditions
    """

    async def run(self, table):

        conditions = {}
        result = []

        if self.conditions:
            conditions = {**self.conditions}

        if self.where:
            conditions.update(self.where)

        conditions.update((x, None)
                          for (x, y) in conditions.items() if y == "null")

        print("RT CONDITIONS {}".format(conditions))

        # mapping fields with new names
        map = {}
        has_fields = []
        try:
            if self.fields:
                for field in self.fields:
                    name = ""
                    alias = ""
                    if " as " in field:
                        el = field.split(" as ")
                        print(el)
                        name = el[0]
                        alias = el[1].replace('"', "")
                        map[alias] = self._engine.row[name]
                        self.fields.remove(field)
                        self.fields.append(name)
                    else:
                        map[field] = self._engine.row[field]
                print("RT FIELDS {}".format(self.fields))
                has_fields = self.fields.copy()

                print("RT MAP IS {}".format(map))
        except Exception as err:
            print("FIELD ERROR {}".format(err))

        try:
            if conditions["filterdate"] == "CURRENT_DATE":
                conditions["filterdate"] = today(mask="%Y-%m-%d")
        except (KeyError, ValueError):
            pass

        filters = []
        try:
            keys = list(conditions.keys())
            has_fields = has_fields + keys
        except (KeyError, ValueError):
            pass

        # build the search element
        # print(self._db)
        search = self._engine.db(self._db).table(table).has_fields(has_fields)

        result = self._engine.expr(True)

        ### build FILTER based on rethink logic
        for key, value in conditions.items():
            if type(value) is list:
                # print(value)
                search = search.filter(
                    (
                        lambda exp: self._engine.expr(value)
                        .coerce_to("array")
                        .contains(exp[key])
                    )
                )
            else:
                if type(value) is str:
                    if value.startswith("!"):
                        # not null
                        result = result.and_(
                            self._engine.row[key].ne(value.replace("!", ""))
                        )
                    elif value.startswith("["):
                        # between
                        result = result.and_(
                            self._engine.row[key].between(10, 20))
                    else:
                        result = result.and_(self._engine.row[key].eq(value))
                else:
                    result = result.and_(self._engine.row[key].eq(value))

        # query options
        if self.qry_options:
            result = self.query_options(result, conditions)

        # print("RESULT IS")
        # print(result)

        # add search criteria
        search = search.filter(result)

        # fields and mapping
        if self.fields:
            if map:
                search = search.pluck(self.fields).map(map)
            else:
                search = search.pluck(self.fields)

        # ordering
        order = None
        if self.ordering:
            if type(self.ordering) is list:
                orderby = self.ordering[0].split(" ")
            else:
                orderby = self.ordering.split(" ")
            if orderby[1] == "DESC":
                order = self._engine.desc(orderby[0])
            else:
                order = orderby[0]
            # add ordering
            search = search.order_by(order)

        # adding distinct
        if self.distinct:
            search = search.distinct()

        if self._connection:
            data = []
            self._result = None
            try:
                try:
                    cursor = await search.run(self._connection)
                except (ReqlRuntimeError, ReqlRuntimeError) as err:
                    print("Error on rql query is %s" % err.message)
                    raise Exception("Error on RQL query is %s" % err.message)
                    return False
                if order or self.distinct:
                    self._result = cursor
                    return self._result
                else:
                    while await cursor.fetch_next():
                        row = await cursor.next()
                        data.append(row)
                    self._result = data
            finally:
                return self._result
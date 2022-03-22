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
    Iterable,
    Union
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
import asyncio
import uvloop
import logging
from asyncdb.utils.functions import today
from .base import CursorBackend


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()


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

    # async def __aenter__(self) -> "rethinkCursor":
    #     return self

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

    def __init__(self, loop: asyncio.AbstractEventLoop = None, params: Dict = None, **kwargs):
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
                await self.db(
                    self.params["db"]
                )
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

    async def createdb(self, dbname: str):
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

    async def dropdb(self, dbname: str):
        """
        Drop a database
        """
        try:
            await self._engine.db_drop(dbname).run(self._connection)
        finally:
            if dbname == self._db:
                self._connection.use("test")
            return self

    async def sync(self, table: str):
        """
        sync
            ensures that writes on a given table are written to permanent storage
        """
        if self._connection:
            if table in await self._engine.db(self._db).table_list().run(
                self._connection
            ):
                return await self._engine.table(table).sync().run(self._connection)

    async def createindex(
            self, table: str, 
            field: str = "", 
            name: str = "", 
            fields: List = None, 
            multi=True
        ):
        """
        CreateIndex
              create and index into a field or multiple fields
              --- r.table('comments').index_create('post_and_date', [r.row["post_id"], r.row["date"]]).run(conn)
        """
        if self._connection:
            if table in await self._engine.db(self._db).table_list().run(
                self._connection
            ):
                # check for a single index
                if type(fields) == list and len(fields) > 0:
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
                    try:
                        return (
                            await self._engine.table(table)
                            .index_create(field, multi=multi)
                            .run(self._connection)
                        )
                    except ReqlOpFailedError as err:
                        raise ProviderError(message=err, code=503)
            else:
                return False
            
    create_index = createindex

    async def create_table(self, table: str, pk: Union[str, List] = None):
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

    async def clean(self, table: str, conditions: Dict = None):
        """
        clean
           Clean a Table based on some conditions.
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

    async def drop_table(self, table: str):
        try:
            future = asyncio.Future()
            lists = await self._engine.db(self._db).table_list().run(self._connection)
            future.set_result(list)
            if not table in [x for x in lists]:
                error = "Table {} not exists in database".format(table)
                raise ProviderError(message=error)
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
        raise NotImplementedError

    async def query(self, table: str, filter: List = None):
        """
        query
            get all rows from a table
        -----
        """
        error = None
        self._result = None
        await self.valid_operation(table)
        data = []
        try:
            self.start_timing()
            self._columns = (
                await self._engine.table(table)
                .nth(0)
                .default(None)
                .keys()
                .run(self._connection)
            )
            if not filter:
                cursor = (
                    await self._engine.db(self._db)
                    .table(table)
                    .run(self._connection)
                )
            else:
                cursor = (
                    await self._engine.db(self._db)
                    .table(table)
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
                    message=f"RethinkDB: Empty Result on {table!s}",
                    code=404
                )
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
            raise ProviderError(message=error)
        except ReqlOpIndeterminateError as err:
            error = f"Query Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except (ReqlNonExistenceError, ReqlRuntimeError) as err:
            error = f"Query Runtime Error: {err!s}"
            raise NoDataFound(error)
        except (rethinkdb.errors.ReqlPermissionError, Exception) as err:
            raise ProviderError(message=err)
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)

    async def fetch_all(self, table: str, filter: Any = None):
        """
        fetch_all
            get all rows from a table (native)
        -----
        """
        error = None
        self._result = None
        await self.valid_operation(table)
        data = []
        try:
            self.start_timing()
            if not filter:
                cursor = (
                    await self._engine.db(self._db)
                    .table(table)
                    .run(self._connection)
                )
            else:
                cursor = (
                    await self._engine.db(self._db)
                    .table(table)
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
                    message=f"RethinkDB: Empty Result on {table!s}",
                    code=404
                )
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
            raise ProviderError(message=error)
        except ReqlOpIndeterminateError as err:
            error = f"Query Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except (ReqlNonExistenceError, ReqlRuntimeError) as err:
            error = f"Query Runtime Error: {err!s}"
            raise NoDataFound(error)
        except (rethinkdb.errors.ReqlPermissionError, Exception) as err:
            raise ProviderError(message=err)
        finally:
            self.generated_at()
            return self._result
    
    fetchall = fetch_all

    async def queryrow(self, table: str, filter: Dict = {}, id: int = 0):
        """
        queryrow
            get only one row.
        """
        error = None
        self._result = None
        await self.valid_operation(table)
        try:
            self.start_timing()
            data = (
                await self._engine.table(table)
                .filter(filter)
                .nth(id)
                .run(self._connection)
            )
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Row Result on {table!s}",
                    code=404
                )
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
            raise ProviderError(message=error)
        except ReqlOpIndeterminateError as err:
            error = f"Query Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except (ReqlNonExistenceError, ReqlRuntimeError) as err:
            error = f"Query Runtime Error: {err!s}"
            raise NoDataFound(error)
        except (rethinkdb.errors.ReqlPermissionError, Exception) as err:
            raise ProviderError(message=err)
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)
        
    query_row = queryrow

    async def fetch_one(self, table: str, filter: Dict = None, id: int = 0):
        """
        queryrow
            get only one row from query
        """
        error = None
        self._result = None
        await self.valid_operation(table)
        try:
            self.start_timing()
            data = (
                await self._engine.table(table)
                .filter(filter)
                .nth(id)
                .run(self._connection)
            )
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Row Result on {table!s}",
                    code=404
                )
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
            raise ProviderError(message=error)
        except ReqlOpIndeterminateError as err:
            error = f"Query Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except (ReqlNonExistenceError, ReqlRuntimeError) as err:
            error = f"Query Runtime Error: {err!s}"
            raise NoDataFound(error)
        except (rethinkdb.errors.ReqlPermissionError, Exception) as err:
            raise ProviderError(message=err)
        finally:
            self.generated_at()
            return self._result

    """
    New Methods
    """
    async def get(self, table: str, id: int = 0):
        """
        get
           get only one row based on primary key or filtering,
           Get a document by primary key.
        -----
        """
        error = None
        await self.valid_operation(table)
        try:
            data = await self._engine.table(
                table
                ).get(id).run(self._connection)
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Row Result on {table!s}",
                    code=404
                )
        except ReqlNonExistenceError as err:
            error = f"Missing (non existence) on {table}: {err!s}"
            raise NoDataFound(error)
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            error = f"RethinkDB Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except Exception as err:
            error = f'Unexpected RethinkDB Error: {err!s}'
            raise ProviderError(message=error)
        finally:
            return await self._serializer(self._result, error)

    async def get_all(self, table: str, filter: List = None, index: str = None):
        """
        get_all.
           get all rows where the given value matches the value of
           the requested index.
        -----
        """
        error = None
        self._result = None
        await self.valid_operation(table)
        try:
            if index:
                cursor = (
                    await self._engine.table(table)
                    .get_all(filter, index=index)
                    .run(self._connection)
                )
            else:
                cursor = (
                    await self._engine.table(table)
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
                    message=f"RethinkDB: Empty Row Result on {table!s}",
                    code=404
                )
        except ReqlNonExistenceError as err:
            error = f"Missing (non existence) on {table}: {err!s}"
            raise NoDataFound(error)
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            error = f"RethinkDB Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except Exception as err:
            error = f'Unexpected RethinkDB Error: {err!s}'
            raise ProviderError(message=error)
        finally:
            return await self._serializer(self._result, error)

    async def match(self, table: str, field: str = "id", regexp="(?i)^[a-z]+$"):
        """
        match
           get all rows where the given value matches with a regular expression
        -----
        """
        self._result = None
        await self.valid_operation(table)
        try:
            data = (
                await self._engine.table(table)
                .filter(lambda doc: doc[field].match(regexp))
                .run(self._connection)
            )
            if data:
                self._result = data
            else:
                raise NoDataFound(
                    message=f"RethinkDB: Empty Row Result on {table!s}",
                    code=404
                )
        except ReqlNonExistenceError as err:
            error = f"Missing (non existence) on {table}: {err!s}"
            raise NoDataFound(error)
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            error = f"RethinkDB Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except Exception as err:
            error = f'Unexpected RethinkDB Error: {err!s}'
            raise ProviderError(message=error)
        finally:
            return await self._serializer(self._result, error)

    async def insert(self, table: str, data: Dict, on_conflict: str = 'replace', changes: bool = True):
        """
        insert
             create a record (insert)
        -----
        """
        try:
            inserted = (
                await self._engine.table(table)
                .insert(data, conflict=on_conflict, durability="soft", return_changes=changes)
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
                f"RethinkDB: Object {table} doesn't exists : {err!s}"
            )
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            raise ProviderError(
                f"RethinkDB: INSERT Runtime Error: {err!s}"
            )
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB INSERT Error: {err!s}'
            )

    async def replace(self, table: str, id, data):
        """
        replace
             replace a record (insert, update or delete)
        -----
        """
        try:
            replaced = (
                await self._engine.table(table)
                .get(id)
                .replace(data, durability="soft")
                .run(self._connection)
            )
            if replaced["errors"] > 0:
                raise ProviderError(
                    "REPLACE Runtime Error: {}".format(
                        self._result["first_error"])
                )
            return replaced
        except ReqlNonExistenceError as err:
            raise NoDataFound(
                f"RethinkDB: Object {table} doesn't exists : {err!s}"
            )
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            raise ProviderError(
                f"RethinkDB: REPLACE Runtime Error: {err!s}"
            )
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB REPLACE Error: {err!s}'
            )

    async def update(self, table: str, data: dict, id: str = None, filter: dict = None):
        """
        update
             update a record based on filter match
        -----
        """
        if id:
            sentence = self._engine.table(table).get(id).update(data)
        elif type(filter) == dict and len(filter) > 0:
            sentence = self._engine.table(table).filter(
                filter).update(data, return_changes=False, durability="soft")
        else:
            # update all documents in table
            sentence = self._engine.table(
                table
            ).update(data, durability="soft", return_changes=False)
        try:
            self._result = (await sentence.run(self._connection))
            return self._result
        except ReqlNonExistenceError:
            raise ProviderError(
                f"Object {table} doesn't exists"
            )
        except ReqlRuntimeError as err:
            raise ProviderError(f"UPDATE Runtime Error: {err!s}")
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB REPLACE Error: {err!s}'
            )

    async def literal(self, table: str, id: str, field: str, data: dict):
        """
        literal
            replace a field with another
        """
        try:
            self._result = (
                await self._engine.table(table)
                .get(id)
                .update(
                    {
                        field: self._engine.literal(data).run(self._connection)
                    }
                )
            )
            return self._result
        except ReqlNonExistenceError:
            raise ProviderError(
                f"LITERAL: Object {table} doesn't exists"
            )
        except ReqlRuntimeError as err:
            raise ProviderError(f"LITERAL Runtime Error: {err!s}")
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB LITERAL Error: {err!s}'
            )

    async def update_conditions(
                self,
                table: str,
                data: dict,
                filter: dict = None,
                field: str = "filterdate"
        ):
        """
        update_conditions
             update a record based on a fieldname
        -----
        """
        try:
            self._result = (
                await self._engine.table(table)
                .filter(~self._engine.row.has_fields(field)
                )
                .filter(filter)
                .update(data, durability="soft", return_changes=False)
                .run(self._connection)
            )
            return self._result
        except ReqlNonExistenceError:
            raise ProviderError(
                f"UPDATE: Object {table} doesn't exists"
            )
        except ReqlRuntimeError as err:
            raise ProviderError(f"UPDATE Runtime Error: {err!s}")
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB UPDATE Error: {err!s}'
            )

    async def delete(
            self,
            table: str,
            id: str = None,
            filter: dict = None,
            changes: bool = True
        ):
        """
        delete
             delete a record based on id or filter search
        -----
        """
        if id:
            sentence = self._engine.table(table).get(
                id).delete(return_changes=changes, durability='soft')
        elif isinstance(filter, dict):
            sentence = self._engine.table(table).filter(
                filter).delete(return_changes=changes)
        else:
            sentence = self._engine.table(table).delete(return_changes=changes)
        try:
            self._result = (await sentence.run(self._connection))
            return self._result
        except ReqlNonExistenceError:
            raise ProviderError(
                f"DELETE: Object {table} doesn't exists"
            )
        except ReqlRuntimeError as err:
            raise ProviderError(f"DELETE Runtime Error: {err!s}")
        except Exception as err:
            raise ProviderError(
                f'Unexpected RethinkDB DELETE Error: {err!s}'
            )

    async def between(
            self,
            table: str,
            min: int = None,
            max: int = None,
            idx: str = None
        ):
        """
        between
             Get all documents between two keys
        -----
        """
        self._result = None
        await self.valid_operation(table)
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
                    await self._engine.table(table)
                    .order_by(index=idx)
                    .between(m, mx, index=idx)
                    .run(self._connection)
                )
            else:
                cursor = (
                    await self._engine.table(table)
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
                    message=f"RethinkDB: Empty Row Result on {table!s}",
                    code=404
                )
        except ReqlNonExistenceError as err:
            error = f"Missing (non existence) on {table}: {err!s}"
            raise NoDataFound(error)
        except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
            error = f"RethinkDB Runtime Error: {err!s}"
            raise ProviderError(message=error)
        except Exception as err:
            error = f'Unexpected RethinkDB Error: {err!s}'
            raise ProviderError(message=error)
        finally:
            return await self._serializer(self._result, error)

    """
    Cursors:
    """

    def cursor(self, table: str, filter: Union[Dict, List] = None):
        """
        cursor
            get all rows from a table, returning a Cursor.
        -----
        """
        error = None
        self._result = None
        try:
            if not filter:
                cursor = self._engine.db(self._db).table(table)
            else:
                cursor = self._engine.db(self._db).table(table).filter(filter)
            return self.__cursor__(
                provider=self,
                sentence=cursor
            )
        except ReqlResourceLimitError as err:
            raise ProviderError(message=f"Query Limit Error: {err!s}")
        except ReqlOpIndeterminateError as err:
            raise ProviderError(message=f"Query Runtime Error: {err!s}")
        except (ReqlNonExistenceError, ReqlRuntimeError) as err:
            raise NoDataFound(f"Query Runtime Error: {err!s}")
        except (rethinkdb.errors.ReqlPermissionError, Exception) as err:
            raise ProviderError(message=err)

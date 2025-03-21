""" RethinkDB async Provider.
Notes on RethinkDB async Provider
--------------------
TODO:
 * Index Manipulation
 * map reductions
 * slice (.slice(3,6).run(conn))
 * Group, aggregation, ungroup and reduce
 * to_json_string, to_json

"""

import asyncio
import time
from typing import Any, Optional, Union
from collections.abc import Iterable
import pandas
import rethinkdb
from rethinkdb.ast import RqlQuery
from rethinkdb.errors import (
    ReqlDriverError,
    ReqlError,
    ReqlNonExistenceError,
    ReqlOpFailedError,
    ReqlOpIndeterminateError,
    ReqlResourceLimitError,
    ReqlRuntimeError,
)
from rethinkdb import RethinkDB, r
from datamodel import BaseModel
from datamodel.parsers.json import JSONContent
from ..interfaces.cursors import DBCursorBackend, CursorBackend
from ..exceptions import DriverError, DataError, NoDataFound, StatementError
from .base import InitDriver
from .cursor import BaseCursor


class JSONEncoder(JSONContent):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Point):
            return obj.as_point()
        return obj.build() if isinstance(obj, RqlQuery) else super().default(obj)



class Point(BaseModel):
    x: float
    y: float

    def as_point(self) -> Any:
        return r.point(self.x, self.y)


class rethinkCursor(BaseCursor):
    """
    Cursor Object for RethinkDB.
    """

    _provider: "rethink"

    async def __aenter__(self) -> CursorBackend:
        try:
            self._cursor = await self._sentence.run(self._connection)
        except Exception as err:  # pylint: disable=W0703
            self._logger.exception(err)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            return await self._cursor.close()
        except Exception as err:  # pylint: disable=W0703
            self._logger.exception(err)

    async def __anext__(self):
        """Use `cursor.fetchrow()` to provide an async iterable."""
        try:
            row = await self._cursor.next()
        except AttributeError:
            row = None
        except rethinkdb.errors.ReqlCursorEmpty:
            row = None
        if row is not None:
            return row
        else:
            raise StopAsyncIteration

    async def fetch_one(self) -> Optional[dict]:
        return await self._cursor.fetchone()

    async def fetch_many(self, size: int = None) -> Iterable[dict]:
        return await self._cursor.fetch(size)

    async def fetch_all(self) -> Iterable[dict]:
        return await self._cursor.fetchall()


class rethink(InitDriver, DBCursorBackend):
    _provider = "rethink"
    _syntax = "rql"
    _dsn_template: str = ''

    def __init__(self, loop: asyncio.AbstractEventLoop = None, params: dict = None, **kwargs):
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
        self._db: str = "test"
        InitDriver.__init__(self, loop=loop, params=params, **kwargs)
        DBCursorBackend.__init__(self)
        # set rt object
        self._engine = RethinkDB()
        # set asyncio type
        self._engine.set_loop_type("asyncio")
        # rethink understand "database" as db
        self.params["db"] = self.params.get("database", None)

    async def connection(self):
        self._connection = None
        self.params["timeout"] = self._timeout
        try:
            self._connection = await self._engine.connect(
                **self.params,
                json_encoder=JSONEncoder,
                json_decoder=JSONEncoder
            )
            if self.params["db"]:
                await self.db(self.params["db"])
        except ReqlRuntimeError as err:
            raise DriverError(f"No database connection could be established: {err!s}") from err
        except ReqlDriverError as err:
            raise DriverError(f"No database connection could be established: {err!s}") from err
        except Exception as err:
            raise DriverError(f"Exception on RethinkDB: {err!s}") from err
        finally:
            if self._connection:
                self._connected = True
        return self

    def engine(self):
        return self._engine

    async def close(self, timeout=10, wait=True):
        try:
            if self._connection:
                await self._connection.close(noreply_wait=wait)
        finally:
            self._connection = None
            self._connected = False

    disconnect = close

    async def release(self):
        await self.close(wait=10)

    ### Basic Methods
    async def use(self, database: str):
        self._db = database
        try:
            self._connection.use(self._db)
        except ReqlError as err:
            raise DriverError(message=f"Error connecting to database: {database}") from err
        return self

    db = use

    async def createdb(self, database: str, use: bool = False):
        """
        CreateDB
              create (if not exists) a new Database
        ------
        """
        try:
            if database not in await self._engine.db_list().run(self._connection):
                await self._engine.db_create(database).run(self._connection)
            if use is True:
                self._db = database
                self._connection.use(self._db)
        except Exception as ex:
            raise DriverError(f"Unable to create database: {ex}") from ex

    create_database = createdb

    async def dropdb(self, database: str):
        """
        Drop a database
        """
        try:
            await self._engine.db_drop(database).run(self._connection)
            return self
        finally:
            if database == self._db:  # current database
                self._db = None
                self._connection.use("test")

    drop_database = dropdb

    async def sync(self, table: str):
        """
        sync
            ensures that writes on a given table are written to permanent storage
        """
        await self.valid_operation(table)
        if table in await self._engine.db(self._db).table_list().run(self._connection):
            return await self._engine.table(table).sync().run(self._connection)

    async def createindex(
        self, table: str, name: str = None, field: str = None, fields: list = None, multi: bool = True
    ):
        """
        CreateIndex
              create and index into a field or multiple fields
              --- r.table('comments').index_create('post_and_date', [r.row["post_id"], r.row["date"]]).run(conn)
        """
        await self.valid_operation(table)
        if table in await self._engine.db(self._db).table_list().run(self._connection):
            # check for a single index
            if isinstance(fields, (list, tuple)) and len(fields) > 0:
                idx = []
                for f in fields:
                    idx.append(self._engine.row[f])
                try:
                    return await self._engine.table(table).index_create(name, idx).run(self._connection)
                except (ReqlDriverError, ReqlRuntimeError) as ex:
                    self._logger.error(f"Failed to create index: {ex}")
                    return False
            else:
                try:
                    return await self._engine.table(table).index_create(field, multi=multi).run(self._connection)
                except ReqlOpFailedError as ex:
                    raise DriverError(f"Failed to create index: {ex}") from ex
        else:
            return False

    create_index = createindex

    async def create_table(self, table: str, pk: Union[str, list] = None, exists_ok: bool = True):
        """
        create_table
           Create a new table with optional primary key
        """
        try:
            if pk:
                creation = self._engine.db(self._db).table_create(table, primary_key=pk)
            else:
                creation = self._engine.db(self._db).table_create(table)
            return await creation.run(self._connection)
        except ReqlOpFailedError as ex:
            if "already exists in" in str(ex) and exists_ok:
                return True
            raise DriverError(f"Cannot create Table {table}, {ex}") from ex
        except (ReqlDriverError, ReqlRuntimeError) as ex:
            raise DriverError(f"Error crating Table {table}, {ex}") from ex
        except Exception as err:
            raise DriverError(f"Unknown ERROR on Table Creation: {err}") from err

    async def clean(self, table: str, conditions: list = None):
        """
        clean
           Clean a Table based on some conditions.
        """
        result = []
        if self.conditions:
            conditions = {**conditions, **self.conditions}
        conditions.update((x, None) for (x, y) in conditions.items() if y == "null")
        self._logger.debug(f"Conditions for clean Table {table}: {conditions!r}")
        try:
            if conditions["filterdate"] == "CURRENT_DATE":
                conditions["filterdate"] = time.strftime("%Y-%m-%d")
        except (KeyError, ValueError):
            conditions["filterdate"] = time.strftime("%Y-%m-%d")
        result = await self.delete(table, filter=conditions, changes=False)
        return result

    async def listdb(self):
        if self._connection:
            return await self._engine.db_list().run(self._connection)
        else:
            return []

    list_databases = listdb

    async def list_tables(self):
        if self._connection:
            tables = await self._engine.db(self._db).table_list().run(self._connection)
            return tables
        else:
            return []

    async def drop_table(self, table: str):
        try:
            return await self._engine.db(self._db).table_drop(table).run(self._connection)
        except ReqlOpFailedError as ex:
            raise DriverError(f"Cannot drop Table {table}, {ex}") from ex
        except (ReqlDriverError, ReqlRuntimeError) as ex:
            raise DriverError(f"Error dropping Table {table}, {ex}") from ex
        except Exception as err:
            raise DriverError(f"Unknown ERROR on Table Drop: {err}") from err

    #### Derived Methods (mandatory)
    async def test_connection(self, **kwargs):
        result = None
        error = None
        try:
            result = await self._engine.db_list().run(self._connection)
        except Exception as err:  # pylint: disable=W0703
            return [None, err]
        finally:
            return [result, error]  # pylint: disable=W0150

    async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]:
        raise NotImplementedError

    async def execute_many(self, sentence: Any, *args, **kwargs) -> Optional[Any]:
        raise NotImplementedError

    async def prepare(self, sentence: Any, **kwargs):
        raise NotImplementedError

    async def query(
        self, table: str, columns: list = None, order_by: list = None, limit: int = None, **kwargs
    ):  # pylint: disable=W0221,W0237
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
            _filter = kwargs.get("filter", kwargs)
            # table:
            tbl = self._engine.db(self._db).table(table)
            if not columns:
                try:
                    self._columns = await tbl.nth(0).default(None).keys().run(self._connection)
                except rethinkdb.errors.ReqlQueryLogicError:
                    self._columns = []
            else:
                self._columns = columns
                tbl = tbl.with_fields(*columns)
            if _filter:
                result = tbl.filter(_filter)
            else:
                result = tbl
            if isinstance(order_by, list):
                order = [r.asc(o) for o in order_by]
                result = result.order_by(*order)
            if limit is not None:
                result = result.limit(limit)
            cursor = await result.run(self._connection)
            if isinstance(cursor, list):
                self._result = cursor
            else:
                while await cursor.fetch_next():
                    row = await cursor.next()
                    data.append(row)
                if data:
                    self._result = data
                else:
                    raise NoDataFound(message=f"RethinkDB: Empty Result on {table!s}")
        except NoDataFound:
            raise
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
        except ReqlOpIndeterminateError as err:
            error = f"Operation indeterminated: {err!s}"
        except ReqlNonExistenceError as err:
            error = f"Object doesn't exist {table}: {err!s}"
        except rethinkdb.errors.ReqlPermissionError as err:
            error = f"Permission error over {table}: {err}"
        except ReqlRuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Unknown RT error: {err}"
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def fetch_all(self, table: str, **kwargs):  # pylint: disable=W0221,W0237
        """
        query
            get all rows from a table
        -----
        """
        self._result = None
        await self.valid_operation(table)
        data = []
        try:
            self.start_timing()
            _filter = kwargs.get("filter", kwargs)
            try:
                self._columns = await self._engine.table(table).nth(0).default(None).keys().run(self._connection)
            except rethinkdb.errors.ReqlQueryLogicError:
                self._columns = []
            if not _filter:
                cursor = await self._engine.db(self._db).table(table).run(self._connection)
            else:
                cursor = await self._engine.db(self._db).table(table).filter(_filter).run(self._connection)
            while await cursor.fetch_next():
                row = await cursor.next()
                data.append(row)
            if data:
                return data
            else:
                raise NoDataFound(message=f"RethinkDB: Empty Result on {table!s}")
        except NoDataFound:
            raise
        except ReqlResourceLimitError as err:
            raise StatementError(f"Query Limit Error: {err!s}") from err
        except ReqlOpIndeterminateError as err:
            raise StatementError(f"Operation indeterminated: {err!s}") from err
        except ReqlNonExistenceError as err:
            raise DriverError(f"Object doesn't exist {table}: {err!s}") from err
        except rethinkdb.errors.ReqlPermissionError as err:
            raise DataError(f"Permission error over {table}: {err}") from err
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:  # pylint: disable=W0703
            raise DriverError(f"Unknown RT error: {err}") from err
        finally:
            self.generated_at()

    fetchall = fetch_all

    async def queryrow(self, table: str, columns: list = None, nth: int = 0, **kwargs):  # pylint: disable=W0221,W0237
        """
        queryrow
            get only one row.
        """
        error = None
        self._result = None
        await self.valid_operation(table)
        try:
            self.start_timing()
            _filter = kwargs.get("filter", kwargs)
            if not columns:
                try:
                    self._columns = await self._engine.table(table).nth(0).default(None).keys().run(self._connection)
                except rethinkdb.errors.ReqlQueryLogicError:
                    self._columns = []
            else:
                self._columns = columns
            # table:
            table = self._engine.db(self._db).table(table)
            if columns:
                table = table.with_fields(*columns)
            if _filter:
                result = table.filter(_filter)
            else:
                result = table
            data = await result.nth(nth).run(self._connection)
            if data:
                self._result = data
            else:
                raise NoDataFound(message=f"RethinkDB: Empty Row Result on {table!s}")
        except NoDataFound:
            raise
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
        except ReqlOpIndeterminateError as err:
            error = f"Operation indeterminated: {err!s}"
        except ReqlNonExistenceError as err:
            error = f"Object doesn't exist {table}: {err!s}"
        except rethinkdb.errors.ReqlPermissionError as err:
            error = f"Permission error over {table}: {err}"
        except ReqlRuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Unknown RT error: {err}"
        finally:
            self.generated_at()
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    query_row = queryrow

    async def fetch_one(self, table: str, nth: int = 0, **kwargs):  # pylint: disable=W0221,W0237
        """
        fetch_one
            get only one row.
        """
        self._result = None
        await self.valid_operation(table)
        try:
            _filter = kwargs.get("filter", kwargs)
            self.start_timing()
            table = self._engine.db(self._db).table(table)
            if kwargs:
                action = table.filter(_filter).nth(nth)
            else:
                action = table.nth(nth)
            data = await action.run(self._connection)
            if not data:
                raise NoDataFound(message=f"RethinkDB: Empty Row Result on {table!s}", code=404)
            return data
        except NoDataFound:
            raise
        except ReqlNonExistenceError as err:
            raise NoDataFound(f"Object doesn't exist {table}: {err!s}")
        except ReqlResourceLimitError as err:
            raise StatementError(f"Query Limit Error: {err!s}") from err
        except ReqlOpIndeterminateError as err:
            raise StatementError(f"Operation indeterminated: {err!s}") from err
        except rethinkdb.errors.ReqlPermissionError as err:
            raise DataError(f"Permission error over {table}: {err}") from err
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:  # pylint: disable=W0703
            raise DriverError(f"Unknown RT error: {err}") from err

    fetch_row = fetch_one

    ### New Methods
    async def get(self, table: str, idx: int = 0, **kwargs):
        """
        get
           get only one row based on primary key or filtering,
           Get a document by primary key.
        -----
        """
        error = None
        await self.valid_operation(table)
        _filter = kwargs.get("filter", kwargs)
        try:
            if _filter:
                data = await self._engine.table(table).get_all(_filter).get(idx).run(self._connection)
            else:
                data = await self._engine.table(table).get(idx).run(self._connection)
            if data:
                self._result = data
            else:
                raise NoDataFound(message=f"RethinkDB: Empty Row Result on {table!s}")
        except NoDataFound:
            raise
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
        except ReqlOpIndeterminateError as err:
            error = f"Operation indeterminated: {err!s}"
        except ReqlNonExistenceError as err:
            error = f"Object doesn't exist {table}: {err!s}"
        except rethinkdb.errors.ReqlPermissionError as err:
            error = f"Permission error over {table}: {err}"
        except ReqlRuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Unknown RT error: {err}"
        finally:
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def get_all(self, table: str, index: str = None, **kwargs):
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
            _filter = kwargs.get("filter", kwargs)
            if index:
                cursor = await self._engine.table(table).get_all(_filter, index=index).run(self._connection)
            else:
                cursor = await self._engine.table(table).get_all(_filter).run(self._connection)
            data = []
            while await cursor.fetch_next():
                item = await cursor.next()
                data.append(item)
            if data:
                self._result = data
            else:
                raise NoDataFound(message=f"RethinkDB: Empty Row Result on {table!s}")
        except NoDataFound:
            raise
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
        except ReqlOpIndeterminateError as err:
            error = f"Operation indeterminated: {err!s}"
        except ReqlNonExistenceError as err:
            error = f"Object doesn't exist {table}: {err!s}"
        except rethinkdb.errors.ReqlPermissionError as err:
            error = f"Permission error over {table}: {err}"
        except ReqlRuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Unknown RT error: {err}"
        finally:
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def match(self, table: str, field: str = "id", regexp="(?i)^[a-z]+$"):
        """
        match
           get all rows where the given value matches with a regular expression
        -----
        """
        self._result = None
        error = None
        await self.valid_operation(table)
        try:
            data = await self._engine.table(table).filter(lambda doc: doc[field].match(regexp)).run(self._connection)
            if data:
                self._result = data
            else:
                raise NoDataFound(message=f"RethinkDB: Empty Row Result on {table!s}")
        except NoDataFound:
            raise
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
        except ReqlOpIndeterminateError as err:
            error = f"Operation indeterminated: {err!s}"
        except ReqlNonExistenceError as err:
            error = f"Object doesn't exist {table}: {err!s}"
        except rethinkdb.errors.ReqlPermissionError as err:
            error = f"Permission error over {table}: {err}"
        except ReqlRuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Unknown RT error: {err}"
        finally:
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    async def _batch_insert(self, table: str, data: list, on_conflict: str, changes: bool, durability: str):
        """
        Helper method to perform batch inserts.
        """
        return (
            await self._engine.table(table)
            .insert(data, conflict=on_conflict, durability=durability, return_changes=changes)
            .run(self._connection)
        )

    async def write(
        self,
        table: str,
        data: Union[list, dict, Any],
        batch_size: int = 100,
        on_conflict: str = "replace",
        changes: bool = True,
        durability: str = "soft",
        **kwargs,
    ):
        """
        write.

        Saving data into a rethinkDB database on batch-async mode.
        """
        try:
            if isinstance(data, pandas.DataFrame):
                # Convert DataFrame to list of dicts
                data = data.to_dict(orient="records")
            if isinstance(data, list) and len(data) > batch_size:
                # Handle batch insertion for large lists
                for start in range(0, len(data), batch_size):
                    self._logger.debug(
                        f"Rethink: Saving batch {start + 1} to {start + batch_size} of {len(data)} records"
                    )
                    batch = data[start:start + batch_size]
                    result = await self._batch_insert(table, batch, on_conflict, changes, durability)
                    if result["errors"] > 0:
                        raise DriverError(
                            f"INSERT Error in batch: {result['first_error']}"
                        )
                return {
                    "inserted": len(data),
                    "batches": (len(data) + batch_size - 1) // batch_size
                }
            else:
                result = await self._batch_insert(table, data, on_conflict, changes, durability)
                if result["errors"] > 0:
                    raise DriverError(f"INSERT Error: {result['first_error']}")
                return result
        except ReqlResourceLimitError as err:
            raise StatementError(f"Query Limit Error: {err!s}") from err
        except ReqlOpIndeterminateError as err:
            raise StatementError(f"Operation indeterminated: {err!s}") from err
        except ReqlNonExistenceError as err:
            raise DriverError(f"Object doesn't exist {table}: {err!s}") from err
        except rethinkdb.errors.ReqlPermissionError as err:
            raise DataError(f"Permission error over {table}: {err}") from err
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:
            raise DriverError(f"Unknown RT error: {err}") from err

    async def insert(
        self,
        table: str,
        data: Union[dict, list],
        on_conflict: str = "replace",
        changes: bool = True,
        durability: str = "soft",
    ):
        """
        insert.
             create a record (insert)
        -----
        """
        try:
            inserted = (
                await self._engine.table(table)
                .insert(data, conflict=on_conflict, durability=durability, return_changes=changes)
                .run(self._connection)
            )
            if inserted["errors"] > 0:
                raise DriverError(f"INSERT Error: {inserted['first_error']}")
            return inserted
        except ReqlResourceLimitError as err:
            raise StatementError(f"Query Limit Error: {err!s}") from err
        except ReqlOpIndeterminateError as err:
            raise StatementError(f"Operation indeterminated: {err!s}") from err
        except ReqlNonExistenceError as err:
            raise DriverError(f"Object doesn't exist {table}: {err!s}") from err
        except rethinkdb.errors.ReqlPermissionError as err:
            raise DataError(f"Permission error over {table}: {err}") from err
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:  # pylint: disable=W0703
            raise DriverError(f"Unknown RT error: {err}") from err

    async def replace(self, table: str, data: Union[dict, list], idx: int = 0, durability: str = "soft"):
        """
        replace
             replace a record (insert, update or delete)
        -----
        """
        try:
            replaced = (
                await self._engine.table(table).get(idx).replace(data, durability=durability).run(self._connection)
            )
            if replaced["errors"] > 0:
                raise DriverError(f"REPLACE Error: {replaced['first_error']}")
            return replaced
        except ReqlResourceLimitError as err:
            raise StatementError(f"Query Limit Error: {err!s}") from err
        except ReqlOpIndeterminateError as err:
            raise StatementError(f"Operation indeterminated: {err!s}") from err
        except ReqlNonExistenceError as err:
            raise DriverError(f"Object doesn't exist {table}: {err!s}") from err
        except rethinkdb.errors.ReqlPermissionError as err:
            raise DataError(f"Permission error over {table}: {err}") from err
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:  # pylint: disable=W0703
            raise DriverError(f"Unknown RT error: {err}") from err

    async def update(
        self, table: str, data: dict, idx: str = None, return_changes: bool = False, durability: str = "soft", **kwargs
    ):
        """
        update
             update a record based on filter match
        -----
        """
        _filter = kwargs.get("filter", kwargs)
        if idx:
            sentence = self._engine.table(table).get(id).update(data)
        elif isinstance(_filter, dict) and len(_filter) > 0:
            sentence = (
                self._engine.table(table)
                .filter(_filter)
                .update(data, return_changes=return_changes, durability=durability)
            )
        else:
            # update all documents in table
            sentence = self._engine.table(table).update(data, durability=durability, return_changes=return_changes)
        try:
            self._result = await sentence.run(self._connection)
            return self._result
        except ReqlResourceLimitError as err:
            raise StatementError(f"Query Limit Error: {err!s}") from err
        except ReqlOpIndeterminateError as err:
            raise StatementError(f"Operation indeterminated: {err!s}") from err
        except ReqlNonExistenceError as err:
            raise DriverError(f"Object doesn't exist {table}: {err!s}") from err
        except rethinkdb.errors.ReqlPermissionError as err:
            raise DataError(f"Permission error over {table}: {err}") from err
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:  # pylint: disable=W0703
            raise DriverError(f"Unknown RT error: {err}") from err

    async def literal(self, table: str, idx: int, field: str, data: dict):
        """
        literal
            replace a field with another
        """
        try:
            self._result = (
                await self._engine.table(table)
                .get(idx)
                .update({field: self._engine.literal(data).run(self._connection)})
            )
            return self._result
        except ReqlResourceLimitError as err:
            raise StatementError(f"Query Limit Error: {err!s}") from err
        except ReqlOpIndeterminateError as err:
            raise StatementError(f"Operation indeterminated: {err!s}") from err
        except ReqlNonExistenceError as err:
            raise DriverError(f"Object doesn't exist {table}: {err!s}") from err
        except rethinkdb.errors.ReqlPermissionError as err:
            raise DataError(f"Permission error over {table}: {err}") from err
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:  # pylint: disable=W0703
            raise DriverError(f"Unknown RT error: {err}") from err

    async def update_conditions(self, table: str, data: dict, field: str = "filterdate", **kwargs):
        """
        update_conditions
             update a record based on a fieldname
        -----
        """
        try:
            _filter = kwargs.get("filter", kwargs)
            self._result = (
                await self._engine.table(table)
                .filter(~self._engine.row.has_fields(field))
                .filter(_filter)
                .update(data, durability="soft", return_changes=False)
                .run(self._connection)
            )
            return self._result
        except ReqlResourceLimitError as err:
            raise StatementError(f"Query Limit Error: {err!s}") from err
        except ReqlOpIndeterminateError as err:
            raise StatementError(f"Operation indeterminated: {err!s}") from err
        except ReqlNonExistenceError as err:
            raise DriverError(f"Object doesn't exist {table}: {err!s}") from err
        except rethinkdb.errors.ReqlPermissionError as err:
            raise DataError(f"Permission error over {table}: {err}") from err
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:  # pylint: disable=W0703
            raise DriverError(f"Unknown RT error: {err}") from err

    async def delete(
        self, table: str, idx: str = None, return_changes: bool = True, durability: str = "soft", **kwargs
    ):
        """
        delete
             delete a record based on id or filter search
        -----
        """
        _filter = kwargs.get("filter", kwargs)
        if idx:
            sentence = self._engine.table(table).get(idx).delete(return_changes=return_changes, durability=durability)
        elif isinstance(_filter, dict):
            sentence = self._engine.table(table).filter(_filter).delete(return_changes=return_changes)
        else:
            sentence = self._engine.table(table).delete(return_changes=return_changes)
        try:
            self._result = await sentence.run(self._connection)
            return self._result
        except ReqlResourceLimitError as err:
            raise StatementError(f"Query Limit Error: {err!s}") from err
        except ReqlOpIndeterminateError as err:
            raise StatementError(f"Operation indeterminated: {err!s}") from err
        except ReqlNonExistenceError as err:
            raise DriverError(f"Object doesn't exist {table}: {err!s}") from err
        except rethinkdb.errors.ReqlPermissionError as err:
            raise DataError(f"Permission error over {table}: {err}") from err
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:  # pylint: disable=W0703
            raise DriverError(f"Unknown RT error: {err}") from err

    async def between(self, table: str, min: int = None, max: int = None, idx: str = None):
        """
        between.
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
                    await self._engine.table(table).order_by(index=idx).between(m, mx, index=idx).run(self._connection)
                )
            else:
                cursor = await self._engine.table(table).between(m, mx).run(self._connection)
            data = []
            while await cursor.fetch_next():
                item = await cursor.next()
                data.append(item)
            if data:
                self._result = data
            else:
                raise NoDataFound(message=f"RethinkDB: Empty Row Result on {table!s}")
        except NoDataFound:
            raise
        except ReqlResourceLimitError as err:
            error = f"Query Limit Error: {err!s}"
        except ReqlOpIndeterminateError as err:
            error = f"Operation indeterminated: {err!s}"
        except ReqlNonExistenceError as err:
            error = f"Object doesn't exist {table}: {err!s}"
        except rethinkdb.errors.ReqlPermissionError as err:
            error = f"Permission error over {table}: {err}"
        except ReqlRuntimeError as err:
            error = f"Runtime Error: {err}"
        except Exception as err:  # pylint: disable=W0703
            error = f"Unknown RT error: {err}"
        finally:
            return await self._serializer(self._result, error)  # pylint: disable=W0150

    # Cursors:
    def cursor(self, table: str, params: Union[dict, list] = None, **kwargs):  # pylint: disable=W0237
        """
        cursor
            get all rows from a table, returning a Cursor.
        -----
        """
        self._result = None
        try:
            if not filter:
                cursor = self._engine.db(self._db).table(table)
            else:
                cursor = self._engine.db(self._db).table(table).filter(params)
            return self.__cursor__(provider=self, sentence=cursor)
        except ReqlResourceLimitError as err:
            raise StatementError(f"Query Limit Error: {err!s}") from err
        except ReqlOpIndeterminateError as err:
            raise StatementError(f"Operation indeterminated: {err!s}") from err
        except ReqlNonExistenceError as err:
            raise DriverError(f"Object doesn't exist {table}: {err!s}") from err
        except rethinkdb.errors.ReqlPermissionError as err:
            raise DataError(f"Permission error over {table}: {err}") from err
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:  # pylint: disable=W0703
            raise DriverError(f"Unknown RT error: {err}") from err

    async def distance(self, p1: Point, p2: Point, unit: str = "km", geo: str = "WGS84") -> float:
        if not isinstance(p1, Point):
            raise TypeError(f"Invalid type for Point 1: {type(p1)}")
        if not isinstance(p2, Point):
            raise TypeError(f"Invalid type for Point 2: {type(p2)}")
        try:
            point1 = p1.as_point()
            point2 = p2.as_point()
            return await self._engine.distance(point1, point2, unit=unit, geo_system=geo).run(self._connection)
        except ReqlRuntimeError as err:
            raise DriverError(f"Runtime Error: {err}") from err
        except Exception as err:  # pylint: disable=W0703
            raise DriverError(f"Unknown RT error: {err}") from err

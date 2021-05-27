""" RethinkDB async Provider.
Notes on RethinkDB async Provider
--------------------
TODO:
 * Cursor Logic
    - cursor.next([wait=True])
    - iter for cursors (for, next, close)
 * Index Manipulation
 * Limits (r.table('marvel').order_by('belovedness').limit(10).run(conn))
 * map reductions
 * slice (.slice(3,6).run(conn)) for pagination
 * Group, aggregation, ungroup and reduce
 * to_json_string, to_json

"""
import ast
import asyncio
import logging
import time
from datetime import datetime
from threading import Thread

from rethinkdb import RethinkDB

from rethinkdb.errors import (
    ReqlDriverError,
    ReqlError,
    ReqlNonExistenceError,
    ReqlOpFailedError,
    ReqlOpIndeterminateError,
    ReqlResourceLimitError,
    ReqlRuntimeError,
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
from asyncdb.utils.functions import *

from . import (
    BaseProvider,
    registerProvider,
)

rt = RethinkDB()


class rethink(BaseProvider):
    _provider = "rethinkdb"
    _syntax = "rql"
    _test_query = ""
    _loop = None
    _engine = None
    _connection = None
    _connected = False
    _prepared = None
    _parameters = ()
    _cursor = None
    _transaction = None
    _initialized_on = None
    _db = None
    conditions = {}
    fields = []
    cond_definition = None
    refresh = False
    where = None
    ordering = None
    qry_options = None
    _group = None
    distinct = None

    def __init__(self, loop=None, params={}, **kwargs):
        super(rethink, self).__init__(loop=loop, params=params, **kwargs)
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.get_event_loop()
        self.conditions = {}
        # set rt object
        self._engine = rt
        # set asyncio type
        self._engine.set_loop_type("asyncio")
        asyncio.set_event_loop(self._loop)

    async def connection(self):
        self._logger.debug(
            "RT Connection to host {} on port {} to database {}".format(
                self._params["host"], self._params["port"], self._params["db"]
            )
        )
        self._params["timeout"] = self._timeout
        try:
            self._connection = await self._engine.connect(**self._params)
            if self._params["db"]:
                await self.db(self._params["db"])
        except ReqlRuntimeError as err:
            error = "No database connection could be established: {}".format(str(err))
            raise ProviderError(message=error, code=503)
            return False
        except ReqlDriverError as err:
            error = "No database connection could be established: {}".format(str(err))
            raise ProviderError(message=error, code=503)
            return False
        except Exception as err:
            error = "Exception on RethinkDB: {}".format(str(err))
            raise ProviderError(message=error, code=503)
            return False
        finally:
            if self._connection:
                self._connected = True
        return self

    def engine(self):
        return self._engine

    def create_dsn(self, params):
        return None

    async def close(self, wait=True):
        try:
            if self._connection:
                await self._connection.close(noreply_wait=wait)
        finally:
            self._connection = None

    def terminate(self):
        self._logger.debug("Closing Rethink Connection")
        self._loop.run_until_complete(self.close())

    async def release(self):
        await self.close(wait=10)

    """
    Basic Methods
    """

    async def db(self, db):
        self._db = db
        try:
            self._connection.use(self._db)
        except ReqlError as err:
            raise ProviderError(message=err, code=503)
        return self

    def use(self, db):
        self._db = db
        # print(self._db)
        try:
            self._connection.use(self._db)
        except ReqlError as err:
            raise ProviderError(message=err, code=503)
        return self

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
            self._db = dbname
            return self

    async def dropdb(self, dbname):
        """
        Drop a database
        """
        try:
            await self._engine.db_drop(dbname).run(self._connection)
        finally:
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

        conditions.update((x, None) for (x, y) in conditions.items() if y == "null")
        print("Conditions for clean {}".format(conditions))

        try:
            if conditions["filterdate"] == "CURRENT_DATE":
                conditions["filterdate"] = today(mask="%Y-%m-%d")
        except (KeyError, ValueError):
            conditions["filterdate"] = today(mask="%Y-%m-%d")
        result = await self.delete(table, filter=conditions, changes=False)
        # print(result)
        if result:
            return result
        else:
            return []

    async def listdb(self):
        if self._connection:
            lists = await self._engine.db_list().run(self._connection)
            return [x for x in lists]
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
            if result:
                return [result, error]
        except Exception as err:
            return [None, err]
        finally:
            return [result, error]

    def execute(self):
        pass

    def prepare(self):
        pass

    async def query(self, table, filter=None):
        """
        query
            get all rows from a table
        -----
        """
        error = None
        if not table:
            raise EmptyStatement("Rethink: Table name is an empty string")

        if self._connection:
            data = []
            try:
                # self._columns = await self._engine.table(table).get(1).keys().run(self._connection)
                self._columns = (
                    await self._engine.table(table)
                    .nth(0)
                    .default(None)
                    .keys()
                    .run(self._connection)
                )
                # print(self._columns)
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
                    raise NoDataFound(message="Empty Result", code=404)
            except ReqlNonExistenceError as err:
                error = "Query Runtime Error: {}".format(str(err))
                raise ReqlNonExistenceError(error)
            except ReqlRuntimeError as err:
                error = "Query Runtime Error: {}".format(str(err))
                raise NoDataFound(error)
            except ReqlResourceLimitError as error:
                error = "Query Runtime Error: {}".format(str(err))
                raise ReqlResourceLimitError(error)
            except ReqlOpIndeterminateError as error:
                error = "Query Runtime Error: {}".format(str(err))
                raise ReqlOpIndeterminateError(error)
            finally:
                # self._generated = datetime.now() - startTime
                return [self._result, error]

    async def queryrow(self, table, filter={}, id=0):
        """
        queryrow
            get only one row
        """
        error = None
        if not table:
            raise EmptyStatement("Rethink: Table name is an empty string")
        if self._connection:
            # startTime = datetime.now()
            try:
                data = (
                    await self._engine.table(table)
                    .filter(filter)
                    .nth(id)
                    .run(self._connection)
                )
                if data:
                    self._result = data
                else:
                    raise NoDataFound(message="Empty Result", code=404)
            except ReqlNonExistenceError as err:
                error = "Empty Result: {}".format(str(err))
                raise NoDataFound(error)
            except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
                error = "QueryRow Runtime Error: {}".format(str(err))
                raise ProviderError(err)
                return False
            finally:
                return [self._result, error]
        else:
            return [None, "Not Connected"]

    """
    New Methods
    """

    async def get(self, table, id):
        """
        get
           get only one row based on primary key or filtering, Get a document by primary key
        -----
        """
        error = None
        if not id:
            raise EmptyStatement("Rethink: Id for get cannot be empty")

        if not table:
            raise EmptyStatement("Rethink: Table name is an empty string")

        if self._connection:
            try:
                data = await self._engine.table(table).get(id).run(self._connection)
                if data:
                    self._result = data
                else:
                    raise NoDataFound(message="Empty Result", code=404)
            except ReqlNonExistenceError as err:
                error = "Empty Result: {}".format(str(err))
                raise NoDataFound(error)
            except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
                error = "QueryRow Runtime Error: {}".format(str(err))
                raise ProviderError(err)
                return False
            finally:
                return [self._result, error]

    async def get_all(self, table, filter=[], index=""):
        """
        get_all
           get all rows where the given value matches the value of the requested index
        -----
        """
        if not table:
            raise EmptyStatement("Rethink: Table name is an empty string")

        if self._connection:
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
                    raise NoDataFound(message="Empty Result", code=404)
                return self._result
            except ReqlRuntimeError as err:
                error = "Query Get All Runtime Error: {}".format(str(err))
                raise ProviderError(error)
                return False

    async def match(self, table, field="id", regexp="(?i)^[a-z]+$"):
        """
        match
           get all rows where the given value matches with a regular expression
        -----
        """
        if not table:
            raise EmptyStatement("Rethink: Table name is an empty string")
        if self._connection:
            try:
                data = (
                    await self._engine.table(table)
                    .filter(lambda doc: doc[field].match(regexp))
                    .run(self._connection)
                )
                if data:
                    self._result = data
                else:
                    raise NoDataFound(message="Empty Result", code=404)
                return self._result
            except ReqlRuntimeError as err:
                error = "Query Get All Runtime Error: {}".format(str(err))
                raise ProviderError(error)
                return False

    async def insert(self, table, data):
        """
        insert
             create a record (insert)
        -----
        """
        if self._connection:
            try:
                inserted = (
                    await self._engine.table(table)
                    .insert(data, conflict="replace")
                    .run(self._connection)
                )
                if inserted["errors"] > 0:
                    raise ProviderError(
                        "INSERT Runtime Error: {}".format(inserted["first_error"])
                    )
                    return False
                return inserted
            except ReqlRuntimeError as err:
                error = "INSERT Runtime Error: {}".format(str(err))
                raise ProviderError(error)
                return False
            except ReqlNonExistenceError:
                raise ProviderError("Object {} doesnt exists".format(table))
                return False
            # finally:
            #    return await self._engine.table(table).sync().run(self._connection)
        else:
            return False

    async def replace(self, table, id, data):
        """
        replace
             replace a record (insert, update or delete)
        -----
        """
        if self._connection:
            try:
                self._result = (
                    await self._engine.table(table)
                    .get(id)
                    .replace(data)
                    .run(self._connection)
                )
                if self._result["errors"] > 0:
                    raise ProviderError(
                        "REPLACE Runtime Error: {}".format(self._result["first_error"])
                    )
                    return False
                return self._result
            except ReqlRuntimeError as err:
                error = "REPLACE Runtime Error: {}".format(str(err))
                raise ProviderError(error)
                return False
            except ReqlNonExistenceError:
                raise ProviderError("Object {} doesnt exists".format(table))
                return False
        else:
            return False

    async def update(self, table, data, id=None, filter={}):
        """
        update
             update a record based on filter match
        -----
        """
        if self._connection:
            if id:
                try:
                    self._result = (
                        await self._engine.table(table)
                        .get(id)
                        .update(data)
                        .run(self._connection)
                    )
                    # self._result = await self._engine.table(table).get(id).update(data).run(self._connection)
                    return self._result
                except ReqlRuntimeError as err:
                    error = "UPDATE Runtime Error: {}".format(str(err))
                    raise ProviderError(error)
                    return False
                except ReqlNonExistenceError:
                    raise ProviderError("Object {} doesnt exists".format(table))
                    return False
            elif type(filter) == dict and len(filter) > 0:
                try:
                    self._result = (
                        await self._engine.table(table)
                        .filter(filter)
                        .update(data, return_changes=False)
                        .run(self._connection)
                    )
                    # self._result = await self._engine.table(table).filter(filter).update(data).run(self._connection)
                    return self._result
                except ReqlRuntimeError as err:
                    error = "REPLACE Runtime Error: {}".format(str(err))
                    raise ProviderError(error)
                    return False
                except ReqlNonExistenceError:
                    raise ProviderError("Object {} doesnt exists".format(table))
                    return False
            else:
                # update all documents in table
                return (
                    await self._engine.table(table).update(data).run(self._connection)
                )
        else:
            return False

    async def literal(self, table, id, data):
        """
        literal
            replace a field with another
        """
        if self._connection:
            try:
                self._result = (
                    await self._engine.table(table)
                    .get(id)
                    .update({field: r.literal(data).run(self._connection)})
                )
                return self._result
            except ReqlRuntimeError as err:
                error = "Literal Runtime Error: {}".format(str(err))
                raise ProviderError(error)
                return False
            except ReqlNonExistenceError:
                raise ProviderError("Object {} doesnt exists".format(table))
                return False
        else:
            return False

    async def update_conditions(self, table, data, filter={}, fieldname="filterdate"):
        """
        update_conditions
             update a record based on a fieldname
        -----
        """
        if self._connection:
            try:
                self._result = (
                    await self._engine.table(table)
                    .filter(~self._engine.row.has_fields(fieldname))
                    .filter(filter)
                    .update(data)
                    .run(self._connection)
                )
                return self._result
            except ReqlRuntimeError as err:
                error = "Update Conditions Error: {}".format(str(err))
                raise ProviderError(error)
                return False
            except ReqlNonExistenceError:
                raise ProviderError("Object {} doesnt exists".format(table))
                return False
        else:
            return False

    async def delete(self, table, id=None, filter={}, changes=True):
        """
        delete
             delete a record based on id or filter search
        -----
        """
        if not table:
            raise EmptyStatement("Rethink: Table name is an empty string")

        if self._connection:
            if id:
                try:
                    self._result = (
                        await self._engine.table(table)
                        .get(id)
                        .delete(return_changes=changes)
                        .run(self._connection)
                    )
                    return self._result
                except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
                    raise ProviderError(err)
                    return False
            elif isinstance(filter, dict):
                try:
                    self._result = (
                        await self._engine.table(table)
                        .filter(filter)
                        .delete(return_changes=changes)
                        .run(self._connection)
                    )
                    return self._result
                except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
                    raise ProviderError(err)
                    return False
            else:
                # delete all documents in table
                return (
                    await self._engine.table(table)
                    .delete(return_changes=changes)
                    .run(self._connection)
                )
        else:
            return False

    async def between(self, table, min=None, max=None, idx=""):
        """
        between
             Get all documents between two keys
        -----
        """
        if not table:
            raise EmptyStatement("Rethink: Table name is an empty string")

        if self._connection:
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
                    raise NoDataFound(message="Empty Result", code=404)
            except (ReqlRuntimeError, ReqlRuntimeError, ReqlError) as err:
                error = str(err)
                raise ProviderError(err)
                return False
            finally:
                return [self._result, error]
        else:
            return None

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
                    selected = hierarchy[get_index + 1 :]
                except (KeyError, IndexError):
                    selected = []
                try:
                    if self.qry_options["null_rolldown"] == "true":
                        # print('null all conditions below selection')
                        if selected:
                            for n in selected:
                                result = result.and_(self._engine.row[n].eq(None))
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
                                result = result.and_(self._engine.row[last].eq(None))
                except (KeyError, ValueError):
                    pass
                try:
                    if self.qry_options["select_child"] == "true":
                        try:
                            child = selected.pop(0)
                            result = result.and_(self._engine.row[child].ne(None))
                            # _where[child] = '!null'
                            for n in selected:
                                result = result.and_(self._engine.row[n].eq(None))
                            return result
                        except (ValueError, IndexError):
                            if get_filter:
                                pass
                            else:
                                child = hierarchy.pop(0)
                                result = result.and_(self._engine.row[child].ne(None))
                                # _where[child] = '!null'
                                for n in hierarchy:
                                    # _where[n] = 'null'
                                    result = result.and_(self._engine.row[n].eq(None))
                except (KeyError, ValueError):
                    pass
                try:
                    if self.qry_options["select_stores"] == "true":
                        try:
                            last = selected.pop()
                            result = result.and_(self._engine.row[last].ne(None))
                            return result
                        except (ValueError, IndexError):
                            last = hierarchy.pop()
                            result = result.and_(self._engine.row[last].ne(None))
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
                raise ReqlNonExistenceError("Empty Result: {}".format(str(err)))
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
        conditions.update((x, None) for (x, y) in conditions.items() if y == "null")
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

        conditions.update((x, None) for (x, y) in conditions.items() if y == "null")

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
                        result = result.and_(self._engine.row[key].between(10, 20))
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


"""
Registering this Provider
"""
registerProvider(rethink)

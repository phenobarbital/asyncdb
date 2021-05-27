import json
import traceback
import importlib
import logging
from asyncdb.providers import BaseProvider
from asyncdb.utils import colors, SafeDict, Msg
from asyncdb.utils.functions import _escapeString, _quoteString
from asyncdb.utils.models import Entity, Model
from asyncdb.utils.encoders import BaseEncoder
from asyncdb.exceptions import StatementError
from dataclasses import is_dataclass, asdict
import asyncpg

from typing import (
    Any,
    List,
    Dict,
    Generator,
    Iterable,
    Optional,
)

from asyncdb.utils import (
    SafeDict,
    _escapeString,
)


class baseCursor:
    """
    baseCursor.

    Iterable Object for Cursor-Like functionality
    """

    _cursor = None
    _connection = None
    _provider: BaseProvider = None
    _result: Any = None
    _sentence: str = ""

    def __init__(
        self,
        provider: BaseProvider,
        sentence: str,
        result: Optional[List] = None,
        parameters: Iterable[Any] = None,
    ):
        self._provider = provider
        self._result = result
        self._sentence = sentence
        self._params = parameters
        self._connection = self._provider.get_connection()

    async def __aenter__(self) -> "baseCursor":
        self._cursor = await self._connection.cursor()
        await self._cursor.execute(self._sentence, self._params)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return await self._provider.close()

    def __aiter__(self) -> "baseCursor":
        """The cursor is also an async iterator."""
        return self

    async def __anext__(self):
        """Use `cursor.fetchone()` to provide an async iterable."""
        row = await self._cursor.fetchone()
        if row is not None:
            return row
        else:
            raise StopAsyncIteration

    async def fetchone(self) -> Optional[Dict]:
        return await self._cursor.fetchone()

    async def fetchmany(self, size: int = None) -> Iterable[List]:
        return await self._cursor.fetchmany(size)

    async def fetchall(self) -> Iterable[List]:
        return await self._cursor.fetchall()


class SQLProvider(BaseProvider):
    """
    SQLProvider.

    Driver methods for SQL-based providers
    """

    _syntax = "sql"
    _test_query = "SELECT 1"
    _prepared = None
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"
    __cursor__ = None

    def __init__(self, dsn="", loop=None, params={}, **kwargs):
        try:
            # dynamic loading of Cursor Class
            cls = f"asyncdb.providers.{self._provider}"
            cursor = f"{self._provider}Cursor"
            module = importlib.import_module(cls, package="providers")
            self.__cursor__ = getattr(module, cursor)
        except ImportError as err:
            print("Error Loading Cursor Class: ", err)
            pass
        super(SQLProvider, self).__init__(dsn=dsn, loop=loop, params=params, **kwargs)

    """
    Context magic Methods
    """

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback, *args):
        self.terminate()
        pass

    async def __aenter__(self) -> "sqlite":
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        return await self.close(timeout=5)

    async def close(self, timeout=5):
        """
        Closing Method for SQL Connector
        """
        try:
            if self._connection:
                if self._cursor:
                    await self._cursor.close()
                await asyncio.wait_for(self._connection.close(), timeout=timeout)
        except Exception as err:
            raise ProviderError("{}: Closing Error: {}".format(__name__, str(err)))
        finally:
            self._connection = None
            self._connected = False
            return True

    async def connect(self, **kwargs):
        """
        Get a proxy connection, alias of connection
        """
        self._connection = None
        self._connected = False
        return await self.connection(self, **kwargs)

    async def release(self):
        """
        Release a Connection
        """
        await self.close()

    async def valid_operation(self, sentence: str):
        self._result = None
        if not sentence:
            raise EmptyStatement("Error: cannot sent an empty SQL sentence")
        if not self._connection:
            await self.connection()

    def cursor(self, sentence: str, parameters: Iterable[Any] = None) -> Iterable:
        """ Returns a iterable Cursor Object """
        if not sentence:
            raise EmptyStatement("Error: cannot sent an empty SQL sentence")
        if parameters is None:
            parameters = []
        try:
            return self.__cursor__(self, sentence=sentence, parameters=parameters)
        except Exception as err:
            print(err)
            return False

    """
    Meta-Operations
    """

    def table(self, table):
        try:
            return self._query_raw.format_map(SafeDict(table=table))
        except Exception as e:
            print(e)
            return False

    def fields(self, sentence, fields=None):
        _sql = False
        if not fields:
            _sql = sentence.format_map(SafeDict(fields="*"))
        elif type(fields) == str:
            _sql = sentence.format_map(SafeDict(fields=fields))
        elif type(fields) == list:
            _sql = sentence.format_map(SafeDict(fields=",".join(fields)))
        return _sql

    """
    where
      add WHERE conditions to SQL
    """

    def where(self, sentence, where):
        sql = ""
        if sentence:
            where_string = ""
            if not where:
                sql = sentence.format_map(SafeDict(where_cond=""))
            elif type(where) == dict:
                where_cond = []
                for key, value in where.items():
                    # print("KEY {}, VAL: {}".format(key, value))
                    if type(value) == str or type(value) == int:
                        if value == "null" or value == "NULL":
                            where_string.append("%s IS NULL" % (key))
                        elif value == "!null" or value == "!NULL":
                            where_string.append("%s IS NOT NULL" % (key))
                        elif key.endswith("!"):
                            where_cond.append("%s != %s" % (key[:-1], value))
                        else:
                            if (
                                type(value) == str
                                and value.startswith("'")
                                and value.endswith("'")
                            ):
                                where_cond.append("%s = %s" % (key, "{}".format(value)))
                            elif type(value) == int:
                                where_cond.append("%s = %s" % (key, "{}".format(value)))
                            else:
                                where_cond.append(
                                    "%s = %s" % (key, "'{}'".format(value))
                                )
                    elif type(value) == bool:
                        val = str(value)
                        where_cond.append("%s = %s" % (key, val))
                    else:
                        val = ",".join(map(str, value))
                        if type(val) == str and "'" not in val:
                            where_cond.append("%s IN (%s)" % (key, "'{}'".format(val)))
                        else:
                            where_cond.append("%s IN (%s)" % (key, val))
                # if 'WHERE ' in sentence:
                #    where_string = ' AND %s' % (' AND '.join(where_cond))
                # else:
                where_string = " WHERE %s" % (" AND ".join(where_cond))
                print("WHERE cond is %s" % where_string)
                sql = sentence.format_map(SafeDict(where_cond=where_string))
            elif type(where) == str:
                where_string = where
                if not where.startswith("WHERE"):
                    where_string = " WHERE %s" % where
                sql = sentence.format_map(SafeDict(where_cond=where_string))
            else:
                sql = sentence.format_map(SafeDict(where_cond=""))
            del where
            del where_string
            return sql
        else:
            return False

    def limit(self, sentence, limit=1):
        """
        LIMIT
          add limiting to SQL
        """
        if sentence:
            return "{q} LIMIT {limit}".format(q=sentence, limit=limit)
        return self

    def orderby(self, sentence, ordering=[]):
        """
        LIMIT
          add limiting to SQL
        """
        if sentence:
            if type(ordering) == str:
                return "{q} ORDER BY {ordering}".format(q=sentence, ordering=ordering)
            elif type(ordering) == list:
                return "{q} ORDER BY {ordering}".format(
                    q=sentence, ordering=", ".join(ordering)
                )
        return self

    def get_query(self, sentence):
        """
        get_query
          Get formmated query
        """
        sql = sentence
        try:
            # remove fields and where_cond
            sql = sentence.format_map(SafeDict(fields="*", where_cond=""))
            if not self.connected:
                self.connection()
            prepared, error = self._loop.run_until_complete(self.prepare(sql))
            if not error:
                self._columns = self.get_columns()
            else:
                return False
        except (ProviderError, StatementError) as err:
            return False
        except Exception as e:
            print(e)
            return False
        return sql

    def column_info(self, table):
        """
        column_info
          get column information about a table
        """
        discover = "SELECT attname AS column_name, atttypid::regtype AS data_type FROM pg_attribute WHERE attrelid = '{}'::regclass AND attnum > 0 AND NOT attisdropped ORDER  BY attnum".format(
            table
        )
        try:
            result, error = self._loop.run_until_complete(self.query(discover))
            if result:
                return result
        except (NoDataFound, ProviderError):
            print(err)
            return False
        except Exception as err:
            print(err)
            return False

    # def insert(self, table, data, **kwargs):
    #     """
    #     insert
    #        insert the result onto a table
    #     """
    #     sql = "INSERT INTO {table} ({fields}) VALUES ({values})"
    #     sql = sql.format_map(SafeDict(table=table))
    #     # set columns
    #     sql = sql.format_map(SafeDict(fields=",".join(data.keys())))
    #     values = ",".join(_escapeString(v) for v in data.values())
    #     sql = sql.format_map(SafeDict(values=values))
    #     # print(sql)
    #     try:
    #         result = self._loop.run_until_complete(
    #             self._connection.execute(sql)
    #         )
    #         if not result:
    #             print(result)
    #             return False
    #         else:
    #             return result
    #     except Exception as err:
    #         print(sql)
    #         print(err)
    #         return False

    # operations over Models:
    """
    The operations over models are:
    filter, get_one, get_any, get_all, create, update, delete, insert, remove
    """

    def _where(self, fields: list, **where):
        """
        TODO: add conditions for BETWEEN, NOT NULL, NULL, etc
        """
        result = ""
        if not where:
            return result
        elif type(where) == str:
            result = "WHERE {}".format(where)
        elif type(where) == dict:
            where_cond = []
            for key, value in where.items():
                # print('HERE> ', fields)
                f = fields[key]
                datatype = f.type
                if value is None or value == "null" or value == "NULL":
                    where_cond.append(f"{key} is NULL")
                elif value == "!null" or value == "!NULL":
                    where_cond.append(f"{key} is NOT NULL")
                elif type(value) == bool:
                    val = str(value)
                    where_cond.append(f"{key} is {value}")
                elif isinstance(datatype, List):
                    val = ", ".join(
                        map(str, [Entity.escapeLiteral(v, type(v)) for v in value])
                    )
                    where_cond.append(f"ARRAY[{val}]<@ {key}::character varying[]")
                elif Entity.is_array(datatype):
                    val = ", ".join(
                        map(str, [Entity.escapeLiteral(v, type(v)) for v in value])
                    )
                    where_cond.append(f"{key} IN ({val})")
                else:
                    # is an scalar value
                    val = Entity.escapeLiteral(value, datatype)
                    where_cond.append(f"{key}={val}")
            result = "\nWHERE %s" % (" AND ".join(where_cond))
            return result
        else:
            return result

    async def delete(self, model: Model, fields: list = [], **kwargs):
        """
        Deleting a row Model based on Primary Key.
        """
        if not self._connection:
            await self.connection()
        result = None
        tablename = f"{model.Meta.schema}.{model.Meta.name}"
        source = []
        pk = {}
        cols = []
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            value = Entity.toSQL(getattr(model, field.name), datatype)
            if field.primary_key is True:
                pk[column] = value
        # TODO: work in an "update, delete, insert" functions on asyncdb to abstract data-insertion
        sql = "DELETE FROM {table} {condition}"
        condition = self._where(fields, **pk)
        sql = sql.format_map(SafeDict(table=tablename))
        sql = sql.format_map(SafeDict(condition=condition))
        try:
            result = await self._connection.execute(sql)
            # DELETE 1
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Delete over table {}: {}".format(model.Meta.name, err)
            )
        return result

    async def insert(self, model: Model, fields: list = [], **kwargs):
        """
        Inserting new object onto database.
        """
        # TODO: option for returning more fields than PK in returning
        if not self._connection:
            await self.connection()
        table = "{schema}.{table}".format(
            table=model.Meta.name, schema=model.Meta.schema
        )
        cols = []
        source = []
        pk = []
        n = 1
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            dbtype = field.get_dbtype()
            val = getattr(model, field.name)
            if is_dataclass(datatype):
                value = json.loads(json.dumps(asdict(val), cls=BaseEncoder))
            else:
                value = val
            if field.required is False and value is None or value == "None":
                default = field.default
                if callable(default):
                    value = default()
                else:
                    continue
            source.append(value)
            cols.append(column)
            n += 1
            if field.primary_key is True:
                pk.append(column)
        try:
            primary = "RETURNING {}".format(",".join(pk)) if pk else ""
            columns = ",".join(cols)
            values = ",".join(["${}".format(a) for a in range(1, n)])
            insert = f"INSERT INTO {table} ({columns}) VALUES({values}) {primary}"
            logging.debug(f"INSERT: {insert}")
            stmt = await self._connection.prepare(insert)
            result = await stmt.fetchrow(*source, timeout=2)
            logging.debug(stmt.get_statusmsg())
            if result:
                # setting the values dynamically from returning
                for f in pk:
                    setattr(model, f, result[f])
                return result
        except asyncpg.exceptions.UniqueViolationError as err:
            raise StatementError("Constraint Error: {}".format(err))
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(model.Meta.name, err)
            )

    async def save(self, model: Model, fields: list = [], **kwargs):
        """
        Updating a Model object based on primary Key or conditions
        TODO: check if row doesnt exists, then, insert
        """
        if not self._connection:
            await self.connection()
        table = f"{model.Meta.schema}.{model.Meta.name}"
        source = []
        pk = {}
        values = []
        n = 1
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            # try:
            #     dbtype = field.get_dbtype()
            # except AttributeError:
            #     dbtype = ''
            value = getattr(model, field.name)
            source.append("{} = {}".format(name, "${}".format(n)))
            values.append(value)
            n += 1
            try:
                if field.primary_key is True:
                    pk[column] = value
            except AttributeError:
                pass
        # TODO: work in an "update, delete, insert" functions on asyncdb to abstract data-insertion
        sql = "UPDATE {table} SET {set_fields} {condition}"
        condition = self._where(fields, **pk)
        sql = sql.format_map(SafeDict(table=table))
        sql = sql.format_map(SafeDict(condition=condition))
        # set the columns
        sql = sql.format_map(SafeDict(set_fields=", ".join(source)))
        print(sql)
        try:
            logging.debug(sql)
            stmt = await self._connection.prepare(sql)
            result = await stmt.fetchrow(*values, timeout=2)
            logging.debug(stmt.get_statusmsg())
            # result = await self._connection.fetchrow(sql)
            return result
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(model.Meta.name, err)
            )

    async def get_all(self, model: Model, **kwargs):
        """
        Get all records on database
        """
        if not self._connection:
            await self.connection()
        table = f"{model.Meta.schema}.{model.Meta.name}"
        sql = f"SELECT * FROM {table}"
        try:
            prepared, error = await self.prepare(sql)
            result = await self._connection.fetch(sql)
            return result
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(model.Meta.name, err)
            )

    async def get_one(self, model: Model, **kwargs):
        # if not self._connection:
        #     await self.connection()
        table = f"{model.Meta.schema}.{model.Meta.name}"
        pk = {}
        cols = []
        source = []
        fields = model.columns(model)
        # print(fields)
        for name, field in fields.items():
            val = getattr(model, field.name)
            # print(name, field, val)
            column = field.name
            datatype = field.type
            value = Entity.toSQL(val, datatype)
            cols.append(column)
            try:
                if field.primary_key is True:
                    pk[column] = value
            except AttributeError:
                pass
        columns = ", ".join(cols)
        condition = self._where(fields, **kwargs)
        sql = f"SELECT {columns} FROM {table} {condition}"
        try:
            return await self._connection.fetchrow(sql)
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Get One over table {}: {}".format(model.Meta.name, err)
            )

    async def fetch_one(self, model: Model, fields: list = [], **kwargs):
        # if not self._connection:
        #     await self.connection()
        table = f"{model.Meta.schema}.{model.Meta.name}"
        pk = {}
        cols = []
        source = {}
        for name, field in fields.items():
            value = getattr(model, field.name)
            column = field.name
            cols.append(column)
            try:
                if field.primary_key is True:
                    pk[column] = value
            except AttributeError:
                pass
        columns = ", ".join(cols)
        arguments = {**pk, **kwargs}
        condition = self._where(fields=fields, **arguments)
        # migration of where to prepared statement
        sql = f"SELECT {columns} FROM {table} {condition}"
        try:
            return await self._connection.fetchrow(sql)
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Get One over table {}: {}".format(model.Meta.name, err)
            )

    async def select(self, model: Model, fields: list = [], **kwargs):
        if not self._connection:
            await self.connection()
        pk = {}
        cols = []
        source = {}
        # print('HERE: ', model.__dict__)
        table = f"{model.Meta.schema}.{model.Meta.name}"
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            value = getattr(model, field.name)
            cols.append(column)
            if value is not None:
                source[column] = Entity.toSQL(value, datatype)
            # if field.primary_key is True:
            #     pk[column] = value
        columns = ", ".join(cols)
        arguments = {**source, **kwargs}
        # print(arguments)
        condition = self._where(fields=fields, **arguments)
        sql = f"SELECT {columns} FROM {table} {condition}"
        print(sql)
        try:
            return await self._connection.fetch(sql)
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(model.Meta.name, err)
            )

    async def filter(self, model: Model, **kwargs):
        if not self._connection:
            await self.connection()
        pk = {}
        cols = []
        fields = model.columns(model)
        table = f"{model.Meta.schema}.{model.Meta.name}"
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            cols.append(column)
            try:
                if field.primary_key is True:
                    pk[column] = Entity.toSQL(getattr(model, field.name), datatype)
            except AttributeError:
                pass
        columns = ", ".join(cols)
        condition = self._where(fields, **kwargs)
        sql = f"SELECT {columns} FROM {table} {condition}"
        logging.debug(sql)
        try:
            return await self._connection.fetch(sql)
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(model.Meta.name, err)
            )

    async def update_rows(self, model: Model, conditions: dict, **kwargs):
        """
        Updating some records and returned.
        """
        if not self._connection:
            await self.connection()
        table = f"{model.Meta.schema}.{model.Meta.name}"
        source = []
        pk = {}
        cols = []
        fields = model.columns(model)
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            cols.append(column)
            if column in kwargs:
                value = Entity.toSQL(kwargs[column], datatype)
                source.append(
                    "{} = {}".format(column, Entity.escapeLiteral(value, datatype))
                )
        set_fields = ", ".join(source)
        condition = self._where(fields, **conditions)
        columns = ", ".join(cols)
        sql = f"UPDATE {table} SET {set_fields} {condition}"
        logging.debug(sql)
        try:
            result = await self._connection.execute(sql)
            if result:
                sql = f"SELECT {columns} FROM {table} {condition}"
                print("UPDATE ", sql)
                return await self._connection.fetch(sql)
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(model.Meta.name, err)
            )

    async def delete_rows(self, model: Model, conditions: dict, **kwargs):
        """
        Deleting some records and returned.
        """
        if not self._connection:
            await self.connection()
        table = f"{model.Meta.schema}.{model.Meta.name}"
        source = []
        pk = {}
        cols = []
        fields = model.columns(model)
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            cols.append(column)
            if column in kwargs:
                value = Entity.toSQL(kwargs[column], datatype)
                source.append(
                    "{} = {}".format(column, Entity.escapeLiteral(value, datatype))
                )
        set_fields = ", ".join(source)
        condition = self._where(fields, **conditions)
        columns = ", ".join(cols)
        sql = f"DELETE FROM {table} {condition}"
        logging.debug(sql)
        try:
            result = await self._connection.execute(sql)
            if result:
                return result
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Deleting table {}: {}".format(model.Meta.name, err)
            )

    async def create_rows(self, model: Model, rows: list):
        """
        Create all records based on rows and returned
        """
        if not self._connection:
            await self.connection()
        table = f"{model.Meta.schema}.{model.Meta.name}"
        fields = model.columns(model)
        results = []
        stmt = None
        for row in rows:
            source = []
            pk = []
            cols = []
            for col, field in fields.items():
                print("HERE ", col, field)
                if col not in row:
                    # field doesnt exists
                    default = field.default
                    if default is not None:
                        if callable(default):
                            source.append(default())
                        else:
                            source.append(default)
                        cols.append(col)
                    else:
                        # val = getattr(model, col)
                        # if val is not None:
                        #     source.append(val)
                        # elif field.required is True or field.primary_key is True:
                        if field.required is True:
                            raise StatementError(f"Missing Required Field: {col}")
                else:
                    try:
                        val = row[col]
                        source.append(val)
                        cols.append(col)
                    except (KeyError, TypeError):
                        continue
                try:
                    if field.primary_key is True:
                        pk.append(col)
                except AttributeError:
                    pass
            if not stmt:
                columns = ", ".join(cols)
                n = len(cols)
                values = ",".join(["${}".format(a) for a in range(1, n + 1)])
                primary = "RETURNING *"
                insert = f"INSERT INTO {table} ({columns}) VALUES ({values}) {primary}"
                logging.debug(f"INSERT: {insert}")
                try:
                    stmt = await self._connection.prepare(insert)
                except Exception as err:
                    print(traceback.format_exc())
                    raise Exception(
                        "Exception creating Prepared Sentence {}: {}".format(
                            model.Meta.name, err
                        )
                    )
            try:
                result = await stmt.fetchrow(*source, timeout=2)
                logging.debug(stmt.get_statusmsg())
                if result:
                    results.append(result)
            except asyncpg.exceptions.UniqueViolationError as err:
                raise StatementError("Constraint Error: {}".format(err))
            except Exception as err:
                print(traceback.format_exc())
                raise Exception("Error Bulk Insert {}: {}".format(table, err))
        else:
            return results

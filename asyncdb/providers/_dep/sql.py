import json
import traceback
import importlib
import logging
import asyncio
from abc import (
    ABC,
    abstractmethod,
)
from asyncdb.providers import BaseProvider
from asyncdb.interfaces import ModelBackend

from asyncdb.utils.functions import (
    SafeDict
)
from asyncdb.utils.models import Entity, Model
from asyncdb.utils.encoders import BaseEncoder
from asyncdb.exceptions import (
    EmptyStatement,
    NoDataFound,
    ProviderError,
    StatementError,
)
from dataclasses import is_dataclass, asdict
import asyncpg

from typing import (
    Any,
    List,
    Dict,
    Iterable,
    Optional,
)


class baseCursor(ABC):
    """
    baseCursor.

    Iterable Object for Cursor-Like functionality
    """

    _provider: BaseProvider

    def __init__(
        self,
        provider: BaseProvider,
        sentence: str,
        result: Optional[List] = None,
        parameters: Iterable[Any] = None,
    ):
        # self._cursor = None
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

    @abstractmethod
    async def execute(sentence: Any, params: dict) -> Any:
        pass

    async def fetchone(self) -> Optional[Dict]:
        return await self._cursor.fetchone()

    async def fetchmany(self, size: int = None) -> Iterable[List]:
        return await self._cursor.fetchmany(size)

    async def fetchall(self) -> Iterable[List]:
        return await self._cursor.fetchall()


class SQLProvider(BaseProvider, ModelBackend):
    """
    SQLProvider.

    Driver methods for SQL-based providers
    """
    _syntax = "sql"
    _test_query = "SELECT 1"

    def __init__(self, dsn: str = "", loop=None, params={}, **kwargs):
        self._query_raw = "SELECT {fields} FROM {table} {where_cond}"
        self._prepared = None
        try:
            # dynamic loading of Cursor Class
            cls = f"asyncdb.providers.{self._provider}"
            cursor = f"{self._provider}Cursor"
            module = importlib.import_module(cls, package="providers")
            self.__cursor__ = getattr(module, cursor)
        except ImportError as err:
            logging.exception(f"Error Loading Cursor Class: {err}")
            pass
        super(SQLProvider, self).__init__(
            dsn=dsn, loop=loop, params=params, **kwargs
        )
        BaseProvider.__init__(self, dsn, loop, params, **kwargs)

    """
    Context magic Methods
    """

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback, *args):
        self.terminate()
        pass

    async def __aenter__(self) -> Any:
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        try:
            return await self.close(timeout=5)
        except Exception as err:
            self._logger.exception(err)

    async def close(self, timeout=5):
        """
        Closing Method for SQL Connector
        """
        try:
            if self._connection:
                if self._cursor:
                    await self._cursor.close()
                await asyncio.wait_for(
                    self._connection.close(), timeout=timeout
                )
        except Exception as err:
            raise ProviderError(
                "{}: Closing Error: {}".format(__name__, str(err)))
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

    async def valid_operation(self, sentence: Any):
        self._result = None
        if not sentence:
            raise EmptyStatement(
                "Error: cannot sent an empty SQL sentence"
            )
        if not self._connection:
            await self.connection()

    def cursor(
                self,
                sentence: str,
                parameters: Iterable[Any] = None
            ) -> Iterable:
        """ Returns a iterable Cursor Object """
        if not sentence:
            raise EmptyStatement(
                "SQL Error: Cannot sent an empty SQL sentence"
            )
        if parameters is None:
            parameters = []
        try:
            return self.__cursor__(
                self,
                sentence=sentence,
                parameters=parameters
            )
        except Exception as err:
            print(err)
            return []

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
                            where_cond.append("%s = %s" %
                                              (key, "{}".format(value)))
                        elif type(value) == int:
                            where_cond.append("%s = %s" %
                                              (key, "{}".format(value)))
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
                        where_cond.append("%s IN (%s)" %
                                          (key, "'{}'".format(val)))
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

    def limit(self, sentence, limit: int = 1):
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
                return "{q} ORDER BY {ordering}".format(
                    q=sentence, ordering=ordering
                )
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
            logging.exception(err)
            return False
        except Exception as err:
            logging.exception(err)
            return False
        return sql

    async def column_info(self, table):
        """
        column_info
          get column information about a table
          TODO: rewrite column info using information schema.
        """
        pass

    # operations over Models:
    """
    The operations over models are:
    filter, get_one, get_any, get_all, create, update, delete, insert, remove
    """

    """
    Relative to Class-based Methods:
    """

    def _where(self, fields: Dict, **where):
        """
        TODO: add conditions for BETWEEN, NOT NULL, NULL, etc
        Re-think functionality for parsing where conditions.
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
                        map(str, [Entity.escapeLiteral(v, type(v))
                            for v in value])
                    )
                    where_cond.append(
                        f"ARRAY[{val}]<@ {key}::character varying[]")
                elif Entity.is_array(datatype):
                    val = ", ".join(
                        map(str, [Entity.escapeLiteral(v, type(v))
                            for v in value])
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

    async def mdl_update(self, model: Model, conditions: dict, **kwargs):
        """
        Updating some records and returned.
        """
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        source = []
        cols = []
        fields = model.columns(model)
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            cols.append(column)
            if column in kwargs:
                value = Entity.toSQL(kwargs[column], datatype)
                source.append(
                    "{} = {}".format(
                        column, Entity.escapeLiteral(value, datatype))
                )
        set_fields = ", ".join(source)
        condition = self._where(fields, **conditions)
        columns = ", ".join(cols)
        sql = f"UPDATE {table} SET {set_fields} {condition}"
        logging.debug(f'UPDATE SQL: {sql}')
        try:
            result = await self._connection.execute(sql)
            if result:
                sql = f"SELECT {columns} FROM {table} {condition}"
                return await self._connection.fetch(sql)
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(
                    model.Meta.name, err)
            )

    async def mdl_create(self, model: Model, rows: list):
        """
        Create all records based on a dataset and return result.
        TODO: migrating from asyncpg prepared.
        """
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
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
                    # field doesn't exists
                    default = field.default
                    if default is not None:
                        if callable(default):
                            # TODO: support for parameters
                            source.append(default())
                        else:
                            source.append(default)
                        cols.append(col)
                    else:
                        if field.required is True:
                            raise StatementError(
                                f"Missing Required Field: {col}"
                            )
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

    async def mdl_delete(self, model: Model, conditions: dict, **kwargs):
        """
        Deleting some records and returned.
        """
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        source = []
        cols = []
        fields = model.columns(model)
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            cols.append(column)
            if column in kwargs:
                value = Entity.toSQL(kwargs[column], datatype)
                source.append(
                    "{} = {}".format(
                        column, Entity.escapeLiteral(value, datatype))
                )
        condition = self._where(fields, **conditions)
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

    async def mdl_filter(self, model: Model, **kwargs):
        """
        Filtering.
        """
        pk = {}
        cols = []
        fields = model.columns(model)
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            cols.append(column)
            try:
                if field.primary_key is True:
                    pk[column] = Entity.toSQL(
                        getattr(model, field.name), datatype)
            except AttributeError:
                pass
        columns = ", ".join(cols)
        condition = self._where(fields, **kwargs)
        sql = f"SELECT {columns} FROM {table} {condition}"
        logging.debug(sql)
        try:
            return await self._connection.fetch(sql)
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(
                    model.Meta.name, err)
            )

    async def mdl_all(self, model: Model, **kwargs):
        """
        Get all records on database
        """
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        sql = f"SELECT * FROM {table}"
        try:
            prepared, error = await self.prepare(sql)
            result = await self._connection.fetch(sql)
            return result
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(
                    model.Meta.name, err)
            )

    async def mdl_get(self, model: Model, **kwargs):
        """
        Get a single record.
        """
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        pk = {}
        cols = []
        fields = model.columns(model)
        for name, field in fields.items():
            val = getattr(model, field.name)
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
            logging.debug(traceback.format_exc())
            raise Exception(
                "Error on Get One over table {}: {}".format(
                    model.Meta.name, err)
            )

    """
    Instance-based Dataclass Methods.
    """
    async def model_save(self, model: Model, fields: Dict = {}, **kwargs):
        """
        Updating a Model object based on primary Key or conditions
        TODO: check if row doesnt exists, then, insert
        """
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        source = []
        pk = {}
        values = []
        n = 1
        for name, field in fields.items():
            column = field.name
            # datatype = field.type
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
        # TODO: work in an "update, delete, insert" functions on asyncdb to
        #  abstract data-insertion
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
            logging.debug(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(
                    model.Meta.name, err)
            )

    async def model_select(self, model: Model, fields: Dict = {}, **kwargs):
        cols = []
        source = {}
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
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
        condition = self._where(fields=fields, **arguments)
        sql = f"SELECT {columns} FROM {table} {condition}"
        logging.debug(sql)
        try:
            return await self._connection.fetch(sql)
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(
                    model.Meta.name, err)
            )

    async def model_all(self, model: Model, fields: Dict = {}):
        cols = []
        source = {}
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            value = getattr(model, field.name)
            cols.append(column)
        columns = ", ".join(cols)
        sql = f"SELECT {columns} FROM {table}"
        logging.debug(sql)
        try:
            return await self._connection.fetch(sql)
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                "Error on Insert over table {}: {}".format(
                    model.Meta.name, err)
            )

    async def model_get(self, model: Model, fields: Dict = {}, **kwargs):
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        pk = {}
        cols = []
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
            logging.debug(traceback.format_exc())
            raise Exception(
                "Error on Get One over table {}: {}".format(
                    model.Meta.name, err)
            )

    async def model_delete(self, model: Model, fields: Dict = {}, **kwargs):
        """
        Deleting a row Model based on Primary Key.
        """
        result = None
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        pk = {}
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            value = Entity.toSQL(getattr(model, field.name), datatype)
            if field.primary_key is True:
                pk[column] = value
        # TODO: work in an "update, delete, insert" functions on
        # asyncdb to abstract data-insertion
        sql = "DELETE FROM {table} {condition}"
        condition = self._where(fields, **pk)
        sql = sql.format_map(SafeDict(table=tablename))
        sql = sql.format_map(SafeDict(condition=condition))
        try:
            result = await self._connection.execute(sql)
            # DELETE 1
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                "Error on Delete over table {}: {}".format(
                    model.Meta.name, err)
            )
        return result

    async def model_insert(self, model: Model, fields: Dict = {}, **kwargs):
        """
        Inserting new object onto database.
        """
        # TODO: option for returning more fields than PK in returning
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        cols = []
        source = []
        pk = []
        n = 1
        for name, field in fields.items():
            column = field.name
            datatype = field.type
            # dbtype = field.get_dbtype()
            val = getattr(model, field.name)
            if is_dataclass(datatype):
                value = json.loads(
                    json.dumps(asdict(val), cls=BaseEncoder)
                )
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
                "Error on Insert over table {}: {}".format(
                    model.Meta.name, err)
            )

"""
SQLProvider.

Abstract class covering all major functionalities for Relational SQL-based databases.
"""
import asyncio
import logging
import traceback
import asyncpg
import json
from asyncdb.utils.functions import (
    SafeDict
)
from asyncdb.utils.types import Entity
from asyncdb.utils.encoders import BaseEncoder
from asyncdb.exceptions import StatementError, ProviderError, EmptyStatement
from dataclasses import is_dataclass, asdict
from typing import (
    Any,
    List,
    Dict,
)
from .base import BaseDBProvider, ModelBackend, BaseCursor
from asyncdb.models import Model


class SQLCursor(BaseCursor):
    _connection = None

    async def __aenter__(self) -> "BaseCursor":
        # if not self._connection:
        #     await self.connection()
        self._cursor = await self._connection.cursor(
            self._sentence, self._params
        )
        return self


class SQLProvider(BaseDBProvider, ModelBackend):
    """SQLProvider.

    Driver for SQL-based providers.
    """
    _syntax = "sql"
    _test_query = "SELECT 1"

    def __init__(self, dsn: str = "", loop=None, params: dict = None, **kwargs):
        self._query_raw = "SELECT {fields} FROM {table} {where_cond}"
        super(SQLProvider, self).__init__(
            dsn=dsn, loop=loop, params=params, **kwargs
        )

    async def close(self, timeout: int = 5):
        """
        Closing Method for any SQL Connector.
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
                message=f"{__name__!s}: Closing Error: {err!s}"
            )
        finally:
            self._connection = None
            self._connected = False
            return True

    # alias for connection
    disconnect = close

    async def valid_operation(self, sentence: Any):
        """
        Returns if is a valid operation.
        TODO: add some validations.
        """
        if not sentence:
            raise EmptyStatement(
                f"{__name__!s} Error: cannot use an empty SQL sentence"
            )
        if not self._connection:
            await self.connection()

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
            return "WHERE {}".format(where)
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

    async def mdl_update(self, model: "Model", conditions: dict, **kwargs):
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
            raise ProviderError(
                message="Error on Insert over table {}: {}".format(
                    model.Meta.name, err)
            )

    async def mdl_create(self, model: "Model", rows: list):
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
                                message=f"Missing Required Field: {col}"
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
                raise StatementError(message="Constraint Error: {}".format(err))
            except Exception as err:
                print(traceback.format_exc())
                raise ProviderError(message="Error Bulk Insert {}: {}".format(table, err))
        return results

    async def mdl_delete(self, model: "Model", conditions: Dict, **kwargs):
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
            raise ProviderError(
                message="Error on Deleting table {}: {}".format(model.Meta.name, err)
            )

    async def mdl_filter(self, model: "Model", **kwargs):
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
            raise ProviderError(
                message="Error FILTER: over table {}: {}".format(
                    model.Meta.name, err)
            )

    async def mdl_all(self, model: "Model", **kwargs):
        """
        Get all records on database
        """
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        if 'fields' in kwargs:
            columns = ','.join(kwargs['fields'])
        else:
            columns = '*'
        sql = f"SELECT {columns} FROM {table}"
        try:
            # prepared, error = await self.prepare(sql)
            result = await self._connection.fetch(sql)
            return result
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise ProviderError(
                f"Error: Model All over {model.Meta.name}: {err}"
            )

    async def mdl_get(self, model: "Model", **kwargs):
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
            raise ProviderError(
                f"Error on Get One over table {model.Meta.name}: {err}"
            )

    """
    Instance-based Dataclass Methods.
    """
    async def model_save(self, model: "Model", fields: Dict = None, **kwargs):
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
            raise ProviderError(
                f"Error on UPDATE over table {model.Meta.name}: {err}"
            )

    async def model_select(self, model: "Model", fields: Dict = None, **kwargs):
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
            raise ProviderError(
                message="Error on SELECT over {}: {}".format(
                    model.Meta.name, err)
            )

    async def model_all(self, model: "Model", fields: Dict = None):
        cols = []
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        if fields:
            for name, field in fields.items():
                column = field.name
                datatype = field.type
                value = getattr(model, field.name)
                cols.append(column)
            columns = ", ".join(cols)
        else:
            columns = "*"
        sql = f"SELECT {columns} FROM {table}"
        logging.debug(sql)
        try:
            return await self._connection.fetch(sql)
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise ProviderError(
                f"Error on SELECT ALL over {model.Meta.name}: {err}"
            )

    async def model_get(self, model: "Model", fields: Dict = None, **kwargs):
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        pk = {}
        cols = []
        if fields:
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
        else:
            columns = "*"
        arguments = {**pk, **kwargs}
        condition = self._where(fields=fields, **arguments)
        # migration of where to prepared statement
        sql = f"SELECT {columns} FROM {table} {condition}"
        try:
            return await self._connection.fetchrow(sql)
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise ProviderError(
                f"Error on Get One over table {model.Meta.name}: {err}"
            )

    async def model_delete(
                self,
                model: "Model",
                fields: Dict,
                connection: Any = None,
                **kwargs
            ):
        """
        Deleting a row Model based on Primary Key.
        """
        result = None
        try:
            table = f"{model.Meta.schema}.{model.Meta.name}"
        except Exception:
            table = model.__name__
        pk = {}
        for _, field in fields.items():
            column = field.name
            datatype = field.type
            value = Entity.toSQL(getattr(model, field.name), datatype)
            if field.primary_key is True:
                pk[column] = value
        # TODO: work in an "update, delete, insert" functions on
        # asyncdb to abstract data-insertion
        sql = "DELETE FROM {table} {condition}"
        condition = self._where(fields, **pk)
        sql = sql.format_map(SafeDict(table=table))
        sql = sql.format_map(SafeDict(condition=condition))
        try:
            result, error = await connection.execute(sql)
            if error:
                logging.error(error)
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise ProviderError(
                f"Error on Delete over table {model.Meta.name}: {err}"
            )
        return result

    async def model_insert(
                self,
                model: "Model",
                fields: Dict,
                connection: Any = None,
                **kwargs
            ):
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
        for _, field in fields.items():
            column = field.name
            datatype = field.type
            # dbtype = field.get_dbtype()
            val = getattr(model, field.name)
            if is_dataclass(datatype) and val is not None:
                if isinstance(val, list):
                    value = json.loads(
                        json.dumps([asdict(d) for d in val], cls=BaseEncoder)
                    )
                else:
                    value = json.loads(
                        json.dumps(asdict(val), cls=BaseEncoder)
                    )
            else:
                value = val
            required = field.required()
            if required is False and value is None or value == "None":
                default = field.default
                if callable(default):
                    value = default()
                else:
                    continue
            elif required is True and value is None or value == "None":
                if 'db_default' in field.metadata:
                    # field get a default value from database
                    continue
                else:
                    raise ProviderError(
                        f"Field {column} required and value is null over {model.Meta.name}"
                    )
            source.append(value)
            cols.append(column)
            n += 1
            if field.primary_key is True:
                pk.append(column)
        try:
            # primary = "RETURNING {}".format(",".join(pk)) if pk else ""
            primary = "RETURNING *"
            columns = ",".join(cols)
            values = ",".join(["${}".format(a) for a in range(1, n)])
            insert = f"INSERT INTO {table} ({columns}) VALUES({values}) {primary}"
            # print(insert)
            logging.debug(f"INSERT: {insert}")
            stmt, error = await connection.prepare(insert)
            if error:
                logging.error(error)
            result = await stmt.fetchrow(*source, timeout=2)
            logging.debug(stmt.get_statusmsg())
            if result:
                # setting the values dynamically from returning
                for f, val in result.items():
                    setattr(model, f, val)
                return model
        except asyncpg.exceptions.UniqueViolationError as err:
            raise StatementError(
                traceback.format_exc(),
                message=f"Constraint Error: {err!r}",
            )
        except Exception as err:
            raise ProviderError(
                traceback.format_exc(),
                message=f"Error on Insert over table {model.Meta.name}: {err!s}"
            )

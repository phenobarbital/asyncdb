#!/usr/bin/env python3
import time
import asyncio
from typing import (
    Any,
    Optional,
    Union
)
from collections.abc import Sequence, Iterable
import aiosqlite
from asyncdb.exceptions import (
    NoDataFound,
    ProviderError
)
from asyncdb.interfaces import DBCursorBackend, ModelBackend
from asyncdb.models import Model
from asyncdb.utils.types import Entity
from .sql import SQLDriver, SQLCursor



class sqliteCursor(SQLCursor):
    """
    Cursor Object for SQLite.
    """
    _provider: "sqlite"
    _connection: aiosqlite.Connection = None

    async def __aenter__(self) -> "sqliteCursor":
        self._cursor = await self._connection.execute(
            self._sentence, self._params
        )
        return self


class sqlite(SQLDriver, DBCursorBackend, ModelBackend):
    _provider: str = 'sqlite'
    _syntax: str = 'sql'
    _dsn: str = "{database}"

    def __init__(
            self,
            dsn: str = "",
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        SQLDriver.__init__(self, dsn, loop, params, **kwargs)
        DBCursorBackend.__init__(self)

    async def prepare(self, sentence: Union[str, list]) -> Any:
        "Ignoring prepared sentences on SQLite"
        raise NotImplementedError()  # pragma: no cover

    async def __aenter__(self) -> Any:
        if not self._connection:
            await self.connection()
        return self

    async def connection(self, **kwargs):
        """
        Get a connection
        """
        self._connection = None
        self._connected = False
        try:
            self._connection = await aiosqlite.connect(
                database=self._dsn, **kwargs
            )
            if self._connection:
                if self._init_func is not None and callable(self._init_func):
                    try:
                        await self._init_func( # pylint: disable=E1102
                            self._connection
                        )
                    except RuntimeError as err:
                        self._logger.exception(
                            f"Error on Init Connection: {err!s}"
                        )
                self._connected = True
                self._initialized_on = time.time()
            return self
        except aiosqlite.OperationalError as e:
            raise ProviderError(
                f"Unable to Open Database: {self._dsn}, {e}"
            ) from e
        except aiosqlite.DatabaseError as e:
            raise ProviderError(
                f"Database Connection Error: {e!s}"
            ) from e
        except aiosqlite.Error as e:
            raise ProviderError(
                f"SQLite Internal Error: {e!s}"
            ) from e
        except Exception as e:
            self._logger.exception(e, stack_info=True)
            raise ProviderError(
                f"SQLite Unknown Error: {e!s}"
            ) from e

    connect = connection

    async def valid_operation(self, sentence: Any):
        await super(sqlite, self).valid_operation(sentence)
        if self._row_format == 'iterable':
            # converting to a dictionary
            self._connection.row_factory = lambda c, r: dict(
                zip([col[0] for col in c.description], r)
            )
        else:
            self._connection.row_factory = None

    async def query(self, sentence: Any, **kwargs) -> Any:
        """
        Getting a Query from Database
        """
        error = None
        cursor = None
        await self.valid_operation(sentence)
        try:
            cursor = await self._connection.execute(sentence, parameters=kwargs)
            self._result = await cursor.fetchall()
            if not self._result:
                return (None, NoDataFound())
        except Exception as err:
            error = f"SQLite Error on Query: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            try:
                await cursor.close()
            except (ValueError, TypeError, RuntimeError) as err:
                self._logger.exception(err)
            return await self._serializer(self._result, error)

    async def queryrow(self, sentence: Any = None) -> Iterable[Any]:
        """
        Getting a single Row from Database
        """
        error = None
        cursor = None
        await self.valid_operation(sentence)
        try:
            self._connection.row_factory = lambda c, r: dict(
                zip([col[0] for col in c.description], r)
            )
            cursor = await self._connection.execute(sentence)
            self._result = await cursor.fetchone()
            if not self._result:
                return (None, NoDataFound())
        except Exception as e:
            error = f"Error on Query: {e}"
            raise ProviderError(
                message=error
            ) from e
        finally:
            try:
                await cursor.close()
            except (ValueError, TypeError, RuntimeError) as err:
                self._logger.exception(err)
            return await self._serializer(self._result, error)

    async def fetch_all(self, sentence: str, **kwargs) -> Sequence:
        """
        Alias for Query, but without error Support.
        """
        cursor = None
        await self.valid_operation(sentence)
        try:
            cursor = await self._connection.execute(sentence, parameters=kwargs)
            self._result = await cursor.fetchall()
            if not self._result:
                raise NoDataFound(
                    "SQLite Fetch All: Data Not Found"
                )
        except Exception as e:
            error = f"Error on Fetch: {e}"
            raise ProviderError(
                message=error
            ) from e
        finally:
            try:
                await cursor.close()
            except (ValueError, TypeError, RuntimeError) as err:
                self._logger.exception(err)
            return self._result

    # alias to be compatible with aiosqlite methods.
    fetchall = fetch_all

    async def fetch_many(self, sentence: str, size: int = None):
        """
        Aliases for query, without error support
        """
        await self.valid_operation(sentence)
        cursor = None
        try:
            cursor = await self._connection.execute(sentence)
            self._result = await cursor.fetchmany(size)
            if not self._result:
                raise NoDataFound()
        except Exception as err:
            error = "Error on Query: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            try:
                await cursor.close()
            except (ValueError, TypeError, RuntimeError) as err:
                self._logger.exception(err)
            return self._result

    fetchmany = fetch_many

    async def fetch_one(
            self,
            sentence: str,
            **kwargs
    ) -> Optional[dict]:
        """
        aliases for queryrow, but without error support
        """
        await self.valid_operation(sentence)
        cursor = None
        try:
            cursor = await self._connection.execute(sentence, **kwargs)
            self._result = await cursor.fetchone()
            if not self._result:
                raise NoDataFound()
        except Exception as err:
            error = "Error on Query: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            try:
                await cursor.close()
            except (ValueError, TypeError, RuntimeError) as err:
                self._logger.exception(err)
            return self._result

    fetchone = fetch_one
    fetchrow = fetch_one

    async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]:
        """Execute a transaction
        get a SQL sentence and execute
        returns: results of the execution
        """
        error = None
        result = None
        await self.valid_operation(sentence)
        try:
            result = await self._connection.execute(sentence, parameters=kwargs)
            if result:
                await self._connection.commit()
        except Exception as err:
            error = "Error on Execute: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            return (result, error)

    async def execute_many(
            self,
            sentence: Union[str, list],
            *args
    ) -> Optional[Any]:
        error = None
        await self.valid_operation(sentence)
        try:
            result = await self._connection.executemany(sentence, *args)
            if result:
                await self._connection.commit()
        except Exception as err:
            error = "Error on Execute Many: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            return (result, error)

    executemany = execute_many

    async def fetch(
                    self,
                    sentence: str,
                    parameters: Iterable[Any] = None
            ) -> Iterable:
        """Helper to create a cursor and execute the given query."""
        await self.valid_operation(sentence)
        if parameters is None:
            parameters = []
        result = await self._connection.execute(
            sentence, parameters
        )
        return result

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError()  # pragma: no cover

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError()  # pragma: no cover

    async def use(self, database: str):
        raise NotImplementedError(
            'SQLite Error: There is no Database in SQLite'
        )

    async def column_info(
            self,
            table: str,
            **kwargs
    ) -> Iterable[Any]:
        """
        Getting Column info from an existing Table in Provider.
        """
        try:
            self._connection.row_factory = lambda c, r: dict(
                zip([col[0] for col in c.description], r))
            cursor = await self._connection.execute(
                f'PRAGMA table_info({table});', parameters=kwargs
            )
            cols = await cursor.fetchall()
            self._columns = []
            for col in cols:
                d = {
                    "name": col['name'],
                    "type": col['type']
                }
                self._columns.append(d)
            if not self._columns:
                raise NoDataFound()
        except Exception as err:
            error = "Error on Column Info: {err}"
            raise ProviderError(
                message=error
            ) from err
        finally:
            return self._columns

    async def create(
        self,
        obj: str = 'table',
        name: str = '',
        fields: Optional[list] = None
    ) -> bool:
        """
        Create is a generic method for Database Objects Creation.
        """
        if obj == 'table':
            sql = "CREATE TABLE {name} ({columns});"
            columns = ", ".join(["{name} {type}".format(**e) for e in fields])
            sql = sql.format(name=name, columns=columns)
            try:
                result = await self._connection.execute(sql)
                if result:
                    await self._connection.commit()
                    return True
                else:
                    return False
            except Exception as err:
                raise ProviderError(
                    f"Error in Object Creation: {err!s}"
                ) from err
        else:
            raise RuntimeError(
                f'SQLite: invalid Object type {object!s}'
            )

## ModelBackend Methods
    async def _insert_(self, _model: Model, **kwargs):
        """
        insert a row from model.
        """
        try:
            table = f"{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        cols = []
        source = []
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
                if 'db_default' in field.metadata:
                    # field get a default value from database
                    continue
                else:
                    raise ValueError(
                        f"Field {name} is required and value is null over {_model.Meta.name}"
                    )
            source.append(value)
            cols.append(column)
            n += 1
            if pk:=self._get_attribute(field, value, attr='primary_key'):
                _filter[column] = pk
        try:
            columns = ",".join(cols)
            values = ",".join(["?" for a in range(1, n)])
            insert = f"INSERT INTO {table}({columns}) VALUES({values})"
            self._logger.debug(f"INSERT: {insert}")
            cursor = await self._connection.execute(insert, parameters=source)
            await self._connection.commit()
            condition = self._where(fields, **_filter)
            get = f"SELECT * FROM {table} {condition}"
            self._connection.row_factory = lambda c, r: dict(
                zip([col[0] for col in c.description], r)
            )
            cursor = await self._connection.execute(get)
            result = await cursor.fetchone()
            if result:
                for f, val in result.items():
                    setattr(_model, f, val)
                return _model
        except Exception as err:
            raise ProviderError(
                message=f"Error on Insert over table {_model.Meta.name}: {err!s}"
            ) from err

    async def _delete_(self, _model: Model, **kwargs):
        """
        delete a row from model.
        """
        try:
            table = f"{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        source = []
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
            source.append(
                value
            )
            n += 1
            if pk:=self._get_attribute(field, value, attr='primary_key'):
                _filter[column] = pk
        try:
            condition = self._where(fields, **_filter)
            _delete = f"DELETE FROM {table} {condition};"
            self._logger.debug(f'DELETE: {_delete}')
            cursor = await self._connection.execute(_delete)
            await self._connection.commit()
            return f'DELETE {cursor.rowcount}: {_filter!s}'
        except Exception as err:
            raise ProviderError(
                message=f"Error on Insert over table {_model.Meta.name}: {err!s}"
            ) from err

    async def _update_(self, _model: Model, **kwargs):
        """
        Updating a row in a Model.
        TODO: How to update when if primary key changed.
        Alternatives: Saving *dirty* status and previous value on dict
        """
        try:
            table = f"{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        cols = []
        source = []
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
                if 'db_default' in field.metadata:
                    # field get a default value from database
                    continue
                else:
                    raise ValueError(
                        f"Field {name} is required and value is null over {_model.Meta.name}"
                    )
            source.append(
                value
            )
            cols.append(
                f"{column} = ?"
            )
            n += 1
            if pk:=self._get_attribute(field, value, attr='primary_key'):
                _filter[column] = pk
        try:
            set_fields = ", ".join(cols)
            condition = self._where(fields, **_filter)
            _update = f"UPDATE {table} SET {set_fields} {condition}"
            self._logger.debug(f'UPDATE: {_update}')
            cursor = await self._connection.execute(_update, parameters=source)
            await self._connection.commit()
            get = f"SELECT * FROM {table} {condition}"
            self._connection.row_factory = lambda c, r: dict(
                zip([col[0] for col in c.description], r)
            )
            cursor = await self._connection.execute(get)
            result = await cursor.fetchone()
            if result:
                for f, val in result.items():
                    setattr(_model, f, val)
                return _model
        except Exception as err:
            raise ProviderError(
                message=f"Error on Insert over table {_model.Meta.name}: {err!s}"
            ) from err

    async def _save_(self, model: Model, *args, **kwargs):
        """
        Save a row in a Model, using Insert-or-Update methodology.
        """

    async def _fetch_(self, _model: Model, **kwargs):
        """
        Returns one Row using Model.
        """
        try:
            table = f"{_model.Meta.name}"
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
            cursor = await self._connection.execute(_get)
            result = await cursor.fetchone()
            return result
        except Exception as e:
            raise ProviderError(
                f"Error: Model Fetch over {table}: {e}"
            ) from e

    async def _filter_(self, _model: Model, *args, **kwargs):
        """
        Filter a Model using Fields.
        """
        try:
            table = f"{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        fields = _model.columns(_model)
        _filter = {}
        if args:
            columns = ','.join(args)
        else:
            columns = '*'
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
            cursor = await self._connection.execute(_get)
            result = await cursor.fetchall()
            return result
        except Exception as e:
            raise ProviderError(
                f"Error: Model GET over {table}: {e}"
            ) from e

    async def _select_(self, *args, **kwargs):
        """
        Get a query from Model.
        """
        try:
            model = kwargs['_model']
        except KeyError as e:
            raise ProviderError(
                f'Missing Model for SELECT {kwargs!s}'
            ) from e
        try:
            table = f"{model.Meta.name}"
        except AttributeError:
            table = model.__name__
        if args:
            condition = '{}'.join(args)
        else:
            condition = None
        if 'fields' in kwargs:
            columns = ','.join(kwargs['fields'])
        else:
            columns = '*'
        _get = f"SELECT {columns} FROM {table} {condition}"
        try:
            cursor = await self._connection.execute(_get)
            result = await cursor.fetchall()
            return result
        except Exception as e:
            raise ProviderError(
                f"Error: Model SELECT over {table}: {e}"
            ) from e

    async def _get_(self, _model: Model, *args, **kwargs):
        """
        Get one row from model.
        """
        try:
            table = f"{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        fields = _model.columns(_model)
        _filter = {}
        if args:
            columns = ','.join(args)
        else:
            columns = '*'
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
            cursor = await self._connection.execute(_get)
            result = await cursor.fetchone()
            return result
        except Exception as e:
            raise ProviderError(
                f"Error: Model GET over {table}: {e}"
            ) from e

    async def _all_(self, _model: Model, *args, **kwargs):
        """
        Get all rows on a Model.
        """
        try:
            table = f"{_model.Meta.name}"
        except AttributeError:
            table = _model.__name__
        if 'fields' in kwargs:
            columns = ','.join(kwargs['fields'])
        else:
            columns = '*'
        _all = f"SELECT {columns} FROM {table}"
        try:
            cursor = await self._connection.execute(_all)
            result = await cursor.fetchall()
            return result
        except Exception as e:
            raise ProviderError(
                f"Error: Model All over {table}: {e}"
            ) from e

    async def _remove_(self, _model: Model, **kwargs):
        """
        Deleting some records using Model.
        """
        try:
            table = f"{_model.Meta.name}"
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
        _delete = f"DELETE FROM {table} {condition}"
        try:
            self._logger.debug(f'DELETE: {_delete}')
            cursor = await self._connection.execute(_delete)
            await self._connection.commit()
            return f'DELETE {cursor.rowcount}: {_filter!s}'
        except Exception as err:
            raise ProviderError(
                message=f"Error on Insert over table {_model.Meta.name}: {err!s}"
            ) from err


    async def _updating_(self, *args, _filter: dict = None, **kwargs):
        """
        Updating records using Model.
        """
        try:
            model = kwargs['_model']
        except KeyError as e:
            raise ProviderError(
                f'Missing Model for SELECT {kwargs!s}'
            ) from e
        try:
            table = f"{model.Meta.name}"
        except AttributeError:
            table = model.__name__
        try:
            table = f"{model.Meta.name}"
        except AttributeError:
            table = model.__name__
        fields = model.columns(model)
        if _filter is None:
            if args:
                _filter = args[0]
        cols = []
        source = []
        new_cond = {}
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
            cols.append(
                f"{name} = ?"
            )
        try:
            set_fields = ", ".join(cols)
            condition = self._where(fields, **_filter)
            _update = f"UPDATE {table} SET {set_fields} {condition}"
            self._logger.debug(f'UPDATE: {_update}')
            cursor = await self._connection.execute(_update, parameters=source)
            await self._connection.commit()
            print(f'UPDATE {cursor.rowcount}: {_filter!s}')
            new_conditions = {**_filter, **new_cond}
            condition = self._where(fields, **new_conditions)
            get = f"SELECT * FROM {table} {condition}"
            self._connection.row_factory = lambda c, r: dict(
                zip([col[0] for col in c.description], r)
            )
            cursor = await self._connection.execute(get)
            result = await cursor.fetchall()
            return [model(**dict(r)) for r in result]
        except Exception as err:
            raise ProviderError(
                message=f"Error on Insert over table {model.Meta.name}: {err!s}"
            ) from err

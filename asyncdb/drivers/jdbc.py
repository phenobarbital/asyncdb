"""Dummy Driver.
"""
import asyncio
from typing import (
    Union,
    Any
)
import time
from collections.abc import Iterable
from pathlib import PurePath
import jaydebeapi
import jpype
from asyncdb.exceptions import (
    DriverError,
    ProviderError
)
from asyncdb import ABS_PATH
from asyncdb.models import Model
from asyncdb.utils.types import Entity
from asyncdb.interfaces import DatabaseBackend, ModelBackend
from .sql import SQLDriver

class jdbc(SQLDriver, DatabaseBackend, ModelBackend):
    _provider = "JDBC"
    _syntax = "sql"

    def __init__(
            self,
            dsn: str = "",
            loop: asyncio.AbstractEventLoop = None,
            params: dict = None,
            **kwargs
    ) -> None:
        self._test_query = "SELECT 1"
        self._file_jar, self._classname = self.get_classdriver(params)
        SQLDriver.__init__(self, dsn, loop, params, **kwargs)
        DatabaseBackend.__init__(self)


    def get_classdriver(self, params):
        driver = params['driver']
        if driver == 'sqlserver':
            classdriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            self._dsn = 'jdbc:{driver}://{host}:{port};DatabaseName={database}'
        elif driver == 'postgresql':
            classdriver = "org.postgresql.Driver"
            self._dsn = 'jdbc:{driver}://{host}:{port}/{database}'
        elif driver == 'sybase':
            classdriver = "com.sybase.jdbc4.jdbc.SybDriver"
            self._dsn = 'jdbc:{driver}://{host}:{port}/{database}'
        elif driver == 'mysql':
            classdriver = "com.mysql.cj.jdbc.Driver"
            self._dsn = 'jdbc:{driver}://{host}:{port}/{database}'
        elif driver == 'oracle':
            classdriver = "oracle.jdbc.driver.OracleDriver"
            self._dsn = 'jdbc:oracle:thin:{user}/{password}@//{host}:{port}/{database}'
        elif driver == 'azure':
            classdriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            self._dsn = 'jdbc:sqlserver://{host}:{port};database={database};encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;Authentication=ActiveDirectoryIntegrated'
            msal = ABS_PATH.joinpath('bin', 'jar', 'msal4j-1.11.1.jar')
            params['jar'].append(msal)
        elif driver == 'cassandra':
            classdriver = "com.simba.cassandra.jdbc4.Driver"
            self._dsn = 'jdbc:cassandra://{host}:{port}/{database}'
        else:
            self._dsn = 'jdbc:{driver}://{host}:{port}/{database}'
            try:
                classdriver = params['class']
            except KeyError as e:
                raise DriverError(
                    f'JDBC Error: a class Driver need to be declared for {self._dsn}'
                ) from e
        # checking for JAR file
        file = params['jar']
        files = []
        if isinstance(file, (str, PurePath)):
            file = [file]
        elif not isinstance(file, list):
            raise ValueError(
                f"Invalid type of Jar Filenames: {file}"
            )
        for f in file:
            if not f.exists():
                raise DriverError(
                    f"JDBC: Invalid or missing binary JDBC driver: {f}"
                )
            files.append(str(f))
        return (files, classdriver)

    async def prepare(self, sentence: Union[str, list]) -> Any:
        "Ignoring prepared sentences on JDBC"
        raise NotImplementedError()  # pragma: no cover

    async def connection(self):
        """connection.

        Get a JDBC connection.
        """
        self._connection = None
        self._connected = False
        try:
            if jpype.isJVMStarted() and not jpype.isThreadAttachedToJVM():
                jpype.attachThreadToJVM()
                jpype.java.lang.Thread.currentThread().setContextClassLoader(
                    jpype.java.lang.ClassLoader.getSystemClassLoader()
                )
            if 'options' in self._params:
                options = ";".join({f'{k}={v}' for k,v in self._params['options'].items()})
                self._dsn = f"{self._dsn};{options}"
            user = self._params['user']
            password = self._params ['password']
            self._connection = jaydebeapi.connect(
                self._classname,
                self._dsn,
                driver_args=[user,password],
                jars=self._file_jar
            )
            print(
                f'{self._provider}: Connected at {self._params["driver"]}:{self._params["host"]}'
            )
            if self._connection:
                self._connected = True
                self._initialized_on = time.time()
        except jpype.JException as ex:
            print(ex.stacktrace())
            raise DriverError(
                f"Driver {self._classname} Error: {ex}"
            ) from ex
        except TypeError as e:
            raise DriverError(
                f"Driver {self._classname} was not found: {e}"
            ) from e
        except Exception as e:
            self._logger.exception(e, stack_info=True)
            raise ProviderError(
                f"JDBC Unknown Error: {e!s}"
            ) from e
        return self

    connect = connection

    async def close(self, timeout: int = 10) -> None:
        try:
            self._connection.close()
        except Exception as e:
            self._logger.exception(e, stack_info=True)
            raise ProviderError(
                f"JDBC Closing Error: {e!s}"
            ) from e

    disconnect = close

    async def get_columns(self):
        return {"id": "value"}

    async def query(self, sentence="", **kwargs):
        error = None
        print(f"Running Query: {sentence}")
        result = [{'col1': [1, 2], 'col2': [3, 4], 'col3': [5, 6]}]
        return await self._serializer(result, error)

    fetch_all = query

    async def execute(self, sentence: str, *args, **kwargs):
        print(f"Execute Query {sentence}")
        data = []
        error = None
        result = [data, error]
        return await self._serializer(result, error)

    execute_many = execute

    async def queryrow(self, sentence=""):
        error = None
        print(f"Running Row {sentence}")
        result = {'col1': [1, 2], 'col2': [3, 4], 'col3': [5, 6]}
        return await self._serializer(result, error)

    fetch_one = queryrow

    def tables(self, schema: str = "") -> Iterable[Any]:
        raise NotImplementedError()  # pragma: no cover

    def table(self, tablename: str = "") -> Iterable[Any]:
        raise NotImplementedError()  # pragma: no cover

    async def use(self, database: str):
        raise NotImplementedError(
            'SQLite Error: There is no Database in SQLite'
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

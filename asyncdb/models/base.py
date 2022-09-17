"""
Basic, Abstract Model.
"""
from __future__ import annotations
import logging
import inspect
import traceback
from dataclasses import (
    is_dataclass,
    make_dataclass,
)
from collections.abc import Callable
from typing import (
    Dict,
    Optional,
    Union
)
from datamodel import BaseModel, Field
from datamodel.base import Meta
from datamodel.types import (
    MODEL_TYPES
)
from asyncdb.utils import Msg
from asyncdb.exceptions import (
    NoDataFound,
    ProviderError,
    StatementError
)
from asyncdb.utils import module_exists
from asyncdb.providers.interfaces import ConnectionBackend


class Model(BaseModel):
    """
    Model.

    DataModel representing connection to databases.
    """
    def set_connection(self, connection: ConnectionBackend) -> None:
        """
        Manually Set the connection of Dataclass.
        """
        try:
            self.Meta.connection = connection
        except Exception as err:
            raise Exception(
                f"{err}"
            ) from err

    def get_connection(self) -> ConnectionBackend:
        """get_connection.
        Getting a database connection and driver based on parameters
        """
        Msg(':: Getting Connection ::', 'DEBUG')
        if self.Meta.datasource:
            # TODO: making a connection using a DataSource.
            pass
        elif self.Meta.driver:
            driver = self.Meta.driver
            provider = f"asyncdb.providers.{driver}"
            try:
                obj = module_exists(driver, provider)
            except Exception as err:
                raise Exception(
                    f"{err}"
                ) from err
            if self.Meta.dsn is not None:
                try:
                    self.Meta.connection = obj(dsn=self.Meta.dsn)
                except ProviderError:
                    raise
                except Exception as err:
                    logging.exception(err)
                    raise Exception(
                        f"{err}"
                    ) from err
            elif hasattr(self.Meta, "credentials"):
                params = self.Meta.credentials
                try:
                    self.Meta.connection = obj(params=params)
                except ProviderError:
                    raise
                except Exception as err:
                    logging.exception(err)
                    raise Exception(
                        f"{err}"
                    ) from err
        return self.Meta.connection


###  Magic Methods
    async def __aenter__(self) -> BaseModel:
        if not self.Meta.connection:
            self.get_connection()
        await self.Meta.connection.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # clean up anything you need to clean up
        return await self.close()

    async def close(self):
        """
        Closing an existing database connection.
        """
        try:
            await self.Meta.connection.close()
        except Exception as err:
            logging.exception(err)
            raise RuntimeError(
                f"{err}"
            ) from err

### Instance method for Dataclasses.
    async def save(self):
        """
        Saving a Dataclass Model to Database.
        """
        if not self.Meta.connection:
            self.get_connection()
        async with await self.Meta.connection.connection() as conn:
            try:
                result = await self.Meta.connection.model_save(
                    model=self, fields=self.columns()
                )
                return result
            except ProviderError:
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    f"Error on Insert over table {self.Meta.name}: {err}"
                ) from err

    async def insert(self):
        """
        Insert a new Dataclass Model to Database.
        """
        if not self.Meta.connection:
            self.get_connection()
        result = None
        async with await self.Meta.connection.connection() as conn:
            try:
                result = await self.Meta.connection.model_insert(
                    model=self, connection=conn, fields=self.columns()
                )
                return result
            except StatementError:
                raise
            except ProviderError:
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    "Error on Insert over table {}: {}".format(
                        self.Meta.name, err)
                )

    async def delete(self, **kwargs):
        """
        Deleting a row Model based on Primary Key
        """
        if not self.Meta.connection:
            self.get_connection()
        result = None
        async with await self.Meta.connection.connection() as conn:
            try:
                result = await self.Meta.connection.model_delete(
                    model=self,
                    fields=self.columns(),
                    connection=conn,
                    **kwargs
                )
                return result
            except StatementError:
                raise
            except ProviderError:
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    "Error on Insert over table {}: {}".format(
                        self.Meta.name, err)
                )

    async def fetch(self, **kwargs):
        """
        Return a new single record based on filter criteria
        """
        if not self.Meta.connection:
            self.get_connection()
        async with await self.Meta.connection.connection() as conn:
            try:
                result = await self.Meta.connection.model_get(
                    model=self, fields=self.columns(), **kwargs
                )
                if result:
                    return self.__class__(**dict(result))
                else:
                    raise NoDataFound(
                        "{} object with condition {} Not Found!".format(
                            self.Meta.name, kwargs
                        )
                    )
            except NoDataFound:
                raise
            except (StatementError, ProviderError):
                raise
            except AttributeError as err:
                raise Exception(
                    "Error on get {}: {}".format(self.Meta.name, err))
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    "Error on get {}: {}".format(self.Meta.name, err))

    # get = fetch

    async def select(self, **kwargs):
        """
        Need to return a ***collection*** of nested DataClasses
        """
        if not self.Meta.connection:
            self.get_connection()
        async with await self.Meta.connection.connection() as conn:
            try:
                result = await self.Meta.connection.model_select(
                    model=self, fields=self.columns(), **kwargs
                )
                if result:
                    return [self.__class__(**dict(r)) for r in result]
                else:
                    raise NoDataFound(
                        "No Data on {} with condition {}".format(
                            self.Meta.name, kwargs)
                    )
            except NoDataFound:
                raise
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    "Error on filter {}: {}".format(self.Meta.name, err))

    async def fetch_all(self, **kwargs):
        """
        Need to return all rows as a ***collection*** of nested DataClasses
        """
        if not self.Meta.connection:
            self.get_connection()
        async with await self.Meta.connection.connection() as conn:
            try:
                result = await self.Meta.connection.model_all(
                    model=self, fields=self.columns()
                )
                if result:
                    return [self.__class__(**dict(r)) for r in result]
                else:
                    raise NoDataFound(
                        f"No Data on {self.Meta.name} with condition {kwargs}"
                    )
            except NoDataFound:
                raise
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    f"Error on filter {self.Meta.name}: {err}"
                )

### Class-based methods for Dataclasses.
    @classmethod
    async def create(cls, records):
        if not cls.Meta.connection:
            cls.get_connection(cls)
        async with await cls.Meta.connection.connection() as conn:
            try:
                # working always with native format:
                cls.Meta.connection.output_format('native')
            except Exception:
                pass
            try:
                result = await cls.Meta.connection.mdl_create(
                    model=cls,
                    rows=records
                )
                if result:
                    return [cls(**dict(r)) for r in result]
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    f"Error Updating Table {cls.Meta.name}: {err}"
                ) from err

    @classmethod
    async def remove(cls, _filter: Dict = None, **kwargs):
        if not cls.Meta.connection:
            cls.get_connection(cls)
        async with await cls.Meta.connection.connection() as conn:
            result = []
            try:
                result = await cls.Meta.connection.mdl_delete(
                    model=cls, _filter=_filter, **kwargs
                )
                return result
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    f"Error Deleting Table {cls.Meta.name}: {err}"
                )

    @classmethod
    async def update(cls, _filter: Dict = None, **kwargs):
        if not cls.Meta.connection:
            cls.get_connection(cls)
        async with await cls.Meta.connection.connection() as conn:
            try:
                result = await cls.Meta.connection.mdl_update(
                    model=cls, _filter=_filter, **kwargs
                )
                if result:
                    return [cls(**dict(r)) for r in result]
                else:
                    return []
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                print(traceback.format_exc())
                raise Exception(
                    f"Error Updating Table {cls.Meta.name}: {err}"
                )

    @classmethod
    async def filter(cls, **kwargs):
        """
        Need to return a ***collection*** of nested DataClasses
        """
        if not cls.Meta.connection:
            cls.get_connection(cls)
        async with await cls.Meta.connection.connection() as conn:
            result = []
            try:
                result = await cls.Meta.connection.mdl_filter(
                    model=cls, **kwargs
                )
                if result:
                    return [cls(**dict(r)) for r in result]
                else:
                    return []
            except NoDataFound:
                raise
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    f"Error on filter {cls.Meta.name}: {err}"
                )

    @classmethod
    async def get(cls, **kwargs):
        """
        Return a new single record based on filter criteria
        """
        if not cls.Meta.connection:
            cls.get_connection(cls)
        async with await cls.Meta.connection.connection() as conn:
            try:
                result = await cls.Meta.connection.mdl_get(model=cls, **kwargs)
                if result:
                    return cls(**dict(result))
                else:
                    raise NoDataFound(
                        message=f"Data not found over {cls.Meta.name!s}")
            except NoDataFound:
                raise NoDataFound(
                    message=f"Data not found over {cls.Meta.name!s}")
            except AttributeError as err:
                raise Exception(
                    f"Error on get {cls.Meta.name}: {err}"
                )
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                print(traceback.format_exc())
                raise Exception(
                    f"Error on get {cls.Meta.name}: {err}"
                )

    # get all data
    @classmethod
    async def all(cls, **kwargs):
        if not cls.Meta.connection:
            cls.get_connection(cls)
        async with await cls.Meta.connection.connection() as conn:
            try:
                result = await cls.Meta.connection.mdl_all(model=cls, **kwargs)
                return [cls(**dict(row)) for row in result]
            except StatementError:
                raise
            except ProviderError:
                raise
            except Exception as err:
                print(traceback.format_exc())
                raise Exception(
                    f"Error on query_all over table {cls.Meta.name}: {err}"
                ) from err

    @classmethod
    async def makeModel(
        cls,
        name: str,
        schema: str = "public",
        fields: list = None,
        db: "ConnectionBackend" = None,
    ):
        """
        Make Model.

        Making a model from field tuples, a JSON schema or a Table.
        """
        tablename = f"{schema}.{name}"
        if not fields:  # we need to look in to it.
            colinfo = await db.column_info(tablename)
            fields = []
            for column in colinfo:
                tp = column["type"]
                col = Field(
                    primary_key=column["is_primary"],
                    notnull=column["notnull"],
                    db_type=column["format_type"],
                )
                # get dtype from database type:
                try:
                    dtype = MODEL_TYPES[tp]
                except KeyError:
                    dtype = str
                fields.append((column["name"], dtype, col))
        parent = inspect.getmro(cls)
        obj = make_dataclass(name, fields, bases=(parent[0],))
        m = Meta()
        m.name = name
        m.schema = schema
        m.app_label = schema
        m.connection = db
        m.frozen = False
        obj.Meta = m
        return obj


    @classmethod
    def model(cls, dialect: str = "sql") -> str:
        clsname = cls.__name__
        schema = cls.Meta.schema
        table = cls.Meta.name if cls.Meta.name else clsname.lower()
        columns = cls.columns(cls).items()
        if dialect == "sql" or dialect == "SQL":
            # TODO: using lexers to different types of SQL
            # And db_types to translate dataclass types to DB types.
            doc = f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n"
            cols = []
            pk = []
            for _, field in columns:
                # print(type(field), field)
                key = field.name
                default = None
                try:
                    default = field.metadata["db_default"]
                except KeyError:
                    if field.default is not None:
                        default = f"{field.default!r}"
                default = (
                    f"DEFAULT {default!s}"
                    if isinstance(default, (str, int))
                    else ""
                )
                if is_dataclass(field.type):
                    tp = "jsonb"
                    nn = ""
                else:
                    try:
                        tp = field.db_type()
                    except (TypeError, ValueError, AttributeError):
                        # print(err)
                        tp = "varchar"
                    nn = "NOT NULL" if field.required() is True else ""
                if hasattr(field, 'primary_key'):
                    if field.primary_key is True:
                        pk.append(key)
                # print(key, tp, nn, default)
                cols.append(f" {key} {tp} {nn} {default}")
            doc = "{}{}".format(doc, ",\n".join(cols))
            if len(pk) >= 1:
                primary = ", ".join(pk)
                cname = f"pk_{schema}_{table}_pkey"
                doc = "{},\n{}".format(
                    doc, f"CONSTRAINT {cname} PRIMARY KEY ({primary})"
                )
            doc = doc + "\n);"
            return doc
        else:
            return super(Model, cls).model(dialect)

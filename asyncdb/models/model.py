"""
Basic, Abstract Model.
"""
from __future__ import annotations

import inspect
import logging
import traceback
from collections.abc import Awaitable
from dataclasses import _MISSING_TYPE, MISSING, is_dataclass, make_dataclass

from datamodel import BaseModel, Field
from datamodel.base import Meta
from datamodel.exceptions import ValidationError
from datamodel.types import MODEL_TYPES

from asyncdb.exceptions import ConnectionMissing, NoDataFound, ProviderError, StatementError
from asyncdb.utils.modules import module_exists


def is_missing(value):
    if value == _MISSING_TYPE:
        return True
    elif value == MISSING:
        return True
    elif isinstance(value, _MISSING_TYPE):
        return True
    else:
        return False

class Model(BaseModel):
    """
    Model.

    DataModel representing connection to databases.
    """
    def set_connection(self, connection: Awaitable) -> None:
        """
        Manually Set the connection of Dataclass.
        """
        try:
            self.Meta.connection = connection
        except Exception as err:
            raise Exception(
                f"{err}"
            ) from err

    def get_connection(self) -> Awaitable:
        """get_connection.
        Getting a database connection and driver based on parameters
        """
        if self.Meta.datasource:
            # TODO: making a connection using a DataSource.
            pass
        elif self.Meta.driver:
            driver = self.Meta.driver
            provider = f"asyncdb.drivers.{driver}"
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
    async def insert(self, **kwargs):
        """
        Insert a new Dataclass Model to Database.
        """
        if not self.Meta.connection:
            self.get_connection()
        if not self.Meta.connection.is_connected():
            await self.Meta.connection.connection()
        result = None
        try:
            result = await self.Meta.connection._insert_(
                _model=self, **kwargs
            )
            return result
        except StatementError:
            raise
        except ProviderError:
            raise
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                f"Error on INSERT {self.Meta.name}: {err}"
            ) from err

    async def update(self, **kwargs):
        """
        Saving a Dataclass Model to Database.
        """
        if not self.Meta.connection:
            self.get_connection()
        if not self.Meta.connection.is_connected():
            await self.Meta.connection.connection()
        result = None
        try:
            result = await self.Meta.connection._update_(
                _model=self, **kwargs
            )
            return result
        except ProviderError:
            raise
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                f"Error on UPDATE {self.Meta.name}: {err}"
            ) from err

    async def delete(self, **kwargs):
        """
        Deleting a row Model based on Primary Key
        """
        if not self.Meta.connection:
            self.get_connection()
        if not self.Meta.connection.is_connected():
            await self.Meta.connection.connection()
        result = None
        try:
            result = await self.Meta.connection._delete_(
                _model=self,
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
                f"Error on DELETE {self.Meta.name}: {err}"
            ) from err

    async def save(self, **kwargs):
        """
        Saving a Dataclass Model to Database.
        """
        if not self.Meta.connection:
            self.get_connection()
        async with await self.Meta.connection.connection() as conn:
            try:
                result = await self.Meta.connection._save_(
                    _model=self, **kwargs
                )
                return result
            except ProviderError:
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    f"Error on SAVE {self.Meta.name}: {err}"
                ) from err

    async def fetch(self, **kwargs):
        """
        Return a new single record based on filter criteria
        """
        if not self.Meta.connection:
            self.get_connection()
        if not self.Meta.connection.is_connected():
            await self.Meta.connection.connection()
        try:
            result = await self.Meta.connection._fetch_(
                _model=self,
                **kwargs
            )
            if result:
                for f, val in result.items():
                    setattr(self, f, val)
                return self
            else:
                raise NoDataFound(
                    f"{self.Meta.name}: Data Not found"
                )
        except ValidationError:
            raise
        except NoDataFound:
            raise
        except (AttributeError, StatementError) as err:
            raise StatementError(
                f"Error on Attribute {self.Meta.name}: {err}"
            ) from err
        except ProviderError:
            raise
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                f"Error on get {self.Meta.name}: {err}"
            ) from err


### Class-based methods for Dataclasses.
    @classmethod
    async def create(cls, records: list):
        if not cls.Meta.connection:
            raise ConnectionMissing(
                f"Missing Connection for Model: {cls}"
            )
        # working always with native format:
        cls.Meta.connection.output_format('native')
        try:
            result = await cls.Meta.connection._create_(
                _model=cls,
                rows=records
            )
            if result:
                return result
        except ValidationError:
            raise
        except (AttributeError, StatementError) as err:
            raise StatementError(
                f"Error on Attribute {cls.Meta.name}: {err}"
            ) from err
        except ProviderError:
            raise
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                f"Error Updating Table {cls.Meta.name}: {err}"
            ) from err

    @classmethod
    async def remove(cls, **kwargs):
        if not cls.Meta.connection:
            raise ConnectionMissing(
                f"Missing Connection for Model: {cls}"
            )
        result = []
        try:
            result = await cls.Meta.connection._remove_(
                _model=cls, **kwargs
            )
            return result
        except (AttributeError, StatementError) as err:
            raise StatementError(
                f"Error on Attribute {cls.Meta.name}: {err}"
            ) from err
        except ProviderError:
            raise
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                f"Error Deleting Table {cls.Meta.name}: {err}"
            ) from err

    @classmethod
    async def updating(cls, *args, _filter: dict = None, **kwargs):
        if not cls.Meta.connection:
            raise ConnectionMissing(
                f"Missing Connection for Model: {cls}"
            )
        try:
            result = await cls.Meta.connection._updating_(
                _model=cls, _filter=_filter, *args, **kwargs
            )
            if result:
                return result
            else:
                return []
        except (AttributeError, StatementError) as err:
            raise StatementError(
                f"Error on Attribute {cls.Meta.name}: {err}"
            ) from err
        except ProviderError:
            raise
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                f"Error Updating Table {cls.Meta.name}: {err}"
            ) from err


    @classmethod
    async def select(cls, *args, **kwargs):
        """Select.
        passing a where condition directly to model.
        :raises ProviderError, Exception
        """
        if not cls.Meta.connection:
            raise ConnectionMissing(
                f"Missing Connection for Model: {cls}"
            )
        result = []
        try:
            result = await cls.Meta.connection._select_(
                _model=cls, *args, **kwargs
            )
            if result:
                return [cls(**dict(r)) for r in result]
            else:
                return []
        except ValidationError:
            raise
        except NoDataFound:
            raise
        except (AttributeError, StatementError) as err:
            raise StatementError(
                f"Error on Attribute {cls.Meta.name}: {err}"
            ) from err
        except ProviderError:
            raise
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                f"Error on Select {cls.Meta.name}: {err}"
            ) from err

    @classmethod
    async def filter(cls, *args, **kwargs):
        """
        Need to return a ***collection*** of nested DataClasses
        """
        if not cls.Meta.connection:
            raise ConnectionMissing(
                f"Missing Connection for Model: {cls}"
            )
        result = []
        try:
            result = await cls.Meta.connection._filter_(
                _model=cls, *args, **kwargs
            )
            if result:
                return [cls(**dict(r)) for r in result]
            else:
                return []
        except ValidationError:
            raise
        except NoDataFound:
            raise
        except (AttributeError, StatementError) as err:
            raise StatementError(
                f"Error on Attribute {cls.Meta.name}: {err}"
            ) from err
        except ProviderError:
            raise
        except Exception as err:
            logging.debug(traceback.format_exc())
            raise Exception(
                f"Error on filter {cls.Meta.name}: {err}"
            ) from err

    @classmethod
    async def get(cls, **kwargs):
        """
        Return a new single record based on filter criteria
        """
        if not cls.Meta.connection:
            raise ConnectionMissing(
                f"Missing Connection for Model: {cls}"
            )
        try:
            result = await cls.Meta.connection._get_(
                _model=cls, **kwargs
            )
            if result:
                fields = cls.get_fields(cls)
                result = {k:v for k,v in dict(result).items() if k in fields}
                return cls(**result)
            else:
                raise NoDataFound(
                    message=f"Data not found over {cls.Meta.name!s}"
                )
        except ValidationError:
            raise
        except NoDataFound as e:
            raise NoDataFound(
                message=f"Data not found over {cls.Meta.name!s}"
            ) from e
        except AttributeError as err:
            raise StatementError(
                f"Error on Attribute {cls.Meta.name}: {err}"
            ) from err
        except (StatementError, ProviderError) as err:
            raise ProviderError(
                f"Error on get {cls.Meta.name}: {err}"
            ) from err
        except Exception as err:
            print(traceback.format_exc())
            raise Exception(
                f"Error on get {cls.Meta.name}: {err}"
            ) from err

    # get all data of a model
    @classmethod
    async def all(cls, **kwargs):
        if not cls.Meta.connection:
            raise ConnectionMissing(
                f"Missing Connection for Model: {cls}"
            )
        try:
            result = await cls.Meta.connection._all_(
                _model=cls, **kwargs
            )
            return [cls(**dict(row)) for row in result]
        except ValidationError:
            raise
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
        db: Awaitable = None,
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
            # re-direct creation to backend driver.
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

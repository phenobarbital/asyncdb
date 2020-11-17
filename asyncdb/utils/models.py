import os
import asyncio
from dataclasses import Field as ff
from dataclasses import dataclass, is_dataclass, fields, _FIELDS, _FIELD, asdict, MISSING, InitVar
import datetime
import typing
import uuid
from decimal import Decimal
from asyncdb import AsyncDB
from asyncdb.utils import colors, SafeDict, Msg
from asyncdb.utils.encoders import DefaultEncoder
from asyncdb.exceptions import NoDataFound
#from navigator.conf import DATABASES
from typing import Any, List, Optional, get_type_hints, Callable, ClassVar, Union
from abc import ABC, abstractmethod
import json
import rapidjson as to_json

import collections
import numpy as np
import traceback

MODELS = {}

DB_TYPES = {
    bool: "boolean",
    int: "integer",
    float: "float",
    str: "character varying",
    bytes: "byte",
    list: "Array",
    Decimal: "numeric",
    datetime.date: "date",
    datetime.datetime: "timestamp with time zone",
    datetime.time: "time",
    datetime.timedelta: "timestamp without time zone",
    uuid.UUID: "uuid"
}

JSON_TYPES = {
    bool: "boolean",
    int: "integer",
    float: "float",
    str: "string",
    bytes: "byte",
    list: "list",
    List: "list",
    Decimal: "decimal",
    datetime.date: "date",
    datetime.datetime: "datetime",
    datetime.time: "time",
    datetime.timedelta: "timedelta",
    uuid.UUID: "uuid"
}


class Entity:
    @classmethod
    def number(cls, type):
        return type in (int, float, Decimal, bytes, bool)

    @classmethod
    def string(cls, type):
        return type in (str, datetime.datetime, datetime.time, datetime.timedelta, uuid.UUID)

    @classmethod
    def is_date(cls, type):
        return type in (datetime.datetime, datetime.time, datetime.timedelta)

    @classmethod
    def is_array(cls, type):
        return isinstance(type, (list, List, collections.Sequence, np.ndarray))

    @classmethod
    def escapeLiteral(cls, value, type):
        v = value if value != 'None' or value is not None else ""
        v = f'{value!r}' if Entity.string(type) else v
        v = value if Entity.number(type) else f'{value!r}'
        return v

    @classmethod
    def toSQL(cls, value, type):
        v = 'NULL' if value == 'None' or value is None or value == 'null' or value == 'NULL' else value
        v = value if Entity.number(type) else value
        v = str(value) if isinstance(value, uuid.UUID) else value
        v = f'{value!s}' if Entity.string(type) else value
        return v


"""
Class for Error validation
"""
@dataclass
class ValidationError:
    field: str
    value: str
    error: str
    value_type: str
    annotation: type
    exception: Optional[Exception]


# list of basic attributes:
"""
required (field is required)
primary_key (field is the primary key of model)
default: (represent the "default_factory" class)

"""
# original metaclass structure
class Field(ff):
    """
    Field.
       Extending Column definition from Dataclass Field
    """
    _default: Any = None
    _default_factory: Callable = MISSING
    _required: bool = False
    _pk: bool = False

    def __init__(
        self,
        default: Any = None,
        init: bool = True,
        primary_key: bool = False,
        notnull: bool = False,
        required: bool = False,
        factory: Callable = MISSING,
        min: Union[int, float, Decimal] = None,
        max: Union[int, float, Decimal] = None,
        validation: Callable = None,
        **kwargs
    ):
        args = {
            "init": True,
            "repr": True,
            "hash": True,
            "compare": True,
            "metadata": None
        }
        if 'compare' in kwargs:
            args['compare'] = kwargs['compare']
            del kwargs['compare']
        meta = {
            "required": required,
            "primary_key": primary_key
        }
        self._required = required
        self._pk = primary_key
        range = {}
        if min is not None:
            range['min'] = min
        if max is not None:
            range['max'] = max
        if required == True or primary_key == True:
            args['init'] = True
        else:
            if 'init' in kwargs:
                args['init'] = kwargs['init']
                del kwargs['init']
            # else:
            #     args['init'] = False
        if validation is not None:
            meta['validation'] = validation
        if 'metadata' in kwargs:
            meta = {**meta, **kwargs['metadata']}
            del kwargs['metadata']
        args['metadata'] = {
            **meta,
            **range,
            **kwargs
        }
        if default is not None:
            self._default = default
            self._default_factory = MISSING
        else:
            if notnull == True:
                # add a default not-null value
                args['default'] = ''
            else:
                if not factory:
                    factory = MISSING
                # get the annotation of field
                self._default_factory = factory
        super().__init__(
            default = self._default,
            default_factory = self._default_factory,
            **args
        )
        #self._field_type = _FIELD

    def __repr__(self):
        return (
            'Field('
            f'column={self.name!r},'
            f'type={self.type!r},'
            f'default={self.default!r})'
            )

    @property
    def required(self):
        return self._required

    @property
    def primary_key(self):
        return self._pk

def _dc_method_setattr(self, name: str, value: Any, *args, **kwargs) -> None:
    """
    method for overwrite setattr in Dataclasses
    """
    if self.Meta.frozen is True and name not in self._columns:
        raise TypeError(f"Cannot Modify attribute {name} of {self.modelName}, This DataClass is frozen (read-only class)")
    else:
        object.__setattr__(self, name, value)
        if name not in self._columns.keys():
            try:
                if self.Meta.strict == True:
                    Msg(
                        'Warning: Field **{}** doesn\'t exists on Model {}'.format(
                            name,
                            self.modelName
                            ), 'WARN'
                    )
                else:
                    f = Field(
                       required=False,
                       default=value
                    )
                    f.name = name
                    f.type = type(value)
                    self._columns[name] = f
                    setattr(self, name, value)
            except Exception as err:
                print(err)

def make_dataclass(new_cls: Any, repr: bool = True, eq: bool = True, validate: bool = True, frozen: bool = False) -> None:
    """
    make_dataclass.
       Create a Dataclass from a Class
    """
    # TODO: can build more complex dataclasses using make dataclass function
    dc = dataclass(unsafe_hash=True, init=True, frozen=frozen)(new_cls)
    # TODO: add method for __post_init__
    __class__ = dc
    setattr(dc, "__setattr__", _dc_method_setattr)
    # adding json encoder:
    dc.__encoder__ = DefaultEncoder(sort_keys=False)
    return dc


class ModelMeta(type):
    """
    ModelMeta.

    MetaClass object to create dataclasses for modeling Data Models.
    """
    __slots__ = ()
    __valid__ = None
    __encoder__ = None

    def __new__(cls, name, bases, attrs):
        """__new__ is a classmethod, even without @classmethod decorator
        """
        if len(bases) > 1:
            raise TypeError(
                "Multiple inheritance of Nav data Models are forbidden"
                )
        # TODO: avoid ValueError: 'associate_id' in __slots__
        # conflicts with class variable
        if '__annotations__' in attrs:
            annotations = attrs['__annotations__']
            cols = []
            for field, type in annotations.items():
                #print(field, type)
                if field in attrs:
                    df = attrs[field]
                    if isinstance(df, Field):
                        setattr(cls, field, df)
                    else:
                        f = Field(
                           factory=type,
                           required=False,
                           default=df
                        )
                        f.name = field
                        f.type = type
                        setattr(cls, field, f)
                else:
                    f = Field(
                       factory=type,
                       required=False
                    )
                    f.name = field
                    f.type = type
                    setattr(cls, field, f)
                cols.append(field)
            # set the slots of this class
            cls.__slots__ = tuple(cols)
        new_cls = super().__new__(cls, name, bases, attrs)
        frozen = False
        try:
            # TODO: mix values from Meta to an existing meta
            try:
                frozen = new_cls.Meta.frozen
            except AttributeError:
                new_cls.Meta.frozen = False
            try:
                strict = new_cls.Meta.strict
            except AttributeError:
                new_cls.Meta.strict = False
            try:
                test = new_cls.Meta.driver
            except AttributeError:
                new_cls.Meta.driver = None
        except AttributeError:
            new_cls.Meta = Meta
        if new_cls.__name__ == 'Model':
            if "__init__" in new_cls.__dict__:
                class_init = getattr(new_cls, "__init__")
                class_init(new_cls)
        dc = make_dataclass(new_cls, frozen=frozen)
        MODELS[new_cls.__name__] = dc
        cols = dc.__dict__['__dataclass_fields__']
        dc._columns = cols
        dc._fields = cols.keys()
        #dc.__slots__ = tuple(cols)
        return dc

    def __init__(cls, *args, **kwargs) -> None:
        cls.modelName = cls.__name__
        if cls.Meta.strict:
            cls.__frozen__ = cls.Meta.strict
        else:
            cls.__frozen__ = False
        cls.__initialised__ = True
        if cls.Meta.driver is not None:
            Msg('Getting Connection', 'DEBUG')
            cls.get_connection(cls)
        super(ModelMeta, cls).__init__(*args, **kwargs)


class Meta:
    name: str = ''
    schema: str = ''
    app_label: str = ''
    frozen: bool = False
    strict: bool = True
    driver: str = None
    credentials: dict = {}


class Model(metaclass=ModelMeta):
    __frozen__ = False
    _columns = []
    _fields = {}
    __columns__ = []
    _connection = None

    def __post_init__(self) -> None:
        """
        Start validation of fields
        """
        self._validation()

    def _validation(self) -> None:
        """
        _validation.
        TODO: cover validations as length, not_null, required, etc
        """
        errors = {}
        for name, field in self.columns():
            #print(name, field)
            key = field.name
            # print({
            #     "name": key,
            #     "value": getattr(self, key),
            #     "type": field.type,
            #     "metadata": field.metadata,
            #     "field": field,
            #     "internal": self.__dict__[key]
            # })
            # first check: data type hint
            val = self.__dict__[key]
            val_type = type(val)
            annotated_type = field.type
            if val_type == 'type' or val == annotated_type or val is None:
                # data not provided
                try:
                    if field.metadata['required'] == True and self.Meta.strict == True:
                        errors[key] = ValidationError(
                                field=key,
                                value=None,
                                value_type=None,
                                error="Field Required",
                                annotation=annotated_type,
                                exception=None
                            )
                except KeyError:
                    continue
                continue
            else:
                #print(key, val, annotated_type)
                try:
                    instance = self._is_instanceof(val, annotated_type)
                    # first: check primitives
                    if instance['result']:
                        # is valid
                        continue
                    else:
                        # TODO check for complex types
                        # adding more complex validations
                        continue
                        # errors[key] = ValidationError(
                        #     field=key,
                        #     value=val,
                        #     error='Validation Error',
                        #     value_type=val_type,
                        #     annotation=annotated_type,
                        #     exception=instance["exception"]
                        # )
                except Exception as err:
                    errors[key] = ValidationError(
                            field=key,
                            value=val,
                            error="Validation Exception",
                            value_type=val_type,
                            annotation=annotated_type,
                            exception=err
                    )
            # second check: length,
            # third validation: formats and patterns
            # fourth validation: function-based validators
        if errors:
            print('=== ERRORS ===')
            print(errors)
            object.__setattr__(self, '__valid__', False)
        else:
            object.__setattr__(self, '__valid__', True)

    def _is_instanceof(self, value: Any, annotated_type: type) -> bool:
        result = False
        exception = None
        try:
            result = isinstance(value, annotated_type)
        except exception as err:
            exception = err
            result = False
        finally:
            return {"result": result, "exception": exception }

    def __unicode__(self):
        return str(__class__)

    def columns(self):
        return self._columns.items()

    def dict(self):
        return asdict(self)

    def json(self):
        return self.__encoder__(asdict(self))

    def is_valid(self):
        return bool(self.__valid__)

    def schema(self, type: str = 'json') -> str:
        result = None
        name = self.__class__.__name__
        schema = self.Meta.schema if self.Meta.schema is not None else ''
        columns = {}
        try:
            if type == 'json':
                for name, field in self.columns():
                    key = field.name
                    type = field.type
                    columns[key] = {
                        "name": key,
                        "type": JSON_TYPES[type]
                    }
                doc = {
                    "name": name,
                    "description": self.__doc__.strip('\n').strip(),
                    "schema": schema,
                    "fields": columns
                }
                result = to_json.dumps(doc)
            elif type == 'sql':
                # TODO: using lexers to different types of SQL
                table = self.Meta.name if self.Meta.name is not None else name
                doc = f'CREATE TABLE {schema}.{table} (\n'
                cols = []
                pk = []
                for name, field in self.columns():
                    key = field.name
                    default = None
                    try:
                        default = field.metadata['db_default']
                    except KeyError:
                        if field.default:
                            default = f'{field.default!r}'
                    default = f'DEFAULT {default!s}' if isinstance(default, (str, int)) else ''
                    type = DB_TYPES[field.type]
                    nn = 'NOT NULL' if field.required is True else ''
                    if field.primary_key is True:
                        pk.append(key)
                    cols.append(f' {key} {type} {nn} {default}')
                doc = "{}{}".format(doc, ",\n".join(cols))
                if len(pk) >= 1:
                    primary = ", ".join(pk)
                    cname = f'pk_{schema}_{table}_pkey'
                    doc = "{},\n{}".format(doc, f'CONSTRAINT {cname} PRIMARY KEY ({primary})')
                doc = doc + '\n);'
                result = doc
        finally:
            return result

    def fields(self, fields: Any =[]):
        """
        fields
        todo: detect when field is missing
        """
        if type(fields) == str:
            self.__columns__ = [x.strip() for x in fields.split(',')]
        elif type(fields) == list:
            self.__columns__ = fields
        return self

    async def insert(self):
        if not self._connection:
            self.get_connection(self)
        async with await self._connection.connection() as conn:
            table = '{schema}.{table}'.format(table=self.Meta.name, schema=self.Meta.schema)
            cols = []
            source = []
            pk = []
            for name, field in self.columns():
                column = field.name
                datatype = field.type
                value = Entity.toSQL(getattr(self, field.name), datatype)
                source.append(value)
                cols.append(column)
                if field.primary_key is True:
                    pk.append(column)
            try:
                primary = 'RETURNING {}'.format(','.join(pk)) if pk else ''
                columns = ','.join(cols)
                values = ','.join(map(str, [Entity.escapeLiteral(v, type(v)) for v in source]))
                insert = f'INSERT INTO {table} ({columns}) VALUES({values}) {primary}'
                result = await conn.get_connection().fetchrow(insert)
                if result:
                    # setting the values dynamically from returning
                    for f in pk:
                        setattr(self, f, result[f])
                return result
            except Exception as err:
                print(traceback.format_exc())
                raise Exception('Error on Insert over table {}: {}'.format(self.Meta.name, err))

    async def save(self, **kwargs):
        if not self._connection:
            self.get_connection(self)
        async with await self._connection.connection() as conn:
            table = f'{self.Meta.schema}.{self.Meta.name}'
            source = []
            pk = {}
            cols = []
            for name, field in self.columns():
                column = field.name
                datatype = field.type
                value = Entity.toSQL(getattr(self, field.name), datatype)
                source.append('{} = {}'.format(name, Entity.escapeLiteral(value, datatype)))
                cols.append(column)
                if field.primary_key is True:
                    pk[column] = value
            # TODO: work in an "update, delete, insert" functions on asyncdb to abstract data-insertion
            sql = 'UPDATE {table} SET {set_fields} {condition}'
            condition = self.where(**pk)
            sql = sql.format_map(SafeDict(table=table))
            sql = sql.format_map(SafeDict(condition=condition))
            # set the columns
            values = ', '.join(source)
            sql = sql.format_map(SafeDict(set_fields=values))
            try:
                result = await conn.get_connection().execute(sql)
                return result
            except Exception as err:
                print(traceback.format_exc())
                raise Exception('Error on Insert over table {}: {}'.format(self.Meta.name, err))

    async def delete(self, **kwargs):
        """
        Deleting a row Model based on Primary Key
        """
        if not self._connection:
            self.get_connection(self)
        result = None
        async with await self._connection.connection() as conn:
            table = f'{self.Meta.schema}.{self.Meta.name}'
            source = []
            pk = {}
            cols = []
            for name, field in self.columns():
                column = field.name
                datatype = field.type
                value = Entity.toSQL(getattr(self, field.name), datatype)
                if field.primary_key is True:
                    pk[column] = value
            # TODO: work in an "update, delete, insert" functions on asyncdb to abstract data-insertion
            sql = 'DELETE FROM {table} {condition}'
            condition = self.where(**pk)
            sql = sql.format_map(SafeDict(table=table))
            sql = sql.format_map(SafeDict(condition=condition))
            try:
                result = await conn.get_connection().execute(sql)
                # DELETE 1
            except Exception as err:
                print(traceback.format_exc())
                raise Exception('Error on Insert over table {}: {}'.format(self.Meta.name, err))
        return result

    def get_connection(self):
        driver = self.Meta.driver if self.Meta.driver else None
        if driver:
            print('Getting data from Database: {}'.format(driver))
            # working with app labels
            try:
                app = self.Meta.app_label if self.Meta.app_label else None
            except AttributeError:
                app = None
            if app:
                # TODO: get formula to got app database list
                # db = DATABASES[app]
                db = {}
                try:
                    params = {
                        'user': db['USER'],
                        'password': db['PASSWORD'],
                        'host': db['HOST'],
                        'port': db['PORT'],
                        'database': db['NAME']
                    }
                    if 'SCHEMA' in db:
                        params['schema'] = db['SCHEMA']
                except KeyError:
                    pass
            elif self.Meta.credentials:
                params = self.Meta.credentials
            self._connection = AsyncDB(driver, params=params)

    def where(self, **where):
        """
        TODO: add conditions for BETWEEN, NOT NULL, NULL, etc
        """
        result = ''
        if not where:
            return result
        elif type(where) == str:
            result = 'WHERE {}'.format(where)
        elif type(where) == dict:
            where_cond = []
            for key, value in where.items():
                f = self._columns[key]
                datatype = f.type
                if value is None or value == 'null' or value == 'NULL':
                    where_cond.append(f'{key} is NULL')
                elif value == '!null' or value == '!NULL':
                    where_cond.append(f'{key} is NOT NULL')
                elif type(value) == bool:
                    val = str(value)
                    where_cond.append(f'{key} is {value}')
                elif isinstance(datatype, List):
                    val = ', '.join(map(str, [Entity.escapeLiteral(v, type(v)) for v in value]))
                    where_cond.append(f'ARRAY[{val}]<@ {key}::character varying[]')
                elif Entity.is_array(datatype):
                    val = ', '.join(map(str, [Entity.escapeLiteral(v, type(v)) for v in value]))
                    where_cond.append(f'{key} IN ({val})')
                else:
                    # is an scalar value
                    val = Entity.escapeLiteral(value, datatype)
                    where_cond.append(f'{key}={val}')
            result = '\nWHERE %s' % (' AND '.join(where_cond))
            return result
        else:
            return result

    # classmethods for Data
    @classmethod
    async def get(cls, **kwargs):
        if not cls._connection:
            cls.get_connection(cls)
        async with await cls._connection.connection() as conn:
            table = '{schema}.{table}'.format(table=cls.Meta.name, schema=cls.Meta.schema)
            columns = ', '.join(cls._columns)
            condition = cls.where(cls, **kwargs)
            sql = f'SELECT {columns} FROM {table} {condition}'
            result, error = await conn.queryrow(sql)
            if error:
                raise Exception(error)
            if not result:
                raise NoDataFound('{} object with condition {} Not Found!'.format(table, condition))
            else:
                return cls(**dict(result))


    @classmethod
    async def filter(cls, **kwargs):
        """
        Need to return a ***collection*** of nested DataClasses
        """
        if not cls._connection:
            cls.get_connection(cls)
        async with await cls._connection.connection() as conn:
            table = '{schema}.{table}'.format(table=cls.Meta.name, schema=cls.Meta.schema)
            columns = ', '.join(cls._columns)
            condition = cls.where(cls, **kwargs)
            sql = f'SELECT {columns} FROM {table} {condition}'
            result, error = await conn.query(sql)
            if error:
                raise Exception(error)
            if not result:
                raise NoDataFound('{} object with condition {} Not Found!'.format(table, condition))
            else:
                return [cls(**dict(r)) for r in result]

    @classmethod
    async def remove(cls, **kwargs):
        if not cls._connection:
            cls.get_connection(cls)
        async with await cls._connection.connection() as conn:
            await conn.test_connection()
            prepared, error = await conn.prepare('SELECT * FROM walmart.stores')
            print(conn.get_columns())

    @classmethod
    async def update(cls, conditions: dict = {}, **kwargs):
        if not cls._connection:
            cls.get_connection(cls)
        async with await cls._connection.connection() as conn:
            await conn.test_connection()
            prepared, error = await conn.prepare('SELECT * FROM walmart.stores')
            print(conn.get_columns())

    @classmethod
    async def create(cls, **kwargs):
        if not cls._connection:
            cls.get_connection(cls)
        async with await cls._connection.connection() as conn:
            await conn.test_connection()
            prepared, error = await conn.prepare('SELECT * FROM walmart.stores')
            print(conn.get_columns())

    async def close(self):
        if self._connection:
            await self._connection.close(wait=5)


    Meta = Meta


def Column(*,
    default: Any = None,
    init: bool = True,
    primary_key: bool = False,
    notnull: bool = False,
    required: bool = False,
    factory: Callable = MISSING,
    min: Union[int, float, Decimal] = None,
    max: Union[int, float, Decimal] = None,
    validation: Callable = None,
    **kwargs
):
    """
    Column Function that returns a Field() object
    """
    if default is not None and factory is not MISSING:
        raise ValueError('Cannot specify both default and default_factory')
    return Field(
        default=default,
        init=init,
        primary_key=primary_key,
        notnull=notnull,
        required=required,
        factory=factory,
        min=min,
        max=max,
        validation=validation,
        **kwargs
    )

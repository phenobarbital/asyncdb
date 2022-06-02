"""
Basic, Abstract Model.
"""
from __future__ import annotations

import types
import logging
import inspect
import traceback
import operator
import rapidjson as to_json
from dataclasses import Field as ff
from dataclasses import (
    dataclass,
    is_dataclass,
    _FIELD,
    fields,
    asdict,
    MISSING,
    InitVar,
    make_dataclass,
)
from decimal import Decimal
from typing import (
    Callable,
    List,
    Dict,
    Optional,
    Union,
    Any,
    Iterable
)
from asyncdb.utils import Msg
from asyncdb.utils.types import (
    DB_TYPES,
    MODEL_TYPES,
    JSON_TYPES
)
from asyncdb.exceptions import NoDataFound, ProviderError, StatementError
from asyncdb.utils.encoders import DefaultEncoder
from asyncdb.utils import module_exists
from asyncdb.providers.interfaces import ConnectionBackend


@dataclass
class ValidationError:
    """
    Class for Error validation
    """
    field: str
    value: Optional[Union[str, Any]]
    error: str
    value_type: Any
    annotation: type
    exception: Optional[Exception]


class Meta:
    name: str = ""
    schema: str = ""
    app_label: str = ""
    frozen: bool = False
    strict: bool = True
    driver: str = None
    credentials: dict = {}
    dsn: str = ""
    datasource: str = ""
    connection = None


def set_connection(cls, conn: Callable):
    cls.connection = conn


class Field(ff):
    """
    Field.
    description: Extending Field/Column definition from Dataclass Field
    """
    description: Optional[str] = ''

    def __init__(
        self,
        default: Any = None,
        init: Optional[bool] = True,
        primary_key: Optional[bool] = False,
        notnull: Optional[bool] = False,
        required: Optional[bool] = False,
        factory: Callable[..., Any] = None,
        min: Union[int, float, Decimal] = None,
        max: Union[int, float, Decimal] = None,
        validator: Optional[Callable] = None,
        db_type: str = None,
        **kwargs,
    ):

        args = {
            "init": True,
            "repr": True,
            "hash": True,
            "compare": True,
            "metadata": None,
        }
        if "compare" in kwargs:
            args["compare"] = kwargs["compare"]
            del kwargs["compare"]
        meta = {
            "required": required,
            "primary_key": primary_key,
            "validation": None
        }
        self._required = required
        self._pk = primary_key
        self._nullable = not required
        if 'description' in kwargs:
            self.description = kwargs['description']
        range = {}
        if min is not None:
            range["min"] = min
        if max is not None:
            range["max"] = max
        if required is True or primary_key is True:
            args["init"] = True
        else:
            if "init" in kwargs:
                args["init"] = kwargs["init"]
                del kwargs["init"]
            # else:
            #     args['init'] = False
        if validator is not None:
            meta["validation"] = validator
        if "metadata" in kwargs:
            meta = {**meta, **kwargs["metadata"]}
            del kwargs["metadata"]
        args["metadata"] = {**meta, **range, **kwargs}
        self._default_factory = MISSING
        if default is not None:
            self._default = default
        else:
            self._default = None
            if notnull is False:
                if not factory:
                    factory = MISSING
                # get the annotation of field
                self._default_factory = factory
        super().__init__(
            default=self._default,
            default_factory=self._default_factory,
            **args
        )
        # set field type and dbtype
        self._field_type = _FIELD
        self._dbtype = db_type

    def __repr__(self):
        return (
            "Field("
            f"column={self.name!r},"
            f"type={self.type!r},"
            f"default={self.default!r})"
        )

    def required(self):
        return self._required

    def get_dbtype(self):
        return self._dbtype

    def db_type(self):
        if self._dbtype is not None:
            if self._dbtype == "array":
                t = DB_TYPES[self.type]
                return f"{t}[]"
            else:
                return self._dbtype
        else:
            return DB_TYPES[self.type]

    @property
    def primary_key(self):
        return self._pk


def Column(
    *,
    default: Any = None,
    init: Optional[bool] = True,
    primary_key: Optional[bool] = False,
    notnull: Optional[bool] = False,
    required: Optional[bool] = False,
    factory: Callable[..., Any] = None,
    min: Union[int, float, Decimal] = None,
    max: Union[int, float, Decimal] = None,
    validator: Optional[Callable] = None,
    db_type: str = None,
    **kwargs,
):
    """
      Column.
      Function that returns a Field() object
    """
    if factory is None:
        factory = MISSING
    if default is not None and factory is not MISSING:
        raise ValueError(
            f"Cannot specify both default: {default} and factory: {factory}"
        )
    return Field(
        default=default,
        init=init,
        primary_key=primary_key,
        notnull=notnull,
        required=required,
        factory=factory,
        db_type=db_type,
        min=min,
        max=max,
        validator=validator,
        **kwargs,
    )


def _dc_method_setattr(
            self,
            name: str,
            value: Any,
            *args,
            **kwargs
        ) -> None:
    """
    _dc_method_setattr.
    method for overwrite the "setattr" of Dataclass.
    """
    if self.Meta.frozen is True and name not in self.__columns__:
        raise TypeError(
            f"Cannot Modify attribute {name} of {self.modelName}, "
            "This DataClass is frozen (read-only class)"
        )
    else:
        object.__setattr__(self, name, value)
        if name not in self.__columns__.keys():
            try:
                if self.Meta.strict is True:
                    Msg(
                        f"Warning: *{name}* doesn't exists on {self.modelName}",
                        "WARN",
                    )
                else:
                    # create a new Field on Model.
                    f = Field(required=False, default=value)
                    f.name = name
                    f.type = type(value)
                    self.__columns__[name] = f
                    setattr(self, name, value)
            except Exception as err:
                logging.exception(err)


def create_dataclass(
    new_cls: Any,
    repr: bool = True,
    eq: bool = True,
    validate: bool = True,
    frozen: bool = False,
    init: bool = True,
) -> Callable:
    """
    create_dataclass.
       Create a Dataclass from a simple Class
    """
    # TODO: can build more complex dataclasses using make dataclass function
    dc = dataclass(unsafe_hash=True, init=True, frozen=frozen)(new_cls)
    # TODO: add method for __post_init__
    # __class__ = dc
    setattr(dc, "__setattr__", _dc_method_setattr)
    # adding a properly internal json encoder:
    dc.__encoder__ = DefaultEncoder(
        sort_keys=False
    )
    return dc


"""
Meta-Classes.
"""


class ModelMeta(type):
    """
    ModelMeta.
    MetaClass object to create dataclasses for modeling Data Models.
    """
    __fields__: Dict[str, Field]

    def __new__(cls, name, bases, attrs, **kwargs):
        """__new__ is a classmethod, even without @classmethod decorator"""
        if len(bases) > 1:
            raise TypeError(
                "Multiple inheritance of AsyncDB data Models are forbidden"
            )
        if "__annotations__" in attrs:
            annotations = attrs["__annotations__"]
            cols = []
            for field, type in annotations.items():
                # logging.debug(f"Field: {field}, Type: {type}")
                if field in attrs:
                    df = attrs[field]
                    if isinstance(df, Field):
                        setattr(cls, field, df)
                    else:
                        f = Field(factory=type, required=False, default=df)
                        f.name = field
                        f.type = type
                        setattr(cls, field, f)
                else:
                    # add a new field, based on type
                    f = Field(factory=type, required=False)
                    f.name = field
                    f.type = type
                    setattr(cls, field, f)
                cols.append(field)
            # set the slots of this class
            cls.__slots__ = tuple(cols)
        attr_meta = attrs.pop("Meta", None)
        new_cls = super().__new__(cls, name, bases, attrs, **kwargs)
        new_cls.Meta = attr_meta or getattr(new_cls, "Meta", Meta)
        if not new_cls.Meta:
            new_cls.Meta = Meta
        new_cls.Meta.set_connection = types.MethodType(
            set_connection, new_cls.Meta
        )
        frozen = False
        # adding a "class init method"
        try:
            new_cls.__model_init__(
                new_cls,
                name,
                attrs
            )
        except AttributeError:
            pass
        try:
            # TODO: mix values from Meta to an existing Meta Class
            try:
                if not new_cls.Meta.schema:
                    new_cls.Meta.schema = "public"
            except AttributeError:
                new_cls.Meta.schema = "public"
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
        dc = create_dataclass(
            new_cls,
            frozen=frozen,
            init=True
        )
        cols = {
            k: v
            for k, v in dc.__dict__["__dataclass_fields__"].items()
            if v._field_type == _FIELD
        }
        dc.__columns__ = cols
        dc.__fields__ = cols.keys()
        return dc

    def __init__(cls, *args, **kwargs) -> None:
        cls.modelName = cls.__name__
        ls = cls.Meta.__dict__
        if "dsn" not in ls:
            cls.Meta.dsn = None
        if "connection" not in ls:
            cls.Meta.connection = None
        if "datasource" not in ls:
            cls.Meta.datasource = None
        if cls.Meta.strict:
            cls.__frozen__ = cls.Meta.strict
        else:
            cls.__frozen__ = False
        # Initialized Data Model = True
        cls.__initialised__ = True
        if cls.Meta.driver is not None:
            if cls.Meta.connection is None:
                try:
                    cls.get_connection(cls)
                except Exception as err:
                    logging.exception(f'Error getting Connection: {err!s}')
        super(ModelMeta, cls).__init__(*args, **kwargs)


class Model(metaclass=ModelMeta):
    """
    Model.

    BaseModel for all DataClasses.
    """

    def __post_init__(self) -> None:
        """
        Fill fields with function-factory or calling validations
        """
        # checking if an attribute is already a dataclass:
        for name, f in self.__columns__.items():
            value = getattr(self, f.name)
            if is_dataclass(f.type):
                if isinstance(value, dict):
                    new_val = f.type(**value)
                    setattr(self, f.name, new_val)
            elif isinstance(value, list):
                try:
                    sub_type = f.type.__args__[0]
                    if is_dataclass(sub_type):
                        # for every item
                        items = []
                        for item in value:
                            try:
                                if isinstance(item, dict):
                                    items.append(sub_type(**item))
                                else:
                                    items.append(item)
                            except Exception as err:
                                logging.exception(err)
                                continue
                        setattr(self, f.name, items)
                except AttributeError:
                    setattr(self, f.name, value)
            else:
                continue
        try:
            self._validation()
        except Exception as err:
            logging.exception(err)

    def _validation(self) -> None:
        """
        _validation.
        TODO: cover validations as length, not_null, required, etc
        """
        errors = {}
        for name, field in self.columns().items():
            key = field.name
            val = self.__dict__[key]
            if hasattr(field, 'default') and callable(val):
                # default is a function:
                try:
                    setattr(self, name, field.default())
                except TypeError as err:
                    logging.warning(
                        f'Missing *Column* declaration {field} with name {name}')
                    setattr(self, name, None)
            # first check: data type hint
            val_type = type(val)
            annotated_type = field.type
            if val_type == "type" or val == annotated_type or val is None:
                # data not provided
                try:
                    if field.metadata["required"] is True \
                            and self.Meta.strict is True:
                        errors[key] = ValidationError(
                            field=key,
                            value=None,
                            value_type=val_type,
                            error="Field Required",
                            annotation=annotated_type,
                            exception=None,
                        )
                except KeyError:
                    continue
                continue
            else:
                # print(key, val, annotated_type)
                try:
                    instance = self._is_instanceof(val, annotated_type)
                    # first: check primitives
                    if instance["result"]:
                        # is valid
                        continue
                    else:
                        # TODO check for complex types
                        # adding more complex validations
                        continue
                except Exception as err:
                    errors[key] = ValidationError(
                        field=key,
                        value=val,
                        error="Validation Exception",
                        value_type=val_type,
                        annotation=annotated_type,
                        exception=err,
                    )
            # second check: length,
            # third validation: formats and patterns
            # fourth validation: function-based validators
        if errors:
            print("=== ERRORS ===")
            print(errors)
            object.__setattr__(self, "__valid__", False)
        else:
            object.__setattr__(self, "__valid__", True)

    def _is_instanceof(self, value: Any, annotated_type: type) -> Dict:
        result = False
        exception = None
        try:
            result = isinstance(value, annotated_type)
        except Exception as err:
            exception = err
            result = False
        finally:
            return {"result": result, "exception": exception}

    def __unicode__(self):
        return str(__class__)

    def columns(self):
        return self.__columns__

    def get_fields(self):
        return self.__fields__

    def column(self, name):
        return self.__columns__[name]

    def dict(self):
        return asdict(self)

    def json(self, **kwargs):
        encoder = self.__encoder__
        if len(kwargs) > 0:
            encoder = DefaultEncoder(sort_keys=False, **kwargs)
        return encoder(asdict(self))

    def is_valid(self):
        return bool(self.__valid__)

    def create_field(self, name: str, value: Any) -> None:
        """create_field.
        create a new Field on Model (when strict is False).
        Args:
            name (str): name of the field
            value (Any): value to be assigned.
        """
        f = Field(required=False, default=value)
        f.name = name
        f.type = type(value)
        self.__columns__[name] = f
        setattr(self, name, value)

    def set(self, name: str, value: Any) -> None:
        """set.
        Alias for Create Field.
        Args:
            name (str): name of the field
            value (Any): value to be assigned.
        """
        if name not in self.__columns__:
            self.create_field(name, value)
        else:
            setattr(self, name, value)

    def set_connection(self, connection: "ConnectionBackend") -> None:
        """
        Manually Set the connection of Dataclass.
        """
        try:
            self.Meta.connection = connection
        except Exception as err:
            raise Exception(err)

    def get_connection(self) -> "ConnectionBackend":
        """
        Getting a database connection and driver based on parameters
        """
        Msg(':: Getting Connection ::', 'DEBUG')
        if self.Meta.datasource:
            # TODO: making a connection using a DataSource.
            pass
        if self.Meta.driver:
            driver = self.Meta.driver
            provider = f"asyncdb.providers.{driver}"
            try:
                obj = module_exists(driver, provider)
            except Exception:
                raise
            if self.Meta.dsn is not None:
                try:
                    self.Meta.connection = obj(dsn=self.Meta.dsn)
                except Exception as err:
                    logging.exception(err)
            elif hasattr(self.Meta, "credentials"):
                params = self.Meta.credentials
                try:
                    self.Meta.connection = obj(params=params)
                except Exception as err:
                    logging.exception(err)
        return self.Meta.connection

    async def close(self):
        """
        Closing an existing database connection.
        """
        try:
            await self.Meta.connection.close()
        except Exception as err:
            logging.exception(err)

    """
    Instance method for Dataclasses.
    """
    async def save(self, **kwargs):
        """
        Saving a Dataclass Model to Database.
        """
        if not self.Meta.connection:
            self.get_connection(self)
        async with await self.Meta.connection.connection() as conn:
            try:
                result = await self.Meta.connection.model_save(
                    model=self, fields=self.columns()
                )
                return result
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    "Error on Insert over table {}: {}".format(
                        self.Meta.name, err)
                )

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
            except StatementError as err:
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
            self.get_connection(self)
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
            self.get_connection(self)
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

    get = fetch

    async def select(self, **kwargs):
        """
        Need to return a ***collection*** of nested DataClasses
        """
        if not self.Meta.connection:
            self.get_connection(self)
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

    async def all(self, **kwargs):
        """
        Need to return all rows as a ***collection*** of nested DataClasses
        """
        if not self.Meta.connection:
            self.get_connection(self)
        async with await self.Meta.connection.connection() as conn:
            try:
                result = await self.Meta.connection.model_all(
                    model=self, fields=self.columns()
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

    """
    Class-based methods for Dataclasses.
    """
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
                    "Error Updating Table {}: {}".format(cls.Meta.name, err)
                )

    @classmethod
    async def remove(cls, conditions: dict = {}, **kwargs):
        if not cls.Meta.connection:
            cls.get_connection(cls)
        async with await cls.Meta.connection.connection() as conn:
            result = []
            try:
                result = await cls.Meta.connection.mdl_delete(
                    model=cls, conditions=conditions, **kwargs
                )
                return result
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                logging.debug(traceback.format_exc())
                raise Exception(
                    "Error Deleting Table {}: {}".format(cls.Meta.name, err)
                )

    @classmethod
    async def update(cls, conditions: dict = {}, **kwargs):
        if not cls.Meta.connection:
            cls.get_connection(cls)
        async with await cls.Meta.connection.connection() as conn:
            try:
                result = await cls.Meta.connection.mdl_update(
                    model=cls, conditions=conditions, **kwargs
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
                    "Error Updating Table {}: {}".format(cls.Meta.name, err)
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
                    "Error on filter {}: {}".format(cls.Meta.name, err))

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
                    "Error on get {}: {}".format(cls.Meta.name, err))
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                print(traceback.format_exc())
                raise Exception(
                    "Error on get {}: {}".format(cls.Meta.name, err))

    # get all data
    @classmethod
    async def all(cls, **kwargs):
        if not cls.Meta.connection:
            cls.get_connection(cls)
        async with await cls.Meta.connection.connection() as conn:
            try:
                result = await cls.Meta.connection.mdl_all(model=cls, **kwargs)
                return [cls(**dict(row)) for row in result]
            except (StatementError, ProviderError):
                raise
            except Exception as err:
                print(traceback.format_exc())
                raise Exception(
                    "Error on query_all over table {}: {}".format(
                        cls.Meta.name, err)
                )
    """
    Class-based methods for dataclass creation, model creation.
    """
    @classmethod
    def model(cls, dialect: str = "json") -> Any:
        result = None
        clsname = cls.__name__
        schema = cls.Meta.schema if cls.Meta.schema is not None else ""
        table = cls.Meta.name if cls.Meta.name is not None else clsname.lower()
        columns = cls.columns(cls).items()
        if dialect == 'json':
            cols = {}
            for name, field in columns:
                key = field.name
                type = field.type
                cols[key] = {"name": key, "type": JSON_TYPES[type]}
            doc = {
                "name": name,
                "description": cls.__doc__.strip("\n").strip(),
                "schema": schema,
                "fields": cols,
            }
            print(doc)
            result = to_json.dumps(doc)
        return result

    @classmethod
    def make_model(cls, name: str, schema: str = "public", fields: list = []):
        parent = inspect.getmro(cls)
        cls = make_dataclass(name, fields, bases=(parent[0],))
        m = Meta()
        m.name = name
        m.schema = schema
        m.app_label = schema
        cls.Meta = m
        return cls

    @classmethod
    async def makeModel(
        cls,
        name: str,
        schema: str = "public",
        fields: list = [],
        db: "ConnectionBackend" = None,
    ):
        """
        Make Model.

        Making a model from field tuples, a JSON schema or a Table.
        """
        tablename = "{}.{}".format(schema, name)
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
        cls = make_dataclass(name, fields, bases=(parent[0],))
        m = Meta()
        m.name = name
        m.schema = schema
        m.app_label = schema
        m.connection = db
        m.frozen = False
        cls.Meta = m
        return cls

    Meta = Meta
    Meta = Meta

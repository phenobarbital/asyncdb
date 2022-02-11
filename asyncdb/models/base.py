"""
Basic, Abstract Model.
"""
import types
import logging
from .types import DB_TYPES
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
    ClassVar
)
from asyncdb.utils import Msg
from asyncdb.utils.encoders import (
    DefaultEncoder
)


class Meta:
    name: str = ""
    schema: str = ""
    app_label: str = ""
    frozen: bool = False
    strict: bool = True
    driver: str = ""
    credentials: dict = {}
    dsn: str = ""
    datasource: str = ""
    connection = None


def set_connection(cls, conn: Callable):
    cls.connection = conn


@dataclass
class ValidationError:
    """
    Class for Error validation
    """
    field: str
    value: str
    error: str
    value_type: str
    annotation: type
    exception: Optional[Exception]
    errors: List = []


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
        if default is not None:
            self._default = default
            self._default_factory = MISSING
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

    @property
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
    if default is not None and factory is not MISSING:
        raise ValueError("Cannot specify both default and default_factory")
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
    if self.Meta.frozen is True and name not in self._columns:
        raise TypeError(
            f"Cannot Modify attribute {name} of {self.modelName}, "
            "This DataClass is frozen (read-only class)"
        )
    else:
        object.__setattr__(self, name, value)
        if name not in self._columns.keys():
            try:
                if self.Meta.strict is True:
                    Msg(
                        "Warning: *{name}* doesn't exists on {self.modelName}",
                        "WARN",
                    )
                else:
                    # create a new Field on Model.
                    f = Field(required=False, default=value)
                    f.name = name
                    f.type = type(value)
                    self._columns[name] = f
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
    Meta: ClassVar[Any] = Meta

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
                logging.debug(f"Field: {field}, Type: {type}")
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
        # print(dc._columns)
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
        if "driver" in ls:
            if cls.Meta.connection is None:
                Msg(':: Getting Connection ::', 'DEBUG')
                try:
                    cls.get_connection(cls, dsn=cls.Meta.dsn)
                except Exception as err:
                    logging.exception(f'Error getting Connection: {err!s}')
        super(ModelMeta, cls).__init__(*args, **kwargs)

"""
Basic, Abstract Model.
"""
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
    Optional,
    Union,
    Any
)


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

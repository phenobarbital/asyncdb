import uuid
from decimal import Decimal
from typing import (
    Dict
)
import datetime
from collections.abc import Sequence
import json
from numpy import int64, ndarray
from asyncdb.utils.encoders import (
    BaseEncoder
)

# TODO: migrate encoder to Cython

class Entity:
    @classmethod
    def number(cls, _type):
        return _type in (int, int64, float, Decimal, bytes, bool)

    @classmethod
    def string(cls, _type):
        return _type in (
            str,
            datetime.datetime,
            datetime.time,
            datetime.timedelta,
            uuid.UUID,
        )

    @classmethod
    def is_date(cls, _type):
        return _type in (datetime.datetime, datetime.time, datetime.timedelta)

    @classmethod
    def is_array(cls, t):
        return isinstance(t,(list, dict, Sequence, ndarray))

    @classmethod
    def is_bool(cls, _type):
        return isinstance(_type, bool)

    @classmethod
    def escapeLiteral(cls, value, ftype, dbtype: str = None):
        # print(value, ftype, dbtype)
        v = value if value != "None" or value is not None else ""
        v = f"{value!r}" if Entity.string(ftype) else v
        v = value if Entity.number(ftype) else f"{value!r}"
        v = f"array{value!s}" if dbtype == "array" else v
        v = f"{value!r}" if dbtype == "hstore" else v
        v = value if (value in ["None", "null", "NULL"]) else v
        return v

    @classmethod
    def toSQL(cls, value, ftype, dbtype: str = None):
        v = f"{value!r}" if Entity.is_date(ftype) else value
        v = f"{value!s}" if Entity.string(ftype) and value is not None else v
        v = value if Entity.number(ftype) else v
        v = str(value) if isinstance(value, uuid.UUID) else v
        v = json.dumps(value, cls=BaseEncoder) if ftype in [dict, Dict] else v
        v = f"{value!s}" if dbtype == "array" and value is not None else v
        # formatting htstore column
        v = (
            ",".join({"{}=>{}".format(k, v) for k, v in value.items()})
            if isinstance(value, dict) and dbtype == "hstore"
            else v
        )
        v = "NULL" if (value in ["None", "null"]) else v
        v = "NULL" if value is None else v
        return v

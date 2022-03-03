import uuid
import json
import numpy as np
from decimal import Decimal
from typing import (
    Dict,
    List
)
import datetime
import collections
from asyncdb.utils.encoders import (
    BaseEncoder
)

DB_TYPES: Dict = {
    bool: "boolean",
    int: "integer",
    np.int64: "bigint",
    float: "float",
    str: "character varying",
    bytes: "byte",
    list: "Array",
    Decimal: "numeric",
    datetime.date: "date",
    datetime.datetime: "timestamp without time zone",
    datetime.time: "time",
    datetime.timedelta: "timestamp without time zone",
    uuid.UUID: "uuid",
    dict: "jsonb",
    Dict: "jsonb",
    List: "jsonb",
}

MODEL_TYPES = {
    "boolean": bool,
    "integer": int,
    "bigint": np.int64,
    "float": float,
    "character varying": str,
    "string": str,
    "varchar": str,
    "byte": bytes,
    "bytea": bytes,
    "Array": list,
    "hstore": dict,
    "character varying[]": list,
    "numeric": Decimal,
    "date": datetime.date,
    "timestamp with time zone": datetime.datetime,
    "time": datetime.time,
    "timestamp without time zone": datetime.datetime,
    "uuid": uuid.UUID,
    "json": dict,
    "jsonb": dict,
    "text": str,
    "serial": int,
    "bigserial": int,
    "inet": str,
}

JSON_TYPES = {
    bool: "boolean",
    int: "integer",
    np.int64: "integer",
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
    uuid.UUID: "uuid",
}


class Entity:
    @classmethod
    def number(cls, type):
        return type in (int, np.int64, float, Decimal, bytes, bool)

    @classmethod
    def string(cls, type):
        return type in (
            str,
            datetime.datetime,
            datetime.time,
            datetime.timedelta,
            uuid.UUID,
        )

    @classmethod
    def is_date(cls, type):
        return type in (datetime.datetime, datetime.time, datetime.timedelta)

    @classmethod
    def is_array(cls, t):
        return isinstance(t,
                          (list, List, Dict, dict,
                           collections.Sequence, np.ndarray)
                          )

    @classmethod
    def is_bool(cls, type):
        return isinstance(type, bool)

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

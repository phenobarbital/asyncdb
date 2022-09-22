# cython: language_level=3, embedsignature=True, boundscheck=False, wraparound=True, initializedcheck=False
# Copyright (C) 2018-present Jesus Lara
#
import time
import orjson
from typing import (
    Dict
)
from collections.abc import Sequence
from decimal import Decimal
from numpy import int64, ndarray
from cpython cimport datetime
from uuid import UUID


cpdef object strtobool (str val):
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return True
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return False
    else:
        raise ValueError(
            f"invalid truth value for {val}"
        )

cdef class Entity:
    @classmethod
    def is_number(cls, _type):
        return _type in (int, int64, float, Decimal, bytes, bool)

    @classmethod
    def is_string(cls, _type):
        return isinstance(_type, (
            str,
            datetime.datetime,
            datetime.time,
            datetime.timedelta,
            UUID
        ))

    @classmethod
    def is_date(cls, _type):
        return _type in (datetime.datetime, time, datetime.time, datetime.timedelta)

    @classmethod
    def is_array(cls, t):
        return isinstance(t,(list, dict, Sequence, ndarray))

    @classmethod
    def is_bool(cls, _type):
        return isinstance(_type, bool)

    @classmethod
    def toSQL(cls, value, _type, dbtype: str = None):
        v = f"{value!r}" if Entity.is_date(_type) else value
        v = f"{value!s}" if Entity.is_string(_type) and value is not None else v
        v = value if Entity.is_number(_type) else v
        v = str(value) if isinstance(value, UUID) else v
        # json conversion
        v = orjson.dumps(value) if _type in [dict, Dict] else v
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

    @classmethod
    def escapeLiteral(cls, value, _type, dbtype: str = None):
        v = value if value != "None" or value is not None else ""
        v = f"{value!r}" if Entity.is_string(_type) else v
        v = value if Entity.is_number(_type) else f"{value!r}"
        v = f"array{value!s}" if dbtype == "array" else v
        v = f"{value!r}" if dbtype == "hstore" else v
        v = value if (value in ["None", "null", "NULL"]) else v
        return v

    @classmethod
    def escapeString(cls, value):
        v = value if value != "None" else ""
        v = str(v).replace("'", "''")
        v = "'{}'".format(v) if Entity.is_string(type(value)) else v
        return v

    @classmethod
    def quoteString(cls, value):
        v = value if value != "None" else ""
        v = "'{}'".format(v) if type(v) == str else v
        return v

class SafeDict(dict):
    """
    SafeDict.

    Allow to using partial format strings

    """

    def __missing__(self, key):
        """Missing method for SafeDict."""
        return "{" + key + "}"

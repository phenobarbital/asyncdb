import json
import decimal
from typing import Any, Union
from datetime import datetime
from decimal import Decimal
from enum import Enum
from dataclasses import _MISSING_TYPE, MISSING
from asyncpg import Range
from numpy import ndarray
import orjson

class DateEncoder(json.JSONEncoder):
    """
    DateEncoder.
       Date and Time encoder
    """
    def default(self, o):
        if isinstance(o, datetime):
            return str(o)
        elif hasattr(o, "isoformat"):
            return o.isoformat()
        else:
            return str(object=o)
        return json.JSONEncoder.default(self, o)


class NpEncoder(json.JSONEncoder):
    """
    npEncoder.

       Numpy number encoder for json
    """
    def default(self, o):
        if isinstance(o, np.integer):
            return int(o)
        elif isinstance(o, np.floating):
            return float(o)
        elif isinstance(o, np.ndarray):
            return o.tolist()
        else:
            return json.JSONEncoder.default(self, o)


class IntRangeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Range):
            up = o.upper
            if isinstance(o.upper, int):
                up = o.upper - 1  # discrete representation
            return [o.lower, up]
        else:
            return str(object=o)
        return json.JSONEncoder.default(self, o)


class pgRangeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Range):
            return [o.lower, o.upper]
        else:
            return str(object=o)
        return json.JSONEncoder.default(self, o)


class EnumEncoder(json.JSONEncoder):
    """
    EnumEncoder
    --------

    Used to format objects into json-strings
    """
    def default(self, o):
        """Format several data types into json-type equivalent.

        Return a new cls JSON EnumEncoder
        """
        if hasattr(o, "hex"):
            return o.hex
        elif isinstance(o, Enum):
            if not o.value:
                return None
            else:
                return str(o.value)
        else:
            return str(object=o)
        return json.JSONEncoder.default(self, o)


class DefaultEncoder:
    """
    Basic Encoder using orjson
    """
    def __init__(self, **kwargs):
        # eventually take into consideration when serializing
        self.options = kwargs

    def __call__(self, obj, **kwargs) -> Any:
        return self.encode(obj, **kwargs)

    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, Decimal):
            return str(obj)
        elif hasattr(obj, "isoformat"):
            return obj.isoformat()
        elif isinstance(obj, asyncpg.Range):
            return [obj.lower, obj.upper]
        elif hasattr(obj, "hex"):
            return obj.hex
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif obj == _MISSING_TYPE:
            return None
        elif obj == MISSING:
            return None
        raise TypeError(f"{obj!r} is not JSON serializable")

    def encode(self, obj, **kwargs) -> str:
        # decode back to str, as orjson returns bytes
        options = {
            "default": self.default,
            "option": orjson.OPT_NAIVE_UTC | orjson.OPT_SERIALIZE_NUMPY| orjson.OPT_UTC_Z
        }
        if kwargs:
            options = {**options, **kwargs}
        return orjson.dumps(
            obj,
            **options
        ).decode('utf-8')

    dumps = encode

    def decode(self, obj) -> Union[dict, list]:
        return orjson.loads(
            obj
        )

    loads = decode


class BaseEncoder:
    """
    Encoder replacement for json.dumps using orjson
    """

    def __init__(self, *args, **kwargs):
        encoder = DefaultEncoder(*args, **kwargs)
        self.encode = encoder.__call__

import json
from rapidjson import Encoder as JSONEncoder
import decimal
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
import asyncpg
import numpy as np


class DateEncoder(json.JSONEncoder):
    """
    DateEncoder.
       Date and Time encoder
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return str(obj)
        elif hasattr(obj, "isoformat"):
            return obj.isoformat()
        else:
            return str(object=obj)
        return json.JSONEncoder.default(self, obj)


class NpEncoder(json.JSONEncoder):
    """
    npEncoder.

       Numpy number encoder for json
    """

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return json.JSONEncoder.default(self, obj)


class IntRangeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, asyncpg.Range):
            up = obj.upper
            if isinstance(obj.upper, int):
                up = obj.upper - 1  # discrete representation
            return [obj.lower, up]
        else:
            return str(object=obj)
        return json.JSONEncoder.default(self, obj)


class pgRangeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, asyncpg.Range):
            return [obj.lower, obj.upper]
        else:
            return str(object=obj)
        return json.JSONEncoder.default(self, obj)


class EnumEncoder(json.JSONEncoder):
    """
    EnumEncoder
    --------

    Used to format objects into json-strings
    """

    def default(self, obj):
        """Format several data types into json-type equivalent.

        Return a new cls JSON EnumEncoder
        """
        if hasattr(obj, "hex"):
            return obj.hex
        elif isinstance(obj, Enum):
            if not obj.value:
                return None
            else:
                return str(obj.value)
        else:
            return str(object=obj)
        return json.JSONEncoder.default(self, obj)


class DefaultEncoder(JSONEncoder):
    """
    Basic Encoder using rapidjson
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return str(obj)
        elif isinstance(obj, timedelta):
            return obj.__str__()
        elif hasattr(obj, "hex"):
            return obj.hex
        elif isinstance(obj, Enum):
            if not obj.value:
                return None
            else:
                return str(obj.value)
        elif isinstance(obj, uuid.UUID):
            try:
                return str(obj)
            except Exception as e:
                return obj.hex
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, Decimal):
            return str(obj)
        elif hasattr(obj, "isoformat"):
            return obj.isoformat()
        elif isinstance(obj, asyncpg.Range):
            return [obj.lower, obj.upper]
        else:
            # return str(obj)
            raise TypeError("%r is not JSON serializable" % obj)


class BaseEncoder:
    """
    Encoder replacement for json.dumps using rapidjson
    """

    def __init__(self, *args, **kwargs):
        # Filter/adapt JSON arguments to RapidJSON ones
        rjargs = ()
        rjkwargs = {}
        encoder = DefaultEncoder(sort_keys=False, *rjargs, **rjkwargs)
        self.encode = encoder.__call__

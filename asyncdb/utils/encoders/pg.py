import json
from datamodel.parsers.encoders import DefaultEncoder
from asyncpg import Range
from numpy import (
    integer,
    int64,
    floating,
    ndarray
)


class pgRangeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Range):
            return [o.lower, o.upper]
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
            return json.JSONEncoder.default(self, o)


class DBEncoder(DefaultEncoder):
    """
    Basic Encoder for PostgreSQL
    """
    def default(self, obj):
        if isinstance(obj, Range):
            return [obj.lower, obj.upper]
        elif isinstance(obj, (integer, int64)):
            return int(obj)
        elif isinstance(obj, floating):
            return float(obj)
        elif isinstance(obj, ndarray):
            return obj.tolist()
        else:
            return super(DBEncoder, self).default(obj)

import os
import sys
from datetime import datetime
import json
import numpy as np
import asyncpg
import uuid
import decimal
from enum import Enum

class DateEncoder(json.JSONEncoder):
    """
    DateEncoder.
       Date and Time encoder
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return str(obj)
        elif hasattr(obj, 'isoformat'):
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
                up = obj.upper - 1 # discrete representation
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
        """Format several data types into json-type equivalent
        Return a new cls JSON EnumEncoder
        """
        if hasattr(obj, 'hex'):
            return obj.hex
        elif isinstance(obj, Enum):
            if not obj.value:
                return None
            else:
                return str(obj.value)
        else:
            return str(object=obj)
        return json.JSONEncoder.default(self, obj)

class DefaultEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'hex'):
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
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif isinstance(obj, asyncpg.Range):
            return [obj.lower, obj.upper]
        else:
            return str(object=obj)
        return json.JSONEncoder.default(self, obj)

""" asyncDB utils
Various functions for asyncdb
--------------------
EnumEncoder: used to encode some native postgresql datatypes into json format.

"""
import binascii
import datetime
import decimal
import hashlib
import json
import os
import time
import uuid
from datetime import timedelta

import dateutil.parser
from dateutil import parser
from dateutil.relativedelta import relativedelta

# from enumfields import Enum
# import redis
# from navigator.settings.settings import QUERYSET_REDIS


def generate_key():
    return binascii.hexlify(os.urandom(20)).decode()


# hash utilities
def get_hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


# date utilities
def current_year():
    return datetime.datetime.now().year


def current_month():
    return datetime.datetime.now().month


def today(mask="%m/%d/%Y"):
    return time.strftime(mask)


def truncate_decimal(value):
    head, sep, tail = value.partition(".")
    return head


def year(value):
    if value:
        try:
            newdate = dateutil.parser.parse(value)
            # dt = datetime.datetime.strptime(newdate.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
            return newdate.date().year
        except ValueError:
            dt = value[:-4]
            dt = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            return dt.date().year
    else:
        return None


def month(value):
    if value:
        try:
            newdate = dateutil.parser.parse(value)
            return newdate.date().month
        except ValueError:
            dt = value[:-4]
            dt = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            return dt.date().month
    else:
        return None


def fdom():
    return (datetime.datetime.now()).strftime("%Y-%m-01")


def ldom():
    return (datetime.datetime.now() + relativedelta(day=31)).strftime("%Y-%m-%d")


def now():
    return datetime.datetime.now()


def due_date(days=1):
    return datetime.datetime.now() + timedelta(days=days)


def yesterday():
    return (datetime.datetime.now() - timedelta(1)).strftime("%Y-%m-%d")


def isdate(value):
    try:
        parser.parse(value)
        return True
    except ValueError:
        return False


is_date = isdate


def isinteger(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def isnumber(value):
    try:
        complex(value)  # for int, long, float and complex
    except ValueError:
        return False
    return True


def is_string(value):
    if type(value) is str:
        return True
    else:
        return False


def is_uuid(value):
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False


def validate_type_uuid(value):
    try:
        uuid.UUID(value)
    except ValueError:
        pass


def is_boolean(value):
    if isinstance(value, bool):
        return True
    elif value == "null" or value == "NULL":
        return True
    elif value == "true" or value == "TRUE":
        return True
    else:
        return False


PG_CONSTANTS = ["CURRENT_DATE", "CURRENT_TIMESTAMP"]


def is_pgconstant(value):
    return value in PG_CONSTANTS


UDF = ["CURRENT_YEAR", "CURRENT_MONTH", "TODAY", "YESTERDAY", "FDOM", "LDOM"]


def is_udf(value, *args):
    if not isinteger(value):
        f = value.lower()
    else:
        f = value
    if value in UDF:
        return globals()[f](*args)
    else:
        return None


# PROGRAM_CONSTANTS = [ 'COMMISSION_DATE', 'POSTPAID_DATE', 'PREPAID_DATE', 'KPI_ACCESSORIES_DATE', 'OPERATIONAL_DATE', 'KPI_CE_DATE' ]
# def is_program_date(value, *args):
#     return value in PROGRAM_CONSTANTS
#
# def get_program_date(value, *args):
#     process = value.replace('_DATE', '')
#     cachedb = redis.StrictRedis.from_url(QUERYSET_REDIS)
#     if cachedb.exists(process):
#         opt = cachedb.get(process)
#         if opt:
#             return str(opt.decode("utf-8"))
#         else:
#             return yesterday()
#     else:
#         return yesterday()


# #TODO: use program.json information to fill program hierarchy
# def get_hierarchy(program):
#     if program == 'walmart' or program == 'retail':
#         hierarchy = [ 'division_id', 'region', 'market_id', 'store_id' ]
#     elif program == 'mso':
#         hierarchy = [ 'territory_id', 'region_id', 'district_id', 'store_id' ]
#     elif program == 'loreal':
#         hierarchy = [ 'customer_id', 'sold_to', 'ship_to', 'platform_code', 'pos_code' ]
#     elif program == 'apple' or program == 'next':
#         hierarchy = [ "cluster_id", "market_id", "store_id", "store_type", "store_designation" ]
#     else:
#         hierarchy = []
#     return hierarchy


class SafeDict(dict):
    """
    SafeDict
    --------
    Allow to using partial format strings

    """

    def __missing__(self, key):
        return "{" + key + "}"


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
        if hasattr(obj, "hex"):
            return obj.hex
        # elif isinstance(obj, Enum):
        #     if not obj.value:
        #         return None
        #     else:
        #         return str(obj.value)
        elif isinstance(obj, uuid.UUID):
            try:
                return str(obj)
            except Exception as err:
                print(err)
                return obj.hex
        elif isinstance(obj, decimal.Decimal):
            return str(obj)
        elif hasattr(obj, "isoformat"):
            return obj.isoformat()
        else:
            try:
                if obj.value:
                    return str(obj.value)
            except (AttributeError, TypeError):
                pass
            return str(object=obj)
        return json.JSONEncoder.default(self, obj)

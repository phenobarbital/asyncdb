""" asyncDB utils.
Various functions for asyncdb
"""
import os
import binascii
import hashlib
import time
import datetime
from datetime import timedelta
import dateutil.parser
from dateutil import parser
from dateutil.relativedelta import relativedelta
from typing import Callable
import builtins
import redis

CACHE_HOST = os.getenv('CACHEHOST', default='localhost')
CACHE_PORT = os.getenv('CACHEPORT', default=6379)
CACHE_URL = "redis://{}:{}".format(CACHE_HOST, CACHE_PORT)
CACHE_DB = os.getenv('QUERYSET_DB', default=0)
QUERY_VARIABLES = f'redis://{CACHE_HOST}:{CACHE_PORT}/{CACHE_DB}'


class SafeDict(dict):
    """
    SafeDict.

    Allow to using partial format strings

    """
    def __missing__(self, key):
        """Missing method for SafeDict."""
        return '{' + key + '}'

def generate_key():
    return binascii.hexlify(os.urandom(20)).decode()


# hash utilities
def get_hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def truncate_decimal(value):
    head, sep, tail = value.partition(".")
    return head


"""
Date-time Functions
"""
# date utilities
def current_year():
    return datetime.datetime.now().year


def current_month():
    return datetime.datetime.now().month


def today(mask="%m/%d/%Y"):
    return time.strftime(mask)

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

"""
Validation and data-type functions
"""

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


"""
"""
PG_CONSTANTS = ["CURRENT_DATE", "CURRENT_TIMESTAMP"]
def is_pgconstant(value):
    return value in PG_CONSTANTS


UDF = ["CURRENT_YEAR", "CURRENT_MONTH", "TODAY", "YESTERDAY", "FDOM", "LDOM"]
def is_udf(value:str, *args, **kwargs) -> Callable:
    fn = None
    try:
        f = value.lower()
        if value in UDF:
            fn = globals()[f](*args, **kwargs)
        else:
            func = globals()[f]
            if not func:
                try:
                    func = getattr(builtins, f)
                except AttributeError:
                    return None
            if func and callable(func):
                try:
                    fn = func(*args, **kwargs)
                except Exception as err:
                    raise Exception(err)
    finally:
        return fn

"""
Redis Functions
"""

def is_program_date(value, *args):
    try:
        cachedb = redis.StrictRedis.from_url(QUERY_VARIABLES)
        if cachedb.exists(value):
            return True
        else:
            return False
    except Exception:
        return False

def get_program_date(value, yesterday:bool = False, *args):
    opt = None
    try:
        cachedb = redis.StrictRedis.from_url(QUERY_VARIABLES)
        if cachedb.exists(value):
            result = cachedb.get(value)
            if result:
                opt = str(result.decode("utf-8"))
    finally:
        if not opt and yesterday:
            return yesterday()
        else:
            return opt

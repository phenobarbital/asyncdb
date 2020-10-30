import json
import rapidjson
import numpy as np
from rapidjson import Encoder as JSONEncoder
from datetime import datetime
from decimal import Decimal
from asyncdb.utils.encoders import EnumEncoder

class DefaultEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return str(obj)
        if isinstance(obj, Point):
            return {'x': obj.x, 'y': obj.y}
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, Decimal):
            return float(obj)
        else:
            raise TypeError('%r is not JSON serializable' % obj)

class BaseEncoder:
    def __init__(self, *args, **kwargs):
        # Filter/adapt JSON arguments to RapidJSON ones
        rjargs = ()
        rjkwargs = {}
        encoder = DefaultEncoder(*rjargs, **rjkwargs)
        self.encode = encoder.__call__

class Point(object):
    def __init__(self, x, y):
        self.x = x
        self.y = y

point = Point(120,81)

user = {
    "userId": 3381293,
    "age": 213,
    "username": "johndoe",
    "fullname": u"John Doe the Second",
    "isAuthorized": True,
    "liked": 31231.31231202,
    "approval": 31.1471,
    "jobs": [ 1, 2 ],
    "currJob": None,
    "updated": datetime.now(),
    "point": point,
    "value": float(21.9)
}
friends = [ user, user, user, user, user, user, user, user ]
complex_object = [
    [user, friends],  [user, friends],  [user, friends],
    [user, friends],  [user, friends],  [user, friends]
]

def benchmark(obj, num:int = 100):
    startTime = datetime.now()
    try:
        for _ in range(num):
            data = obj()
    except Exception as err:
        raise Exception(err)
    endTime = datetime.now() - startTime
    print(f'Duration for {num} was: {endTime}')

from functools import partial

print('Starting benchmark using json with Default Encoder: ')
fn = partial(json.dumps, complex_object, cls=EnumEncoder)
benchmark(fn, 10000)

print('Starting benchmark using json with RapidJSON Encoder: ')
fn = partial(json.dumps, complex_object, cls=BaseEncoder)
benchmark(fn, 10000)

print('Starting benchmark using only RapidJSON Encoder: ')
fn = partial(DefaultEncoder, complex_object)
benchmark(fn, 10000)

# reading a json file:
with open('tests/twitter.json') as f:
    data = f.read()

obj = rapidjson.loads(data)

print('====== SECOND BENCHMARK, using Twitter DATA ===========')
print('Starting benchmark using json with Default Encoder: ')
fn = partial(json.dumps, obj, cls=EnumEncoder)
benchmark(fn, 10000)

print('Starting benchmark using json with RapidJSON Encoder: ')
fn = partial(json.dumps, obj, cls=BaseEncoder)
benchmark(fn, 10000)

print('Starting benchmark using only RapidJSON Encoder: ')
fn = partial(DefaultEncoder, obj)
benchmark(fn, 10000)

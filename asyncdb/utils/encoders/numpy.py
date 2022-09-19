import json
from numpy import (
    integer,
    int64,
    floating,
    ndarray
)


class NpEncoder(json.JSONEncoder):
    """
    npEncoder.

       Numpy number encoder for json
    """
    def default(self, o):
        if isinstance(o, (integer, int64)):
            return int(o)
        elif isinstance(o, floating):
            return float(o)
        elif isinstance(o, ndarray):
            return o.tolist()
        else:
            return json.JSONEncoder.default(self, o)

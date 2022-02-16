import json
from asyncdb.utils.encoders import BaseEncoder
from .base import OutputFormat


class jsonFormat(OutputFormat):
    """
    Most Basic Definition of Format.
    """
    async def serialize(self, result, error, *args, **kwargs):
        dump = [dict(r) for r in result]
        return (json.dumps(dump, cls=BaseEncoder), error)

import json
from asyncdb.utils.encoders import BaseEncoder
from .base import OutputFormat


class jsonFormat(OutputFormat):
    """
    Most Basic Definition of Format.
    """
    async def __call__(self, result, error, *args, **kwargs):
        return await self.serialize(result, error, *args, **kwargs)

    async def serialize(self, result, error, *args, **kwargs):
        return (json.dumps(result, cls=BaseEncoder), error)

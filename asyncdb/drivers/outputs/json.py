from asyncdb.utils.encoders import DefaultEncoder
from .base import OutputFormat


class jsonFormat(OutputFormat):
    """
    Most Basic Definition of Format.
    """
    _encoder = DefaultEncoder()

    async def serialize(self, result, error, *args, **kwargs):
        if error:
            return (None, error)
        dump = [dict(r) for r in result]
        return (self._encoder.dumps(dump), error)

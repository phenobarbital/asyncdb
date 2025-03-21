import pandas
from ...utils.encoders import DefaultEncoder
from .base import OutputFormat


class jsonFormat(OutputFormat):
    """
    Most Basic Definition of Format.
    """

    _encoder = DefaultEncoder()

    async def serialize(self, result, error, *args, **kwargs):
        if error:
            return (None, error)
        if isinstance(result, pandas.DataFrame):
            dump = result.to_dict(orient="records")
        elif isinstance(result, pandas.Series):
            dump = result.to_dict()
        else:
            dump = [dict(r) for r in result]
        return (self._encoder.dumps(dump), error)

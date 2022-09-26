"""
DataClass Format.

Output format returning a list of Dataclasses based on Data.
"""
from dataclasses import make_dataclass
from .base import OutputFormat


class dataclassFormat(OutputFormat):
    """
    Most Basic Definition of Format.
    """
    def __init__(self, **kwargs):
        self._model = None
        if 'model' in kwargs:
            self._model = kwargs['model']
        else:
            # TODO: making analysis of resultset:
            pass
            # cls = make_dataclass('Output', )

    async def serialize(self, result, error, *args, **kwargs):
        lsgen = [self._model(**dict(row)) for row in result]
        return (lsgen, error)

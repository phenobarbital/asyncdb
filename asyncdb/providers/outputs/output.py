"""
All Output formats supported by asyncdb.
"""
from typing import Callable


class OutputFactory(object):
    _format: dict = {}

    def __new__(cls, provider, format="native", *args, **kwargs):
        if format is None or format == 'native':
            return provider.output
        else:
            return cls._format[format](*args, **kwargs)

    @classmethod
    def register_format(self, format, cls):
        self._format[format] = cls

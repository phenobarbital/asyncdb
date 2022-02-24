"""
All Output formats supported by asyncdb.
"""


class OutputFactory(object):
    _format: dict = {}

    def __new__(cls, provider, frmt="native", *args, **kwargs):
        if frmt is None or frmt == 'native':
            return provider.output
        else:
            return cls._format[frmt](*args, **kwargs)

    @classmethod
    def register_format(cls, frmt, obj):
        cls._format[frmt] = obj

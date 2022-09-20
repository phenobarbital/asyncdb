"""
All Output formats supported by asyncdb.
"""
from importlib import import_module

class OutputFactory(object):
    _format: dict = {}

    def __new__(cls, driver, frmt: str, *args, **kwargs):
        if frmt is None or frmt == 'native':
            return driver.output
        else:
            if frmt not in cls._format:
                try:
                    # dynamically load format:
                    module_name = f"{frmt}Format"
                    classpath = f'asyncdb.drivers.outputs.{frmt}'
                    mdl = import_module(classpath, package=frmt)
                    obj = getattr(mdl, module_name)
                    cls._format[frmt] = obj
                except ImportError as e:
                    raise RuntimeError(
                        f"Error Loading Output Format {module_name}: {e}"
                    ) from e
            return cls._format[frmt](*args, **kwargs)

    @classmethod
    def register_format(cls, frmt, obj):
        cls._format[frmt] = obj

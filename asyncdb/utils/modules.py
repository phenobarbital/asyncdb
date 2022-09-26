import logging
from importlib import import_module

### Module Loading
def module_exists(module_name, classpath):
    try:
        # try to using importlib
        module = import_module(classpath, package="providers")
        obj = getattr(module, module_name)
        return obj
    except ImportError:
        try:
            # try to using __import__
            obj = __import__(classpath, fromlist=[module_name])
            return obj
        except ImportError as e:
            logging.exception(
                f"No Driver for provider {module_name} was found: {e}"
            )
            raise ImportError(
                f"No Provider {module_name} Found"
            ) from e

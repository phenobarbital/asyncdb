"""
AsyncDB Providers.
"""
from asyncdb.utils.functions import module_exists
from .base import (
    BasePool,
    BaseProvider,
    InitProvider,
    BaseCursor,
    SQLProvider,
    DDLBackend
)
_PROVIDERS = {}

__all__ = [
    "BasePool",
    "InitProvider",
    "BaseProvider",
    "BaseCursor",
    "SQLProvider",
    "DDLBackend"
]


def registerProvider(provider: InitProvider):
    global _PROVIDERS
    name = provider.driver()
    classpath = f"asyncdb.providers.{name}"
    try:
        cls = module_exists(name, classpath)
        _PROVIDERS[name] = cls
    except ImportError as err:
        raise ImportError(err)

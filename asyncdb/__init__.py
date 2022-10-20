# -*- coding: utf-8 -*-
"""AsyncDB.

Asyncio-based database connectors.
"""
from pathlib import Path

from .connections import AsyncDB, AsyncPool
from .version import __author__, __author_email__, __description__, __title__, __version__


def get_project_root() -> Path:
    return Path(__file__).parent.parent

ABS_PATH = get_project_root()

__all__ = ('AsyncDB', 'AsyncPool', )

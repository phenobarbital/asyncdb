# -*- coding: utf-8 -*-
"""AsyncDB.

Asyncio-based database connectors.
"""
from pathlib import Path
from .version import (
    __title__, __description__, __version__, __author__, __author_email__
)
from .connections import AsyncDB, AsyncPool

def get_project_root() -> Path:
    return Path(__file__).parent.parent

ABS_PATH = get_project_root()

__all__ = ('AsyncDB', 'AsyncPool', )

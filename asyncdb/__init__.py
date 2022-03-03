# -*- coding: utf-8 -*-
"""AsyncDB.

Asyncio-based database connectors for NAV.
"""
import asyncio
import uvloop
from .version import (
    __title__, __description__, __version__, __author__, __author_email__
)
from .providers import (
    InitProvider,
    BaseProvider
)
from .connections import AsyncPool, AsyncDB

__all__ = ["InitProvider", "BaseProvider", "AsyncPool", "AsyncDB", ]

# install uvloop and set as default loop for asyncio.
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()

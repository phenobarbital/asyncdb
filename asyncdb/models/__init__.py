"""
AsyncDB Models.

AsyncDB Models is a simple library to use Dataclass-syntax for interacting with
Databases, using the same syntax of Dataclass, users can write Python Objects
and work with Data in the same way, no matter which is the DB Backend.

AsyncDB Models are based on python Dataclasses and type annotations.
"""
__version__ = "0.0.1"
from typing import (
    Dict
)
from .base import Field


__all__ = ['Field', ]

MODELS: Dict = {}

"""
AsyncDB Models.

AsyncDB Models is a simple library to use Dataclass-syntax for interacting with
Databases, using the same syntax of Dataclass, users can write Python Objects
and work with Data in the same way, no matter which is the DB Backend.

AsyncDB Models are based on python Dataclasses and type annotations.
"""
from dataclasses import is_dataclass
from datamodel import Field, Column
from .model import Model, is_missing


__all__ = ('Field', 'Column', 'Model', 'is_dataclass', 'is_missing', )

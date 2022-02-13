from .output import OutputFactory
from .json import jsonFormat


__all__ = ['OutputFactory']


OutputFactory.register_format('json', jsonFormat)

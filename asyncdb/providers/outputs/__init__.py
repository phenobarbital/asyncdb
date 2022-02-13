from .output import OutputFactory
from .json import jsonFormat
from .pandas import pandasFormat
from .generator import genFormat
from .polar import PolarFormat
from .datatable import dtFormat
from .csv import csvFormat


__all__ = ['OutputFactory']


OutputFactory.register_format('json', jsonFormat)
OutputFactory.register_format('pandas', pandasFormat)
OutputFactory.register_format('iterable', genFormat)
OutputFactory.register_format('polars', PolarFormat)
OutputFactory.register_format('datatable', dtFormat)
OutputFactory.register_format('csv', csvFormat)

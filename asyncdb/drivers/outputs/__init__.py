from .output import OutputFactory
from .json import jsonFormat
from .pandas import pandasFormat
from .generator import genFormat
from .polar import PolarFormat
from .datatable import dtFormat
from .csv import csvFormat
from .arrow import arrowFormat
from .record import recordFormat
from .recordset import recordsetFormat

__all__ = ['OutputFactory']


OutputFactory.register_format('json', jsonFormat)
OutputFactory.register_format('pandas', pandasFormat)
OutputFactory.register_format('iterable', genFormat)
OutputFactory.register_format('polars', PolarFormat)
OutputFactory.register_format('datatable', dtFormat)
OutputFactory.register_format('csv', csvFormat)
OutputFactory.register_format('arrow', arrowFormat)
OutputFactory.register_format('record', recordFormat)
OutputFactory.register_format('recordset', recordsetFormat)

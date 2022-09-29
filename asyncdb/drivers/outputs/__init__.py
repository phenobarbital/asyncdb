from .output import OutputFactory
from .json import jsonFormat
from .record import recordFormat
from .recordset import recordsetFormat
from .generator import genFormat

__all__ = ['OutputFactory']

OutputFactory.register_format('json', jsonFormat)
OutputFactory.register_format('record', recordFormat)
OutputFactory.register_format('recordset', recordsetFormat)
OutputFactory.register_format('iterable', genFormat)

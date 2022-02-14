"""
Recordset.

Returning a asyncdb Recordset Result Format.
"""
from .base import OutputFormat
from asyncdb.meta import Recordset

class recordsetFormat(OutputFormat):
    """
    Returns a List of Records from a Resultset
    """
    async def serialize(self, result, error, *args, **kwargs):
        self._result = None
        try:
            self._result = Recordset.from_result(result)
        except Exception as err:
            print(err)
            error = Exception(f"recordsetFormat: Error on Data: error: {err}")
        finally:
            return (self._result, error)

"""
Recordset.

Returning a asyncdb Recordset Result Format.
"""
import logging
from asyncdb.meta import Recordset
from .base import OutputFormat


class recordsetFormat(OutputFormat):
    """
    Returns a List of Records from a Resultset
    """
    async def serialize(self, result, error, *args, **kwargs):
        self._result = None
        if error:
            return (None, error)
        try:
            self._result = Recordset.from_result(result)
        except (TypeError, ValueError, AttributeError) as err:
            logging.exception(
                f'Recordset Serialization Error: {err}',
                stack_info=True
            )
            error = Exception(f"recordsetFormat: Error on Data: error: {err}")
        finally:
            return (self._result, error)

"""
Record.

Returning a asyncdb Record row Format.
"""
import logging
from cassandra.cluster import ResultSet
from asyncdb.meta import Record
from .base import OutputFormat


class recordFormat(OutputFormat):
    """
    Returns a List of Records from a Resultset
    """
    async def serialize(self, result, error, *args, **kwargs):
        self._result = None
        try:
            if isinstance(result, list):
                _set = [Record.from_dict(row) for row in result]
            elif isinstance(result, ResultSet):
                _set = [Record.from_dict(row) for row in result]
            else:
                _set = Record.from_dict(result)
            self._result = _set
        except Exception as err:
            logging.exception(
                f'Record Serialization Error: {err}',
                stack_info=True
            )
            error = Exception(f"recordFormat Error: {err}")
        finally:
            return (self._result, error)

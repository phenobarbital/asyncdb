"""
Record.

Returning a asyncdb Record row Format.
"""
import logging
from asyncdb.meta import Record
from .base import OutputFormat


class recordFormat(OutputFormat):
    """
    Returns a List of Records from a Resultset
    """
    async def serialize(self, result, error, *args, **kwargs):
        self._result = None
        if error:
            return (None, error)
        try:
            if isinstance(result, list):
                _set = [Record.from_dict(row) for row in result]
            elif hasattr(result, "one"):
                if callable(result.one):
                    _set = [Record.from_dict(row) for row in result]
            else:
                _set = Record.from_dict(result)
            self._result = _set
        except (TypeError, ValueError, AttributeError) as err:
            logging.exception(
                f'Record Serialization Error: {err}',
                stack_info=True
            )
            error = Exception(f"recordFormat Error: {err}")
        finally:
            return (self._result, error)

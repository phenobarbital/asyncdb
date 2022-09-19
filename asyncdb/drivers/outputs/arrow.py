import logging
import pyarrow as pa
from .base import OutputFormat


class arrowFormat(OutputFormat):
    """
    Returns an Apache Arrow Table from a Resultset
    """
    async def serialize(self, result, error, *args, **kwargs):
        table = None
        try:
            names = result[0].keys()
            table = pa.Table.from_arrays(
                result,
                names=names,
                **kwargs
            )
            self._result = table
        except ValueError as err:
            logging.error(f'Arrow Serialization Error: {err}')
            error = Exception(
                f"arrowFormat: Error Parsing Column: {err}"
            )
        except Exception as err:
            logging.exception(
                f'Arrow Serialization Error: {err}',
                stack_info=True
            )
            error = Exception(
                f"arrowFormat: Error on Data: error: {err}"
            )
        finally:
            return (table, error)

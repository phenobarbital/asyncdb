import logging
import datatable as dt
from .base import OutputFormat


class dtFormat(OutputFormat):
    """
    Returns a Pandas Dataframe from a Resultset
    """
    async def serialize(self, result, error, *args, **kwargs):
        df = None
        if error:
            return (None, error)
        try:
            data = [dict(row) for row in result]
            df = dt.Frame(
                data,
                **kwargs
            )
            self._result = df
        except ValueError as err:
            print(err)
            error = Exception(f"Error Parsing a Column, error: {err}")
        except Exception as err:
            logging.exception(
                f'Datatable Serialization Error: {err}',
                stack_info=True
            )
            error = Exception(f"dtFormat: Error on Data: error: {err}")
        finally:
            return (df, error)

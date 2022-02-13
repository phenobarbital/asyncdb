import pandas
import datatable as dt
from .base import OutputFormat


class dtFormat(OutputFormat):
    """
    Returns a Pandas Dataframe from a Resultset
    """
    async def serialize(self, result, error, *args, **kwargs):
        df = None
        try:
            df = dt.Frame(
                result,
                **kwargs
            )
            self._result = df
        except ValueError as err:
            error = Exception(f"Error Parsing a Column, error: {err}")
        except Exception as err:
            error = Exception(f"dtFormat: Error on Data: error: {err}")
        finally:
            return (df, error)

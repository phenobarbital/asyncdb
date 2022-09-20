import logging
import pandas
import polars as polar
from .base import OutputFormat


class polarsFormat(OutputFormat):
    """
    Returns a PyPolars Dataframe from a Resultset
    """
    async def serialize(self, result, error, *args, **kwargs):
        df = None
        try:
            result = [dict(row) for row in result]
            a = pandas.DataFrame(
                data=result,
                **kwargs
            )
            df = polar.from_pandas(
                a,
                **kwargs
            )
            self._result = df
        except ValueError as err:
            print(err)
            error = Exception(f"PolarFormat: Error Parsing Column: {err}")
        except Exception as err:
            logging.exception(
                f'Polars Serialization Error: {err}',
                stack_info=True
            )
            error = Exception(f"PolarFormat: Error on Data: error: {err}")
        finally:
            return (df, error)

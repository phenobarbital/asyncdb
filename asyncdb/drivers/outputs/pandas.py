import logging
import pandas
from .base import OutputFormat


class pandasFormat(OutputFormat):
    """
    Returns a Pandas Dataframe from a Resultset
    """
    async def serialize(self, result, error, *args, **kwargs):
        df = None
        try:
            result = [dict(row) for row in result]
            df = pandas.DataFrame(
                data=result,
                **kwargs
            )
            self._result = df
        except pandas.errors.EmptyDataError as err:
            error = Exception(f"Error with Empty Data: error: {err}")
        except pandas.errors.ParserError as err:
            logging.error(error)
            error = Exception(f"Error parsing Data: error: {err}")
        except ValueError as err:
            logging.error(error)
            error = Exception(f"Error Parsing a Column, error: {err}")
        except Exception as err:
            logging.error(error)
            error = Exception(f"PandasFormat: Error on Data: error: {err}")
        finally:
            return (df, error)

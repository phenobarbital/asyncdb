from io import StringIO
import pandas
from .base import OutputFormat


class csvFormat(OutputFormat):
    """
    Returns a CSV string from a Resultset
    TODO: migrate to aiocsv
    """
    async def serialize(self, result, error, *args, **kwargs):
        df = None
        try:
            df = pandas.DataFrame(
                data=result,
                **kwargs
            )
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            self._result = csv_buffer.getvalue()
        except pandas.errors.EmptyDataError as err:
            error = Exception(f"Error with Empty Data: error: {err}")
        except pandas.errors.ParserError as err:
            error = Exception(f"Error parsing Data: error: {err}")
        except ValueError as err:
            error = Exception(f"Error Parsing a Column, error: {err}")
        except Exception as err:
            error = Exception(f"PandasFormat: Error on Data: error: {err}")
        finally:
            return (self._result, error)

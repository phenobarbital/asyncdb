import logging
from io import StringIO
import datatable as dt
from .base import OutputFormat


def dt_parser(csv_stream: StringIO, chunksize: int, delimiter: str = ",", columns: list = None, quote: str = '"'):
    """
    Reads CSV text from `csv_stream` using datatable.fread, yields a single Frame.
    """
    # datatable.fread cannot read directly from a file-like object,
    # so we pass the CSV text via "text" parameter.
    csv_text = csv_stream.getvalue()

    # Create the Frame
    yield dt.fread(
        text=csv_text,
        sep=delimiter,
        nthreads=0,         # use all available threads
        header=None,      # no separate header row in the text
        columns=columns,     # the list of column names we captured
        quotechar=quote,   # pass None or some unusual char if you want to avoid standard " quoting
        fill=True          # fill shorter lines with NA if needed
    )



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
            df = dt.Frame(data, **kwargs)
            self._result = df
        except ValueError as err:
            print(err)
            error = Exception(f"Error Parsing a Column, error: {err}")
        except Exception as err:
            logging.exception(f"Datatable Serialization Error: {err}", stack_info=True)
            error = Exception(f"dtFormat: Error on Data: error: {err}")
        finally:
            return (df, error)

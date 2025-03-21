import logging
from io import StringIO
from asyncpg import Record
import pandas
from .base import OutputFormat



def pandas_parser(csv_stream: StringIO, chunksize: int, delimiter: str = ",", columns: list = None):
    """
    Parser function that reads the CSV text in `csv_stream`
    and yields DataFrame chunks using Pandas' chunked read_csv.
    """
    # `pd.read_csv(..., chunksize=...)` returns an iterator of DataFrames
    # We'll just return that iterator
    yield from pandas.read_csv(csv_stream, chunksize=chunksize, sep=delimiter, names=columns, header=None)


class pandasFormat(OutputFormat):
    """
    Returns a Pandas Dataframe from a Resultset
    """

    async def serialize(self, result, error, *args, **kwargs):
        df = None
        try:
            if isinstance(result, pandas.DataFrame):
                df = result
            elif isinstance(result, Record):
                result = [dict(result)]
                df = pandas.DataFrame(data=result, **kwargs)
            elif isinstance(result, list):
                if len(result) == 0:
                    error = Exception("Empty Data")
                else:
                    result = [dict(row) for row in result]
                    df = pandas.DataFrame(data=result, **kwargs)
            else:
                result = [dict(row) for row in result]
                df = pandas.DataFrame(data=result, **kwargs)
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

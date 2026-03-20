import logging
from io import StringIO
import pandas
from google.cloud.bigquery.table import RowIterator
import pyarrow as pa
import pyarrow.csv as pc
from .base import OutputFormat


def arrow_parser(csv_stream: StringIO, chunksize: int, delimiter: str = ",", columns: list = None):
    """
    Read a block of CSV text from `csv_stream` into a single PyArrow Table
    and yield that Table.
    - `columns`: a list of column names from the first CSV header line.
      We'll tell Arrow to use those as the schema field names, skipping
      any separate header row in the text.
    - `delimiter`: the field separator (default '|').
    - `chunksize`: (unused for Arrow, but kept for signature compatibility).
    """
    # Convert CSV text to bytes for Arrow
    data_bytes = csv_stream.getvalue().encode("utf-8")

    read_opts = pc.ReadOptions(
        use_threads=True
    )
    parse_opts = pc.ParseOptions(
        delimiter=delimiter,
        quote_char='"',   # If you do not want quoting at all, set to None
        double_quote=True,
        escape_char=None
    )
    convert_opts = pc.ConvertOptions()

    if columns is not None:
        # If we've already extracted the header line ourselves,
        # then we tell Arrow to treat all lines as data (no separate header),
        # and *assign* those column names to each field.
        read_opts.column_names = columns
        # Also skip the first line if it was the header in the actual CSV text
        # But in your current code, you've already stripped the header line,
        # so there's no real first line to skip.
        # Hence skip_rows=0 is correct (the entire data block are data lines).
        read_opts.skip_rows = 0

    # Read the CSV into a single Table
    table = pc.read_csv(
        pa.BufferReader(data_bytes),
        read_options=read_opts,
        parse_options=parse_opts,
        convert_options=convert_opts
    )

    # We yield one table per chunk
    yield table


class arrowFormat(OutputFormat):
    """
    Returns an Apache Arrow Table from a Resultset
    """

    async def serialize(self, result, error, *args, **kwargs):
        table = None
        try:
            if isinstance(result, RowIterator):
                for chunk in arrow_parser(result.csv_stream, chunksize=1000):
                    yield chunk
            elif isinstance(result, pa.Table):
                table = result
            elif isinstance(result, pandas.DataFrame):
                table = pa.Table.from_pandas(result, **kwargs)
            else:
                names = result[0].keys()
                table = pa.Table.from_arrays(result, names=names, **kwargs)
            self._result = table
        except ValueError as err:
            logging.error(f"Arrow Serialization Error: {err}")
            error = Exception(f"arrowFormat: Error Parsing Column: {err}")
        except Exception as err:
            logging.exception(f"Arrow Serialization Error: {err}", stack_info=True)
            error = Exception(f"arrowFormat: Error on Data: error: {err}")
        finally:
            return (table, error)

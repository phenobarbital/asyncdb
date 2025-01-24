import logging
import pandas
import io
import polars as pl
from .base import OutputFormat


def polars_parser(
    csv_stream: io.StringIO,
    chunksize: int,
    delimiter: str = "|",
    columns: list = None
):
    """
    Parser for Polars. Reads entire CSV text from `csv_stream` into one Polars DataFrame
    and yields that DataFrame.

    - If `columns` is provided, we assume the CSV text has *no* header row (because
      your code extracted it already). Then we manually assign those column names.
    - Otherwise, if columns=None, Polars will interpret the first line in `csv_stream`
      as the header (has_header=True).
    """

    csv_text = csv_stream.getvalue()
    has_header = columns is None

    # Polars read_csv can either infer the header or we can specify new_columns=...
    # We'll pass 'has_header=False' if we already stripped the header line,
    # and assign 'new_columns=columns' to rename them.

    yield pl.read_csv(
        io.StringIO(csv_text),
        separator=delimiter,
        has_header=has_header,
        new_columns=None if has_header else columns,
        ignore_errors=True  # optional, in case of mismatch
    )


class polarsFormat(OutputFormat):
    """
    Returns a PyPolars Dataframe from a Resultset
    """

    async def serialize(self, result, error, *args, **kwargs):
        df = None
        try:
            result = [dict(row) for row in result]
            a = pandas.DataFrame(data=result, **kwargs)
            df = pl.from_pandas(a, **kwargs)
            self._result = df
        except ValueError as err:
            print(err)
            error = Exception(f"PolarFormat: Error Parsing Column: {err}")
        except Exception as err:
            logging.exception(f"Polars Serialization Error: {err}", stack_info=True)
            error = Exception(f"PolarFormat: Error on Data: error: {err}")
        finally:
            return (df, error)

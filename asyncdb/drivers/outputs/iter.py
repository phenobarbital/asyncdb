"""
Iterable.

Output format returning a simple list of dictionaries.
"""
import pandas
from google.cloud.bigquery.table import RowIterator
from .base import OutputFormat


class iterFormat(OutputFormat):
    """
    Most Basic Definition of Format.
    """

    async def serialize(self, result, error, *args, **kwargs):
        try:
            if isinstance(result, (RowIterator, list)):
                data = [dict(row) for row in result]
            elif isinstance(result, pandas.DataFrame):
                data = result.to_dict(orient="records")
            else:
                data = dict(result)
        except (ValueError, TypeError):
            return (result, error)
        return (data, error)

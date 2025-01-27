from typing import List
from asyncpg import Record
from pyarrow import Table
import rst_convert
# from .rst_convert import todict, toarrow

def to_dict(records: List[Record]) -> List[dict]:
    """
    Convert asyncpg.Record to a list of dictionaries.

    Args:

    records: List[Record] - List of asyncpg.Record objects.

    Returns:

    dict - List of dictionaries.
    """
    return rst_convert.todict(records)


def to_arrow(records: List[Record]) -> Table:
    """
    Convert asyncpg.Record to Arrow IPC format.

    This function converts the asyncpg.Record to a list of dictionaries and then to Arrow IPC format.

    Args:

    records: List[Record] - List of asyncpg.Record objects.

    Returns:

    Arrow - Arrow IPC format.
    """
    return rst_convert.toarrow(records)

"""
Recordset.

Sequence of Records.
"""
from collections.abc import Sequence
from typing import (
    Any,
    List,
    Dict,
    Iterator,
    Union
)
from .record import Record
from cassandra.cluster import ResultSet


class Recordset(Sequence):
    """
    Recordset.
         Class for a Resultset Object
    ----
      params:
          result: any resultset
    """
    def __init__(self, result: Any, columns: List = []):
        self._columns = columns
        self._result = result
        self._idx = 0

    def get_result(self) -> Any:
        return self._result

    @classmethod
    def from_result(cls, result: Iterator) -> "Recordset":
        cols = []
        try:
            if isinstance(result, ResultSet):
                cols = result.one().keys
                result = list(result)
            else:
                cols = result[0].keys()
            return cls(result, columns = cols)
        except Exception as err:
            raise ValueError(f"Recordset: Invalid data set {err}")

    """
     Section: Simple magic methods
    """
    def __getitem__(self, key: Union[int, str]):
        if isinstance(key, int):
            if key >= len(self._result):
                raise IndexError('Recordset: Result Index out of Range')
            return self._result[key]
        elif isinstance(key, slice):
            # works with slices
            # print(key, key.start, key.stop)
            return self._result[key]
        else:
            raise TypeError(f"Recordset: Invalid request {key!s}")

    def __repr__(self) -> str:
        return f"<Recordset {self._result!r}>"

    def __len__(self) -> int:
        return len(self._result)

    def __iter__(self):
        return self

    def __next__(self):
        """
        Next: next object from iterator
        """
        if self._idx < len(self._result):
            row = self._result[self._idx]
            self._idx += 1
            return Record(row, self._columns)
        # End of Iteration
        raise StopIteration

"""
Record Object.

Physical representation of a row in a class-based object.
"""
from collections.abc import MutableMapping, Iterator
from typing import (
    Any,
    Union
)


class Record(MutableMapping):
    """
    Record.
        Class for Record object
    ----
      params:
          row: any resultset
    """
    __slots__ = ('_row', '_columns')

    def __init__(self, row: Any, columns: list = None):
        self._row = row
        self._columns = columns

    def result(self, key: Union[str, Any]) -> Any:
        if self._row:
            try:
                return self._row[key]
            except KeyError:
                print(f"Error on key: {key}")
                return None
        else:
            return None

    def get_result(self):
        return self._row

    @classmethod
    def from_dict(cls, row: dict) -> "Record":
        return cls(row = row, columns = row.keys())
        # keys, values = zip(*row.items())
        # return cls(row = values, columns = [[name] for name in keys])

    @property
    def row(self) -> Any:
        return self._row

    def columns(self) -> list:
        return self._columns

    def items(self) -> zip:  # type: ignore
        return zip(self._columns, self._row)

    def keys(self) -> list:
        return self._columns

### Section: Simple magic methods
    def __len__(self) -> int:
        return len(self._row)

    def __str__(self) -> str:
        return ' '.join(f"{key}={val!r}" for key, val in self._row.items())

    def __repr__(self) -> str:
        return f"<Record {self._row!r}>"

    def __contains__(self, key: str) -> bool:
        return key in self._columns

    def __delitem__(self, key) -> None:
        if self._row:
            del self._row[key]

    def __getitem__(self, key: Union[str, int]) -> Any:
        """
        Sequence-like operators
        """
        try:
            return self._row[key]
        except (KeyError, TypeError):
            return False

    def __setitem__(self, key: Union[str, Any], value: Any) -> None:
        # optional processing here
        self._row[key] = value
        super(Record, self).__setitem__(key, value)

    def __getattr__(self, attr: str) -> Any:
        """
        Attributes for dict keys
        """
        if self._row:
            try:
                return self._row[attr]
            except KeyError as err:
                raise KeyError(
                    f"Record Error: invalid column name {attr} on {self._row!r}"
                ) from err
            except TypeError as err:
                raise TypeError(
                    f"Record Error: invalid Result on {self._row!r} for {attr}"
                ) from err
        else:
            return False

    def __setattr__(self, key: Union[str, int], value: Any) -> None:
        try:
            super(Record, self).__setattr__(key, value)
        except AttributeError:
            self._row[key] = value

    def __iter__(self) -> Iterator:
        for value in self._row:
            yield value

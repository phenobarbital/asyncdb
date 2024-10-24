from typing import Any, Optional, Union
from collections.abc import Sequence, Iterable
from abc import ABC, abstractmethod
from ..exceptions import DriverError


class TransactionBackend(ABC):
    """
    Interface for Drivers with Transaction Support.
    """

    def __init__(self):
        self._connection = None
        self._transaction = None

    @abstractmethod
    async def transaction(self, options: dict[Any]) -> Any:
        """
        Getting a Transaction Object.
        """
        raise NotImplementedError()  # pragma: no cover

    async def transaction_start(self, options: dict) -> None:
        """
        Starts a Transaction.
        """
        self._transaction = self.transaction(options)

    @abstractmethod
    async def commit(self) -> None:
        pass

    @abstractmethod
    async def rollback(self) -> None:
        pass


class DatabaseBackend(ABC):
    """
    Interface for Basic Methods on Databases (query, fetch, execute).
    """

    _test_query: Optional[Any] = None

    def __init__(self) -> None:
        self._columns: list = []
        self._attributes = None
        self._result: list = []
        self._prepared: Any = None

    @property
    def columns(self):
        return self._columns

    def get_columns(self):
        return self._columns

    @property
    def result(self):
        return self._result

    def get_result(self):
        return self._result

    async def test_connection(self, **kwargs):
        """Test Connnection.
        Making a connection Test using the basic Query Method.
        """
        if self._test_query is None:
            raise NotImplementedError()
        try:
            return await self.query(self._test_query, **kwargs)
        except Exception as err:
            raise DriverError(message=str(err)) from err

    @abstractmethod
    async def use(self, database: str) -> None:
        """
        Change the current Database.
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    async def execute(self, sentence: Any, *args, **kwargs) -> Optional[Any]:
        """
        Execute a sentence
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    async def execute_many(self, sentence: list, *args) -> Optional[Any]:
        """
        Execute many sentences at once.
        """

    @abstractmethod
    async def query(self, sentence: Union[str, list], **kwargs) -> Optional[Sequence]:
        """queryrow.

        Making a Query and returns a resultset.
        Args:
            sentence (Union[str, list]): sentence(s) to be executed.
            kwargs: Optional attributes to query.

        Returns:
            Optional[Record]: Returns a Resultset
        """

    @abstractmethod
    async def queryrow(self, sentence: Union[str, list]) -> Optional[Iterable]:
        """queryrow.

        Returns a single row of a query sentence.
        Args:
            sentence (Union[str, list]): sentence to be executed.

        Returns:
            Optional[Record]: Return one single row of a query.
        """

    @abstractmethod
    async def prepare(self, sentence: Union[str, list]) -> Any:
        """
        Making Prepared statement.
        """

    def prepared_statement(self):
        return self._prepared

    def prepared_attributes(self):
        return self._attributes

    @abstractmethod
    async def fetch_all(self, sentence: str, **kwargs) -> list[Sequence]:
        pass

    @abstractmethod
    async def fetch_one(self, sentence: str, **kwargs) -> Optional[dict]:
        """
        Fetch only one record, optional getting an offset using "number".
        """

    async def fetch_val(self, sentence: str, column: Any = None, number: int = None) -> Any:
        """
        Fetch the value of a Column in a record.
        """
        row = await self.fetch_many(sentence, number)
        return None if row is None else row[column]

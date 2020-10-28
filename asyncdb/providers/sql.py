from asyncdb.providers import BaseProvider

SQLProvider(BaseProvider):
    _syntax = "sql"
    _test_query = "SELECT 1"
    _prepared = None
    _initialized_on = None
    _query_raw = "SELECT {fields} FROM {table} {where_cond}"


    """
    Context magic Methods
    """
    async def __aenter__(self) -> "sqlite":
        if not self._connection:
            await self.connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return await self.close()

    async def close(self, timeout=5):
        """
        Closing Method for SQL Connector
        """
        try:
            if self._connection:
                if self._cursor:
                    await self._cursor.close()
                await asyncio.wait_for(
                    self._connection.close(), timeout=timeout
                )
        except Exception as err:
            raise ProviderError("{}: Closing Error: {}".format(__name__, str(err)))
        finally:
            self._connection = None
            self._connected = False
            return True

    async def connect(self, **kwargs):
        """
        Get a proxy connection, alias of connection
        """
        self._connection = None
        self._connected = False
        return await self.connection(self, **kwargs)

    async def release(self):
        """
        Release a Connection
        """
        await self.close()

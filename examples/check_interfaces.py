from typing import Any
from asyncdb.interfaces.connection import ConnectionBackend


class TestDriver(ConnectionBackend):
    async def connection(self) -> Any:
        return await super().connection()

    async def close(self):
        return await super().close()


if __name__ == '__main__':
    a = TestDriver()

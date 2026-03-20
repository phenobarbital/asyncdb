import asyncio
from asyncdb import Asyncdb

credentials = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator",
    "DEBUG": True,
}

async def main():
    async with await Asyncdb('pg', credentials=credentials) as conn:
        print(await conn.test_connection())


if __name__ == "__main__":
    asyncio.run(main())

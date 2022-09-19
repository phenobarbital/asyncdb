import asyncio
from asyncdb.drivers.dummy import dummy
from asyncdb.utils import cPrint


async def db():
    d = dummy()
    async with await d.connection() as conn:
        cPrint(f"Is Connected: {d.is_connected()}")
        result, _ = await conn.query('SELECT TEST')
        cPrint(f"Result is: {result}")

if __name__ == '__main__':
    asyncio.run(db())

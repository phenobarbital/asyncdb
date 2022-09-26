import asyncio
from datamodel import BaseModel
from asyncdb.drivers.dummy import dummy
from asyncdb.utils import cPrint


class Point(BaseModel):
    col1: list
    col2: list
    col3: list

async def db():
    d = dummy()
    async with await d.connection() as conn:
        cPrint(f"Is Connected: {d.is_connected()}")
        result, _ = await conn.query('SELECT TEST')
        cPrint(f"Result is: {result}")
        # changing output format to Pandas:
        conn.output_format('pandas')  # change output format to pandas
        result, _ = await conn.query('SELECT TEST')
        print(result)
        conn.output_format('iterable')  # change output format to Iterable
        result, _ = await conn.query('SELECT TEST')
        print(result)
        conn.output_format('dataclass', model=Point)  # change output format to Iterable
        result, _ = await conn.query('SELECT TEST')
        print(result)
        conn.output_format('pyspark')  # change output format to a PySpark Dataframe
        result, _ = await conn.query('SELECT TEST')
        print(result)

if __name__ == '__main__':
    asyncio.run(db())

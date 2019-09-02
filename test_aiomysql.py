import asyncio
from asyncdb.providers.mysql import mysqlPool, mysql
loop = asyncio.get_event_loop()

# create a pool with parameters
params = {
    'user': 'root',
    'password': '*090Guille',
    'host': '127.0.0.1',
    'port': '3306',
    'database': 'ips',
    'DEBUG': True
}
sql = 'select * from materials'
pool = mysql(loop=loop, params=params)
loop.run_until_complete(pool.connection())

async def select():

    async with pool._connection.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)
            r = await cur.fetchone()
            print(r)


loop.run_until_complete(select())
pool.terminate()


#async def main():
   # c1 = select(sql)
   # c2 = insert(loop=loop, sql="insert into minifw (name) values ('hello')")
   # tasks = [
       # asyncio.ensure_future(c1),
     #   asyncio.ensure_future(c2)
    #]
    #return await asyncio.gather(*tasks)

#loop.run_until_complete(main())
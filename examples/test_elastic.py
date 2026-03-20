"""Demonstrate using the ElasticSearch driver with Navigator credentials."""
"""Example ElasticSearch connection using the asyncdb elastic driver."""
import asyncio
from pprint import pprint
from asyncdb.drivers.elastic import elastic, ElasticConfig


async def main():
    # Connection details supplied by the Navigator cluster.
    params = ElasticConfig(
        protocol="https",
        host="vpc-trocglobal-ud5wemlxj65n6xndxb4vcxwzdy.us-east-2.es.amazonaws.com",
        port=443,
        db="datajobindex_v2",
        user="navigator_ro",
        password="/N/vC}1q8JQ1"
    )

    client = elastic(params=params)
    await client.connection()
    print(f"Connected: {client.is_connected()}")

    query = {"query": {"match_all": {}}, "size": 5}
    results = await client.query(query)
    pprint(results)

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())

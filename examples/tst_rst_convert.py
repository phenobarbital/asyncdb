import asyncio
import asyncpg
import time
import pandas as pd
import rst_convert

credentials = {
    "user": "troc_pgdata",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "navigator"
}

async def main():
    conn = await asyncpg.connect(**credentials)
    try:
        # Fetch records
        records = await conn.fetch("SELECT user_id, username, created_at FROM auth.users LIMIT 2")


        # Benchmark Python's list comprehension
        start_py = time.time()
        dicts_py = [dict(row) for row in records]
        end_py = time.time()
        print(f"Python list comprehension Time taken: {end_py - start_py} seconds")

        # Benchmark Rust's todict
        start_rust = time.time()
        records_list = list(records)
        dicts_rust = rst_convert.todict(records_list)
        end_rust = time.time()
        print(f"Rust todict Time taken: {end_rust - start_rust} seconds")

        # Optional: Convert to pandas DataFrame
        df_rust = pd.DataFrame(dicts_rust)
        df_py = pd.DataFrame(dicts_py)

    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""Example: Apache Iceberg driver for AsyncDB.

Demonstrates the full lifecycle of the Iceberg driver:
  1. Connect to a local SQLite-backed catalog (no external services needed)
  2. Create a namespace and table
  3. Write data (PyArrow and Pandas)
  4. Read data in multiple formats (arrow, pandas, polars)
  5. Query via DuckDB SQL
  6. Inspect metadata and snapshot history
  7. Cleanup (drop table and namespace)

Run:
    python examples/test_iceberg.py
"""
import asyncio
import tempfile
from pathlib import Path

import pyarrow as pa
import pandas as pd
import polars as pl

from asyncdb.drivers.iceberg import iceberg


async def main() -> None:
    """Demonstrate the Iceberg driver end-to-end with a local catalog."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # ------------------------------------------------------------------
        # 1. Connection parameters — SQLite-backed catalog (no infra needed)
        # ------------------------------------------------------------------
        params = {
            "catalog_name": "demo_catalog",
            "catalog_type": "sql",
            "catalog_properties": {
                "uri": f"sqlite:///{tmp_path}/catalog.db",
                "warehouse": str(tmp_path / "warehouse"),
            },
            "namespace": "demo",
        }

        # ------------------------------------------------------------------
        # 2. Connect via context manager
        # ------------------------------------------------------------------
        async with iceberg(params=params) as driver:
            print(f"Connected to catalog: {driver._catalog_name}")

            # ----------------------------------------------------------------
            # 3. Create namespace
            # ----------------------------------------------------------------
            await driver.create_namespace("demo", properties={"owner": "asyncdb-example"})
            namespaces = await driver.list_namespaces()
            print(f"Namespaces: {namespaces}")

            # ----------------------------------------------------------------
            # 4. Define schema and create table
            # ----------------------------------------------------------------
            schema = pa.schema(
                [
                    pa.field("city", pa.string(), nullable=False),
                    pa.field("population", pa.int64(), nullable=False),
                    pa.field("country", pa.string(), nullable=True),
                ]
            )

            tbl = await driver.create_table("demo.cities", schema=schema)
            print(f"Created table: {tbl.identifier}")
            print(f"Table exists: {await driver.table_exists('demo.cities')}")

            # ----------------------------------------------------------------
            # 5. Write data — PyArrow Table
            # ----------------------------------------------------------------
            arrow_data = pa.Table.from_pylist(
                [
                    {"city": "Amsterdam", "population": 921402, "country": "NL"},
                    {"city": "San Francisco", "population": 808988, "country": "US"},
                    {"city": "Paris", "population": 2103000, "country": "FR"},
                ],
                schema=schema,
            )
            await driver.write(arrow_data, "demo.cities", mode="append")
            print("Appended 3 rows via PyArrow Table")

            # ----------------------------------------------------------------
            # 6. Append more data — Pandas DataFrame
            # ----------------------------------------------------------------
            pandas_df = pd.DataFrame(
                {
                    "city": ["Berlin", "Tokyo"],
                    "population": [3432000, 13960000],
                    "country": ["DE", "JP"],
                }
            )
            await driver.write(pandas_df, "demo.cities", mode="append")
            print("Appended 2 rows via Pandas DataFrame")

            # ----------------------------------------------------------------
            # 7. Read as PyArrow Table
            # ----------------------------------------------------------------
            arrow_result = await driver.scan("demo.cities")
            print(f"\nAll rows (arrow): {arrow_result.num_rows} rows")
            print(arrow_result.to_pydict())

            # ----------------------------------------------------------------
            # 8. Read as Pandas DataFrame
            # ----------------------------------------------------------------
            pandas_result = await driver.get("demo.cities", factory="pandas")
            print(f"\nPandas DataFrame:\n{pandas_result}")

            # ----------------------------------------------------------------
            # 9. Read as Polars DataFrame
            # ----------------------------------------------------------------
            polars_result = await driver.get("demo.cities", factory="polars")
            print(f"\nPolars DataFrame:\n{polars_result}")

            # ----------------------------------------------------------------
            # 10. Query via DuckDB SQL
            # ----------------------------------------------------------------
            result, error = await driver.query(
                "SELECT city, population FROM iceberg_table WHERE population > 1000000 ORDER BY population DESC",
                table_id="demo.cities",
                factory="pandas",
            )
            if error is None:
                print(f"\nDuckDB SQL query — cities with population > 1M:\n{result}")
            else:
                print(f"Query error: {error}")

            # ----------------------------------------------------------------
            # 11. Single-row query
            # ----------------------------------------------------------------
            row, error = await driver.queryrow(
                "SELECT * FROM iceberg_table ORDER BY population ASC",
                table_id="demo.cities",
                factory="pandas",
            )
            if error is None:
                print(f"\nSmallest city (queryrow): {row.to_dict()}")

            # ----------------------------------------------------------------
            # 12. Convert full table to Pandas via to_df()
            # ----------------------------------------------------------------
            df = await driver.to_df("demo.cities", factory="pandas")
            print(f"\nto_df() row count: {len(df)}")

            # ----------------------------------------------------------------
            # 13. Metadata inspection
            # ----------------------------------------------------------------
            meta = await driver.metadata("demo.cities")
            print(f"\nTable metadata:")
            print(f"  location      : {meta['location']}")
            print(f"  snapshot_count: {meta['snapshot_count']}")
            print(f"  current_snap  : {meta['current_snapshot_id']}")

            # ----------------------------------------------------------------
            # 14. Snapshot history
            # ----------------------------------------------------------------
            history = await driver.history("demo.cities")
            print(f"\nSnapshot history ({len(history)} entries):")
            for h in history:
                print(f"  id={h['snapshot_id']}  ts={h['timestamp_ms']}  summary={h['summary']}")

            # ----------------------------------------------------------------
            # 15. Overwrite all data
            # ----------------------------------------------------------------
            replacement = pa.Table.from_pylist(
                [{"city": "London", "population": 9000000, "country": "GB"}],
                schema=schema,
            )
            await driver.write(replacement, "demo.cities", mode="overwrite")
            check = await driver.scan("demo.cities")
            print(f"\nAfter overwrite — row count: {check.num_rows}")

            # ----------------------------------------------------------------
            # 16. Schema inspection
            # ----------------------------------------------------------------
            await driver.load_table("demo.cities")
            tbl_schema = driver.schema()
            print(f"\nTable schema: {tbl_schema}")

            # ----------------------------------------------------------------
            # 17. List tables in namespace
            # ----------------------------------------------------------------
            table_list = driver.tables("demo")
            print(f"\nTables in namespace 'demo': {table_list}")

            # ----------------------------------------------------------------
            # 18. Cleanup — drop table then namespace
            # ----------------------------------------------------------------
            await driver.drop_table("demo.cities", purge=True)
            print(f"\nDropped table. Exists: {await driver.table_exists('demo.cities')}")

            await driver.drop_namespace("demo")
            remaining = await driver.list_namespaces()
            print(f"Remaining namespaces: {remaining}")

        print("\nDone! Context manager disconnected cleanly.")


if __name__ == "__main__":
    asyncio.run(main())

import os

import duckdb

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "fuzzyfactory")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "analyst")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "analyst")

WAREHOUSE_PATH = os.environ.get(
    "WAREHOUSE_PATH", "/opt/airflow/warehouse/warehouse.duckdb"
)

TABLES = [
    "website_sessions",
    "website_pageviews",
    "products",
    "orders",
    "order_items",
    "order_item_refunds",
]


def main() -> None:
    con = duckdb.connect(WAREHOUSE_PATH)

    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres;")

    dsn = (
        f"dbname={POSTGRES_DB} user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD} host={POSTGRES_HOST}"
    )
    con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres, READ_ONLY);")

    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    for table in TABLES:
        print(f"Snapshotting pg.{table} -> raw.{table} ...")
        con.execute(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM pg.{table};")
        count = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
        print(f"  {table}: {count} rows")

    con.execute("DETACH pg;")
    con.close()

    print("Extract-load complete.")


if __name__ == "__main__":
    main()

"""Load curated Parquet into ClickHouse with an atomic, idempotent partition swap.

Flow for one day:
  1. Empty the staging table.
  2. Insert the day's Parquet into staging.
  3. Check the row count in staging matches the Parquet row count.
  4. ``ALTER TABLE orders REPLACE PARTITION <day> FROM orders_staging``.

Step 4 is atomic in ClickHouse, so dashboards never see a half-loaded day,
and re-running a day replaces it instead of duplicating it.
"""

from __future__ import annotations

import logging
import time
from datetime import date

import clickhouse_connect
import pyarrow as pa
import pyarrow.parquet as pq
from clickhouse_connect.driver.client import Client

from pipeline.config import Settings

log = logging.getLogger(__name__)

DDL = [
    """
    CREATE TABLE IF NOT EXISTS orders
    (
        order_id      String,
        customer_id   String,
        order_ts      DateTime64(6, 'UTC'),
        product_id    String,
        category      LowCardinality(String),
        quantity      Int32,
        unit_price    Decimal(12, 2),
        currency      LowCardinality(String),
        status        LowCardinality(String),
        country       LowCardinality(String),
        amount        Decimal(18, 2),
        _ingested_at  DateTime64(6, 'UTC'),
        order_date    Date
    )
    ENGINE = MergeTree
    PARTITION BY order_date
    ORDER BY (order_date, category, order_id)
    """,
    "CREATE TABLE IF NOT EXISTS orders_staging AS orders",
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs
    (
        run_id              String,
        ds                  Date,
        raw_rows            UInt64,
        invalid_rows        UInt64,
        duplicates_removed  UInt64,
        loaded_rows         UInt64,
        invalid_ratio       Float64,
        transform_seconds   Float64,
        load_seconds        Float64,
        finished_at         DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (ds, finished_at)
    """,
]


class LoadVerificationError(RuntimeError):
    """Row counts did not match after loading into staging."""


def get_client(settings: Settings) -> Client:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def ensure_schema(client: Client) -> None:
    for statement in DDL:
        client.command(statement)


def read_partition(settings: Settings, ds: str) -> pa.Table:
    """Read one day of curated Parquet and add the partition column back."""
    day = date.fromisoformat(ds)
    table = pq.read_table(settings.curated_partition(ds))
    order_date = pa.repeat(pa.scalar(day, type=pa.date32()), table.num_rows)
    return table.append_column("order_date", order_date)


def load_day(client: Client, settings: Settings, ds: str) -> tuple[int, float]:
    """Load one day into ClickHouse. Returns (rows loaded, seconds taken)."""
    started = time.monotonic()
    partition = date.fromisoformat(ds).isoformat()  # validates format -> safe to inline
    table = read_partition(settings, ds)

    client.command("TRUNCATE TABLE orders_staging")
    client.insert_arrow("orders_staging", table)

    staged = client.query(
        "SELECT count() FROM orders_staging WHERE order_date = {d:Date}",
        parameters={"d": partition},
    ).result_rows[0][0]
    if staged != table.num_rows:
        raise LoadVerificationError(
            f"{ds}: parquet has {table.num_rows} rows but staging has {staged}"
        )

    client.command(f"ALTER TABLE orders REPLACE PARTITION '{partition}' FROM orders_staging")
    client.command("TRUNCATE TABLE orders_staging")

    seconds = round(time.monotonic() - started, 2)
    log.info("%s: loaded %d rows into ClickHouse in %.2fs", ds, staged, seconds)
    return staged, seconds


def record_run(client: Client, run_id: str, metrics: dict) -> None:
    """Append one row to the pipeline_runs audit table (data-quality history)."""
    columns = [
        "run_id",
        "ds",
        "raw_rows",
        "invalid_rows",
        "duplicates_removed",
        "loaded_rows",
        "invalid_ratio",
        "transform_seconds",
        "load_seconds",
    ]
    row = [
        run_id,
        date.fromisoformat(metrics["ds"]),
        metrics["raw_rows"],
        metrics["invalid_rows"],
        metrics["duplicates_removed"],
        metrics["loaded_rows"],
        metrics["invalid_ratio"],
        metrics["transform_seconds"],
        metrics["load_seconds"],
    ]
    client.insert("pipeline_runs", [row], column_names=columns)

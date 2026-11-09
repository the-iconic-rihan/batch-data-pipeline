"""Daily orders pipeline: extract -> transform (PySpark) -> load (ClickHouse).

Each run handles exactly one day (``ds``), so loading is incremental by design,
and every step is idempotent: re-running or backfilling a day replaces that day.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "rihan",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="daily_orders_pipeline",
    description="Raw CSV -> validated Parquet (PySpark) -> ClickHouse, one day per run",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,  # use `make backfill` to load history on purpose
    max_active_runs=1,  # staging table is shared, so days load one at a time
    default_args=DEFAULT_ARGS,
    tags=["etl", "pyspark", "clickhouse"],
    doc_md=__doc__,
)
def daily_orders_pipeline():
    # Heavy imports (pyspark, pyarrow, clickhouse) live inside tasks so the
    # scheduler can parse this file quickly.

    @task
    def extract(ds: str | None = None) -> str:
        """Land the day's raw extract. Here a generator stands in for the upstream system."""
        from datetime import date

        from pipeline.config import Settings
        from pipeline.generate import generate_day

        settings = Settings()
        path = settings.raw_path(ds)
        if path.exists():
            log.info("Source extract for %s already landed: %s", ds, path)
        else:
            generate_day(date.fromisoformat(ds), settings.rows_per_day, path)
            log.info("Landed %s", path)
        return str(path)

    @task
    def transform(ds: str | None = None) -> dict:
        from pipeline.config import Settings
        from pipeline.transform import DataQualityError, build_spark, transform_day

        spark = build_spark(f"orders-{ds}")
        try:
            return transform_day(spark, ds, Settings())
        except DataQualityError as exc:
            # Bad data won't fix itself on retry: fail fast and alert instead.
            raise AirflowFailException(str(exc)) from exc
        finally:
            spark.stop()

    @task
    def load(metrics: dict, ds: str | None = None, run_id: str | None = None) -> dict:
        from pipeline.config import Settings
        from pipeline.load import ensure_schema, get_client, load_day, record_run

        settings = Settings()
        client = get_client(settings)
        ensure_schema(client)
        loaded_rows, load_seconds = load_day(client, settings, ds)
        metrics = {**metrics, "loaded_rows": loaded_rows, "load_seconds": load_seconds}
        record_run(client, run_id, metrics)
        return metrics

    landed = extract()
    metrics = transform()
    landed >> metrics
    load(metrics)


daily_orders_pipeline()

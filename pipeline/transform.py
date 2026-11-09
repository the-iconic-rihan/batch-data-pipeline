"""PySpark transform: raw CSV -> typed, validated, deduplicated, partitioned Parquet."""

from __future__ import annotations

import logging
import os
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.config import Settings
from pipeline.generate import HEADER
from pipeline.validate import split_valid_invalid

log = logging.getLogger(__name__)

# Read everything as string first. Casting afterwards turns bad values into NULLs
# that the validation rules can catch, instead of the whole job failing on row 1.
RAW_SCHEMA = StructType([StructField(name, StringType(), True) for name in HEADER])


class DataQualityError(RuntimeError):
    """Raised when a batch is too broken to publish. Retrying will not fix it."""


def build_spark(app_name: str = "batch-data-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _clean(col: str):
    trimmed = F.trim(F.col(col))
    return F.when(trimmed == "", None).otherwise(trimmed)


def cast_types(raw: DataFrame) -> DataFrame:
    return raw.select(
        _clean("order_id").alias("order_id"),
        _clean("customer_id").alias("customer_id"),
        F.to_timestamp(_clean("order_ts"), "yyyy-MM-dd HH:mm:ss").alias("order_ts"),
        _clean("product_id").alias("product_id"),
        F.lower(_clean("category")).alias("category"),
        _clean("quantity").cast("int").alias("quantity"),
        _clean("unit_price").cast("decimal(12,2)").alias("unit_price"),
        F.upper(_clean("currency")).alias("currency"),
        F.lower(_clean("status")).alias("status"),
        F.upper(_clean("country")).alias("country"),
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the latest version of each order (upstream re-sends status updates)."""
    latest_first = Window.partitionBy("order_id").orderBy(F.col("order_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(latest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def transform_day(spark: SparkSession, ds: str, settings: Settings) -> dict:
    """Process one day. Safe to re-run: the day's output partition is overwritten."""
    started = time.monotonic()
    raw_path = settings.raw_path(ds)
    if not raw_path.exists():
        raise FileNotFoundError(f"No source extract for {ds}: {raw_path}")

    raw = spark.read.csv(str(raw_path), header=True, schema=RAW_SCHEMA, mode="PERMISSIVE")
    typed = cast_types(raw).cache()
    valid, invalid = split_valid_invalid(typed)

    raw_rows = typed.count()
    valid_rows = valid.count()
    invalid_rows = raw_rows - valid_rows
    invalid_ratio = invalid_rows / raw_rows if raw_rows else 1.0

    # Quality gate runs BEFORE anything is written, so a bad batch never reaches
    # the curated layer or ClickHouse.
    if raw_rows == 0:
        raise DataQualityError(f"{ds}: source extract is empty")
    if invalid_ratio > settings.max_invalid_ratio:
        raise DataQualityError(
            f"{ds}: {invalid_ratio:.2%} of rows failed validation "
            f"(limit {settings.max_invalid_ratio:.2%})"
        )

    curated = (
        deduplicate(valid)
        .withColumn("amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    # Writing straight to the partition directory with mode=overwrite replaces
    # exactly this day and nothing else -> re-runs and backfills are idempotent.
    curated.coalesce(4).write.mode("overwrite").parquet(str(settings.curated_partition(ds)))
    invalid.coalesce(1).write.mode("overwrite").parquet(str(settings.quarantine_partition(ds)))

    curated_rows = spark.read.parquet(str(settings.curated_partition(ds))).count()
    error_counts = {
        row["error"]: row["count"]
        for row in invalid.select(F.explode("_errors").alias("error"))
        .groupBy("error")
        .count()
        .collect()
    }
    typed.unpersist()

    metrics = {
        "ds": ds,
        "raw_rows": raw_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "duplicates_removed": valid_rows - curated_rows,
        "curated_rows": curated_rows,
        "invalid_ratio": round(invalid_ratio, 6),
        "error_counts": error_counts,
        "transform_seconds": round(time.monotonic() - started, 2),
    }
    log.info("transform metrics: %s", metrics)
    return metrics

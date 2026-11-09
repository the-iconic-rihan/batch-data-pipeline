"""Row-level data quality rules.

Each rule is a named Spark condition that is True for a GOOD row. Instead of
dropping bad rows silently, every row gets an ``_errors`` array listing the rules
it broke. Bad rows go to a quarantine area where they can be inspected and
replayed, and the error counts feed the data-quality gate.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

SUPPORTED_CURRENCIES = ("INR", "USD", "EUR")
KNOWN_STATUSES = ("placed", "shipped", "delivered", "cancelled", "returned")


def rules() -> dict[str, Column]:
    """Rule name -> condition that must hold. Built lazily: Columns need a SparkContext."""
    return {
        "order_id_missing": F.col("order_id").isNotNull(),
        "customer_id_missing": F.col("customer_id").isNotNull(),
        "order_ts_unparseable": F.col("order_ts").isNotNull(),
        "quantity_not_positive": F.col("quantity") > 0,
        "unit_price_invalid": F.col("unit_price") >= 0,
        "currency_unsupported": F.col("currency").isin(*SUPPORTED_CURRENCIES),
        "status_unknown": F.col("status").isin(*KNOWN_STATUSES),
    }


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid rows, invalid rows with an ``_errors`` array column)."""
    # A NULL condition (e.g. NULL > 0) counts as a failure, hence the coalesce.
    checks = [
        F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules().items()
    ]
    flagged = df.withColumn("_errors", F.filter(F.array(*checks), lambda e: e.isNotNull()))
    valid = flagged.filter(F.size("_errors") == 0).drop("_errors")
    invalid = flagged.filter(F.size("_errors") > 0)
    return valid, invalid

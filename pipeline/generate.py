"""Synthetic upstream source.

Stands in for an upstream system that drops one CSV extract per day. The data is
realistic enough to be interesting: about 1% of rows are deliberately broken and
about 0.5% of orders are re-sent later with a newer status, so the pipeline has
real validation and deduplication work to do.

Output is deterministic per day (seeded by the date), so reruns and backfills
produce the same input and the pipeline's idempotency can be checked.
"""

from __future__ import annotations

import csv
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

HEADER = [
    "order_id",
    "customer_id",
    "order_ts",
    "product_id",
    "category",
    "quantity",
    "unit_price",
    "currency",
    "status",
    "country",
]
CATEGORIES = ["electronics", "fashion", "grocery", "home", "books", "sports"]
STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]
CURRENCIES = ["INR", "USD", "EUR"]
COUNTRIES = ["IN", "US", "GB", "DE", "AE", "SG"]
DEFECTS = ["null_customer", "negative_quantity", "bad_timestamp", "bad_currency", "bad_price"]
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _good_row(rng: random.Random, day: date, i: int) -> dict[str, str]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    ts = start + timedelta(seconds=rng.randrange(86_400))
    return {
        "order_id": f"{day:%Y%m%d}-{i:08d}",
        "customer_id": f"C{rng.randrange(1, 200_000):06d}",
        "order_ts": ts.strftime(TS_FORMAT),
        "product_id": f"P{rng.randrange(1, 5_000):05d}",
        "category": rng.choice(CATEGORIES),
        "quantity": str(rng.randint(1, 5)),
        "unit_price": f"{rng.uniform(50, 5_000):.2f}",
        "currency": rng.choices(CURRENCIES, weights=[80, 15, 5])[0],
        "status": rng.choice(STATUSES[:2]),
        "country": rng.choices(COUNTRIES, weights=[70, 10, 5, 5, 5, 5])[0],
    }


def _break(row: dict[str, str], rng: random.Random) -> dict[str, str]:
    defect = rng.choice(DEFECTS)
    if defect == "null_customer":
        row["customer_id"] = ""
    elif defect == "negative_quantity":
        row["quantity"] = str(-rng.randint(1, 5))
    elif defect == "bad_timestamp":
        row["order_ts"] = "N/A"
    elif defect == "bad_currency":
        row["currency"] = "XYZ"
    elif defect == "bad_price":
        row["unit_price"] = "free"
    return row


def generate_day(
    day: date,
    rows: int,
    out_path: Path,
    bad_ratio: float = 0.01,
    resend_ratio: float = 0.005,
    seed: int | None = None,
) -> Path:
    """Write one day of raw orders to ``out_path`` and return the path.

    The file is written to a temp name and renamed at the end, so a reader never
    sees a half-written extract.
    """
    rng = random.Random(seed if seed is not None else day.toordinal())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(out_path.name + ".tmp")

    with tmp_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER)
        writer.writeheader()
        for i in range(rows):
            row = _good_row(rng, day, i)
            roll = rng.random()
            if roll < bad_ratio:
                row = _break(row, rng)
            writer.writerow(row)
            if bad_ratio <= roll < bad_ratio + resend_ratio:
                # Upstream re-sends the same order later with a newer status.
                update = dict(row)
                later = datetime.strptime(row["order_ts"], TS_FORMAT) + timedelta(minutes=30)
                update["order_ts"] = later.strftime(TS_FORMAT)
                update["status"] = "delivered"
                writer.writerow(update)

    tmp_path.replace(out_path)
    return out_path

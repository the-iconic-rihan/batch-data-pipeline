"""Runtime settings, read from environment variables (12-factor style)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    data_dir: Path = field(default_factory=lambda: Path(_env("DATA_DIR", "./data")))
    rows_per_day: int = field(default_factory=lambda: int(_env("ROWS_PER_DAY", "1000000")))
    max_invalid_ratio: float = field(
        default_factory=lambda: float(_env("MAX_INVALID_RATIO", "0.05"))
    )
    clickhouse_host: str = field(default_factory=lambda: _env("CLICKHOUSE_HOST", "localhost"))
    clickhouse_port: int = field(default_factory=lambda: int(_env("CLICKHOUSE_PORT", "8123")))
    clickhouse_user: str = field(default_factory=lambda: _env("CLICKHOUSE_USER", "pipeline"))
    clickhouse_password: str = field(
        default_factory=lambda: _env("CLICKHOUSE_PASSWORD", "pipeline")
    )
    clickhouse_db: str = field(default_factory=lambda: _env("CLICKHOUSE_DB", "analytics"))

    # Data lake layout (bronze / silver / quarantine):
    #   raw/order_date=YYYY-MM-DD/orders.csv            <- what upstream sends
    #   curated/orders/order_date=YYYY-MM-DD/*.parquet  <- clean, typed, deduplicated
    #   quarantine/orders/order_date=YYYY-MM-DD/*.parquet <- rejected rows + reasons
    def raw_path(self, ds: str) -> Path:
        return self.data_dir / "raw" / f"order_date={ds}" / "orders.csv"

    @property
    def curated_dir(self) -> Path:
        return self.data_dir / "curated" / "orders"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine" / "orders"

    def curated_partition(self, ds: str) -> Path:
        return self.curated_dir / f"order_date={ds}"

    def quarantine_partition(self, ds: str) -> Path:
        return self.quarantine_dir / f"order_date={ds}"

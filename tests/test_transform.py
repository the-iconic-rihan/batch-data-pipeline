from datetime import date

import pyarrow.parquet as pq
import pytest

from pipeline.generate import HEADER
from pipeline.load import read_partition
from pipeline.transform import DataQualityError, cast_types, deduplicate, transform_day
from pipeline.validate import split_valid_invalid

DS = "2026-11-08"

GOOD = ["1", "C1", "2026-11-08 10:00:00", "P1", "Books", "2", "100.50", "inr", "placed", "in"]


def _row(**overrides):
    values = dict(zip(HEADER, GOOD, strict=True))
    values.update(overrides)
    return [values[c] for c in HEADER]


def _df(spark, rows):
    return cast_types(spark.createDataFrame(rows, HEADER))


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(HEADER)] + [",".join(r) for r in rows]
    path.write_text("\n".join(lines) + "\n")


def test_cast_types_normalises_values(spark):
    row = _df(spark, [_row()]).collect()[0]
    assert row.category == "books"
    assert row.currency == "INR"
    assert row.country == "IN"
    assert row.quantity == 2
    assert str(row.unit_price) == "100.50"


@pytest.mark.parametrize(
    ("override", "expected_error"),
    [
        ({"customer_id": ""}, "customer_id_missing"),
        ({"order_ts": "N/A"}, "order_ts_unparseable"),
        ({"quantity": "-1"}, "quantity_not_positive"),
        ({"quantity": "abc"}, "quantity_not_positive"),
        ({"unit_price": "free"}, "unit_price_invalid"),
        ({"currency": "XYZ"}, "currency_unsupported"),
        ({"status": "lost"}, "status_unknown"),
    ],
)
def test_each_rule_catches_its_defect(spark, override, expected_error):
    valid, invalid = split_valid_invalid(_df(spark, [_row(), _row(order_id="2", **override)]))
    assert valid.count() == 1
    errors = invalid.collect()[0]["_errors"]
    assert errors == [expected_error]


def test_row_with_several_defects_lists_all_of_them(spark):
    _, invalid = split_valid_invalid(_df(spark, [_row(customer_id="", currency="XYZ")]))
    assert set(invalid.collect()[0]["_errors"]) == {"customer_id_missing", "currency_unsupported"}


def test_deduplicate_keeps_latest_version(spark):
    rows = [
        _row(order_ts="2026-11-08 10:00:00", status="placed"),
        _row(order_ts="2026-11-08 10:30:00", status="delivered"),
    ]
    result = deduplicate(_df(spark, rows)).collect()
    assert len(result) == 1
    assert result[0].status == "delivered"


def test_transform_day_end_to_end_and_idempotent(spark, settings):
    rows = [_row(order_id=str(i)) for i in range(100)]
    rows.append(_row(order_id="5", order_ts="2026-11-08 11:00:00", status="delivered"))  # resend
    rows.append(_row(order_id="bad", currency="XYZ"))  # 1 bad row out of 102 < 5%
    _write_csv(settings.raw_path(DS), rows)

    first = transform_day(spark, DS, settings)
    second = transform_day(spark, DS, settings)  # re-run must not duplicate

    for metrics in (first, second):
        assert metrics["raw_rows"] == 102
        assert metrics["invalid_rows"] == 1
        assert metrics["duplicates_removed"] == 1
        assert metrics["curated_rows"] == 100
        assert metrics["error_counts"] == {"currency_unsupported": 1}

    table = pq.read_table(settings.curated_partition(DS))
    assert table.num_rows == 100
    assert {"amount", "_ingested_at"} <= set(table.column_names)
    assert pq.read_table(settings.quarantine_partition(DS)).num_rows == 1

    # What the ClickHouse loader will send: partition column re-attached
    loaded = read_partition(settings, DS)
    assert loaded.column("order_date").unique().to_pylist() == [date(2026, 11, 8)]


def test_quality_gate_blocks_bad_batch_before_writing(spark, settings):
    rows = [_row(order_id=str(i)) for i in range(10)] + [
        _row(order_id=f"bad{i}", quantity="-1") for i in range(5)
    ]
    _write_csv(settings.raw_path(DS), rows)

    with pytest.raises(DataQualityError, match="failed validation"):
        transform_day(spark, DS, settings)
    assert not settings.curated_partition(DS).exists()


def test_missing_source_fails_clearly(spark, settings):
    with pytest.raises(FileNotFoundError, match=DS):
        transform_day(spark, DS, settings)

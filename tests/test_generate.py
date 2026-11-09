import csv
from datetime import date

from pipeline.generate import HEADER, generate_day

DAY = date(2026, 11, 8)


def _read(path):
    with path.open() as fh:
        return list(csv.DictReader(fh))


def test_writes_header_and_expected_volume(tmp_path):
    path = generate_day(DAY, 1_000, tmp_path / "orders.csv")
    rows = _read(path)
    assert list(rows[0].keys()) == HEADER
    # 1,000 orders plus a handful of re-sent updates
    assert 1_000 <= len(rows) < 1_050


def test_same_day_is_deterministic(tmp_path):
    a = generate_day(DAY, 500, tmp_path / "a.csv")
    b = generate_day(DAY, 500, tmp_path / "b.csv")
    assert a.read_bytes() == b.read_bytes()


def test_contains_bad_rows_and_resent_orders(tmp_path):
    rows = _read(generate_day(DAY, 5_000, tmp_path / "orders.csv"))
    order_ids = [r["order_id"] for r in rows]
    assert len(order_ids) > len(set(order_ids)), "expected re-sent (duplicate) orders"
    assert any(r["customer_id"] == "" for r in rows) or any(
        r["currency"] == "XYZ" for r in rows
    ), "expected deliberately broken rows"


def test_no_partial_file_left_behind(tmp_path):
    generate_day(DAY, 100, tmp_path / "orders.csv")
    assert [p.name for p in tmp_path.iterdir()] == ["orders.csv"]

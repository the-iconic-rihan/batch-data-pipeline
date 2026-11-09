-- Example analytical queries. Run with: make query

-- Daily revenue by category and currency
SELECT
    order_date,
    category,
    currency,
    count()      AS orders,
    sum(amount)  AS revenue
FROM orders
GROUP BY order_date, category, currency
ORDER BY order_date DESC, revenue DESC
LIMIT 20
FORMAT PrettyCompact;

-- Pipeline run history: volume and data quality over time
SELECT
    ds,
    raw_rows,
    loaded_rows,
    invalid_rows,
    duplicates_removed,
    round(invalid_ratio * 100, 2) AS invalid_pct,
    transform_seconds,
    load_seconds,
    finished_at
FROM pipeline_runs
ORDER BY finished_at DESC
LIMIT 10
FORMAT PrettyCompact;

-- Proof of idempotency: exactly one row per order_id, even after re-runs
SELECT
    order_date,
    count()                   AS rows,
    uniqExact(order_id)       AS unique_orders,
    rows = unique_orders      AS no_duplicates
FROM orders
GROUP BY order_date
ORDER BY order_date DESC
FORMAT PrettyCompact;

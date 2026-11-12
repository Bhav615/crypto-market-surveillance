"""Print a results summary (throughput, latency, reliability, accuracy) from PostgreSQL –
handy for the presentation.  Run:  docker compose exec sink python -m app.tools.report"""
from __future__ import annotations

import psycopg

from app import config

QUERIES = {
    "Trades per exchange (whole run)": """
        SELECT exchange, count(*) AS trades, count(DISTINCT symbol) AS symbols,
               min(event_ts) AS first_trade, max(event_ts) AS last_trade,
               round(count(*) / greatest(extract(epoch FROM max(sink_ts) - min(sink_ts)), 1)::numeric, 2) AS avg_trades_per_s,
               count(*) FILTER (WHERE source = 'rest_backfill') AS backfilled
        FROM trades GROUP BY exchange ORDER BY exchange""",
    "Peak throughput (best 10-second window, trades/s)": """
        SELECT round(max(n) / 10.0, 1) AS peak_trades_per_s FROM (
            SELECT date_bin('10 seconds', sink_ts, TIMESTAMPTZ '2000-01-01') AS t, count(*) AS n
            FROM trades GROUP BY 1) x""",
    "Latency in ms (live trades)": """
        SELECT 'pipeline: producer -> Kafka -> detector/sink -> DB' AS path,
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p95,
               percentile_cont(0.99) WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p99
        FROM trades WHERE source = 'ws'
        UNION ALL
        SELECT 'end-to-end: exchange match -> DB',
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY e2e_latency_ms),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e_latency_ms),
               percentile_cont(0.99) WITHIN GROUP (ORDER BY e2e_latency_ms)
        FROM trades WHERE source = 'ws'
        UNION ALL
        SELECT 'anomaly detection: event -> alert',
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000),
               percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000)
        FROM anomalies""",
    "Accuracy: sequence-gap check on stored trades": """
        SELECT exchange, symbol, count(*) AS stored, max(trade_id) - min(trade_id) + 1 AS expected,
               round(100.0 * count(*) / (max(trade_id) - min(trade_id) + 1), 3) AS completeness_pct
        FROM trades GROUP BY exchange, symbol ORDER BY exchange, symbol""",
    "Duplicates in DB (must be 0 thanks to idempotent writes)": """
        SELECT count(*) - count(DISTINCT (exchange, symbol, trade_id)) AS duplicate_rows FROM trades""",
    "Anomalies by type": """
        SELECT type, count(*) AS n, round(avg(score)::numeric, 2) AS avg_score FROM anomalies
        GROUP BY type ORDER BY n DESC""",
    "Producer reliability counters (latest report)": """
        SELECT DISTINCT ON (service) service, payload->'extra'->>'reconnects' AS reconnects,
               payload->'extra'->>'gaps' AS gaps, payload->'extra'->>'missing' AS missing,
               payload->'extra'->>'backfilled' AS backfilled,
               payload->'extra'->>'delivery_failed' AS delivery_failed, payload->'extra'->>'dlq' AS dlq
        FROM pipeline_metrics WHERE service LIKE 'producer-%' ORDER BY service, ts DESC""",
}


def main() -> None:
    with psycopg.connect(config.POSTGRES_DSN) as conn, conn.cursor() as cur:
        for title, sql in QUERIES.items():
            cur.execute(sql)
            cols = [c.name for c in cur.description]
            rows = cur.fetchall()
            print(f"\n=== {title} ===")
            widths = [max(len(str(c)), *(len(_fmt(r[i])) for r in rows)) if rows else len(c)
                      for i, c in enumerate(cols)]
            print("  ".join(str(c).ljust(w) for c, w in zip(cols, widths)))
            for r in rows:
                print("  ".join(_fmt(v).ljust(w) for v, w in zip(r, widths)))


def _fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:,.2f}"
    return "" if v is None else str(v)


if __name__ == "__main__":
    main()

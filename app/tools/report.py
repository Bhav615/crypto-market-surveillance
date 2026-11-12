"""Print a results summary (throughput, latency, reliability, accuracy) from PostgreSQL –
handy for the presentation.

Run:  docker compose exec sink python -m app.tools.report               (last 60 minutes)
      docker compose exec sink python -m app.tools.report --minutes 180

All measures are computed over a time window in which the pipeline was running. Over a period
when the laptop was off or asleep, no trades are collected, so "completeness" over that whole
period would measure downtime, not the pipeline.
"""
from __future__ import annotations

import argparse

import psycopg

from app import config

W = "now() - make_interval(mins => %(m)s)"

QUERIES = {
    "Trades per exchange": f"""
        SELECT exchange, count(*) AS trades, count(DISTINCT symbol) AS symbols,
               count(*) FILTER (WHERE source = 'rest_backfill') AS backfilled,
               round((count(*) / greatest(extract(epoch FROM max(sink_ts) - min(sink_ts)), 1))::numeric, 2)
                   AS avg_trades_per_s
        FROM trades WHERE sink_ts > {W} GROUP BY exchange ORDER BY exchange""",
    "Peak throughput (best 10-second window)": f"""
        SELECT round(max(n) / 10.0, 1) AS peak_trades_per_s FROM (
            SELECT date_bin('10 seconds', sink_ts, TIMESTAMPTZ '2000-01-01') AS t, count(*) AS n
            FROM trades WHERE sink_ts > {W} GROUP BY 1) x""",
    "Latency in ms (live trades)": f"""
        SELECT 'pipeline: producer -> Kafka -> sink -> DB' AS path,
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p95,
               percentile_cont(0.99) WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p99
        FROM trades WHERE source = 'ws' AND sink_ts > {W}
        UNION ALL
        SELECT 'end-to-end: exchange match -> DB',
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY e2e_latency_ms),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e_latency_ms),
               percentile_cont(0.99) WITHIN GROUP (ORDER BY e2e_latency_ms)
        FROM trades WHERE source = 'ws' AND sink_ts > {W}
        UNION ALL
        SELECT 'whale detection: trade -> alert',
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000),
               percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000)
        FROM anomalies WHERE type IN ('WHALE_TRADE', 'CROSS_EXCHANGE_DIVERGENCE') AND event_ts > {W}
             AND coalesce(details->>'source', 'ws') = 'ws'
        UNION ALL
        SELECT 'bar detection: bar end -> alert (incl. 2 s lateness wait)',
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000),
               percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000),
               percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000)
        FROM anomalies WHERE type IN ('PRICE_SPIKE', 'VOLUME_BURST') AND event_ts > {W}
             AND detected_ts - event_ts < interval '5 minutes'""",
    "Accuracy: trade-id continuity per market (within the window)": f"""
        SELECT exchange, symbol, count(*) AS stored, max(trade_id) - min(trade_id) + 1 AS expected,
               round(100.0 * count(*) / (max(trade_id) - min(trade_id) + 1), 3) AS completeness_pct
        FROM trades WHERE event_ts > {W} GROUP BY exchange, symbol ORDER BY exchange, symbol""",
    "Duplicates in DB, whole history (must be 0 thanks to idempotent writes)": """
        SELECT count(*) AS stored_trades,
               count(*) - count(DISTINCT (exchange, symbol, trade_id)) AS duplicate_rows FROM trades""",
    "Anomalies by type": f"""
        SELECT type, count(*) AS n, round(avg(abs(score))::numeric, 2) AS avg_abs_score FROM anomalies
        WHERE event_ts > {W} GROUP BY type ORDER BY n DESC""",
    "Producer reliability counters (latest report of the running producers)": """
        SELECT DISTINCT ON (service) service, payload->'extra'->>'reconnects' AS reconnects,
               payload->'extra'->>'gaps' AS gaps, payload->'extra'->>'missing' AS missing,
               payload->'extra'->>'backfilled' AS backfilled,
               payload->'extra'->>'completeness_pct' AS completeness_pct,
               payload->'extra'->>'delivery_failed' AS delivery_failed, payload->'extra'->>'dlq' AS dlq
        FROM pipeline_metrics WHERE service LIKE 'producer-%%' ORDER BY service, ts DESC""",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=60, help="time window to report on (default 60)")
    args = ap.parse_args()
    print(f"Report window: last {args.minutes} minutes")
    with psycopg.connect(config.POSTGRES_DSN) as conn, conn.cursor() as cur:
        for title, sql in QUERIES.items():
            cur.execute(sql, {"m": args.minutes})
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

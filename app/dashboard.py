"""Live dashboard (Streamlit) on top of the Postgres serving layer + live Kafka cluster metadata.
Open http://localhost:8501 . Auto-refreshes every 5 seconds.

Pages
  1. Business objectives  – (1) large-trade / market-abuse surveillance (compliance)
                            (2) cross-exchange arbitrage monitoring (trading desk)
                            (3) market risk: volatility, order-flow pressure, liquidity (risk team)
  2. Market               – live prices, volume and the anomaly feed
  3. Pipeline & servers   – Kafka brokers/replicas, service heartbeats, throughput, latency, data quality
"""
from __future__ import annotations

import pandas as pd
import psycopg
import streamlit as st
from confluent_kafka.admin import AdminClient
from psycopg.types.numeric import FloatLoader

from app import config

st.set_page_config(page_title="Real-time Crypto Market Surveillance", layout="wide")

EXPECTED_BROKERS = 3
SERVICES = ["producer-coinbase", "producer-kraken", "detector", "sink"]


@st.cache_resource
def get_conn():
    conn = psycopg.connect(config.POSTGRES_DSN, autocommit=True, connect_timeout=3)
    conn.adapters.register_loader("numeric", FloatLoader)  # Decimal -> float for pandas/charts
    return conn


@st.cache_resource
def get_admin():
    return AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "socket.timeout.ms": 3000})


def q(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    except psycopg.Error:
        get_conn.clear()
        raise


def money(v) -> str:
    if v is None or pd.isna(v):
        return "–"
    v = float(v)
    return f"${v / 1e6:,.2f}M" if abs(v) >= 1e6 else f"${v:,.0f}"


def ms(v) -> str:
    return "–" if v is None or pd.isna(v) else f"{float(v):,.0f} ms"


st.title("Real-time Crypto Market Surveillance")
st.caption("Live trades from Coinbase + Kraken WebSockets → Kafka (3 brokers) → anomaly detector → PostgreSQL")

page = st.sidebar.radio("Page", ["Business objectives", "Market", "Pipeline & servers"])
symbol = st.sidebar.selectbox("Symbol", config.SYMBOLS, index=0)
minutes = st.sidebar.slider("Time window (minutes)", 5, 240, 30)
st.sidebar.caption("Auto-refresh every 5 s")


# =====================================================================================
# Page 1 – Business objectives
# =====================================================================================
def objectives_page():
    # ---------------------------------------------------------------- Objective 1
    st.header("1 · Large-trade & market-abuse surveillance")
    st.caption("Who: compliance team · Goal: flag unusually large trades (whales) and abnormal volume "
               "the moment they happen, so they can be reviewed for market manipulation.")
    k = q("""SELECT count(*) FILTER (WHERE type='WHALE_TRADE')  AS whales,
                    coalesce(sum(value) FILTER (WHERE type='WHALE_TRADE'), 0) AS whale_usd,
                    max(value) FILTER (WHERE type='WHALE_TRADE') AS biggest,
                    count(*) FILTER (WHERE type='VOLUME_BURST') AS bursts,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM detected_ts - event_ts) * 1000)
                        FILTER (WHERE type='WHALE_TRADE') AS whale_detect_ms
             FROM anomalies WHERE event_ts > now() - make_interval(mins => %s)""", (minutes,)).iloc[0]
    share = q("""SELECT coalesce(sum(notional) FILTER (WHERE notional >= %s), 0) / nullif(sum(notional), 0) * 100 AS pct
                 FROM trades WHERE event_ts > now() - make_interval(mins => %s)""",
              (config.WHALE_MIN_NOTIONAL_USD, minutes)).iloc[0]["pct"]
    c = st.columns(5)
    c[0].metric(f"Whale alerts ({minutes} min)", int(k["whales"]))
    c[1].metric("Whale volume flagged", money(k["whale_usd"]))
    c[2].metric("Largest flagged trade", money(k["biggest"]))
    c[3].metric(f"Share of volume in trades ≥ {money(config.WHALE_MIN_NOTIONAL_USD)}",
                "–" if share is None or pd.isna(share) else f"{float(share):.1f} %")
    c[4].metric("Volume-burst alerts", int(k["bursts"]))
    whales = q("""SELECT event_ts AT TIME ZONE 'UTC' AS time_utc, exchange, symbol,
                         details->>'side' AS side, round(value::numeric, 0) AS usd,
                         round(score::numeric, 1) AS z_score, description
                  FROM anomalies WHERE type IN ('WHALE_TRADE', 'VOLUME_BURST')
                  ORDER BY event_ts DESC LIMIT 10""")
    if whales.empty:
        st.info("No large-trade alerts yet. Whale detection starts after ~200 trades per market.")
    else:
        st.dataframe(whales, hide_index=True, use_container_width=True)

    # ---------------------------------------------------------------- Objective 2
    st.header("2 · Cross-exchange arbitrage monitoring")
    st.caption("Who: trading desk · Goal: see in real time where the same coin trades at a different price "
               "on Coinbase vs Kraken (an arbitrage opportunity or a sign that one venue is stressed).")
    latest = q("""SELECT DISTINCT ON (symbol, exchange) symbol, exchange, close, bar_start
                  FROM bars WHERE bar_start > now() - interval '5 minutes'
                  ORDER BY symbol, exchange, bar_start DESC""")
    if latest.empty:
        st.info("Waiting for bars from both exchanges…")
    else:
        pv = latest.pivot_table(index="symbol", columns="exchange", values="close")
        if {"coinbase", "kraken"} <= set(pv.columns):
            pv["spread_bps"] = (pv["coinbase"] - pv["kraken"]) / ((pv["coinbase"] + pv["kraken"]) / 2) * 1e4
            pv["cheaper_on"] = pv["spread_bps"].apply(lambda s: "kraken" if s > 0 else "coinbase")
            pv["opportunity"] = pv["spread_bps"].abs().apply(
                lambda s: "🔴 above alert threshold" if s >= config.DIVERGENCE_BPS else
                ("🟡 watch" if s >= config.DIVERGENCE_BPS / 2 else "🟢 normal"))
            left, right = st.columns([2, 3])
            with left:
                st.caption("Latest price per exchange and spread (1 bps = 0.01 %)")
                st.dataframe(pv.round({"coinbase": 4, "kraken": 4, "spread_bps": 1}), use_container_width=True)
                n_div = q("""SELECT count(*) AS n FROM anomalies WHERE type='CROSS_EXCHANGE_DIVERGENCE'
                             AND event_ts > now() - make_interval(mins => %s)""", (minutes,)).iloc[0]["n"]
                st.metric(f"Divergence alerts (≥ {config.DIVERGENCE_BPS:g} bps, {minutes} min)", int(n_div))
            with right:
                sp = q("""SELECT c.bar_start, (c.close - k.close) / ((c.close + k.close) / 2) * 1e4 AS spread_bps
                          FROM bars c JOIN bars k ON k.symbol = c.symbol AND k.bar_start = c.bar_start
                               AND k.bar_seconds = c.bar_seconds AND k.exchange = 'kraken'
                          WHERE c.exchange = 'coinbase' AND c.symbol = %s
                            AND c.bar_start > now() - make_interval(mins => %s)
                          ORDER BY c.bar_start""", (symbol, minutes))
                st.caption(f"{symbol}: Coinbase − Kraken spread over time (bps)")
                if not sp.empty:
                    st.line_chart(sp.set_index("bar_start"))
        else:
            st.dataframe(pv, use_container_width=True)
            st.info("Need data from both exchanges to compute spreads.")

    # ---------------------------------------------------------------- Objective 3
    st.header("3 · Market risk: volatility, order-flow pressure & liquidity")
    st.caption("Who: risk team · Goal: know which markets are getting volatile, whether buyers or sellers "
               "dominate, and how much liquidity there is, before positions are hit.")
    risk = q("""
        WITH b AS (
            SELECT symbol, exchange, bar_start, notional, buy_notional, sell_notional, trades,
                   ln(close / lag(close) OVER (PARTITION BY exchange, symbol ORDER BY bar_start)) AS r
            FROM bars WHERE bar_start > now() - make_interval(mins => %s))
        SELECT symbol,
               round((stddev_samp(r) * 1e4)::numeric, 2)                                  AS volatility_bps_per_bar,
               round((100 * sum(buy_notional) / nullif(sum(buy_notional + sell_notional), 0))::numeric, 1) AS buy_pressure_pct,
               round(sum(notional)::numeric, 0)                                           AS traded_usd,
               sum(trades)                                                                AS trades
        FROM b GROUP BY symbol ORDER BY volatility_bps_per_bar DESC NULLS LAST""", (minutes,))
    if risk.empty:
        st.info("Waiting for bars…")
    else:
        risk["signal"] = risk["buy_pressure_pct"].astype(float).apply(
            lambda p: "🟢 buyers dominate" if p >= 60 else ("🔴 sellers dominate" if p <= 40 else "⚪ balanced"))
        spikes = q("""SELECT symbol, count(*) AS price_spikes FROM anomalies WHERE type='PRICE_SPIKE'
                      AND event_ts > now() - make_interval(mins => %s) GROUP BY symbol""", (minutes,))
        risk = risk.merge(spikes, on="symbol", how="left")
        risk["price_spikes"] = risk["price_spikes"].fillna(0).astype(int)
        left, right = st.columns([3, 2])
        with left:
            st.caption(f"Per market over the last {minutes} min (volatility = std. dev. of {config.BAR_SECONDS}s returns)")
            st.dataframe(risk, hide_index=True, use_container_width=True)
        with right:
            st.caption("Volatility ranking (bps per bar)")
            st.bar_chart(risk.set_index("symbol")["volatility_bps_per_bar"].astype(float))


# =====================================================================================
# Page 2 – Market
# =====================================================================================
def market_page():
    kpi = q("""SELECT count(*) FILTER (WHERE sink_ts > now() - interval '60 seconds') / 60.0 AS tps,
                      count(*) AS total FROM trades""").iloc[0]
    n_anom = q("SELECT count(*) AS n FROM anomalies WHERE event_ts > now() - interval '1 hour'").iloc[0]["n"]
    c = st.columns(3)
    c[0].metric("Throughput (trades/s, last 60 s)", f"{float(kpi['tps'] or 0):,.1f}")
    c[1].metric("Trades stored", f"{int(kpi['total']):,}")
    c[2].metric("Anomalies (last hour)", int(n_anom))
    left, right = st.columns([3, 2])
    with left:
        st.subheader(f"{symbol}: close price per exchange ({config.BAR_SECONDS}s bars)")
        bars = q("""SELECT bar_start, exchange, close, notional FROM bars
                    WHERE symbol=%s AND bar_start > now() - make_interval(mins => %s)
                    ORDER BY bar_start""", (symbol, minutes))
        if bars.empty:
            st.info("No bars yet. The first bars appear ~15 s after start.")
        else:
            st.line_chart(bars.pivot_table(index="bar_start", columns="exchange", values="close"))
            st.caption("Traded notional (USD) per bar")
            st.bar_chart(bars.pivot_table(index="bar_start", columns="exchange", values="notional"))
    with right:
        st.subheader("Latest anomalies")
        an = q("""SELECT event_ts AT TIME ZONE 'UTC' AS event_utc, type, exchange, symbol,
                         round(score::numeric, 2) AS score, description
                  FROM anomalies ORDER BY event_ts DESC LIMIT 25""")
        st.dataframe(an, hide_index=True, use_container_width=True, height=420)
        counts = q("""SELECT type, count(*) AS n FROM anomalies
                      WHERE event_ts > now() - interval '24 hours' GROUP BY type ORDER BY n DESC""")
        if not counts.empty:
            st.bar_chart(counts.set_index("type"))


# =====================================================================================
# Page 3 – Pipeline & servers
# =====================================================================================
def kafka_health():
    st.subheader("Kafka cluster (live from broker metadata)")
    try:
        md = get_admin().list_topics(timeout=3)
    except Exception as exc:
        st.error(f"Kafka cluster not reachable: {exc}")
        return
    alive = sorted(b.id for b in md.brokers.values())
    rows, total, under, offline = [], 0, 0, 0
    for name, t in sorted(md.topics.items()):
        if name.startswith("__"):
            continue
        for pid, p in sorted(t.partitions.items()):
            total += 1
            under += len(p.isrs) < len(p.replicas)
            offline += p.leader < 0
            rows.append({"topic": name, "partition": pid, "leader": p.leader, "replicas": str(p.replicas),
                         "in_sync": str(p.isrs),
                         "status": "🔴 offline" if p.leader < 0 else
                         ("🟡 under-replicated" if len(p.isrs) < len(p.replicas) else "🟢 healthy")})
    c = st.columns(4)
    c[0].metric("Brokers online", f"{len(alive)} / {EXPECTED_BROKERS}",
                delta=None if len(alive) == EXPECTED_BROKERS else f"{EXPECTED_BROKERS - len(alive)} down",
                delta_color="inverse")
    c[1].metric("Partitions", total)
    c[2].metric("Under-replicated", under)
    c[3].metric("Offline (no leader)", offline)
    if len(alive) < EXPECTED_BROKERS and offline == 0:
        st.warning(f"Broker(s) {sorted(set(range(1, EXPECTED_BROKERS + 1)) - set(alive))} down. All partitions "
                   "still have a leader (replication factor 3, min.insync.replicas 2), so the pipeline keeps "
                   "running without data loss.")
    with st.expander("Partition leaders and in-sync replicas"):
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


def services_health():
    st.subheader("Services (heartbeat = metrics report every 10 s)")
    hb = q("""SELECT service, instance, max(ts) AS last_seen,
                     extract(epoch FROM now() - max(ts)) AS age_s
              FROM pipeline_metrics WHERE ts > now() - interval '1 hour'
              GROUP BY service, instance""")
    rows = []
    for s in SERVICES:
        sub = hb[hb["service"] == s] if not hb.empty else hb
        live = sub[sub["age_s"] < 30] if not sub.empty else sub
        age = None if sub.empty else float(sub["age_s"].min())
        rows.append({"service": s, "instances_up": len(live),
                     "last_heartbeat_s_ago": None if age is None else round(age, 1),
                     "status": "🟢 up" if len(live) else "🔴 down / not reporting"})
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


def pipeline_page():
    kafka_health()
    services_health()
    kpi = q("""SELECT count(*) FILTER (WHERE sink_ts > now() - interval '60 seconds') / 60.0 AS tps,
                      percentile_cont(0.5)  WITHIN GROUP (ORDER BY pipeline_latency_ms)
                          FILTER (WHERE sink_ts > now() - interval '5 minutes') AS p50,
                      percentile_cont(0.95) WITHIN GROUP (ORDER BY pipeline_latency_ms)
                          FILTER (WHERE sink_ts > now() - interval '5 minutes') AS p95,
                      percentile_cont(0.99) WITHIN GROUP (ORDER BY pipeline_latency_ms)
                          FILTER (WHERE sink_ts > now() - interval '5 minutes') AS p99,
                      percentile_cont(0.95) WITHIN GROUP (ORDER BY e2e_latency_ms)
                          FILTER (WHERE sink_ts > now() - interval '5 minutes') AS e2e_p95
               FROM trades""").iloc[0]
    st.subheader("Throughput & latency")
    c = st.columns(4)
    c[0].metric("Throughput (trades/s, last 60 s)", f"{float(kpi['tps'] or 0):,.1f}")
    c[1].metric("Pipeline latency p50", ms(kpi["p50"]))
    c[2].metric("Pipeline latency p95 / p99", f"{ms(kpi['p95'])} / {ms(kpi['p99'])}")
    c[3].metric("Exchange → DB p95", ms(kpi["e2e_p95"]))
    a, b = st.columns(2)
    with a:
        tp = q("""SELECT date_bin('10 seconds', sink_ts, TIMESTAMPTZ '2000-01-01') AS t, exchange,
                         count(*) / 10.0 AS trades_per_s
                  FROM trades WHERE sink_ts > now() - interval '15 minutes'
                  GROUP BY 1, 2 ORDER BY 1""")
        st.caption("Throughput (trades/s) per exchange")
        if not tp.empty:
            st.line_chart(tp.pivot_table(index="t", columns="exchange", values="trades_per_s"))
    with b:
        lat = q("""SELECT date_bin('10 seconds', sink_ts, TIMESTAMPTZ '2000-01-01') AS t,
                          percentile_cont(0.5)  WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p50,
                          percentile_cont(0.95) WITHIN GROUP (ORDER BY pipeline_latency_ms) AS p95
                   FROM trades WHERE sink_ts > now() - interval '15 minutes' AND source='ws'
                   GROUP BY 1 ORDER BY 1""")
        st.caption("Pipeline latency producer → Kafka → DB (ms)")
        if not lat.empty:
            st.line_chart(lat.set_index("t"))

    st.subheader("Data quality & burst handling (reported by the producers)")
    dq = q("""SELECT DISTINCT ON (service) service,
                     payload->'extra'->>'connected' AS connected,
                     (payload->'extra'->>'received')::bigint AS received,
                     (payload->'extra'->>'missing')::bigint AS missing_trades,
                     (payload->'extra'->>'backfilled')::bigint AS backfilled,
                     (payload->'extra'->>'completeness_pct')::float AS completeness_pct,
                     (payload->'extra'->>'reconnects')::int AS reconnects,
                     (payload->'extra'->>'dlq')::int AS dead_letters,
                     (payload->'extra'->>'delivery_failed')::int AS delivery_failed,
                     (payload->'extra'->>'queue_depth')::int AS buffer_queue_depth,
                     payload->>'rate_in' AS msgs_per_s
              FROM pipeline_metrics WHERE service LIKE 'producer-%%'
              ORDER BY service, ts DESC""")
    st.dataframe(dq, hide_index=True, use_container_width=True)
    det = q("""SELECT DISTINCT ON (instance) instance, ts AT TIME ZONE 'UTC' AS reported_utc,
                      payload->>'rate_in' AS msgs_per_s,
                      payload->'extra'->>'assigned_partitions' AS partitions,
                      payload->'latency_ms'->'ingest_to_detect_ms'->>'p95' AS ingest_to_detect_p95_ms
               FROM pipeline_metrics WHERE service='detector' AND ts > now() - interval '30 seconds'
               ORDER BY instance, ts DESC""")
    st.caption("Detector instances (scale with `docker compose up -d --scale detector=3`)")
    st.dataframe(det, hide_index=True, use_container_width=True)


@st.fragment(run_every="5s")
def live():
    try:
        {"Business objectives": objectives_page, "Market": market_page,
         "Pipeline & servers": pipeline_page}[page]()
    except psycopg.Error as exc:
        st.warning(f"Database not reachable right now ({exc.__class__.__name__}). Data keeps buffering "
                   "in Kafka, and the sink will catch up automatically when PostgreSQL is back.")
        if page == "Pipeline & servers":
            kafka_health()


live()

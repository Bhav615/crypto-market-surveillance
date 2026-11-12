-- Serving layer for the dashboard. Primary keys make every write idempotent, so the
-- at-least-once Kafka consumer never creates duplicate rows.

CREATE TABLE IF NOT EXISTS trades (
    exchange            TEXT        NOT NULL,
    symbol              TEXT        NOT NULL,
    trade_id            BIGINT      NOT NULL,
    price               DOUBLE PRECISION NOT NULL,
    size                DOUBLE PRECISION NOT NULL,
    notional            DOUBLE PRECISION NOT NULL,
    side                TEXT        NOT NULL,
    source              TEXT        NOT NULL,
    event_ts            TIMESTAMPTZ NOT NULL,   -- exchange match time
    ingest_ts           TIMESTAMPTZ NOT NULL,   -- received by our producer
    sink_ts             TIMESTAMPTZ NOT NULL,   -- written to the database
    kafka_partition     INT,
    kafka_offset        BIGINT,
    ingest_latency_ms   INT,                    -- exchange -> producer (network + exchange clock skew)
    pipeline_latency_ms INT,                    -- producer -> Kafka -> sink (same host clock)
    e2e_latency_ms      INT,                    -- exchange -> sink
    PRIMARY KEY (exchange, symbol, trade_id)
);
CREATE INDEX IF NOT EXISTS trades_sink_ts_idx  ON trades (sink_ts);
CREATE INDEX IF NOT EXISTS trades_event_ts_idx ON trades (symbol, event_ts);

CREATE TABLE IF NOT EXISTS bars (
    exchange       TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    bar_seconds    INT  NOT NULL,
    bar_start      TIMESTAMPTZ NOT NULL,
    open           DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION, close DOUBLE PRECISION,
    vwap           DOUBLE PRECISION,
    volume         DOUBLE PRECISION,
    notional       DOUBLE PRECISION,
    buy_notional   DOUBLE PRECISION,
    sell_notional  DOUBLE PRECISION,
    trades         INT,
    PRIMARY KEY (exchange, symbol, bar_seconds, bar_start)
);
CREATE INDEX IF NOT EXISTS bars_start_idx ON bars (symbol, bar_start);

CREATE TABLE IF NOT EXISTS anomalies (
    anomaly_id   TEXT PRIMARY KEY,
    type         TEXT NOT NULL,
    exchange     TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    event_ts     TIMESTAMPTZ NOT NULL,
    detected_ts  TIMESTAMPTZ NOT NULL,
    sink_ts      TIMESTAMPTZ NOT NULL,
    score        DOUBLE PRECISION,
    value        DOUBLE PRECISION,
    threshold    DOUBLE PRECISION,
    description  TEXT,
    details      JSONB
);
CREATE INDEX IF NOT EXISTS anomalies_event_idx ON anomalies (event_ts DESC);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL,
    service   TEXT NOT NULL,
    instance  TEXT NOT NULL,
    payload   JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS pipeline_metrics_ts_idx ON pipeline_metrics (service, ts DESC);

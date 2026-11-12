"""Central configuration. Every value can be overridden with an environment variable
(see .env.example and docker-compose.yml)."""
import os


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


# ---------------------------------------------------------------- Kafka
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "kafka1:9092,kafka2:9092,kafka3:9092")
TOPIC_TRADES = _env("TOPIC_TRADES", "trades.raw")
TOPIC_BARS = _env("TOPIC_BARS", "ohlcv.bars")
TOPIC_ANOMALIES = _env("TOPIC_ANOMALIES", "market.anomalies")
TOPIC_DLQ = _env("TOPIC_DLQ", "trades.dlq")
TOPIC_METRICS = _env("TOPIC_METRICS", "pipeline.metrics")
TOPIC_LOADTEST = _env("TOPIC_LOADTEST", "loadtest.trades")

# ---------------------------------------------------------------- Sources
EXCHANGE = _env("EXCHANGE", "coinbase").lower()
SYMBOLS = [s.strip().upper() for s in
           _env("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD,XRP-USD,DOGE-USD,ADA-USD").split(",")
           if s.strip()]
COINBASE_WS_URL = _env("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
COINBASE_REST_URL = _env("COINBASE_REST_URL", "https://api.exchange.coinbase.com")
KRAKEN_WS_URL = _env("KRAKEN_WS_URL", "wss://ws.kraken.com/v2")
WS_WATCHDOG_SECONDS = _float("WS_WATCHDOG_SECONDS", 30.0)   # reconnect if the feed goes silent
BACKFILL_MAX_TRADES = _int("BACKFILL_MAX_TRADES", 10000)     # max gap size repaired via REST

# ---------------------------------------------------------------- Analytics
BAR_SECONDS = _int("BAR_SECONDS", 10)
BAR_LATENESS_MS = _int("BAR_LATENESS_MS", 2000)
WHALE_MIN_NOTIONAL_USD = _float("WHALE_MIN_NOTIONAL_USD", 50_000)
WHALE_Z = _float("WHALE_Z", 3.5)
WHALE_WINDOW = _int("WHALE_WINDOW", 2000)
WHALE_MIN_HISTORY = _int("WHALE_MIN_HISTORY", 200)
SPIKE_Z = _float("SPIKE_Z", 4.0)
SPIKE_MIN_BPS = _float("SPIKE_MIN_BPS", 10.0)
VOLUME_Z = _float("VOLUME_Z", 4.0)
VOLUME_MIN_RATIO = _float("VOLUME_MIN_RATIO", 5.0)
BAR_WINDOW = _int("BAR_WINDOW", 180)
BAR_MIN_HISTORY = _int("BAR_MIN_HISTORY", 18)
DIVERGENCE_BPS = _float("DIVERGENCE_BPS", 25.0)
DIVERGENCE_MAX_AGE_MS = _int("DIVERGENCE_MAX_AGE_MS", 5000)
DIVERGENCE_COOLDOWN_MS = _int("DIVERGENCE_COOLDOWN_MS", 30_000)

# ---------------------------------------------------------------- Storage / metrics
POSTGRES_DSN = _env("POSTGRES_DSN", "postgresql://surveillance:surveillance@postgres:5432/surveillance")
METRICS_INTERVAL_SECONDS = _float("METRICS_INTERVAL_SECONDS", 10.0)
LOG_LEVEL = _env("LOG_LEVEL", "INFO")

"""Domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class LaunchEvent:
    developer_wallet: str
    token_mint: str
    token_name: str | None
    token_symbol: str | None
    platform: str
    signature: str
    slot: int
    block_time: datetime | None
    source: str  # "websocket" | "backfill"
    dev_buy_sol: float | None = None
    dev_buy_tokens: float | None = None
    dev_buy_pct: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "developer_wallet": self.developer_wallet,
            "token_mint": self.token_mint,
            "token_name": self.token_name,
            "token_symbol": self.token_symbol,
            "platform": self.platform,
            "signature": self.signature,
            "slot": self.slot,
            "block_time": self.block_time.isoformat() if self.block_time else None,
            "source": self.source,
        }


@dataclass(slots=True)
class TradeEvent:
    wallet: str
    side: str  # "buy" | "sell"
    token_mint: str
    token_name: str | None
    token_symbol: str | None
    developer_wallet: str | None
    sol_amount: float
    token_amount: float
    market_cap_usd: float | None
    slot_diff: int | None
    pnl_pct: float | None
    sold_pct: float | None
    signature: str
    slot: int
    block_time: datetime | None
    source: str


@dataclass(slots=True)
class TransactionEvent:
    """Normalized transaction passed to the launch detector."""

    signature: str
    slot: int
    block_time: int | None
    transaction: dict[str, Any]
    meta: dict[str, Any]
    source: str
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class ServiceMetrics:
    events_received: int = 0
    launches_detected: int = 0
    trades_detected: int = 0
    duplicates_ignored: int = 0
    telegram_sent: int = 0
    telegram_failed: int = 0
    reconnect_count: int = 0
    backfill_runs: int = 0
    detection_latency_ms_total: float = 0.0
    detection_latency_count: int = 0
    processing_time_ms_total: float = 0.0
    processing_time_count: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_stream_event_at: datetime | None = None

    @property
    def avg_detection_latency_ms(self) -> float:
        if self.detection_latency_count == 0:
            return 0.0
        return self.detection_latency_ms_total / self.detection_latency_count

    @property
    def avg_processing_time_ms(self) -> float:
        if self.processing_time_count == 0:
            return 0.0
        return self.processing_time_ms_total / self.processing_time_count

    @property
    def uptime_seconds(self) -> float:
        return (datetime.now(timezone.utc) - self.started_at).total_seconds()


@dataclass(slots=True)
class WatcherStatus:
    running: bool
    environment: str
    stream_mode: str
    developer_count: int
    uptime_seconds: float
    stream_events: int
    events_processed: int
    launches_today: int
    launches_detected: int
    reconnect_count: int
    last_stream_event_at: datetime | None
    rpc_rps_limit: float
    backfill_enabled: bool
    tx_queue_size: int

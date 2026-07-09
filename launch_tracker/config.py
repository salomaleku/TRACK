"""Configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _int(value: str | None, default: int) -> int:
    if value is None or not value.strip():
        return default
    return int(value)


def _collect_endpoints(*keys: str) -> list[str]:
    seen: set[str] = set()
    endpoints: list[str] = []
    for key in keys:
        value = os.getenv(key, "").strip()
        if value and value not in seen:
            seen.add(value)
            endpoints.append(value)
    return endpoints


@dataclass(frozen=True, slots=True)
class Config:
    environment: str  # "local" | "server"
    ws_endpoints: list[str]
    rpc_endpoints: list[str]
    devs_file: Path
    database: Path
    telegram_token: str
    telegram_chat_id: str
    backfill_enabled: bool
    backfill_interval_minutes: int
    log_level: str

    # Rate limiting
    rpc_rps_limit: float
    rpc_max_retries: int

    # WebSocket tuning
    ws_reconnect_base_seconds: float
    ws_reconnect_max_seconds: float
    ws_heartbeat_seconds: float
    ws_subscription_batch_size: int

    # Backfill tuning
    backfill_signatures_per_dev: int
    backfill_concurrency: int

    # Processing
    tx_queue_size: int
    devs_reload_seconds: float
    explorer_base: str

    @classmethod
    def from_env(cls) -> Config:
        environment = os.getenv("ENVIRONMENT", "local").strip().lower()

        rpc_endpoints = _collect_endpoints(
            "RPC_ENDPOINT", "RPC_ENDPOINT_2", "RPC_ENDPOINT_3"
        )
        ws_endpoints = _collect_endpoints(
            "WS_ENDPOINT", "WS_ENDPOINT_2", "WS_ENDPOINT_3"
        )

        # Presets: local = conservative, server = private node (200 RPS)
        default_rps = 200.0 if environment == "server" else 10.0
        default_backfill_concurrency = 15 if environment == "server" else 2

        rpc_rps_raw = os.getenv("RPC_RPS_LIMIT", "").strip()
        rpc_rps_limit = float(rpc_rps_raw) if rpc_rps_raw else default_rps

        backfill_concurrency_raw = os.getenv("BACKFILL_CONCURRENCY", "").strip()
        backfill_concurrency = (
            _int(backfill_concurrency_raw, default_backfill_concurrency)
            if backfill_concurrency_raw
            else default_backfill_concurrency
        )

        return cls(
            environment=environment,
            ws_endpoints=ws_endpoints,
            rpc_endpoints=rpc_endpoints,
            devs_file=Path(os.getenv("DEVS_FILE", "data/devs.txt")),
            database=Path(os.getenv("DATABASE", "data/launch_tracker.db")),
            telegram_token=os.getenv("TELEGRAM_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            backfill_enabled=_bool(os.getenv("BACKFILL_ENABLED"), True),
            backfill_interval_minutes=_int(os.getenv("BACKFILL_INTERVAL_MINUTES"), 10),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            rpc_rps_limit=rpc_rps_limit,
            rpc_max_retries=_int(os.getenv("RPC_MAX_RETRIES"), 3),
            ws_reconnect_base_seconds=float(os.getenv("WS_RECONNECT_BASE_SECONDS", "1.0")),
            ws_reconnect_max_seconds=float(os.getenv("WS_RECONNECT_MAX_SECONDS", "60.0")),
            ws_heartbeat_seconds=float(os.getenv("WS_HEARTBEAT_SECONDS", "30.0")),
            ws_subscription_batch_size=_int(os.getenv("WS_SUBSCRIPTION_BATCH_SIZE"), 20),
            backfill_signatures_per_dev=_int(os.getenv("BACKFILL_SIGNATURES_PER_DEV"), 5),
            backfill_concurrency=backfill_concurrency,
            tx_queue_size=_int(os.getenv("TX_QUEUE_SIZE"), 10_000),
            devs_reload_seconds=float(os.getenv("DEVS_RELOAD_SECONDS", "30.0")),
            explorer_base=os.getenv("EXPLORER_BASE", "https://solscan.io/tx/").strip(),
        )

    @property
    def ws_endpoint(self) -> str:
        return self.ws_endpoints[0] if self.ws_endpoints else ""

    @property
    def rpc_endpoint(self) -> str:
        return self.rpc_endpoints[0] if self.rpc_endpoints else ""

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.rpc_endpoints:
            errors.append("RPC_ENDPOINT is required")
        if not self.ws_endpoints:
            errors.append("WS_ENDPOINT is required")
        if not self.telegram_token:
            errors.append("TELEGRAM_TOKEN is required")
        if not self.telegram_chat_id:
            errors.append("TELEGRAM_CHAT_ID is required")
        return errors

"""Compact human-readable logging."""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

# Short labels for common events (message is optional fallback).
EVENT_LABELS: dict[str, str] = {
    "service_started": "Started",
    "service_stopped": "Stopped",
    "shutdown_signal": "Shutdown",
    "devs_file_created": "Devs file created",
    "devs_empty": "No dev wallets loaded",
    "devs_loaded": "Devs loaded",
    "devs_reloaded": "Devs reloaded",
    "devs_reload_error": "Devs reload failed",
    "ws_connecting": "WS connecting",
    "ws_connected": "WS connected",
    "ws_disconnected": "WS disconnected",
    "ws_closed": "WS closed",
    "ws_heartbeat_failed": "WS heartbeat failed",
    "ws_subscribed": "WS subscribed",
    "ws_malformed": "WS bad message",
    "ws_message_error": "WS message error",
    "ws_tx_received": "WS tx",
    "geyser_connecting": "Geyser connecting",
    "geyser_ready": "Geyser ready",
    "geyser_connected": "Geyser subscribed",
    "geyser_disconnected": "Geyser disconnected",
    "geyser_tx_received": "Geyser tx",
    "queue_full": "Queue full",
    "duplicate_ignored": "Duplicate",
    "tx_fetch_failed": "RPC failed",
    "launch_detected": "Launch",
    "startup_alert_sent": "Startup alert sent",
    "startup_alert_failed": "Startup alert failed",
    "telegram_sent": "Telegram sent",
    "telegram_failed": "Telegram failed",
    "backfill_startup_skipped": "Backfill skipped",
    "backfill_started": "Backfill started",
    "backfill_finished": "Backfill done",
    "backfill_error": "Backfill error",
    "backfill_wallet_error": "Backfill wallet error",
}

# Keys shown first when present.
_FIELD_ORDER = (
    "environment",
    "stream_mode",
    "mode",
    "developers",
    "devs",
    "wallets",
    "batches",
    "count",
    "endpoint",
    "signature",
    "mint",
    "platform",
    "developer",
    "slot",
    "total",
    "signatures",
    "processed",
    "launches",
    "error",
)

# Truncate long addresses / signatures.
_SHORT_KEYS = frozenset(
    {
        "signature",
        "mint",
        "wallet",
        "developer",
        "developer_wallet",
        "creator",
        "dev",
    }
)

# Shorter key names in output.
_KEY_ALIASES: dict[str, str] = {
    "environment": "env",
    "stream_mode": "stream",
    "developers": "devs",
    "wallet_count": "wallets",
    "rpc_endpoints": "rpc",
    "rpc_rps_limit": "rps",
    "backfill_concurrency": "backfill",
    "reconnect_in_seconds": "retry_in",
    "reconnect_count": "retries",
    "has_token": "token",
}


class CompactFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.now().strftime("%H:%M:%S")
        level = record.levelname[:4].ljust(4)
        message = record.getMessage()
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        return f"{ts} {level} {message}"


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(CompactFormatter())
    root.addHandler(handler)

    for name in ("websockets", "aiogram", "grpc", "grpc.aio"):
        logging.getLogger(name).setLevel(logging.WARNING)


def _short_endpoint(value: str) -> str:
    if "://" in value:
        parsed = urlparse(value)
        host = parsed.hostname or value
        if parsed.port:
            return f"{host}:{parsed.port}"
        return host
    return value


def _format_value(key: str, value: Any) -> str:
    text = str(value)
    if key == "endpoint":
        return _short_endpoint(text)
    if key in _SHORT_KEYS and len(text) > 12:
        return f"{text[:8]}…"
    if key == "error" and len(text) > 80:
        return f"{text[:77]}…"
    return text


def _format_fields(**fields: Any) -> str:
    if not fields:
        return ""

    ordered: list[tuple[str, Any]] = []
    seen: set[str] = set()

    for key in _FIELD_ORDER:
        if key in fields:
            ordered.append((key, fields[key]))
            seen.add(key)

    for key in sorted(fields):
        if key not in seen and key != "event":
            ordered.append((key, fields[key]))

    parts = [f"{_KEY_ALIASES.get(key, key)}={_format_value(key, val)}" for key, val in ordered]
    return " ".join(parts)


def log_extra(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    event = fields.get("event")
    label = EVENT_LABELS.get(str(event), message) if event else message
    suffix = _format_fields(**fields)
    text = f"{label} | {suffix}" if suffix else label
    logger.log(level, text)

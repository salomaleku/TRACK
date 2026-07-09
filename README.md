# Launch Tracker

Realtime Solana developer launch monitoring service.

Monitors a growing list of developer wallets via **WebSocket subscriptions** and sends **Telegram notifications** when a new token launch is detected.

This is a standalone monitoring service. No trading, no copytrading, no wallet scoring.

## Features

- WebSocket-first architecture (no polling loop)
- Automatic reconnect with exponential backoff
- Heartbeat keepalive
- Isolated `LaunchDetector` module
- SQLite persistence with deduplication
- Telegram launch alerts + bot commands
- Lightweight backfill safety net (configurable interval)
- Hot-reload of `data/devs.txt`
- Structured JSON logging
- Provider-agnostic WebSocket client (swap `websocket.py` to change provider)

## Local vs Server

Two deployment profiles:

| | **Local** (`ENVIRONMENT=local`) | **Server** (`ENVIRONMENT=server`) |
|---|---|---|
| Use case | Dev, backtest | Production private node |
| Default RPC RPS | 10 | 200 |
| Backfill concurrency | 2 | 15 |
| Config template | `.env.local.example` | `.env.server.example` |

```bash
# Local
cp .env.local.example .env

# Server (private node, 200 RPS Starter)
cp .env.server.example .env
# Set WS_ENDPOINT and RPC_ENDPOINT to your private node URLs
```

RPC requests are rate-limited and retried on 429. Multiple RPC endpoints rotate round-robin (`RPC_ENDPOINT`, `_2`, `_3`).

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .

cp .env.example .env
# Edit .env — set WS_ENDPOINT, RPC_ENDPOINT, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
# Add developer wallets to data/devs.txt

launch-tracker
```

## Configuration

| Variable | Description |
|----------|-------------|
| `WS_ENDPOINT` | WebSocket URL (primary event source) |
| `RPC_ENDPOINT` | Solana RPC URL (transaction fetch + backfill) |
| `DEVS_FILE` | Developer wallet list (default: `data/devs.txt`) |
| `DATABASE` | SQLite path (default: `data/launch_tracker.db`) |
| `TELEGRAM_TOKEN` | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Telegram chat ID for notifications |
| `BACKFILL_ENABLED` | Enable periodic backfill (default: `true`) |
| `BACKFILL_INTERVAL_MINUTES` | Backfill interval (default: `10`) |
| `LOG_LEVEL` | Log level (default: `INFO`) |

## Bot Commands

| Command | Description |
|---------|-------------|
| `/today` | Launches detected today |
| `/last` | Latest 10 launches |
| `/dev <wallet>` | Launch history for a developer |
| `/stats` | Tracked devs, uptime, reconnect count, metrics |

## Architecture

```
WebSocket ──► signature queue ──► RPC fetch ──► LaunchDetector ──► SQLite
                                                      │
                                                      ▼
                                                 Telegram notify

Backfill (every N min) ──► latest signatures ──► same pipeline
```

## Project Structure

```
launch_tracker/
  config.py           # Environment configuration
  websocket.py        # WebSocket client (replace to change provider)
  launch_detector.py  # Launch detection logic
  database.py           # SQLite persistence
  telegram.py           # Notifications + bot commands
  backfill.py           # Safety-net backfill
  rpc.py                # RPC transaction fetcher
  models.py             # Domain models
  service.py            # Service orchestration
  main.py               # Entry point
```

## Scalability

Designed for growth from 70 → 5000 developers:

- Wallet subscriptions are batched (`WS_SUBSCRIPTION_BATCH_SIZE`)
- Transaction processing is fully async with a bounded queue
- Database writes never block the WebSocket loop
- Telegram sending runs on a separate async queue

## Changing WebSocket Provider

Replace or subclass `JsonRpcWebSocket` in `websocket.py`. Implement the `TransactionWebSocket` interface. The rest of the service remains unchanged.

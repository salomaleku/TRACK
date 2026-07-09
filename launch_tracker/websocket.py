"""Provider-agnostic WebSocket transaction stream.

Replace this module to swap WebSocket providers. The rest of the service
only depends on the TransactionWebSocket interface and normalized events.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection

from launch_tracker.logging_setup import log_extra

logger = logging.getLogger(__name__)


class SubscriptionBuilder(ABC):
    """Build provider-specific subscription payloads for tracked wallets."""

    @abstractmethod
    def build(self, wallets: list[str], batch_id: int) -> dict[str, Any]:
        ...


class LogsSubscribeBuilder(SubscriptionBuilder):
    """Default JSON-RPC logsSubscribe — works on standard Solana WebSocket nodes."""

    def __init__(self, commitment: str = "confirmed") -> None:
        self._commitment = commitment
        self._next_id = 1

    def build(self, wallets: list[str], batch_id: int) -> dict[str, Any]:
        msg_id = self._next_id
        self._next_id += 1
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "method": "logsSubscribe",
            "params": [
                {"mentions": wallets},
                {"commitment": self._commitment},
            ],
        }


class TransactionWebSocket(ABC):
    """Abstract WebSocket stream. Subclass to integrate a new provider."""

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def update_wallets(self, wallets: set[str]) -> None: ...

    @abstractmethod
    def signatures(self) -> AsyncIterator[str]: ...

    @property
    @abstractmethod
    def reconnect_count(self) -> int: ...


class JsonRpcWebSocket(TransactionWebSocket):
    """JSON-RPC WebSocket client with reconnect, backoff, and heartbeat."""

    def __init__(
        self,
        endpoint: str,
        subscription_builder: SubscriptionBuilder | None = None,
        batch_size: int = 20,
        reconnect_base: float = 1.0,
        reconnect_max: float = 60.0,
        heartbeat_seconds: float = 30.0,
        on_reconnect: Callable[[], None] | None = None,
    ) -> None:
        self._endpoint = endpoint
        self._builder = subscription_builder or LogsSubscribeBuilder()
        self._batch_size = batch_size
        self._reconnect_base = reconnect_base
        self._reconnect_max = reconnect_max
        self._heartbeat_seconds = heartbeat_seconds
        self._on_reconnect = on_reconnect

        self._wallets: set[str] = set()
        self._ws: ClientConnection | None = None
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._running = False
        self._reconnect_count = 0
        self._subscription_ids: list[int] = []
        self._tasks: list[asyncio.Task] = []

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    async def connect(self) -> None:
        if self._running:
            return
        self._running = True
        self._tasks.append(asyncio.create_task(self._run_loop(), name="ws-loop"))

    async def disconnect(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        await self._close_ws()

    async def update_wallets(self, wallets: set[str]) -> None:
        self._wallets = set(wallets)
        if self._ws and self._ws.state.name == "OPEN":
            await self._subscribe_all()

    def signatures(self) -> AsyncIterator[str]:
        return self._iter_signatures()

    async def _iter_signatures(self) -> AsyncIterator[str]:
        while self._running:
            try:
                sig = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                yield sig
            except asyncio.TimeoutError:
                continue

    async def _run_loop(self) -> None:
        attempt = 0
        while self._running:
            try:
                await self._connect_once()
                attempt = 0
            except asyncio.CancelledError:
                break
            except Exception as exc:
                attempt += 1
                delay = min(self._reconnect_base * (2 ** (attempt - 1)), self._reconnect_max)
                self._reconnect_count += 1
                if self._on_reconnect:
                    self._on_reconnect()
                log_extra(
                    logger,
                    logging.WARNING,
                    "WebSocket disconnected",
                    event="ws_disconnected",
                    error=str(exc),
                    reconnect_in_seconds=delay,
                    reconnect_count=self._reconnect_count,
                )
                await asyncio.sleep(delay)

    async def _connect_once(self) -> None:
        log_extra(logger, logging.INFO, "WebSocket connecting", event="ws_connecting", endpoint=self._endpoint)
        async with websockets.connect(
            self._endpoint,
            ping_interval=None,
            close_timeout=5,
            max_size=2**24,
        ) as ws:
            self._ws = ws
            log_extra(logger, logging.INFO, "WebSocket connected", event="ws_connected")
            await self._subscribe_all()
            heartbeat = asyncio.create_task(self._heartbeat_loop(), name="ws-heartbeat")
            try:
                async for raw in ws:
                    await self._handle_message(raw)
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                self._ws = None
                log_extra(logger, logging.WARNING, "WebSocket connection closed", event="ws_closed")

    async def _close_ws(self) -> None:
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_seconds)
            if self._ws and self._ws.state.name == "OPEN":
                try:
                    pong = await self._ws.ping()
                    await asyncio.wait_for(pong, timeout=10)
                except Exception as exc:
                    log_extra(logger, logging.WARNING, "WebSocket heartbeat failed", event="ws_heartbeat_failed", error=str(exc))
                    await self._close_ws()
                    return

    async def _subscribe_all(self) -> None:
        if not self._ws or not self._wallets:
            return
        wallet_list = sorted(self._wallets)
        batches = [
            wallet_list[i : i + self._batch_size]
            for i in range(0, len(wallet_list), self._batch_size)
        ]
        for batch_id, batch in enumerate(batches):
            msg = self._builder.build(batch, batch_id)
            await self._ws.send(json.dumps(msg))
        log_extra(
            logger,
            logging.INFO,
            "WebSocket subscriptions sent",
            event="ws_subscribed",
            wallet_count=len(self._wallets),
            batch_count=len(batches),
        )

    async def _handle_message(self, raw: str | bytes) -> None:
        try:
            data = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
        except (json.JSONDecodeError, TypeError):
            log_extra(logger, logging.DEBUG, "Malformed WebSocket message ignored", event="ws_malformed")
            return

        try:
            await self._process_message(data)
        except Exception as exc:
            log_extra(
                logger,
                logging.WARNING,
                "WebSocket message processing error",
                event="ws_message_error",
                error=str(exc),
            )

    async def _process_message(self, data: dict[str, Any]) -> None:
        # Subscription confirmation
        if "result" in data and "id" in data:
            return

        params = data.get("params")
        if not params:
            return

        result = params.get("result", {})
        value = result.get("value", result)

        # logsNotification format
        signature = value.get("signature")
        if signature:
            await self._queue.put(signature)
            return

        # transactionNotification format (some providers)
        tx = value.get("transaction")
        if tx:
            sigs = tx.get("signatures", [])
            if sigs:
                await self._queue.put(sigs[0])

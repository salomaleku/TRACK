"""Yellowstone Geyser gRPC transaction stream (Corvus Labs compatible)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable
from urllib.parse import urlparse

import grpc

from launch_tracker.logging_setup import log_extra
from launch_tracker.proto import geyser_pb2, geyser_pb2_grpc
from launch_tracker.websocket import TransactionWebSocket

logger = logging.getLogger(__name__)

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

_GRPC_CHANNEL_OPTIONS = (
    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
    ("grpc.keepalive_time_ms", 30_000),
    ("grpc.keepalive_timeout_ms", 10_000),
    ("grpc.keepalive_permit_without_calls", 1),
)

DEFAULT_WALLET_BATCH_SIZE = 20


def _b58encode(data: bytes) -> str:
    n = int.from_bytes(data, "big") if data else 0
    if n == 0:
        return ""
    chars: list[str] = []
    while n:
        n, rem = divmod(n, 58)
        chars.append(_B58[rem])
    pad = 0
    for b in data:
        if b == 0:
            pad += 1
        else:
            break
    return "1" * pad + "".join(reversed(chars))


def _normalize_grpc_target(endpoint: str) -> str:
    value = endpoint.strip()
    if "://" in value:
        parsed = urlparse(value)
        host = parsed.hostname or ""
        if parsed.port:
            return f"{host}:{parsed.port}"
        return host
    return value


def _format_grpc_error(exc: BaseException) -> str:
    if isinstance(exc, grpc.RpcError):
        code = exc.code()
        details = exc.details() or ""
        return f"{code.name}: {details}".strip(": ")
    return str(exc)


def _valid_wallets(wallets: Iterable[str]) -> list[str]:
    valid: list[str] = []
    for wallet in wallets:
        w = wallet.strip()
        if len(w) < 32 or len(w) > 44:
            continue
        if all(c in _B58 for c in w):
            valid.append(w)
    return sorted(valid)


def _build_subscribe_request(wallets: list[str], batch_size: int) -> geyser_pb2.SubscribeRequest:
    req = geyser_pb2.SubscribeRequest()
    batches = [wallets[i : i + batch_size] for i in range(0, len(wallets), batch_size)]
    for batch_id, batch in enumerate(batches):
        tx_filter = req.transactions[f"devs_{batch_id}"]
        tx_filter.account_include.extend(batch)
        tx_filter.vote = False
        tx_filter.failed = False
    req.commitment = geyser_pb2.CommitmentLevel.CONFIRMED
    return req


class GeyserTransactionStream(TransactionWebSocket):
    """Stream dev-wallet transactions via Yellowstone gRPC."""

    def __init__(
        self,
        endpoint: str,
        x_token: str | None = None,
        reconnect_base: float = 1.0,
        reconnect_max: float = 60.0,
        wallet_batch_size: int = DEFAULT_WALLET_BATCH_SIZE,
    ) -> None:
        self._endpoint = _normalize_grpc_target(endpoint)
        self._x_token = x_token
        self._reconnect_base = reconnect_base
        self._reconnect_max = reconnect_max
        self._wallet_batch_size = wallet_batch_size

        self._wallets: set[str] = set()
        self._sig_queue: asyncio.Queue[str] = asyncio.Queue()
        self._request_queue: asyncio.Queue[geyser_pb2.SubscribeRequest | None] = asyncio.Queue()
        self._running = False
        self._reconnect_count = 0
        self._events_received = 0
        self._task: asyncio.Task | None = None
        self._resubscribe = asyncio.Event()

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    @property
    def stream_events(self) -> int:
        return self._events_received

    async def connect(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="geyser-stream")

    async def disconnect(self) -> None:
        self._running = False
        await self._request_queue.put(None)
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def update_wallets(self, wallets: set[str]) -> None:
        self._wallets = set(wallets)
        if self._wallets and self._running:
            self._resubscribe.set()

    def signatures(self) -> AsyncIterator[str]:
        return self._iter_signatures()

    async def _iter_signatures(self) -> AsyncIterator[str]:
        while self._running:
            try:
                sig = await asyncio.wait_for(self._sig_queue.get(), timeout=1.0)
                yield sig
            except asyncio.TimeoutError:
                continue

    async def _run_loop(self) -> None:
        attempt = 0
        while self._running:
            try:
                await self._subscribe_once()
                attempt = 0
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if not self._running:
                    break
                attempt += 1
                delay = min(self._reconnect_base * (2 ** (attempt - 1)), self._reconnect_max)
                self._reconnect_count += 1
                log_extra(
                    logger,
                    logging.WARNING,
                    "Geyser disconnected",
                    event="geyser_disconnected",
                    error=_format_grpc_error(exc),
                    reconnect_in_seconds=delay,
                    reconnect_count=self._reconnect_count,
                )
                await asyncio.sleep(delay)

    def _grpc_metadata(self) -> list[tuple[str, str]] | None:
        if self._x_token:
            return [("x-token", self._x_token)]
        return None

    async def _request_generator(
        self,
        initial: geyser_pb2.SubscribeRequest,
    ) -> AsyncIterator[geyser_pb2.SubscribeRequest]:
        yield initial
        while self._running:
            req = await self._request_queue.get()
            if req is None:
                break
            yield req

    async def _subscribe_once(self) -> None:
        wallets = _valid_wallets(self._wallets)
        if not wallets:
            raise RuntimeError("No valid developer wallets for Geyser subscription")

        batch_count = (len(wallets) + self._wallet_batch_size - 1) // self._wallet_batch_size
        log_extra(
            logger,
            logging.INFO,
            "Geyser connecting",
            event="geyser_connecting",
            endpoint=self._endpoint,
            wallets=len(wallets),
            batches=batch_count,
        )

        channel = grpc.aio.insecure_channel(self._endpoint, options=_GRPC_CHANNEL_OPTIONS)
        stub = geyser_pb2_grpc.GeyserStub(channel)
        metadata = self._grpc_metadata()

        try:
            version = await stub.GetVersion(
                geyser_pb2.GetVersionRequest(),
                metadata=metadata,
                timeout=10,
            )
            log_extra(
                logger,
                logging.INFO,
                "Geyser ready",
                event="geyser_ready",
                version=version.version,
                wallets=len(wallets),
            )

            initial = _build_subscribe_request(wallets, self._wallet_batch_size)
            stream = stub.Subscribe(
                self._request_generator(initial),
                metadata=metadata,
            )
            log_extra(
                logger,
                logging.INFO,
                "Geyser subscribed",
                event="geyser_connected",
                wallets=len(wallets),
                batches=batch_count,
            )

            self._resubscribe.clear()
            async for update in stream:
                if not self._running:
                    break
                if self._resubscribe.is_set():
                    self._resubscribe.clear()
                    refreshed = _build_subscribe_request(
                        _valid_wallets(self._wallets),
                        self._wallet_batch_size,
                    )
                    await self._request_queue.put(refreshed)
                await self._handle_update(update)
        finally:
            await channel.close()

    async def _handle_update(self, update: geyser_pb2.SubscribeUpdate) -> None:
        if update.HasField("ping"):
            await self._request_queue.put(
                geyser_pb2.SubscribeRequest(
                    ping=geyser_pb2.SubscribeRequestPing(id=1),
                )
            )
            return

        if not update.HasField("transaction"):
            return

        tx_info = update.transaction.transaction
        if tx_info.is_vote:
            return

        sig_bytes = bytes(tx_info.signature)
        if not sig_bytes:
            return

        signature = _b58encode(sig_bytes)
        self._events_received += 1

        if self._events_received <= 5 or self._events_received % 100 == 0:
            log_extra(
                logger,
                logging.INFO,
                "Geyser transaction received",
                event="geyser_tx_received",
                signature=signature[:16],
                total=self._events_received,
                slot=update.transaction.slot,
            )

        await self._sig_queue.put(signature)

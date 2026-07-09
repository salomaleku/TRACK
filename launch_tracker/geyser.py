"""Yellowstone Geyser gRPC transaction stream (Corvus Labs compatible)."""

from __future__ import annotations

import asyncio
import logging
import queue
import threading
from collections.abc import AsyncIterator, Callable
from typing import Any

import grpc

from launch_tracker.logging_setup import log_extra
from launch_tracker.proto import geyser_pb2, geyser_pb2_grpc
from launch_tracker.websocket import TransactionWebSocket

logger = logging.getLogger(__name__)

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


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


def _build_subscribe_request(wallets: list[str]) -> geyser_pb2.SubscribeRequest:
    req = geyser_pb2.SubscribeRequest()
    tx_filter = geyser_pb2.SubscribeRequestFilterTransactions(
        vote=False,
        failed=False,
        account_include=sorted(wallets),
    )
    req.transactions["dev_wallets"].CopyFrom(tx_filter)
    req.commitment = geyser_pb2.CommitmentLevel.CONFIRMED
    return req


class GeyserTransactionStream(TransactionWebSocket):
    """Stream dev-wallet transactions via Yellowstone gRPC.

    Corvus endpoint example: http://fra.corvus-labs.io:10101
    Starter tier (200 RPS): up to 200 accounts in account_include — all 76 devs fit in one filter.
    """

    def __init__(
        self,
        endpoint: str,
        x_token: str | None = None,
        reconnect_base: float = 1.0,
        reconnect_max: float = 60.0,
    ) -> None:
        self._endpoint = endpoint.replace("grpc://", "http://")
        if not self._endpoint.startswith("http"):
            self._endpoint = f"http://{self._endpoint}"
        self._x_token = x_token
        self._reconnect_base = reconnect_base
        self._reconnect_max = reconnect_max

        self._wallets: set[str] = set()
        self._sig_queue: asyncio.Queue[str] = asyncio.Queue()
        self._request_queue: queue.Queue[Any] = queue.Queue()
        self._running = False
        self._reconnect_count = 0
        self._events_received = 0
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    async def connect(self) -> None:
        if self._running:
            return
        self._running = True
        self._loop = asyncio.get_running_loop()
        self._thread = threading.Thread(target=self._run_thread, name="geyser-stream", daemon=True)
        self._thread.start()

    async def disconnect(self) -> None:
        self._running = False
        self._request_queue.put(None)
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    async def update_wallets(self, wallets: set[str]) -> None:
        self._wallets = set(wallets)
        if self._wallets and self._running:
            self._request_queue.put(_build_subscribe_request(list(self._wallets)))

    def signatures(self) -> AsyncIterator[str]:
        return self._iter_signatures()

    async def _iter_signatures(self) -> AsyncIterator[str]:
        while self._running:
            try:
                sig = await asyncio.wait_for(self._sig_queue.get(), timeout=1.0)
                yield sig
            except asyncio.TimeoutError:
                continue

    def _run_thread(self) -> None:
        attempt = 0
        while self._running:
            try:
                self._subscribe_once()
                attempt = 0
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
                    error=str(exc),
                    reconnect_in_seconds=delay,
                    reconnect_count=self._reconnect_count,
                )
                threading.Event().wait(delay)

    def _subscribe_once(self) -> None:
        assert self._loop is not None
        log_extra(
            logger,
            logging.INFO,
            "Geyser connecting",
            event="geyser_connecting",
            endpoint=self._endpoint,
        )

        channel = grpc.insecure_channel(
            self._endpoint,
            options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)],
        )
        stub = geyser_pb2_grpc.GeyserStub(channel)

        if self._wallets:
            self._request_queue.put(_build_subscribe_request(list(self._wallets)))

        def request_iterator():
            while self._running:
                req = self._request_queue.get()
                if req is None:
                    break
                yield req

        metadata = [("x-token", self._x_token)] if self._x_token else None

        stream = stub.Subscribe(request_iterator(), metadata=metadata)
        log_extra(
            logger,
            logging.INFO,
            "Geyser connected",
            event="geyser_connected",
            wallet_count=len(self._wallets),
        )

        for update in stream:
            if not self._running:
                break
            self._handle_update(update)

    def _handle_update(self, update: geyser_pb2.SubscribeUpdate) -> None:
        assert self._loop is not None

        if update.HasField("ping"):
            self._request_queue.put(
                geyser_pb2.SubscribeRequest(ping=geyser_pb2.SubscribeRequestPing(id=1))
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

        asyncio.run_coroutine_threadsafe(self._sig_queue.put(signature), self._loop)

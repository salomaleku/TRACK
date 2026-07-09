"""Lightweight backfill safety net — never the primary monitor."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from launch_tracker.logging_setup import log_extra
from launch_tracker.rpc import RpcClient

logger = logging.getLogger(__name__)


class BackfillWorker:
    def __init__(
        self,
        rpc: RpcClient,
        get_wallets: Callable[[], set[str]],
        on_signature: Callable[[str], Awaitable[None]],
        interval_minutes: int = 10,
        signatures_per_dev: int = 5,
        concurrency: int = 5,
        on_run: Callable[[], None] | None = None,
    ) -> None:
        self._rpc = rpc
        self._get_wallets = get_wallets
        self._on_signature = on_signature
        self._on_run = on_run
        self._interval = interval_minutes * 60
        self._signatures_per_dev = signatures_per_dev
        self._concurrency = concurrency
        self._running = False
        self._task: asyncio.Task | None = None
        self._semaphore = asyncio.Semaphore(concurrency)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="backfill")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _loop(self) -> None:
        await asyncio.sleep(30)  # initial delay — let WebSocket connect first
        while self._running:
            try:
                await self._run_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log_extra(logger, logging.ERROR, "Backfill error", event="backfill_error", error=str(exc))
            await asyncio.sleep(self._interval)

    async def _run_once(self) -> None:
        wallets = self._get_wallets()
        if not wallets:
            return

        if self._on_run:
            self._on_run()

        started = time.monotonic()
        log_extra(
            logger,
            logging.INFO,
            "Backfill started",
            event="backfill_started",
            developer_count=len(wallets),
        )

        tasks = [self._backfill_wallet(w) for w in wallets]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        signatures_found = sum(r for r in results if isinstance(r, int))

        elapsed = time.monotonic() - started
        log_extra(
            logger,
            logging.INFO,
            "Backfill finished",
            event="backfill_finished",
            developer_count=len(wallets),
            signatures_queued=signatures_found,
            duration_seconds=round(elapsed, 2),
        )

    async def _backfill_wallet(self, wallet: str) -> int:
        async with self._semaphore:
            try:
                sigs = await self._rpc.get_signatures_for_address(
                    wallet, limit=self._signatures_per_dev
                )
                for sig in sigs:
                    await self._on_signature(sig)
                return len(sigs)
            except Exception as exc:
                log_extra(
                    logger,
                    logging.WARNING,
                    "Backfill wallet failed",
                    event="backfill_wallet_error",
                    wallet=wallet,
                    error=str(exc),
                )
                return 0

"""Core service orchestration."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

import aiohttp

from launch_tracker.backfill import BackfillWorker
from launch_tracker.config import Config
from launch_tracker.database import Database
from launch_tracker.launch_detector import LaunchDetector
from launch_tracker.logging_setup import log_extra
from launch_tracker.models import LaunchEvent, ServiceMetrics, TransactionEvent
from launch_tracker.rpc import RpcClient
from launch_tracker.telegram import TelegramService
from launch_tracker.websocket import JsonRpcWebSocket

logger = logging.getLogger(__name__)


class DeveloperRegistry:
    """In-memory wallet set with automatic file reload."""

    def __init__(self, path: Path, reload_seconds: float = 30.0) -> None:
        self._path = path
        self._reload_seconds = reload_seconds
        self._wallets: set[str] = set()
        self._mtime: float = 0.0
        self._lock = asyncio.Lock()

    @property
    def wallets(self) -> set[str]:
        return set(self._wallets)

    def load(self) -> set[str]:
        if not self._path.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.touch()
            log_extra(
                logger,
                logging.WARNING,
                "Developer file not found, created empty file",
                event="devs_file_created",
                path=str(self._path.resolve()),
            )
            return set()

        wallets: set[str] = set()
        lines = self._path.read_text(encoding="utf-8").splitlines()
        for line in lines:
            wallet = line.strip()
            if wallet and not wallet.startswith("#"):
                wallets.add(wallet)

        self._wallets = wallets
        self._mtime = self._path.stat().st_mtime

        if not wallets:
            log_extra(
                logger,
                logging.WARNING,
                "Developer file has no wallets",
                event="devs_empty",
                path=str(self._path.resolve()),
                total_lines=len(lines),
                hint="Add one wallet per line (comments starting with # are ignored)",
            )
        else:
            log_extra(
                logger,
                logging.INFO,
                "Developers loaded from file",
                event="devs_loaded",
                path=str(self._path.resolve()),
                count=len(wallets),
            )
        return wallets

    async def reload_if_changed(self) -> bool:
        if not self._path.exists():
            return False
        mtime = self._path.stat().st_mtime
        if mtime <= self._mtime:
            return False
        async with self._lock:
            old_count = len(self._wallets)
            self.load()
            new_count = len(self._wallets)
            log_extra(
                logger,
                logging.INFO,
                "Developer list reloaded",
                event="devs_reloaded",
                previous_count=old_count,
                current_count=new_count,
            )
            return True

    async def watch_loop(self, on_change: Callable[[set[str]], Awaitable[None]]) -> None:
        while True:
            try:
                if await self.reload_if_changed():
                    await on_change(self._wallets)
            except Exception as exc:
                log_extra(logger, logging.WARNING, "Developer reload error", event="devs_reload_error", error=str(exc))
            await asyncio.sleep(self._reload_seconds)


class LaunchTrackerService:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._metrics = ServiceMetrics()
        self._db = Database(config.database)
        self._registry = DeveloperRegistry(config.devs_file, config.devs_reload_seconds)
        self._detector = LaunchDetector(set())
        self._tx_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=config.tx_queue_size)
        self._session: aiohttp.ClientSession | None = None
        self._rpc: RpcClient | None = None
        self._ws: JsonRpcWebSocket | None = None
        self._telegram: TelegramService | None = None
        self._backfill: BackfillWorker | None = None
        self._tasks: list[asyncio.Task] = []
        self._running = False

    @property
    def metrics(self) -> ServiceMetrics:
        return self._metrics

    def _get_metrics(self) -> ServiceMetrics:
        if self._ws:
            self._metrics.reconnect_count = self._ws.reconnect_count
        return self._metrics

    async def start(self) -> None:
        errors = self._config.validate()
        if errors:
            raise RuntimeError(f"Configuration errors: {', '.join(errors)}")

        await self._db.connect()

        wallets = self._registry.load()

        await self._db.sync_developers(wallets)
        self._detector.update_wallets(wallets)

        self._session = aiohttp.ClientSession()
        self._rpc = RpcClient(
            endpoints=self._config.rpc_endpoints,
            session=self._session,
            max_rps=self._config.rpc_rps_limit,
            max_retries=self._config.rpc_max_retries,
        )

        self._ws = JsonRpcWebSocket(
            endpoint=self._config.ws_endpoint,
            batch_size=self._config.ws_subscription_batch_size,
            reconnect_base=self._config.ws_reconnect_base_seconds,
            reconnect_max=self._config.ws_reconnect_max_seconds,
            heartbeat_seconds=self._config.ws_heartbeat_seconds,
        )
        await self._ws.update_wallets(wallets)
        await self._ws.connect()

        self._telegram = TelegramService(
            token=self._config.telegram_token,
            chat_id=self._config.telegram_chat_id,
            database=self._db,
            metrics=self._metrics,
            get_metrics=self._get_metrics,
            explorer_base=self._config.explorer_base,
        )
        await self._telegram.start()
        await self._telegram.send_startup_alert(
            developer_count=len(wallets),
            backfill_enabled=self._config.backfill_enabled,
            backfill_interval_minutes=self._config.backfill_interval_minutes,
            environment=self._config.environment,
            rpc_rps_limit=self._config.rpc_rps_limit,
        )

        if self._config.backfill_enabled:
            assert self._rpc is not None
            self._backfill = BackfillWorker(
                rpc=self._rpc,
                get_wallets=lambda: self._registry.wallets,
                on_signature=self._enqueue_signature,
                interval_minutes=self._config.backfill_interval_minutes,
                signatures_per_dev=self._config.backfill_signatures_per_dev,
                concurrency=self._config.backfill_concurrency,
                on_run=lambda: setattr(self._metrics, "backfill_runs", self._metrics.backfill_runs + 1),
            )
            await self._backfill.start()

        self._running = True
        self._tasks.append(asyncio.create_task(self._ws_consumer_loop(), name="ws-consumer"))
        self._tasks.append(asyncio.create_task(self._processor_loop(), name="tx-processor"))
        self._tasks.append(
            asyncio.create_task(
                self._registry.watch_loop(self._on_wallets_changed),
                name="devs-watcher",
            )
        )

        log_extra(
            logger,
            logging.INFO,
            "Launch Tracker started",
            event="service_started",
            environment=self._config.environment,
            developers=len(wallets),
            rpc_endpoints=len(self._config.rpc_endpoints),
            rpc_rps_limit=self._config.rpc_rps_limit,
            backfill_concurrency=self._config.backfill_concurrency,
        )

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        if self._backfill:
            await self._backfill.stop()
        if self._ws:
            await self._ws.disconnect()
        if self._telegram:
            await self._telegram.stop()
        if self._session:
            await self._session.close()
        await self._db.close()

        log_extra(logger, logging.INFO, "Launch Tracker stopped", event="service_stopped")

    async def _on_wallets_changed(self, wallets: set[str]) -> None:
        self._detector.update_wallets(wallets)
        await self._db.sync_developers(wallets)
        if self._ws:
            await self._ws.update_wallets(wallets)

    async def _ws_consumer_loop(self) -> None:
        assert self._ws is not None
        async for signature in self._ws.signatures():
            await self._enqueue_signature(signature, source="websocket")

    async def _enqueue_signature(self, signature: str, source: str = "backfill") -> None:
        try:
            self._tx_queue.put_nowait((signature, source))
        except asyncio.QueueFull:
            log_extra(logger, logging.WARNING, "Transaction queue full", event="queue_full", signature=signature)

    async def _processor_loop(self) -> None:
        while self._running:
            try:
                signature, source = await asyncio.wait_for(self._tx_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            asyncio.create_task(
                self._process_signature(signature, source),
                name=f"process-{signature[:8]}",
            )

    async def _process_signature(self, signature: str, source: str) -> None:
        started = time.monotonic()
        self._metrics.events_received += 1

        if await self._db.is_processed(signature):
            self._metrics.duplicates_ignored += 1
            log_extra(logger, logging.DEBUG, "Duplicate ignored", event="duplicate_ignored", signature=signature)
            return

        assert self._rpc is not None
        try:
            event = await self._rpc.get_transaction(signature, source=source)
        except Exception as exc:
            log_extra(
                logger,
                logging.WARNING,
                "Transaction fetch failed",
                event="tx_fetch_failed",
                signature=signature,
                error=str(exc),
            )
            return

        if not event:
            return

        proc_ms = (time.monotonic() - started) * 1000
        self._metrics.processing_time_ms_total += proc_ms
        self._metrics.processing_time_count += 1

        launch = self._detector.detect(event)
        if not launch:
            await self._db.mark_processed(signature, source)
            return

        if not await self._db.mark_processed(signature, source):
            self._metrics.duplicates_ignored += 1
            log_extra(logger, logging.DEBUG, "Duplicate ignored", event="duplicate_ignored", signature=signature)
            return

        saved = await self._db.save_launch(launch)
        if not saved:
            self._metrics.duplicates_ignored += 1
            return

        self._metrics.launches_detected += 1

        if event.block_time:
            latency_ms = (time.time() - event.block_time) * 1000
            self._metrics.detection_latency_ms_total += latency_ms
            self._metrics.detection_latency_count += 1

        log_extra(
            logger,
            logging.INFO,
            "Launch detected",
            event="launch_detected",
            developer=launch.developer_wallet,
            mint=launch.token_mint,
            platform=launch.platform,
            signature=launch.signature,
            source=source,
        )

        if self._telegram:
            await self._telegram.notify_launch(launch)

"""Telegram notifications and bot commands."""

from __future__ import annotations

import asyncio
import logging
import resource
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message

from launch_tracker.logging_setup import log_extra
from launch_tracker.models import LaunchEvent

if TYPE_CHECKING:
    from launch_tracker.database import Database
    from launch_tracker.models import ServiceMetrics

logger = logging.getLogger(__name__)


def _memory_usage_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return usage / (1024 * 1024)
    return usage / 1024


def format_launch_message(event: LaunchEvent, explorer_base: str) -> str:
    time_str = (
        event.block_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        if event.block_time
        else "unknown"
    )
    name = event.token_name or "—"
    symbol = event.token_symbol or "—"
    explorer = f"{explorer_base}{event.signature}"

    return (
        "🚀 <b>NEW DEV LAUNCH</b>\n\n"
        f"<b>Developer</b>\n<code>{event.developer_wallet}</code>\n\n"
        f"<b>Token</b>\n{name}\n\n"
        f"<b>Ticker</b>\n{symbol}\n\n"
        f"<b>Mint</b>\n<code>{event.token_mint}</code>\n\n"
        f"<b>Platform</b>\n{event.platform}\n\n"
        f"<b>Time</b>\n{time_str}\n\n"
        f"<b>Slot</b>\n{event.slot}\n\n"
        f"<b>Signature</b>\n<code>{event.signature}</code>\n\n"
        f'<a href="{explorer}">Explorer Link</a>'
    )


class TelegramService:
    def __init__(
        self,
        token: str,
        chat_id: str,
        database: Database,
        metrics: ServiceMetrics,
        get_metrics: Callable[[], ServiceMetrics],
        explorer_base: str,
    ) -> None:
        self._chat_id = chat_id
        self._db = database
        self._metrics = metrics
        self._get_metrics = get_metrics
        self._explorer_base = explorer_base
        self._bot = Bot(token=token)
        self._dp = Dispatcher()
        self._send_queue: asyncio.Queue[LaunchEvent] = asyncio.Queue()
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._register_handlers()

    def _register_handlers(self) -> None:
        @self._dp.message(Command("today"))
        async def cmd_today(message: Message) -> None:
            launches = await self._db.get_launches_today()
            if not launches:
                await message.answer("No launches detected today.")
                return
            lines = [f"<b>Launches today ({len(launches)})</b>\n"]
            for launch in launches:
                sym = launch.get("token_symbol") or "?"
                lines.append(
                    f"• <code>{launch['token_mint'][:8]}…</code> "
                    f"({sym}) — {launch['platform']}\n"
                    f"  dev: <code>{launch['developer_wallet'][:8]}…</code>"
                )
            await message.answer("\n".join(lines))

        @self._dp.message(Command("last"))
        async def cmd_last(message: Message) -> None:
            launches = await self._db.get_latest_launches(limit=10)
            if not launches:
                await message.answer("No launches recorded yet.")
                return
            lines = ["<b>Latest launches</b>\n"]
            for launch in launches:
                sym = launch.get("token_symbol") or "?"
                lines.append(
                    f"• <code>{launch['token_mint'][:8]}…</code> "
                    f"({sym}) — {launch['platform']}"
                )
            await message.answer("\n".join(lines))

        @self._dp.message(Command("dev"))
        async def cmd_dev(message: Message) -> None:
            parts = (message.text or "").split(maxsplit=1)
            if len(parts) < 2:
                await message.answer("Usage: /dev &lt;wallet&gt;")
                return
            wallet = parts[1].strip()
            launches = await self._db.get_dev_launches(wallet)
            if not launches:
                await message.answer(f"No launches found for <code>{wallet}</code>")
                return
            lines = [f"<b>Launches by {wallet[:8]}…</b> ({len(launches)})\n"]
            for launch in launches:
                sym = launch.get("token_symbol") or "?"
                lines.append(
                    f"• <code>{launch['token_mint'][:8]}…</code> "
                    f"({sym}) — {launch['detected_at'][:16]}"
                )
            await message.answer("\n".join(lines))

        @self._dp.message(Command("stats"))
        async def cmd_stats(message: Message) -> None:
            m = self._get_metrics()
            dev_count = await self._db.count_developers()
            today_count = await self._db.count_launches_today()
            db_size = self._db.path.stat().st_size if self._db.path.exists() else 0
            uptime_h = m.uptime_seconds / 3600

            await message.answer(
                "<b>Launch Tracker Stats</b>\n\n"
                f"<b>Tracked developers</b>\n{dev_count}\n\n"
                f"<b>Today's launches</b>\n{today_count}\n\n"
                f"<b>Uptime</b>\n{uptime_h:.1f}h\n\n"
                f"<b>Reconnect count</b>\n{m.reconnect_count}\n\n"
                f"<b>Events received</b>\n{m.events_received}\n\n"
                f"<b>Launches detected</b>\n{m.launches_detected}\n\n"
                f"<b>Backfill runs</b>\n{m.backfill_runs}\n\n"
                f"<b>Avg detection latency</b>\n{m.avg_detection_latency_ms:.0f}ms\n\n"
                f"<b>Avg processing time</b>\n{m.avg_processing_time_ms:.0f}ms\n\n"
                f"<b>Memory usage</b>\n{_memory_usage_mb():.1f} MB\n\n"
                f"<b>SQLite size</b>\n{db_size / 1024:.1f} KB"
            )

    async def start(self) -> None:
        self._running = True
        self._tasks.append(asyncio.create_task(self._sender_loop(), name="telegram-sender"))
        self._tasks.append(asyncio.create_task(self._polling_loop(), name="telegram-polling"))

    async def send_startup_alert(
        self,
        developer_count: int,
        backfill_enabled: bool,
        backfill_interval_minutes: int,
        environment: str = "local",
        rpc_rps_limit: float = 10.0,
    ) -> None:
        backfill_line = (
            f"Backfill: каждые {backfill_interval_minutes} мин"
            if backfill_enabled
            else "Backfill: выключен"
        )
        env_label = "🖥 Сервер (private node)" if environment == "server" else "💻 Локально"
        text = (
            "🟢 <b>Launch Tracker запущен</b>\n\n"
            "Слежу за новыми лаунчами в реальном времени.\n"
            "Уведомлю, как только один из отслеживаемых "
            "разработчиков запустит токен.\n\n"
            f"<b>Режим</b>: {env_label}\n"
            f"<b>Разработчиков</b>: {developer_count}\n"
            f"<b>RPC лимит</b>: {rpc_rps_limit:.0f} RPS\n"
            f"<b>WebSocket</b>: подключён\n"
            f"<b>{backfill_line}</b>"
        )
        try:
            await self._bot.send_message(
                self._chat_id,
                text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            log_extra(
                logger,
                logging.INFO,
                "Startup alert sent",
                event="startup_alert_sent",
                chat_id=self._chat_id,
                developer_count=developer_count,
            )
        except Exception as exc:
            log_extra(
                logger,
                logging.ERROR,
                "Startup alert failed",
                event="startup_alert_failed",
                chat_id=self._chat_id,
                error=str(exc),
            )

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        await self._bot.session.close()

    async def notify_launch(self, event: LaunchEvent) -> None:
        await self._send_queue.put(event)

    async def _sender_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._send_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                text = format_launch_message(event, self._explorer_base)
                await self._bot.send_message(
                    self._chat_id,
                    text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                self._metrics.telegram_sent += 1
                log_extra(
                    logger,
                    logging.INFO,
                    "Telegram sent",
                    event="telegram_sent",
                    signature=event.signature,
                    mint=event.token_mint,
                )
            except Exception as exc:
                self._metrics.telegram_failed += 1
                log_extra(
                    logger,
                    logging.ERROR,
                    "Telegram send failed",
                    event="telegram_failed",
                    signature=event.signature,
                    error=str(exc),
                )

    async def _polling_loop(self) -> None:
        try:
            await self._dp.start_polling(self._bot)
        except asyncio.CancelledError:
            pass

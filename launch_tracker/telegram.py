"""Telegram notifications and bot commands."""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Awaitable, Callable

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message

from launch_tracker.logging_setup import log_extra
from launch_tracker.models import LaunchEvent, TradeEvent, WatcherStatus

if TYPE_CHECKING:
    from launch_tracker.database import Database
    from launch_tracker.models import ServiceMetrics

logger = logging.getLogger(__name__)


def _memory_usage_mb() -> float | None:
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return usage / (1024 * 1024)
        return usage / 1024
    except (ImportError, ModuleNotFoundError):
        # resource module is Unix-only; not available on Windows
        return None


def _format_token_amount(amount: float) -> str:
    if amount >= 1_000_000:
        return f"{amount / 1_000_000:.2f}M"
    if amount >= 1_000:
        return f"{amount / 1_000:.2f}K"
    return f"{amount:,.2f}"


def _format_token_amount_full(amount: float) -> str:
    if amount >= 1:
        return f"{amount:,.2f}".rstrip("0").rstrip(".")
    return f"{amount:.6f}".rstrip("0").rstrip(".")


def _format_dev_buy_section(event: LaunchEvent) -> str:
    if event.dev_buy_sol is None or event.dev_buy_tokens is None:
        return ""

    symbol = event.token_symbol or "TOKEN"
    tokens_full = _format_token_amount_full(event.dev_buy_tokens)
    tokens_short = _format_token_amount(event.dev_buy_tokens)

    lines = [
        "💰 <b>Dev buy</b>",
        (
            f"Swapped <code>{event.dev_buy_sol:.2f} SOL</code> "
            f"for <code>{tokens_full}</code> {symbol}"
        ),
    ]
    if event.dev_buy_pct is not None:
        lines.append(f"✊ Holds <code>{tokens_short} ({event.dev_buy_pct:.2f}%)</code>")
    return "\n".join(lines) + "\n\n"


def _format_market_cap(usd: float | None) -> str:
    if usd is None:
        return "—"
    if usd >= 1_000_000:
        return f"${usd / 1_000_000:.2f}M"
    if usd >= 1_000:
        return f"${usd / 1_000:.2f}K"
    return f"${usd:.2f}"


def format_buy_message(
    event: TradeEvent,
    explorer_base: str,
    gmgn_token_base: str = "https://gmgn.ai/sol/token/",
) -> str:
    symbol = event.token_symbol or "TOKEN"
    name = event.token_name or symbol
    explorer = f"{explorer_base}{event.signature}"
    gmgn = f"{gmgn_token_base.rstrip('/')}/{event.token_mint}"
    sol = f"{event.sol_amount:.2f}"
    tokens = _format_token_amount_full(event.token_amount)
    mc = _format_market_cap(event.market_cap_usd)

    lines = [
        f'<a href="{explorer}">🟢 BUY</a>',
        f"<code>{name}</code> <b>(${symbol})</b>",
        f"💰 <code>{sol} SOL</code> → <code>{tokens} tokens</code>",
        f"📊 MC <code>{mc}</code>",
    ]
    if event.slot_diff is not None:
        sign = "+" if event.slot_diff >= 0 else ""
        lines.append(f"🧱 Slot diff <code>{sign}{event.slot_diff:,}</code>")
    lines.extend(
        [
            "🪙 Mint",
            f"<code>{event.token_mint}</code>",
            "👨‍💻 Dev",
            f"<code>{event.developer_wallet or '—'}</code>",
            f'<a href="{gmgn}">📈 GMGN</a>',
        ]
    )
    return "\n".join(lines)


def format_sell_message(
    event: TradeEvent,
    explorer_base: str,
    gmgn_token_base: str = "https://gmgn.ai/sol/token/",
) -> str:
    symbol = event.token_symbol or "TOKEN"
    name = event.token_name or symbol
    explorer = f"{explorer_base}{event.signature}"
    gmgn = f"{gmgn_token_base.rstrip('/')}/{event.token_mint}"
    tokens = _format_token_amount_full(event.token_amount)
    sol = f"{event.sol_amount:.2f}"
    mc = _format_market_cap(event.market_cap_usd)

    pnl_line = "📉 PnL —"
    if event.pnl_pct is not None:
        sold = f"{event.sold_pct:.0f}%" if event.sold_pct is not None else "—"
        pnl_line = f"📉 PnL <code>{event.pnl_pct:+.2f}%</code> · Sold <code>{sold}</code>"

    return (
        f'<a href="{explorer}">🔴 SELL</a>\n'
        f"<code>{name}</code> <b>(${symbol})</b>\n"
        f"💰 <code>{tokens} tokens</code> → <code>{sol} SOL</code>\n"
        f"📊 MC <code>{mc}</code>\n"
        f"{pnl_line}\n"
        f"🪙 Mint\n"
        f"<code>{event.token_mint}</code>\n"
        f"👨‍💻 Dev\n"
        f"<code>{event.developer_wallet or '—'}</code>\n"
        f'<a href="{gmgn}">📈 GMGN</a>'
    )


def format_launch_message(
    event: LaunchEvent,
    explorer_base: str,
    gmgn_token_base: str = "https://gmgn.ai/sol/token/",
    axiom_token_base: str = "https://axiom.trade/t/",
) -> str:
    time_str = (
        event.block_time.strftime("%H:%M:%S UTC")
        if event.block_time
        else "unknown"
    )
    name = event.token_name or "—"
    symbol = event.token_symbol or "—"
    explorer = f"{explorer_base}{event.signature}"
    gmgn = f"{gmgn_token_base.rstrip('/')}/{event.token_mint}"
    axiom = f"{axiom_token_base.rstrip('/')}/{event.token_mint}"
    dev_buy = _format_dev_buy_section(event)

    return (
        "🚀 <b>NEW DEV LAUNCH</b>\n\n"
        f"<code>{name}</code> <b>(${symbol})</b>\n\n"
        f"{dev_buy}"
        "👨‍💻 <b>Developer</b>\n"
        f"<code>{event.developer_wallet}</code>\n\n"
        "🪙 <b>Mint</b>\n"
        f"<code>{event.token_mint}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📍 <b>{event.platform}</b>\n"
        f"🕒 <code>{time_str}</code>\n"
        f"🧱 Slot: <code>{event.slot}</code>\n\n"
        f'<a href="{explorer}">🔎 Solscan</a> • '
        f'<a href="{gmgn}">📈 GMGN</a> • '
        f'<a href="{axiom}">⚡ Axiom</a>'
    )


def _format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _format_ago(when: datetime | None) -> str:
    if when is None:
        return "never"
    seconds = (datetime.now(timezone.utc) - when).total_seconds()
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h ago"
    return f"{int(seconds // 86400)}d ago"


def _stream_health(status: WatcherStatus) -> tuple[str, str]:
    if not status.running:
        return "🔴", "Stopped"
    if status.developer_count == 0:
        return "🔴", "No devs"
    if status.last_stream_event_at is not None:
        idle = (datetime.now(timezone.utc) - status.last_stream_event_at).total_seconds()
        if idle < 600:
            return "🟢", "Live"
        if idle < 1800:
            return "🟡", "Quiet"
        return "🔴", "Stale"
    if status.uptime_seconds < 180:
        return "🟡", "Warming up"
    if status.stream_events == 0:
        return "🔴", "No stream data"
    return "🟡", "Unknown"


def format_status_message(status: WatcherStatus) -> str:
    health_emoji, health_label = _stream_health(status)
    stream_label = "Geyser gRPC" if status.stream_mode == "geyser" else "WebSocket"
    env_label = "Server" if status.environment == "server" else "Local"
    backfill = "on" if status.backfill_enabled else "off"

    return (
        f"{health_emoji} <b>Watcher {health_label}</b>\n\n"
        f"📡 <b>Stream</b>: {stream_label}\n"
        f"🌍 <b>Env</b>: {env_label}\n"
        f"👥 <b>Devs</b>: <code>{status.developer_count}</code>\n"
        f"⏱ <b>Uptime</b>: <code>{_format_duration(status.uptime_seconds)}</code>\n\n"
        f"📥 <b>Stream tx</b>: <code>{status.stream_events}</code>\n"
        f"⚙️ <b>Processed</b>: <code>{status.events_processed}</code>\n"
        f"🕒 <b>Last tx</b>: <code>{_format_ago(status.last_stream_event_at)}</code>\n"
        f"📋 <b>Queue</b>: <code>{status.tx_queue_size}</code>\n\n"
        f"🚀 <b>Launches today</b>: <code>{status.launches_today}</code>\n"
        f"🎯 <b>Total detected</b>: <code>{status.launches_detected}</code>\n"
        f"🔄 <b>Reconnects</b>: <code>{status.reconnect_count}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"RPC <code>{status.rpc_rps_limit:.0f}</code> rps · "
        f"Backfill <code>{backfill}</code>"
    )


class TelegramService:
    def __init__(
        self,
        token: str,
        chat_id: str,
        database: Database,
        metrics: ServiceMetrics,
        get_metrics: Callable[[], ServiceMetrics],
        get_status: Callable[[], Awaitable[WatcherStatus]],
        explorer_base: str,
        gmgn_token_base: str = "https://gmgn.ai/sol/token/",
        axiom_token_base: str = "https://axiom.trade/t/",
    ) -> None:
        self._chat_id = chat_id
        self._db = database
        self._metrics = metrics
        self._get_metrics = get_metrics
        self._get_status = get_status
        self._explorer_base = explorer_base
        self._gmgn_token_base = gmgn_token_base
        self._axiom_token_base = axiom_token_base
        self._bot = Bot(token=token)
        self._dp = Dispatcher()
        self._send_queue: asyncio.Queue[LaunchEvent | TradeEvent] = asyncio.Queue()
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

        @self._dp.message(Command("status"))
        async def cmd_status(message: Message) -> None:
            status = await self._get_status()
            await message.answer(
                format_status_message(status),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )

        @self._dp.message(Command("stats"))
        async def cmd_stats(message: Message) -> None:
            m = self._get_metrics()
            dev_count = await self._db.count_developers()
            today_count = await self._db.count_launches_today()
            db_size = self._db.path.stat().st_size if self._db.path.exists() else 0
            uptime_h = m.uptime_seconds / 3600

            mem = _memory_usage_mb()
            mem_line = f"{mem:.1f} MB" if mem is not None else "N/A (Windows)"

            await message.answer(
                "<b>Launch Tracker Stats</b>\n\n"
                f"<b>Tracked developers</b>\n{dev_count}\n\n"
                f"<b>Today's launches</b>\n{today_count}\n\n"
                f"<b>Uptime</b>\n{uptime_h:.1f}h\n\n"
                f"<b>Reconnect count</b>\n{m.reconnect_count}\n\n"
                f"<b>Events received</b>\n{m.events_received}\n\n"
                f"<b>Launches detected</b>\n{m.launches_detected}\n\n"
                f"<b>Trades detected</b>\n{m.trades_detected}\n\n"
                f"<b>Backfill runs</b>\n{m.backfill_runs}\n\n"
                f"<b>Avg detection latency</b>\n{m.avg_detection_latency_ms:.0f}ms\n\n"
                f"<b>Avg processing time</b>\n{m.avg_processing_time_ms:.0f}ms\n\n"
                f"<b>Memory usage</b>\n{mem_line}\n\n"
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
        stream_mode: str = "websocket",
        my_wallet_count: int = 0,
    ) -> None:
        backfill_line = (
            f"Backfill: каждые {backfill_interval_minutes} мин"
            if backfill_enabled
            else "Backfill: выключен"
        )
        env_label = "🖥 Сервер (private node)" if environment == "server" else "💻 Локально"
        stream_label = "Geyser gRPC" if stream_mode == "geyser" else "WebSocket"
        text = (
            "🟢 <b>Launch Tracker запущен</b>\n\n"
            "Слежу за новыми лаунчами в реальном времени.\n"
            "Уведомлю, как только один из отслеживаемых "
            "разработчиков запустит токен.\n\n"
            f"<b>Режим</b>: {env_label}\n"
            f"<b>Stream</b>: {stream_label}\n"
            f"<b>Разработчиков</b>: {developer_count}\n"
            f"<b>Мои кошельки</b>: {my_wallet_count}\n"
            f"<b>RPC лимит</b>: {rpc_rps_limit:.0f} RPS\n"
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

    async def notify_trade(self, event: TradeEvent) -> None:
        await self._send_queue.put(event)

    async def _sender_loop(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._send_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                if isinstance(event, LaunchEvent):
                    text = format_launch_message(
                        event,
                        self._explorer_base,
                        self._gmgn_token_base,
                        self._axiom_token_base,
                    )
                elif event.side == "buy":
                    text = format_buy_message(
                        event,
                        self._explorer_base,
                        self._gmgn_token_base,
                    )
                else:
                    text = format_sell_message(
                        event,
                        self._explorer_base,
                        self._gmgn_token_base,
                    )
                await self._bot.send_message(
                    self._chat_id,
                    text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                self._metrics.telegram_sent += 1
                sig = event.signature
                mint = event.token_mint if hasattr(event, "token_mint") else getattr(event, "token_mint", "")
                log_extra(
                    logger,
                    logging.INFO,
                    "Telegram sent",
                    event="telegram_sent",
                    signature=sig,
                    mint=mint,
                )
            except Exception as exc:
                self._metrics.telegram_failed += 1
                sig = getattr(event, "signature", "")
                log_extra(
                    logger,
                    logging.ERROR,
                    "Telegram send failed",
                    event="telegram_failed",
                    signature=sig,
                    error=str(exc),
                )

    async def _polling_loop(self) -> None:
        try:
            await self._dp.start_polling(self._bot)
        except asyncio.CancelledError:
            pass

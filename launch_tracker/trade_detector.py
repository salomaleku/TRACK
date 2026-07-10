"""Detect pump.fun buy/sell trades for tracked bot wallets."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from launch_tracker.models import TradeEvent, TransactionEvent
from launch_tracker.pump_utils import (
    MIN_TRADE_SOL,
    PUMP_TOTAL_SUPPLY,
    display_symbol,
    estimate_market_cap_usd,
    extract_tip_sol,
    fee_payer,
    largest_sol_transfer_out,
    pump_trade_side,
    seen_seconds_from_slots,
    sol_sell_proceeds,
    token_balance_pre_post,
    tx_fee_sol,
    wallet_sol_delta,
    wallet_token_delta,
)

logger = logging.getLogger(__name__)


class TradeDetector:
    def __init__(self, my_wallets: set[str]) -> None:
        self._wallets = my_wallets

    def update_wallets(self, wallets: set[str]) -> None:
        self._wallets = wallets

    def detect(
        self,
        event: TransactionEvent,
        *,
        sol_usd: float,
        launch_slot: int | None = None,
        token_name: str | None = None,
        token_symbol: str | None = None,
    ) -> TradeEvent | None:
        if event.meta.get("err"):
            return None

        wallet = fee_payer(event.transaction)
        if not wallet or wallet not in self._wallets:
            return None

        side = pump_trade_side(event.meta, event.transaction)
        if side not in ("buy", "sell"):
            return None

        mint, delta = wallet_token_delta(event.meta, wallet)
        if not mint:
            return None

        tokens = abs(delta)
        if tokens <= 0:
            return None

        pre_bal, post_bal = token_balance_pre_post(event.meta, wallet, mint)
        fee_sol = tx_fee_sol(event.meta)
        sol_delta = wallet_sol_delta(event.meta, event.transaction, wallet)

        if side == "buy":
            if delta <= 0:
                return None
            sol = largest_sol_transfer_out(event.meta, event.transaction, wallet)
        else:
            if delta >= 0:
                return None
            sol = sol_sell_proceeds(event.meta, event.transaction, wallet)

        if sol is None or sol < MIN_TRADE_SOL:
            return None

        main_lamports = int(sol * 1_000_000_000)
        tip_sol = extract_tip_sol(event.meta, event.transaction, wallet, main_lamports)

        price_usd = (sol / tokens) * sol_usd if tokens > 0 else None
        swap_usd = sol * sol_usd
        token_delta_usd = swap_usd if side == "buy" else -swap_usd
        sol_delta_val = sol_delta if sol_delta is not None else (sol if side == "sell" else -sol)
        sol_delta_usd = sol_delta_val * sol_usd

        holds_tokens = post_bal
        holds_pct = holds_tokens / PUMP_TOTAL_SUPPLY * 100.0

        sold_pct: float | None = None
        if side == "sell" and pre_bal > 0:
            sold_pct = round(min(100.0, tokens / pre_bal * 100.0), 1)

        symbol = display_symbol(token_symbol, mint)

        return TradeEvent(
            wallet=wallet,
            side=side,
            token_mint=mint,
            token_symbol=symbol,
            token_name=token_name,
            sol_amount=sol,
            token_amount=tokens,
            sol_delta=sol_delta_val,
            fee_sol=fee_sol,
            tip_sol=tip_sol,
            price_usd=price_usd,
            swap_usd=swap_usd,
            sol_delta_usd=sol_delta_usd,
            token_delta_usd=token_delta_usd,
            market_cap_usd=estimate_market_cap_usd(sol, tokens, sol_usd),
            holds_tokens=holds_tokens,
            holds_pct=holds_pct,
            sold_pct=sold_pct,
            pnl_pct=None,
            pnl_usd=None,
            upnl_usd=None,
            seen_seconds=seen_seconds_from_slots(event.slot, launch_slot),
            signature=event.signature,
            slot=event.slot,
            block_time=self._block_time_dt(event.block_time),
            source=event.source,
        )

    @staticmethod
    def _block_time_dt(block_time: int | None) -> datetime | None:
        if block_time is None:
            return None
        return datetime.fromtimestamp(block_time, tz=timezone.utc)

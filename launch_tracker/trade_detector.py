"""Detect pump.fun buy/sell trades for tracked bot wallets."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from launch_tracker.models import TradeEvent, TransactionEvent
from launch_tracker.pump_utils import (
    MIN_TRADE_SOL,
    estimate_market_cap_usd,
    fee_payer,
    largest_sol_transfer_out,
    pump_trade_side,
    sol_sell_proceeds,
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
        developer_wallet: str | None = None,
        token_name: str | None = None,
        token_symbol: str | None = None,
        pnl_pct: float | None = None,
        sold_pct: float | None = None,
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

        mc_usd = estimate_market_cap_usd(sol, tokens, sol_usd)
        slot_diff: int | None = None
        if side == "buy" and launch_slot is not None:
            slot_diff = event.slot - launch_slot

        return TradeEvent(
            wallet=wallet,
            side=side,
            token_mint=mint,
            token_name=token_name,
            token_symbol=token_symbol,
            developer_wallet=developer_wallet,
            sol_amount=sol,
            token_amount=tokens,
            market_cap_usd=mc_usd,
            slot_diff=slot_diff,
            pnl_pct=pnl_pct if side == "sell" else None,
            sold_pct=sold_pct if side == "sell" else None,
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

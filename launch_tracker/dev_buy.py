"""Extract developer initial buy from launch transactions."""

from __future__ import annotations

from typing import Any

PUMP_TOTAL_SUPPLY = 1_000_000_000.0
MIN_DEV_BUY_SOL = 0.01


def extract_dev_buy(
    meta: dict[str, Any],
    transaction: dict[str, Any],
    developer: str,
    mint: str,
    platform: str,
) -> tuple[float | None, float | None, float | None]:
    """Return (sol_spent, tokens_received, supply_pct) for the dev's bundled buy."""
    tokens = _dev_tokens_received(meta, developer, mint)
    if tokens is None or tokens <= 0:
        return None, None, None

    sol = _dev_sol_buy_amount(meta, transaction, developer)
    if sol is None or sol < MIN_DEV_BUY_SOL:
        return None, None, None

    pct: float | None = None
    if platform == "pump.fun":
        pct = tokens / PUMP_TOTAL_SUPPLY * 100.0

    return sol, tokens, pct


def _dev_tokens_received(meta: dict[str, Any], developer: str, mint: str) -> float | None:
    for entry in meta.get("postTokenBalances") or []:
        if entry.get("owner") != developer or entry.get("mint") != mint:
            continue
        ui = entry.get("uiTokenAmount") or {}
        ui_amount = ui.get("uiAmount")
        if ui_amount is not None:
            return float(ui_amount)
        raw = ui.get("amount")
        decimals = ui.get("decimals")
        if raw is not None and decimals is not None:
            return int(raw) / (10 ** int(decimals))
    return None


def _dev_sol_buy_amount(
    meta: dict[str, Any],
    transaction: dict[str, Any],
    developer: str,
) -> float | None:
    """Largest SOL transfer from the developer in the tx (bonding-curve buy)."""
    max_lamports = 0
    for ix in _iter_instructions(meta, transaction):
        parsed = ix.get("parsed")
        if not parsed or parsed.get("type") != "transfer":
            continue
        info = parsed.get("info") or {}
        if info.get("source") != developer:
            continue
        lamports = int(info.get("lamports") or 0)
        if lamports > max_lamports:
            max_lamports = lamports
    if max_lamports <= 0:
        return None
    return max_lamports / 1_000_000_000


def _iter_instructions(
    meta: dict[str, Any],
    transaction: dict[str, Any],
) -> list[dict[str, Any]]:
    instructions: list[dict[str, Any]] = []
    message = transaction.get("message", {})
    instructions.extend(message.get("instructions") or [])
    for group in meta.get("innerInstructions") or []:
        instructions.extend(group.get("instructions") or [])
    return instructions

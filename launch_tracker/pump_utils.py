"""Shared pump.fun helpers for launch and trade parsing."""

from __future__ import annotations

import re
from typing import Any

PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
WSOL_MINT = "So11111111111111111111111111111111111111112"
PUMP_TOTAL_SUPPLY = 1_000_000_000.0
MIN_TRADE_SOL = 0.001

PUMP_BUY_DISCRIMINATORS = frozenset(
    {
        bytes.fromhex("66063d1201daebea"),  # buy
        bytes.fromhex("38fc74089edfcd5f"),  # buy_exact_sol_in
        bytes.fromhex("c62e1552b4d9e870"),  # buy_exact_quote_in
        bytes.fromhex("c2ab1c46684d5b2f"),  # buy_exact_quote_in_v2
    }
)

PUMP_SELL_DISCRIMINATORS = frozenset(
    {
        bytes.fromhex("33e685a4017f83ad"),  # sell
        bytes.fromhex("9527de9bd37c981a"),  # sell_exact_in
        bytes.fromhex("5df6823ce7e940b2"),  # sell_v2
        bytes.fromhex("c733ba3c7651ae66"),  # sell_exact_quote_in
        bytes.fromhex("9892de9e6289f898"),  # sell_exact_quote_out
    }
)

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BALANCE_GAIN_RE = re.compile(r"Balance gain:\s*(\d+)")


def b58decode(data: str) -> bytes:
    n = 0
    for char in data:
        n = n * 58 + _B58_ALPHABET.index(char)
    pad = 0
    for char in data:
        if char == "1":
            pad += 1
        else:
            break
    full = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    return b"\x00" * pad + full


def iter_instructions(meta: dict[str, Any], transaction: dict[str, Any]) -> list[dict[str, Any]]:
    instructions: list[dict[str, Any]] = []
    message = transaction.get("message", {})
    instructions.extend(message.get("instructions") or [])
    for group in meta.get("innerInstructions") or []:
        instructions.extend(group.get("instructions") or [])
    return instructions


def account_keys(meta: dict[str, Any], transaction: dict[str, Any]) -> list[str]:
    message = transaction.get("message", {})
    keys: list[str] = []
    for ak in message.get("accountKeys", []):
        if isinstance(ak, dict):
            keys.append(ak["pubkey"])
        else:
            keys.append(str(ak))
    loaded = meta.get("loadedAddresses", {})
    keys.extend(loaded.get("writable", []))
    keys.extend(loaded.get("readonly", []))
    return keys


def fee_payer(transaction: dict[str, Any]) -> str | None:
    message = transaction.get("message", {})
    keys = message.get("accountKeys", [])
    if not keys:
        return None
    first = keys[0]
    if isinstance(first, dict):
        return first.get("pubkey")
    return str(first)


def wallet_token_delta(meta: dict[str, Any], wallet: str) -> tuple[str | None, float]:
    """Return (mint, delta) for the largest non-WSOL token balance change."""
    pre: dict[str, float] = {}
    post: dict[str, float] = {}

    for entry in meta.get("preTokenBalances") or []:
        if entry.get("owner") != wallet:
            continue
        mint = entry.get("mint")
        if not mint or mint == WSOL_MINT:
            continue
        ui = entry.get("uiTokenAmount") or {}
        pre[mint] = float(ui.get("uiAmount") or 0)

    for entry in meta.get("postTokenBalances") or []:
        if entry.get("owner") != wallet:
            continue
        mint = entry.get("mint")
        if not mint or mint == WSOL_MINT:
            continue
        ui = entry.get("uiTokenAmount") or {}
        post[mint] = float(ui.get("uiAmount") or 0)

    best_mint: str | None = None
    best_delta = 0.0
    for mint in set(pre) | set(post):
        delta = post.get(mint, 0.0) - pre.get(mint, 0.0)
        if abs(delta) > abs(best_delta):
            best_delta = delta
            best_mint = mint
    return best_mint, best_delta


def largest_sol_transfer_out(meta: dict[str, Any], transaction: dict[str, Any], wallet: str) -> float | None:
    max_lamports = 0
    for ix in iter_instructions(meta, transaction):
        parsed = ix.get("parsed")
        if not parsed or parsed.get("type") != "transfer":
            continue
        info = parsed.get("info") or {}
        if info.get("source") != wallet:
            continue
        lamports = int(info.get("lamports") or 0)
        if lamports > max_lamports:
            max_lamports = lamports
    if max_lamports <= 0:
        return None
    return max_lamports / 1_000_000_000


def sol_sell_proceeds(meta: dict[str, Any], transaction: dict[str, Any], wallet: str) -> float | None:
    """Prefer pump SellV2 balance-gain logs; fall back to wallet SOL delta + fee."""
    for log in meta.get("logMessages") or []:
        match = _BALANCE_GAIN_RE.search(log)
        if match:
            return int(match.group(1)) / 1_000_000_000

    keys = account_keys(meta, transaction)
    payer = fee_payer(transaction)
    if payer != wallet:
        return None
    try:
        idx = keys.index(wallet)
    except ValueError:
        return None
    delta = meta["postBalances"][idx] - meta["preBalances"][idx]
    fee = int(meta.get("fee") or 0)
    proceeds = delta + fee
    if proceeds <= 0:
        return None
    return proceeds / 1_000_000_000


def pump_trade_side(meta: dict[str, Any], transaction: dict[str, Any]) -> str | None:
    for log in meta.get("logMessages") or []:
        if "Instruction: Sell" in log or "Instruction: SellV2" in log:
            return "sell"
        if "Instruction: Buy" in log or "Instruction: BuyExact" in log:
            return "buy"

    for ix in iter_instructions(meta, transaction):
        program = ix.get("programId")
        if program != PUMP_FUN_PROGRAM:
            continue
        data = ix.get("data", "")
        if not data:
            continue
        try:
            raw = b58decode(data)
        except (ValueError, IndexError):
            continue
        if len(raw) < 8:
            continue
        disc = raw[:8]
        if disc in PUMP_BUY_DISCRIMINATORS:
            return "buy"
        if disc in PUMP_SELL_DISCRIMINATORS:
            return "sell"
    return None


def estimate_market_cap_usd(sol_amount: float, token_amount: float, sol_usd: float) -> float | None:
    if sol_amount <= 0 or token_amount <= 0 or sol_usd <= 0:
        return None
    mc_sol = (sol_amount / token_amount) * PUMP_TOTAL_SUPPLY
    return mc_sol * sol_usd

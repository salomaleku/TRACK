"""Isolated launch detection logic."""

from __future__ import annotations

import logging
import struct
from datetime import datetime, timezone
from typing import Any

from launch_tracker.models import LaunchEvent, TransactionEvent

logger = logging.getLogger(__name__)

SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
METAPLEX_METADATA = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Anchor discriminators for pump.fun create instructions (global:<name>).
PUMP_CREATE_DISCRIMINATORS = frozenset(
    {
        bytes([24, 30, 200, 40, 5, 28, 7, 119]),  # create
        bytes([214, 144, 76, 236, 95, 139, 49, 180]),  # create_v2
        bytes([246, 164, 68, 157, 140, 248, 240, 90]),  # create_v2 variant (Token-2022)
    }
)

# create_v2 account layout: user/creator is index 5, mint is index 0.
PUMP_CREATE_V2_MIN_ACCOUNTS = 16
PUMP_CREATE_USER_ACCOUNT_INDEX = 5
PUMP_CREATE_MINT_ACCOUNT_INDEX = 0

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58decode(data: str) -> bytes:
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


class LaunchDetector:
    """Detects new token launches from parsed Solana transactions."""

    def __init__(self, tracked_wallets: set[str]) -> None:
        self._tracked = tracked_wallets

    def update_wallets(self, wallets: set[str]) -> None:
        self._tracked = wallets

    def detect(self, event: TransactionEvent) -> LaunchEvent | None:
        if event.meta.get("err"):
            return None

        fee_payer = self._fee_payer(event.transaction)
        if not fee_payer or fee_payer not in self._tracked:
            return None

        launch = (
            self._detect_pump_fun(event, fee_payer)
            or self._detect_spl_mint(event, fee_payer)
            or self._detect_metaplex(event, fee_payer)
        )
        return launch

    def _fee_payer(self, transaction: dict[str, Any]) -> str | None:
        message = transaction.get("message", {})
        keys = message.get("accountKeys", [])
        if not keys:
            return None
        first = keys[0]
        if isinstance(first, dict):
            return first.get("pubkey")
        return str(first)

    def _all_account_keys(self, event: TransactionEvent) -> list[str]:
        message = event.transaction.get("message", {})
        keys: list[str] = []
        for ak in message.get("accountKeys", []):
            if isinstance(ak, dict):
                keys.append(ak["pubkey"])
            else:
                keys.append(str(ak))
        loaded = event.meta.get("loadedAddresses", {})
        keys.extend(loaded.get("writable", []))
        keys.extend(loaded.get("readonly", []))
        return keys

    def _block_time_dt(self, block_time: int | None) -> datetime | None:
        if block_time is None:
            return None
        return datetime.fromtimestamp(block_time, tz=timezone.utc)

    def _resolve_account(self, ref: int | str, keys: list[str]) -> str | None:
        """Resolve account ref from jsonParsed (pubkey) or legacy (index)."""
        if isinstance(ref, str):
            return ref
        if isinstance(ref, int) and 0 <= ref < len(keys):
            return keys[ref]
        return None

    def _detect_pump_fun(self, event: TransactionEvent, developer: str) -> LaunchEvent | None:
        message = event.transaction.get("message", {})
        keys = self._all_account_keys(event)

        for ix in message.get("instructions", []):
            if ix.get("programId") != PUMP_FUN_PROGRAM:
                continue

            accounts = ix.get("accounts", [])
            if len(accounts) < PUMP_CREATE_V2_MIN_ACCOUNTS:
                continue

            mint = self._resolve_account(
                accounts[PUMP_CREATE_MINT_ACCOUNT_INDEX], keys
            )
            user = self._resolve_account(
                accounts[PUMP_CREATE_USER_ACCOUNT_INDEX], keys
            )
            if not mint or user != developer:
                continue

            name: str | None = None
            symbol: str | None = None
            data = ix.get("data", "")
            if data:
                try:
                    raw = _b58decode(data)
                except (ValueError, IndexError):
                    continue
                if len(raw) < 8 or raw[:8] not in PUMP_CREATE_DISCRIMINATORS:
                    continue
                name, symbol = self._extract_pump_metadata(raw)
            elif len(accounts) != PUMP_CREATE_V2_MIN_ACCOUNTS:
                continue

            return LaunchEvent(
                developer_wallet=developer,
                token_mint=mint,
                token_name=name,
                token_symbol=symbol,
                platform="pump.fun",
                signature=event.signature,
                slot=event.slot,
                block_time=self._block_time_dt(event.block_time),
                source=event.source,
            )
        return None

    def _extract_pump_metadata(self, data: bytes) -> tuple[str | None, str | None]:
        """Parse name/symbol from pump.fun create / create_v2 instruction data."""
        try:
            offset = 8  # skip discriminator
            name_len = struct.unpack_from("<I", data, offset)[0]
            offset += 4
            name = data[offset : offset + name_len].decode("utf-8", errors="replace")
            offset += name_len
            symbol_len = struct.unpack_from("<I", data, offset)[0]
            offset += 4
            symbol = data[offset : offset + symbol_len].decode("utf-8", errors="replace")
            return name or None, symbol or None
        except (struct.error, IndexError, UnicodeDecodeError):
            return None, None

    def _detect_spl_mint(self, event: TransactionEvent, developer: str) -> LaunchEvent | None:
        message = event.transaction.get("message", {})
        keys = self._all_account_keys(event)

        for ix in message.get("instructions", []):
            parsed = ix.get("parsed")
            if not parsed:
                continue
            program_id = ix.get("programId", "")
            if program_id not in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
                continue
            ix_type = parsed.get("type", "")
            if ix_type not in ("initializeMint", "initializeMint2"):
                continue
            info = parsed.get("info", {})
            mint_authority = info.get("mintAuthority")
            mint = info.get("mint")
            if mint_authority != developer or not mint:
                continue
            platform = "spl-token-2022" if program_id == TOKEN_2022_PROGRAM else "spl-token"
            return LaunchEvent(
                developer_wallet=developer,
                token_mint=mint,
                token_name=None,
                token_symbol=None,
                platform=platform,
                signature=event.signature,
                slot=event.slot,
                block_time=self._block_time_dt(event.block_time),
                source=event.source,
            )

        for group in event.meta.get("innerInstructions", []) or []:
            for inner in group.get("instructions", []):
                parsed = inner.get("parsed")
                if not parsed:
                    continue
                program_id = inner.get("programId", "")
                if program_id not in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
                    continue
                if parsed.get("type") not in ("initializeMint", "initializeMint2"):
                    continue
                info = parsed.get("info", {})
                if info.get("mintAuthority") != developer:
                    continue
                mint = info.get("mint")
                if not mint:
                    continue
                platform = "spl-token-2022" if program_id == TOKEN_2022_PROGRAM else "spl-token"
                return LaunchEvent(
                    developer_wallet=developer,
                    token_mint=mint,
                    token_name=None,
                    token_symbol=None,
                    platform=platform,
                    signature=event.signature,
                    slot=event.slot,
                    block_time=self._block_time_dt(event.block_time),
                    source=event.source,
                )
        return None

    def _detect_metaplex(self, event: TransactionEvent, developer: str) -> LaunchEvent | None:
        """Detect Metaplex metadata creation as a launch signal."""
        message = event.transaction.get("message", {})
        keys = self._all_account_keys(event)

        for ix in message.get("instructions", []):
            if ix.get("programId") != METAPLEX_METADATA:
                continue
            accounts = ix.get("accounts", [])
            if len(accounts) < 2:
                continue
            # mint is typically account index 1 in create metadata
            mint_idx = accounts[1] if len(accounts) > 1 else None
            mint = self._resolve_account(mint_idx, keys) if mint_idx is not None else None
            if not mint:
                continue
            return LaunchEvent(
                developer_wallet=developer,
                token_mint=mint,
                token_name=None,
                token_symbol=None,
                platform="metaplex",
                signature=event.signature,
                slot=event.slot,
                block_time=self._block_time_dt(event.block_time),
                source=event.source,
            )
        return None

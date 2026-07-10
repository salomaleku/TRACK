"""SQLite persistence layer."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from launch_tracker.models import LaunchEvent

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS developers (
    wallet TEXT PRIMARY KEY,
    added_at TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS launches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    developer_wallet TEXT NOT NULL,
    token_mint TEXT NOT NULL,
    token_name TEXT,
    token_symbol TEXT,
    platform TEXT NOT NULL,
    signature TEXT NOT NULL UNIQUE,
    slot INTEGER NOT NULL,
    block_time TEXT,
    source TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    FOREIGN KEY (developer_wallet) REFERENCES developers(wallet)
);

CREATE TABLE IF NOT EXISTS processed_signatures (
    signature TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL,
    source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS service_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_launches_developer ON launches(developer_wallet);
CREATE INDEX IF NOT EXISTS idx_launches_detected_at ON launches(detected_at);
CREATE INDEX IF NOT EXISTS idx_launches_block_time ON launches(block_time);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    mint TEXT NOT NULL,
    side TEXT NOT NULL,
    sol REAL NOT NULL,
    tokens REAL NOT NULL,
    signature TEXT NOT NULL UNIQUE,
    slot INTEGER NOT NULL,
    block_time TEXT,
    detected_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
    wallet TEXT NOT NULL,
    mint TEXT NOT NULL,
    tokens_held REAL NOT NULL DEFAULT 0,
    cost_basis_sol REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (wallet, mint)
);

CREATE INDEX IF NOT EXISTS idx_trades_wallet_mint ON trades(wallet, mint);
CREATE INDEX IF NOT EXISTS idx_launches_mint ON launches(token_mint);
"""


class Database:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(_SCHEMA)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @property
    def path(self) -> Path:
        return self._path

    async def sync_developers(self, wallets: set[str]) -> None:
        assert self._conn is not None
        now = datetime.now(timezone.utc).isoformat()
        for wallet in wallets:
            await self._conn.execute(
                """
                INSERT INTO developers (wallet, added_at, active)
                VALUES (?, ?, 1)
                ON CONFLICT(wallet) DO UPDATE SET active = 1
                """,
                (wallet, now),
            )
        await self._conn.commit()

    async def is_processed(self, signature: str) -> bool:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT 1 FROM processed_signatures WHERE signature = ?",
            (signature,),
        ) as cursor:
            return await cursor.fetchone() is not None

    async def mark_processed(self, signature: str, source: str) -> bool:
        """Returns False if signature was already processed."""
        assert self._conn is not None
        now = datetime.now(timezone.utc).isoformat()
        try:
            await self._conn.execute(
                "INSERT INTO processed_signatures (signature, processed_at, source) VALUES (?, ?, ?)",
                (signature, now, source),
            )
            await self._conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def save_launch(self, event: LaunchEvent) -> bool:
        """Returns False if launch signature already exists."""
        assert self._conn is not None
        now = datetime.now(timezone.utc).isoformat()
        try:
            await self._conn.execute(
                """
                INSERT INTO launches (
                    developer_wallet, token_mint, token_name, token_symbol,
                    platform, signature, slot, block_time, source, detected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.developer_wallet,
                    event.token_mint,
                    event.token_name,
                    event.token_symbol,
                    event.platform,
                    event.signature,
                    event.slot,
                    event.block_time.isoformat() if event.block_time else None,
                    event.source,
                    now,
                ),
            )
            await self._conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def get_launches_today(self, limit: int = 50) -> list[dict]:
        assert self._conn is not None
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        async with self._conn.execute(
            """
            SELECT * FROM launches
            WHERE detected_at LIKE ?
            ORDER BY detected_at DESC
            LIMIT ?
            """,
            (f"{today}%", limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_latest_launches(self, limit: int = 10) -> list[dict]:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT * FROM launches ORDER BY detected_at DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_dev_launches(self, wallet: str, limit: int = 20) -> list[dict]:
        assert self._conn is not None
        async with self._conn.execute(
            """
            SELECT * FROM launches
            WHERE developer_wallet = ?
            ORDER BY detected_at DESC
            LIMIT ?
            """,
            (wallet, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def count_developers(self) -> int:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT COUNT(*) FROM developers WHERE active = 1"
        ) as cursor:
            row = await cursor.fetchone()
            return int(row[0]) if row else 0

    async def count_launches_today(self) -> int:
        assert self._conn is not None
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        async with self._conn.execute(
            "SELECT COUNT(*) FROM launches WHERE detected_at LIKE ?",
            (f"{today}%",),
        ) as cursor:
            row = await cursor.fetchone()
            return int(row[0]) if row else 0

    async def set_state(self, key: str, value: str) -> None:
        assert self._conn is not None
        await self._conn.execute(
            """
            INSERT INTO service_state (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        await self._conn.commit()

    async def get_launch_by_mint(self, mint: str) -> dict | None:
        assert self._conn is not None
        async with self._conn.execute(
            """
            SELECT * FROM launches
            WHERE token_mint = ?
            ORDER BY slot ASC
            LIMIT 1
            """,
            (mint,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_position(self, wallet: str, mint: str) -> tuple[float, float]:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT tokens_held, cost_basis_sol FROM positions WHERE wallet = ? AND mint = ?",
            (wallet, mint),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return 0.0, 0.0
            return float(row[0]), float(row[1])

    async def save_trade(
        self,
        wallet: str,
        mint: str,
        side: str,
        sol: float,
        tokens: float,
        signature: str,
        slot: int,
        block_time: datetime | None,
    ) -> bool:
        """Returns False if trade signature already exists."""
        assert self._conn is not None
        now = datetime.now(timezone.utc).isoformat()
        try:
            await self._conn.execute(
                """
                INSERT INTO trades (
                    wallet, mint, side, sol, tokens, signature, slot, block_time, detected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    wallet,
                    mint,
                    side,
                    sol,
                    tokens,
                    signature,
                    slot,
                    block_time.isoformat() if block_time else None,
                    now,
                ),
            )
            await self._conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def apply_buy_position(self, wallet: str, mint: str, sol: float, tokens: float) -> None:
        assert self._conn is not None
        held, cost = await self.get_position(wallet, mint)
        await self._conn.execute(
            """
            INSERT INTO positions (wallet, mint, tokens_held, cost_basis_sol)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(wallet, mint) DO UPDATE SET
                tokens_held = excluded.tokens_held,
                cost_basis_sol = excluded.cost_basis_sol
            """,
            (wallet, mint, held + tokens, cost + sol),
        )
        await self._conn.commit()

    async def preview_sell_position(
        self, wallet: str, mint: str, tokens_sold: float
    ) -> tuple[float, float]:
        """Return (cost_basis_sol_for_sale, sold_pct) without updating position."""
        held, cost = await self.get_position(wallet, mint)
        if held <= 0 or tokens_sold <= 0:
            return 0.0, 100.0 if tokens_sold > 0 else 0.0
        sold_pct = min(100.0, tokens_sold / held * 100.0)
        cost_for_sale = cost * (tokens_sold / held)
        return cost_for_sale, sold_pct

    async def apply_sell_position(
        self, wallet: str, mint: str, tokens_sold: float
    ) -> tuple[float, float]:
        """Return (cost_basis_sol_for_sale, sold_pct). Updates position."""
        assert self._conn is not None
        held, cost = await self.get_position(wallet, mint)
        if held <= 0 or tokens_sold <= 0:
            return 0.0, 100.0 if tokens_sold > 0 else 0.0

        sold_pct = min(100.0, tokens_sold / held * 100.0)
        cost_for_sale = cost * (tokens_sold / held)
        remaining_tokens = max(0.0, held - tokens_sold)
        remaining_cost = max(0.0, cost - cost_for_sale)

        if remaining_tokens <= 0:
            await self._conn.execute(
                "DELETE FROM positions WHERE wallet = ? AND mint = ?",
                (wallet, mint),
            )
        else:
            await self._conn.execute(
                """
                INSERT INTO positions (wallet, mint, tokens_held, cost_basis_sol)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(wallet, mint) DO UPDATE SET
                    tokens_held = excluded.tokens_held,
                    cost_basis_sol = excluded.cost_basis_sol
                """,
                (wallet, mint, remaining_tokens, remaining_cost),
            )
        await self._conn.commit()
        return cost_for_sale, sold_pct

    async def get_state(self, key: str) -> str | None:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT value FROM service_state WHERE key = ?",
            (key,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

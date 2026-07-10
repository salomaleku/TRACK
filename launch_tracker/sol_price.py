"""Cached SOL/USD price for market-cap estimates."""

from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

logger = logging.getLogger(__name__)

DEFAULT_SOL_USD = 75.0
_CACHE_TTL_SECONDS = 120.0


class SolPriceCache:
    def __init__(self, default_usd: float = DEFAULT_SOL_USD, ttl_seconds: float = _CACHE_TTL_SECONDS) -> None:
        self._default = default_usd
        self._ttl = ttl_seconds
        self._price = default_usd
        self._updated_at = 0.0
        self._lock = asyncio.Lock()

    @property
    def cached(self) -> float:
        return self._price

    async def get_usd(self, session: aiohttp.ClientSession | None = None) -> float:
        now = time.monotonic()
        if now - self._updated_at < self._ttl:
            return self._price

        async with self._lock:
            now = time.monotonic()
            if now - self._updated_at < self._ttl:
                return self._price
            price = await self._fetch(session)
            if price is not None:
                self._price = price
                self._updated_at = now
            return self._price

    async def _fetch(self, session: aiohttp.ClientSession | None) -> float | None:
        url = (
            "https://api.coingecko.com/api/v3/simple/price"
            "?ids=solana&vs_currencies=usd"
        )
        try:
            if session is None:
                async with aiohttp.ClientSession() as own_session:
                    return await self._fetch_from_url(own_session, url)
            return await self._fetch_from_url(session, url)
        except Exception as exc:
            logger.debug("SOL price fetch failed: %s", exc)
            return None

    async def _fetch_from_url(self, session: aiohttp.ClientSession, url: str) -> float | None:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            price = data.get("solana", {}).get("usd")
            if price is None:
                return None
            return float(price)

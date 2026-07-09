"""Async token-bucket rate limiter."""

from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """Limits requests to max_rps using a token bucket."""

    def __init__(self, max_rps: float) -> None:
        self._max_rps = max(0.1, max_rps)
        self._tokens = self._max_rps
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    @property
    def max_rps(self) -> float:
        return self._max_rps

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self._max_rps, self._tokens + elapsed * self._max_rps)
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            wait = (1.0 - self._tokens) / self._max_rps
        await asyncio.sleep(wait)
        await self.acquire()

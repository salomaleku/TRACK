"""Async RPC client with endpoint rotation, rate limiting, and 429 retry."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from launch_tracker.models import TransactionEvent
from launch_tracker.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class RpcClient:
    def __init__(
        self,
        endpoints: list[str],
        session: aiohttp.ClientSession,
        max_rps: float = 10.0,
        max_retries: int = 3,
    ) -> None:
        if not endpoints:
            raise ValueError("At least one RPC endpoint is required")
        self._endpoints = endpoints
        self._session = session
        self._limiter = RateLimiter(max_rps)
        self._max_retries = max_retries
        self._id = 0
        self._endpoint_idx = 0

    @property
    def endpoint_count(self) -> int:
        return len(self._endpoints)

    @property
    def max_rps(self) -> float:
        return self._limiter.max_rps

    def _next_endpoint(self) -> str:
        ep = self._endpoints[self._endpoint_idx % len(self._endpoints)]
        self._endpoint_idx += 1
        return ep

    async def _call(self, method: str, params: list[Any]) -> Any:
        last_error: Exception | None = None

        for attempt in range(self._max_retries):
            await self._limiter.acquire()
            endpoint = self._next_endpoint()
            self._id += 1
            payload = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params}

            try:
                async with self._session.post(
                    endpoint,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", 1 + attempt))
                        await asyncio.sleep(min(retry_after, 5.0))
                        last_error = aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=429,
                            message="Too Many Requests",
                        )
                        continue
                    resp.raise_for_status()
                    data = await resp.json()
                if "error" in data:
                    raise RuntimeError(data["error"])
                return data.get("result")
            except aiohttp.ClientResponseError as exc:
                last_error = exc
                if exc.status == 429:
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                await asyncio.sleep(0.3 * (2 ** attempt))

        raise RuntimeError(f"RPC call failed after {self._max_retries} retries: {last_error}")

    async def get_transaction(
        self,
        signature: str,
        source: str = "websocket",
    ) -> TransactionEvent | None:
        result = await self._call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "confirmed",
                },
            ],
        )
        if not result:
            return None
        return TransactionEvent(
            signature=signature,
            slot=result.get("slot", 0),
            block_time=result.get("blockTime"),
            transaction=result["transaction"],
            meta=result["meta"],
            source=source,
        )

    async def get_signatures_for_address(
        self,
        address: str,
        limit: int = 5,
    ) -> list[str]:
        result = await self._call(
            "getSignaturesForAddress",
            [address, {"limit": limit, "commitment": "confirmed"}],
        )
        if not result:
            return []
        return [entry["signature"] for entry in result if not entry.get("err")]

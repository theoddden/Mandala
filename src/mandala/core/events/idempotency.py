"""Idempotent webhook deduplication.

Each inbound webhook carries (or is hashed into) an ``ingest_id``. We
record the id in Redis with a TTL; if a duplicate arrives within the TTL
window we drop it instead of re-emitting the normalized event.
"""
from __future__ import annotations

import hashlib
from typing import Protocol


class IdempotencyStore(Protocol):
    """Protocol implemented by Redis (default) or any other dedupe backend."""

    async def claim(self, key: str, ttl_seconds: int) -> bool:
        """Return ``True`` if this is the first time we've seen ``key``.

        Implementations must be atomic (Redis ``SET NX EX``).
        """
        ...


def hash_payload(*parts: str | bytes) -> str:
    """Stable SHA-256 over the given parts for use as a fallback ingest id."""
    h = hashlib.sha256()
    for p in parts:
        if isinstance(p, str):
            p = p.encode("utf-8")
        h.update(p)
        h.update(b"\x1f")  # unit separator
    return h.hexdigest()


class RedisIdempotencyStore:
    """:class:`IdempotencyStore` backed by Redis ``SET NX EX``."""

    def __init__(self, redis: "object", prefix: str = "mandala:idemp:") -> None:
        self._redis = redis
        self._prefix = prefix

    async def claim(self, key: str, ttl_seconds: int) -> bool:
        result = await self._redis.set(  # type: ignore[attr-defined]
            f"{self._prefix}{key}", "1", nx=True, ex=ttl_seconds
        )
        return bool(result)

"""HMAC verification helpers shared by all webhook receivers.

Verifies webhook signatures using HMAC-SHA256 to ensure authenticity
and prevent replay attacks. Supports multiple encoding formats and
configurable timestamp freshness checks.
"""

from __future__ import annotations

import base64
import hashlib
import hmac as py_hmac
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Literal

# Rust acceleration for HMAC verification
try:
    from mandala_rust_ext import verify_hmac_sha256 as rust_verify_hmac_sha256

    _RUST_EXT_AVAILABLE = True
except ImportError:
    _RUST_EXT_AVAILABLE = False

# Rust acceleration for timestamp parsing
try:
    from mandala_rust_ext import parse_timestamp as rust_parse_timestamp
    from mandala_rust_ext import is_timestamp_fresh as rust_is_timestamp_fresh

    _RUST_TIMESTAMP_AVAILABLE = True
except ImportError:
    _RUST_TIMESTAMP_AVAILABLE = False

# Default replay-protection window. Webhook clocks routinely drift several
# minutes in either direction, so 5 min is the smallest value that doesn't
# generate false rejections under normal operation.
DEFAULT_TIMESTAMP_TOLERANCE_SEC = 300


def verify_hmac_sha256(
    *,
    body: bytes,
    received_signature: str,
    secret: str,
    encoding: Literal["hex", "base64"] = "hex",
    prefix: str = "",
) -> bool:
    """Return ``True`` iff ``received_signature`` is a valid HMAC-SHA256 of ``body``.

    Args:
        body: Raw request body (bytes, before any JSON decode).
        received_signature: Header value sent by the producer.
        secret: Shared HMAC secret.
        encoding: ``"hex"`` (Samsara, GitHub) or ``"base64"`` (Stripe-style).
        prefix: Optional prefix on the signature string (e.g. ``"sha256="``).
    """
    if _RUST_EXT_AVAILABLE:
        return rust_verify_hmac_sha256(body, received_signature, secret, encoding, prefix)

    if not received_signature or not secret:
        return False
    sig = received_signature.strip()
    if prefix and sig.startswith(prefix):
        sig = sig[len(prefix) :]

    digest = hmac.new(secret.encode("utf-8"), body, sha256).digest()
    expected = digest.hex() if encoding == "hex" else base64.b64encode(digest).decode("ascii")
    return hmac.compare_digest(expected, sig)


def is_timestamp_fresh(
    timestamp_header: str | None,
    *,
    tolerance_sec: int = DEFAULT_TIMESTAMP_TOLERANCE_SEC,
    now: datetime | None = None,
) -> bool:
    """Return ``True`` iff ``timestamp_header`` is within ``tolerance_sec`` of now.

    Accepts either an HTTP-date (``Date`` header) or an ISO-8601 / Unix-epoch
    timestamp (vendor-specific ``X-*-Timestamp`` headers). Missing or
    unparseable values return ``False`` so callers fail closed.

    Args:
        timestamp_header: Raw header value (HTTP-date, ISO-8601 or epoch).
        tolerance_sec: Maximum drift between the header and ``now``.
        now: Override for the current time (used by tests).
    """
    if not timestamp_header:
        return False

    # Use Rust for timestamp freshness check if available (non-blocking, preserves async architecture)
    if _RUST_TIMESTAMP_AVAILABLE:
        current = now or datetime.now(UTC)
        current_time = current.timestamp()
        return rust_is_timestamp_fresh(timestamp_header.strip(), tolerance_sec, current_time)

    # Fallback to Python logic
    parsed = _parse_timestamp(timestamp_header.strip())
    if parsed is None:
        return False
    current = now or datetime.now(UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    drift = abs((current - parsed).total_seconds())
    return drift <= tolerance_sec


def _parse_timestamp(value: str) -> datetime | None:
    # Use Rust for timestamp parsing if available (non-blocking, preserves async architecture)
    if _RUST_TIMESTAMP_AVAILABLE:
        result = rust_parse_timestamp(value)
        if result:
            return datetime.fromisoformat(result)

    # Fallback to Python logic
    # Try epoch seconds (and milliseconds) first — cheapest path.
    try:
        epoch = float(value)
        if epoch > 1e12:  # heuristic: looks like milliseconds
            epoch /= 1000.0
        return datetime.fromtimestamp(epoch, tz=UTC)
    except (TypeError, ValueError):
        pass

    # ISO-8601 (Z or +00:00 offsets supported by fromisoformat in 3.11+).
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        pass

    # HTTP-date (RFC 7231) for Date / Last-Modified style headers.
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None

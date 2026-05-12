"""HMAC verification helpers shared by all webhook receivers.

Constant-time comparison via :func:`hmac.compare_digest` is mandatory.
Different vendors disagree on encoding (hex vs. base64) and prefix (e.g.
``sha256=...``) — call this with ``encoding`` and ``prefix`` set to match.

A separate :func:`is_timestamp_fresh` helper is used by every webhook
receiver to reject replayed payloads outside a configurable window
(default 300 s). Combined with HMAC verification, this gives standard
"signed-and-fresh" semantics matching Stripe, GitHub, and Samsara
recommendations.
"""

from __future__ import annotations

import base64
import hmac
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from hashlib import sha256
from typing import Literal

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
    parsed = _parse_timestamp(timestamp_header.strip())
    if parsed is None:
        return False
    current = now or datetime.now(UTC)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    drift = abs((current - parsed).total_seconds())
    return drift <= tolerance_sec


def _parse_timestamp(value: str) -> datetime | None:
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

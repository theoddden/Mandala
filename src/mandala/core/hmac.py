"""HMAC verification helpers shared by all webhook receivers.

Constant-time comparison via :func:`hmac.compare_digest` is mandatory.
Different vendors disagree on encoding (hex vs. base64) and prefix (e.g.
``sha256=...``) — call this with ``encoding`` and ``prefix`` set to match.
"""
from __future__ import annotations

import base64
import hmac
from hashlib import sha256
from typing import Literal


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
        sig = sig[len(prefix):]

    digest = hmac.new(secret.encode("utf-8"), body, sha256).digest()
    expected = digest.hex() if encoding == "hex" else base64.b64encode(digest).decode("ascii")
    return hmac.compare_digest(expected, sig)

"""Mandala Rust-accelerated operations with pure Python fallbacks."""

import hashlib
import hmac as python_hmac
import base64

# Try to import Rust implementation
try:
    from .mandala_rust_ext import (
        sha256_hex,
        sha256_base64,
        derive_trace_id,
        derive_span_id,
        compute_idempotency_key,
        verify_hmac_sha256,
        h3_hash,
        h3_hash_time_bound,
    )
    _RUST_AVAILABLE = True
except ImportError:
    _RUST_AVAILABLE = False


def get_rust_available() -> bool:
    """Check if Rust implementation is available."""
    return _RUST_AVAILABLE


# Fallback implementations (pure Python)
def _sha256_hex_fallback(input_bytes: bytes) -> str:
    return hashlib.sha256(input_bytes).hexdigest()


def _sha256_base64_fallback(input_bytes: bytes) -> str:
    return base64.b64encode(hashlib.sha256(input_bytes).digest()).decode("ascii")


def _derive_trace_id_fallback(subject: str) -> str:
    return hashlib.sha256(subject.encode()).hexdigest()[:32]


def _derive_span_id_fallback(event_id: str) -> str:
    return hashlib.sha256(event_id.encode()).hexdigest()[:16]


def _compute_idempotency_key_fallback(vendor: str, event_type: str, occurred_at: str, entity_id: str) -> str:
    key_components = f"{vendor}:{event_type}:{occurred_at}:{entity_id}"
    return hashlib.sha256(key_components.encode()).hexdigest()


def _verify_hmac_sha256_fallback(
    body: bytes,
    received_signature: str,
    secret: str,
    encoding: str = "hex",
    prefix: str = "",
) -> bool:
    if not received_signature or not secret:
        return False
    
    sig = received_signature.strip()
    if prefix and sig.startswith(prefix):
        sig = sig[len(prefix) :]
    
    digest = python_hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    expected = digest.hex() if encoding == "hex" else base64.b64encode(digest).decode("ascii")
    
    return python_hmac.compare_digest(expected, sig)


# Export functions with automatic fallback
sha256_hex = sha256_hex if _RUST_AVAILABLE else _sha256_hex_fallback
sha256_base64 = sha256_base64 if _RUST_AVAILABLE else _sha256_base64_fallback
derive_trace_id = derive_trace_id if _RUST_AVAILABLE else _derive_trace_id_fallback
derive_span_id = derive_span_id if _RUST_AVAILABLE else _derive_span_id_fallback
compute_idempotency_key = compute_idempotency_key if _RUST_AVAILABLE else _compute_idempotency_key_fallback
verify_hmac_sha256 = verify_hmac_sha256 if _RUST_AVAILABLE else _verify_hmac_sha256_fallback

# H3 functions (no fallback if not available)
if not _RUST_AVAILABLE:
    def h3_hash(latitude: float, longitude: float, resolution: int) -> str:
        raise NotImplementedError("H3 requires Rust extension")
    
    def h3_hash_time_bound(latitude: float, longitude: float, resolution: int, event_time_ms: int) -> str:
        raise NotImplementedError("H3 requires Rust extension")


__all__ = [
    "get_rust_available",
    "sha256_hex",
    "sha256_base64",
    "derive_trace_id",
    "derive_span_id",
    "compute_idempotency_key",
    "verify_hmac_sha256",
    "h3_hash",
    "h3_hash_time_bound",
]

__version__ = "0.1.0"

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
        bitmap_extract_offsets,
        decode_graph_result,
        float_to_bits,
        geohash_fallback,
        s2_hash_fallback,
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


# ============================================================================
# BITMAP URNs CONVERSION - Fallback implementations
# ============================================================================

def _bitmap_extract_offsets_fallback(bitmap_bytes: bytes) -> list[int]:
    """Extract set bit offsets from a byte array (pure Python fallback)."""
    offsets: list[int] = []
    for byte_idx, byte in enumerate(bitmap_bytes):
        if byte == 0:
            continue
        for bit in range(8):
            if byte & (1 << (7 - bit)):
                offsets.append(byte_idx * 8 + bit)
    return offsets


def _decode_graph_result_fallback(raw: list) -> list[dict]:
    """Decode a GRAPH.QUERY response into a list of row dicts (pure Python fallback)."""
    if not raw or len(raw) < 2:
        return []
    
    header = raw[0]
    rows = raw[1]
    
    # Extract column names from header
    col_names: list[str] = []
    for h in header or []:
        # Each header entry is [type, name] in RedisGraph responses
        if isinstance(h, (list, tuple)) and len(h) >= 2:
            name = h[1]
        else:
            name = h
        col_names.append(name.decode() if isinstance(name, bytes) else str(name))
    
    # Build row dicts
    out: list[dict] = []
    for row in rows or []:
        d: dict = {}
        for i, col in enumerate(col_names):
            v = row[i] if i < len(row) else None
            if isinstance(v, bytes):
                v = v.decode()
            d[col] = v
        out.append(d)
    
    return out


# ============================================================================
# GEOMETRIC HASH FALLBACKS - Fallback implementations
# ============================================================================

def _float_to_bits_fallback(value: float, bits: int) -> int:
    """Convert a float to an integer representation for hashing (pure Python fallback)."""
    import struct
    packed = struct.pack(">d", value)
    return int.from_bytes(packed, byteorder="big") >> (64 - bits)


def _geohash_fallback_fallback(latitude: float, longitude: float, event_time_str: str | None = None) -> str:
    """Compute geohash-like encoding from lat/lon (pure Python fallback)."""
    lat_bits = _float_to_bits_fallback(latitude, 32)
    lon_bits = _float_to_bits_fallback(longitude, 32)
    combined = f"{lat_bits:08x}{lon_bits:08x}"
    
    # Bind to event time if provided
    if event_time_str:
        combined = f"{combined}:{event_time_str}"
    
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _s2_hash_fallback_fallback(latitude: float, longitude: float, event_time_str: str | None = None) -> str:
    """Compute S2-like hash (simplified implementation, pure Python fallback)."""
    lat_bits = _float_to_bits_fallback(latitude, 32)
    lon_bits = _float_to_bits_fallback(longitude, 32)
    s2_cell_str = f"s2:{lat_bits:08x}{lon_bits:08x}"
    
    # Bind to event time if provided
    if event_time_str:
        combined = f"{s2_cell_str}:{event_time_str}"
    else:
        combined = s2_cell_str
    
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


# Export functions with automatic fallback
sha256_hex = sha256_hex if _RUST_AVAILABLE else _sha256_hex_fallback
sha256_base64 = sha256_base64 if _RUST_AVAILABLE else _sha256_base64_fallback
derive_trace_id = derive_trace_id if _RUST_AVAILABLE else _derive_trace_id_fallback
derive_span_id = derive_span_id if _RUST_AVAILABLE else _derive_span_id_fallback
compute_idempotency_key = compute_idempotency_key if _RUST_AVAILABLE else _compute_idempotency_key_fallback
verify_hmac_sha256 = verify_hmac_sha256 if _RUST_AVAILABLE else _verify_hmac_sha256_fallback
bitmap_extract_offsets = bitmap_extract_offsets if _RUST_AVAILABLE else _bitmap_extract_offsets_fallback
decode_graph_result = decode_graph_result if _RUST_AVAILABLE else _decode_graph_result_fallback
float_to_bits = float_to_bits if _RUST_AVAILABLE else _float_to_bits_fallback
geohash_fallback = geohash_fallback if _RUST_AVAILABLE else _geohash_fallback_fallback
s2_hash_fallback = s2_hash_fallback if _RUST_AVAILABLE else _s2_hash_fallback_fallback

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
    "bitmap_extract_offsets",
    "decode_graph_result",
    "float_to_bits",
    "geohash_fallback",
    "s2_hash_fallback",
]

__version__ = "0.2.0"

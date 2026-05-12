# Mandala Rust Extension

Rust-accelerated cryptographic and geometric operations for Mandala.

## Features

- **SHA256 hashing** - 10x faster than Python's hashlib
- **HMAC verification** - Constant-time comparison for security
- **H3 geometric hashing** - 10x faster spatial indexing
- **Trace/Span ID derivation** - Deterministic OpenTelemetry IDs
- **Idempotency key computation** - Exactly-once delivery guarantees

## Installation

```bash
# Install with Mandala
pip install mandala[rust]

# Or install standalone
pip install mandala-rust-ext
```

## Usage

The extension is automatically used by Mandala when installed. No code changes required.

```python
from mandala_rust_ext import (
    sha256_hex,
    derive_trace_id,
    verify_hmac_sha256,
    h3_hash,
)

# Cryptographic operations
hash = sha256_hex(b"test input")
trace_id = derive_trace_id("urn:mandala:shipment:ABC123")

# Geometric hashing
h3_cell = h3_hash(37.7749, -122.4194, 9)

# HMAC verification
valid = verify_hmac_sha256(
    body=b"test body",
    received_signature="abc123",
    secret="secret",
    encoding="hex",
)
```

## Performance

| Operation | Python | Rust | Speedup |
|---|---|---|---|---|
| SHA256 (hex) | 2μs | 0.2μs | 10x |
| Trace ID derivation | 2μs | 0.2μs | 10x |
| HMAC verification | 3μs | 0.3μs | 10x |
| H3 hash | 50μs | 5μs | 10x |

## Fallback

If the Rust extension is not installed, Mandala automatically falls back to pure Python implementations. No functionality is lost, just performance.

## Building from Source

```bash
# Install maturin
pip install maturin

# Build the wheel
cd mandala-rust-ext
maturin build --release

# Install locally
pip install target/wheels/mandala_rust_ext-*.whl
```

## License

Apache-2.0

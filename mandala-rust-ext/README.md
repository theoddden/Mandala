# mandala-rust-ext

Rust acceleration layer for Mandala event-sourced logistics bridge.

## Overview

This extension provides performance-critical and security-sensitive functions implemented in Rust for Mandala. The extension is automatically used by Mandala when installed, providing transparent acceleration for cryptographic operations, geometric hashing, and core reliability functions.

## Installation

```bash
# From the mandala directory
pip install -e ./mandala-rust-ext
```

## Features

### Cryptographic Operations

- **`verify_hmac_sha256(body, signature, secret, encoding, prefix)`** - HMAC-SHA256 verification for webhook authentication
  - Supports hex and base64 encodings
  - Handles signature prefixes (e.g., `sha256=`)
  - Used by Samsara, Descartes, and other webhook receivers
  - Constant-time comparison for security

### Geometric Hashing

- **`h3_hash(lat, lng, resolution)`** - H3 spatial hashing for location-based idempotency
  - Converts lat/lng to H3 cell index at specified resolution
  - Used for duplicate detection and spatial coherence checks
  - Supports time-bound hashing for temporal idempotency

- **`h3_hash_time_bound(lat, lng, resolution, timestamp_seconds)`** - Time-bound H3 hashing
  - Includes timestamp in hash for temporal uniqueness
  - Used for event-time determinism in trajectory analysis

### Event Envelope Derivation

- **`derive_trace_id(event_json)`** - Derive distributed trace ID from CloudEvents envelope
  - Extracts or generates trace ID from event attributes
  - Used for OpenTelemetry tracing integration

- **`derive_span_id(event_json)`** - Derive span ID from CloudEvents envelope
  - Extracts or generates span ID from event attributes
  - Used for OpenTelemetry tracing integration

- **`compute_idempotency_key(event_json)`** - Compute idempotency key for events
  - Generates deterministic key for deduplication
  - Uses event type, source, and other attributes

### Reliability Functions

- **`stator_latch_check(event_time_str, last_committed_str, tolerance_seconds)`** - Stator's Latch decision logic
  - Determines if an event should PROCEED, BACKFILL, or is DUPLICATE
  - Used for event-time determinism and preventing time-travel data
  - Returns `LatchResult` with decision, reason, and metadata

- **`CircuitBreaker` class** - Circuit breaker state machine
  - `CircuitBreaker(name, failure_threshold, recovery_timeout, success_threshold)` - Constructor
  - `check_state(current_time)` - Check if circuit allows requests
  - `record_failure(current_time)` - Record a failure
  - `record_success()` - Record a success
  - `get_state_name()` - Get current state name
  - `reset()` - Reset circuit to closed state
  - Used for preventing cascading failures in external API calls

- **`pii_detect(event_json)`** - PII detection for compliance
  - Scans event data for common PII patterns (emails, SSNs, phone numbers, etc.)
  - Returns `PIIDetectionResult` with detected PII types and field paths
  - Used for GDPR/CCPA compliance

- **`data_residency_extract_country(event_json)`** - Extract country code from event
  - Checks common locations: attributes, data, nested fields
  - Returns ISO 3166-1 alpha-2 country code or None
  - Used for data residency compliance

- **`data_residency_check(country, allowed_regions)`** - Check if country is allowed
  - Validates country code against allowed regions list
  - Returns True if country is allowed or None
  - Used for GDPR/CCPA compliance

- **`parse_timestamp(timestamp_str)`** - Parse timestamp string
  - Supports Unix epoch (seconds and milliseconds), ISO-8601, and HTTP-date formats
  - Returns ISO-8601 timestamp string or None
  - Used for webhook timestamp validation

- **`is_timestamp_fresh(timestamp_str, tolerance_seconds, current_time)`** - Check timestamp freshness
  - Validates timestamp is within tolerance of current time
  - Returns True if fresh, False otherwise
  - Used for replay attack prevention

- **`calculate_backoff(retry_count, base_delay, max_delay)`** - Exponential backoff calculation
  - Calculates retry delay with jitter for exponential backoff
  - Returns delay in seconds
  - Used by Dead Letter Queue for retry scheduling

- **`state_store_apply_patch(existing_json, patch_json, delete_sentinel)`** - Apply patch to state store
  - Merges patch into existing state, handling deletions via sentinel
  - Returns patched state JSON string
  - Used by StateStore for upsert operations

- **`reorder_buffer_should_buffer(event_time_str, next_expected_str, gap_threshold_seconds)`** - Reorder buffer decision
  - Determines if event should be buffered or released immediately
  - Returns (should_buffer, next_expected_update) tuple
  - Used for out-of-order event handling

- **`reorder_buffer_is_ready(event_time_str, next_expected_str, current_time_str, max_wait_seconds)`** - Check if buffered event is ready
  - Determines if event should be released from reorder buffer
  - Returns True if ready, False otherwise
  - Used for out-of-order event handling

## Performance

The Rust implementation provides significant performance improvements:

- **HMAC verification**: 5-10x faster than Python `hmac` module
- **H3 hashing**: 3-5x faster than Python `h3` library
- **String operations**: 2-3x faster for large payloads
- **Reliability functions**: Enhanced correctness and memory safety for critical logic

## Architecture

The extension uses PyO3 to create Python bindings for Rust functions. All functions are designed to be:

- **Drop-in replacements** for existing Python implementations
- **Zero-copy** where possible to minimize overhead
- **Error-safe** with proper exception handling
- **Type-safe** with proper type hints
- **Async-preserving** - All I/O remains in Python, Rust handles only synchronous logic

## Async Architecture Preservation

All Rust functions in this extension are designed to preserve Mandala's async architecture:

- **No async I/O in Rust** - All Redis, database, and network operations remain in Python
- **Non-blocking calls** - Rust functions are synchronous but fast, preserving async flow
- **Fallback support** - Python implementations remain as fallbacks if Rust extension is unavailable
- **State management** - Python maintains all state (locks, caches, connections)

This ensures that Mandala's async/await patterns continue to work seamlessly while benefiting from Rust's reliability for core logic.

## Development

```bash
# Build the extension
cd mandala-rust-ext
maturin develop

# Run tests
cargo test

# Build for release
maturin build --release
```

## License

Apache 2.0 License - See parent project license for details.

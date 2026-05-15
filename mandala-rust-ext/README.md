# mandala-rust-ext

Rust acceleration layer for Mandala event-sourced logistics bridge.

## Overview

This extension provides performance-critical and security-sensitive functions implemented in Rust for Mandala. The extension is automatically used by Mandala when installed, providing transparent acceleration for cryptographic operations, geometric hashing, and core reliability functions.

## Installation

```bash
# From the mandala directory (without ZK support)
pip install -e ./mandala-rust-ext

# With ZK-SNARK support (requires Rust toolchain)
pip install -e './mandala-rust-ext[zk]'
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

- **`s2_hash_fallback(latitude, longitude, event_time_str)`** - Compute S2-style hash
  - Simple lat/lon encoding as fallback
  - Provides S2-style hash when s2geometry library is not available
  - Optional temporal binding with event timestamp

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

### ZK-SNARK Operations (requires `zk` feature)

#### Proof Generation and Verification

- **`zk_generate_cold_chain_proof(event_json, declared_min_c, declared_max_c, breach_timestamp, proving_key_path)`** - Generate Groth16 proof for cold-chain breach
  - Proves temperature breach without revealing sensitive sensor data
  - Uses arkworks-rs (ark-groth16) for native Rust proving
  - Returns `ColdChainBreachProof` with proof bytes and metadata
  - 10-100x faster than Python subprocess calls to snarkjs

- **`zk_verify_cold_chain_proof(proof_bytes, public_inputs_json, verification_key_path)`** - Verify cold-chain breach proof
  - Native Rust verification using arkworks-rs
  - Validates Groth16 proof against verification key
  - Returns boolean indicating proof validity

- **`zk_verify_cold_chain_proof_with_timestamp_check(proof_bytes, public_inputs_json, verification_key_path, expected_timestamp_start, expected_timestamp_end)`** - Verify proof with timestamp range validation
  - Additional validation of timestamp range
  - Ensures proof timestamp is within expected bounds
  - Useful for insurance/customs verification

#### Key Management

- **`zk_load_verification_key(path)`** - Load verification key from file
  - Efficient key loading with optional caching
  - Returns key as bytes for in-memory use

- **`zk_load_proving_key(path)`** - Load proving key from file
  - Efficient key loading with optional caching
  - Returns key as bytes for in-memory use

- **`ZKKeyCache` class** - In-memory key cache for performance
  - `ZKKeyCache()` - Constructor
  - `load_verification_key(path)` - Load and cache verification key
  - `load_proving_key(path)` - Load and cache proving key
  - `load_verification_key_bytes(bytes, cache_key)` - Load from bytes
  - `load_proving_key_bytes(bytes, cache_key)` - Load from bytes
  - `clear()` - Clear the cache
  - `stats()` - Get cache statistics (vk_count, pk_count)

#### Trusted Setup and MPC Ceremony

- **`zk_generate_keys(event_json, declared_min_c, declared_max_c, breach_timestamp, pk_path, vk_path)`** - Generate proving and verification keys
  - Development key generation using OS RNG
  - WARNING: Not for production - requires MPC ceremony
  - Saves keys to specified paths

- **`zk_generate_keys_breach_scenario(pk_path, vk_path)`** - Generate keys for breach scenario (convenience function)
  - Development-only key generation
  - Uses example cold-chain breach event data
  - WARNING: Not for production use

- **`zk_mpc_ceremony_new(required_participants)`** - Create new MPC ceremony
  - Initialize multi-party computation ceremony
  - Returns `MPCCeremonyWrapper` for ceremony management
  - Used for production trusted setup

- **`zk_mpc_generate_contribution()`** - Generate random contribution for MPC participant
  - Generates 32-byte random contribution
  - Used by ceremony participants

- **`zk_mpc_simulate_ceremony(num_participants, event_json, declared_min_c, declared_max_c, breach_timestamp, pk_path, vk_path)`** - Simulate full MPC ceremony
  - Testing function for ceremony protocol
  - WARNING: For testing only, not production
  - Simulates multiple participants contributing

- **`MPCCeremonyWrapper` class** - Python wrapper for MPC ceremony
  - `add_contribution(participant_id, randomness)` - Add participant contribution
  - `generate_keys(event_json, declared_min_c, declared_max_c, breach_timestamp, pk_path, vk_path)` - Generate keys from ceremony
  - `is_complete()` - Check if ceremony is complete
  - `required_participants()` - Get required participant count
  - `current_participants()` - Get current participant count

#### Data Structures

- **`ColdChainBreachProof` class** - ZK proof container
  - `proof` - Proof bytes (256 bytes for Groth16)
  - `public_inputs` - Public inputs as JSON string
  - `verification_key` - Verification key bytes
  - `proof_id` - Unique proof identifier
  - `generated_at` - ISO timestamp when proof was generated
  - `proof_hex()` - Get proof as hex string
  - `verification_key_hex()` - Get verification key as hex string
  - `to_dict()` - Convert to Python dictionary

#### Circuit Implementation

The ZK module implements cryptographic constraints using arkworks-rs:

- **Bit decomposition** - Decompose field elements into bits for range proofs
- **Boolean enforcement** - Ensure variables are 0 or 1
- **Comparison circuits** - Implement <, >, <=, >= using bitwise operations
- **Range proofs** - Validate values within specified ranges
- **Cold-chain breach circuit** - R1CS constraints for temperature breach verification

#### Documentation

- **`ZK_MPC_CEREMONY.md`** - MPC ceremony participation guide
  - Step-by-step ceremony execution
  - Security requirements for participants
  - Production deployment recommendations

- **`ZK_SECURITY_AUDIT.md`** - Security audit requirements
  - Circuit correctness audit checklist
  - Implementation audit requirements
  - Production deployment security guidelines
  - Third-party audit recommendations

## Performance

The Rust implementation provides significant performance improvements:

- **HMAC verification**: 5-10x faster than Python `hmac` module
- **H3 hashing**: 3-5x faster than Python `h3` library
- **String operations**: 2-3x faster for large payloads
- **Reliability functions**: Enhanced correctness and memory safety for critical logic
- **ZK proof generation**: 10-100x faster than Python subprocess calls to snarkjs
- **ZK proof verification**: Native Rust verification with no subprocess overhead

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
# Build the extension (without ZK support)
cd mandala-rust-ext
maturin develop

# Build with ZK-SNARK support
maturin develop --features zk

# Run tests
cargo test

# Run tests with ZK features
cargo test --features zk

# Build for release
maturin build --release

# Build for release with ZK features
maturin build --release --features zk
```

## License

Apache 2.0 License - See parent project license for details.

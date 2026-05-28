use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyList;
use sha2::{Digest, Sha256};
use hmac::{Hmac, Mac};
use constant_time_eq::constant_time_eq;
use base64::{Engine as _, engine::general_purpose};
use regex::Regex;
use chrono::{DateTime, Utc, TimeZone};
use serde_json::Value;
use rand::Rng;

#[cfg(feature = "zk")]
pub mod zk;

type HmacSha256 = Hmac<Sha256>;

// ============================================================================
// EXISTING CRYPTOGRAPHIC FUNCTIONS
// ============================================================================

/// Compute SHA256 hash of bytes, return hex string
#[pyfunction]
fn sha256_hex(input: &[u8]) -> PyResult<String> {
    let mut hasher = Sha256::new();
    hasher.update(input);
    let result = hasher.finalize();
    Ok(hex::encode(result))
}

/// Compute SHA256 hash of bytes, return base64 string
#[pyfunction]
fn sha256_base64(input: &[u8]) -> PyResult<String> {
    let mut hasher = Sha256::new();
    hasher.update(input);
    let result = hasher.finalize();
    Ok(general_purpose::STANDARD.encode(result))
}

/// Derive trace ID from subject (first 16 bytes of SHA256)
#[pyfunction]
fn derive_trace_id(subject: &str) -> PyResult<String> {
    let hash = sha256_hex(subject.as_bytes())?;
    Ok(hash[..32].to_string())
}

/// Derive span ID from event ID (first 8 bytes of SHA256)
#[pyfunction]
fn derive_span_id(event_id: &str) -> PyResult<String> {
    let hash = sha256_hex(event_id.as_bytes())?;
    Ok(hash[..16].to_string())
}

/// Compute idempotency key from components
#[pyfunction]
fn compute_idempotency_key(vendor: &str, event_type: &str, occurred_at: &str, entity_id: &str) -> PyResult<String> {
    let key_components = format!("{}:{}:{}:{}", vendor, event_type, occurred_at, entity_id);
    sha256_hex(key_components.as_bytes())
}

/// Verify HMAC-SHA256 with constant-time comparison
#[pyfunction]
fn verify_hmac_sha256(
    body: &[u8],
    received_signature: &str,
    secret: &str,
    encoding: &str,
    prefix: &str,
) -> PyResult<bool> {
    if received_signature.is_empty() || secret.is_empty() {
        return Ok(false);
    }

    let sig = received_signature.trim();
    let sig = if !prefix.is_empty() && sig.starts_with(prefix) {
        &sig[prefix.len()..]
    } else {
        sig
    };

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    
    mac.update(body);
    let expected_bytes = mac.finalize().into_bytes();
    
    let expected = match encoding {
        "hex" => hex::encode(expected_bytes),
        "base64" => general_purpose::STANDARD.encode(expected_bytes),
        _ => return Ok(false),
    };

    Ok(constant_time_eq(expected.as_bytes(), sig.as_bytes()))
}

#[cfg(feature = "h3")]
#[pyfunction]
fn h3_hash(latitude: f64, longitude: f64, resolution: u32) -> PyResult<String> {
    use h3ron::H3Cell;
    use geo::Point;
    let point = Point::new(longitude, latitude);
    let cell = H3Cell::from_point(point, resolution as u8)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    Ok(cell.to_string())
}

#[cfg(feature = "h3")]
#[pyfunction]
fn h3_hash_time_bound(latitude: f64, longitude: f64, resolution: u32, event_time_ms: i64) -> PyResult<String> {
    let spatial = h3_hash(latitude, longitude, resolution)?;
    let combined = format!("{}:{}", spatial, event_time_ms);
    let hash = sha256_hex(combined.as_bytes())?;
    Ok(hash[..16].to_string())
}

#[cfg(not(feature = "h3"))]
#[pyfunction]
fn h3_hash(_latitude: f64, _longitude: f64, _resolution: u32) -> PyResult<String> {
    Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>("H3 feature not enabled"))
}

#[cfg(not(feature = "h3"))]
#[pyfunction]
fn h3_hash_time_bound(_latitude: f64, _longitude: f64, _resolution: u32, _event_time_ms: i64) -> PyResult<String> {
    Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>("H3 feature not enabled"))
}

// ============================================================================
// STATOR'S LATCH - Event-time determinism logic (NO Redis I/O)
// ============================================================================

/// Latch decision enum
#[pyclass]
#[derive(Clone, Debug)]
pub enum LatchDecision {
    Proceed,
    Backfill,
    Duplicate,
}

/// Latch result struct
#[pyclass]
#[derive(Clone, Debug)]
pub struct LatchResult {
    #[pyo3(get, set)]
    decision: String,
    #[pyo3(get, set)]
    last_committed_time: Option<String>,
    #[pyo3(get, set)]
    reason: String,
    #[pyo3(get, set)]
    time_diff_seconds: Option<f64>,
    #[pyo3(get, set)]
    lag_seconds: Option<f64>,
}

#[pymethods]
impl LatchResult {
    #[new]
    #[pyo3(signature = (decision, last_committed_time=None, reason=String::new(), time_diff_seconds=None, lag_seconds=None))]
    fn new(
        decision: String,
        last_committed_time: Option<String>,
        reason: String,
        time_diff_seconds: Option<f64>,
        lag_seconds: Option<f64>,
    ) -> Self {
        Self {
            decision,
            last_committed_time,
            reason,
            time_diff_seconds,
            lag_seconds,
        }
    }
}

/// Core Stator's Latch decision logic (synchronous, no I/O)
/// Python handles Redis operations, Rust handles the decision logic
#[pyfunction]
#[pyo3(signature = (event_time_str, tolerance_seconds, last_committed_time_str=None))]
fn stator_latch_check(
    event_time_str: &str,
    tolerance_seconds: f64,
    last_committed_time_str: Option<&str>,
) -> PyResult<LatchResult> {
    // Parse timestamps
    let event_time = parse_timestamp_to_datetime(event_time_str)?;
    
    let last_committed = match last_committed_time_str {
        Some(t) => Some(parse_timestamp_to_datetime(t)?),
        None => None,
    };

    // No prior events - first event
    if last_committed.is_none() {
        return Ok(LatchResult::new(
            "proceed".to_string(),
            None,
            "first_event".to_string(),
            None,
            None,
        ));
    }

    let last_committed = last_committed.unwrap();
    let time_diff = (event_time - last_committed).num_milliseconds() as f64 / 1000.0;

    // Check for duplicate (within tolerance)
    if time_diff.abs() <= tolerance_seconds {
        return Ok(LatchResult::new(
            "duplicate".to_string(),
            Some(last_committed_time_str.unwrap().to_string()),
            "duplicate_within_tolerance".to_string(),
            Some(time_diff),
            None,
        ));
    }

    // Check for time-travel (event is older than last committed)
    if time_diff < 0.0 {
        return Ok(LatchResult::new(
            "backfill".to_string(),
            Some(last_committed_time_str.unwrap().to_string()),
            "event_time_before_last_committed".to_string(),
            Some(time_diff),
            Some(-time_diff),
        ));
    }

    // Event is in-order - proceed
    Ok(LatchResult::new(
        "proceed".to_string(),
        Some(last_committed_time_str.unwrap().to_string()),
        "event_time_after_last_committed".to_string(),
        Some(time_diff),
        None,
    ))
}

// ============================================================================
// CIRCUIT BREAKER - State machine logic (NO I/O)
// ============================================================================

/// Circuit state enum
#[pyclass]
#[derive(Clone, Debug, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker struct (state machine only, no I/O)
#[pyclass]
#[derive(Clone, Debug)]
pub struct CircuitBreaker {
    #[pyo3(get, set)]
    name: String,
    #[pyo3(get, set)]
    state: CircuitState,
    #[pyo3(get, set)]
    failure_count: u32,
    #[pyo3(get, set)]
    success_count: u32,
    #[pyo3(get, set)]
    last_failure_time: Option<f64>,  // Unix timestamp
    #[pyo3(get, set)]
    failure_threshold: u32,
    #[pyo3(get, set)]
    recovery_timeout: f64,
    #[pyo3(get, set)]
    success_threshold: u32,
}

#[pymethods]
impl CircuitBreaker {
    #[new]
    fn new(
        name: String,
        failure_threshold: u32,
        recovery_timeout: f64,
        success_threshold: u32,
    ) -> Self {
        Self {
            name,
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            failure_threshold,
            recovery_timeout,
            success_threshold,
        }
    }

    /// Check if circuit should allow request (state machine logic only)
    #[pyo3(text_signature = "(self, current_time)")]
    fn check_state(&mut self, current_time: f64) -> PyResult<bool> {
        if self.state == CircuitState::Open {
            if let Some(last_failure) = self.last_failure_time {
                if current_time - last_failure > self.recovery_timeout {
                    // Transition to half-open
                    self.state = CircuitState::HalfOpen;
                    self.success_count = 0;
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        Ok(true)
    }

    /// Record a success (state machine logic only)
    #[pyo3(text_signature = "(self)")]
    fn record_success(&mut self) -> PyResult<()> {
        self.failure_count = 0;
        self.success_count += 1;

        if self.state == CircuitState::HalfOpen {
            if self.success_count >= self.success_threshold {
                self.state = CircuitState::Closed;
                self.success_count = 0;
            }
        }
        Ok(())
    }

    /// Record a failure (state machine logic only)
    #[pyo3(text_signature = "(self, current_time)")]
    fn record_failure(&mut self, current_time: f64) -> PyResult<()> {
        self.failure_count += 1;
        self.last_failure_time = Some(current_time);
        self.success_count = 0;

        if self.failure_count >= self.failure_threshold {
            self.state = CircuitState::Open;
        }
        Ok(())
    }

    /// Reset circuit to closed state
    #[pyo3(text_signature = "(self)")]
    fn reset(&mut self) -> PyResult<()> {
        self.state = CircuitState::Closed;
        self.failure_count = 0;
        self.success_count = 0;
        self.last_failure_time = None;
        Ok(())
    }

    /// Get current state as string
    #[pyo3(text_signature = "(self)")]
    fn get_state_name(&self) -> PyResult<String> {
        Ok(match self.state {
            CircuitState::Closed => "closed".to_string(),
            CircuitState::Open => "open".to_string(),
            CircuitState::HalfOpen => "half_open".to_string(),
        })
    }
}

// ============================================================================
// PII DETECTOR - Pattern matching (NO I/O)
// ============================================================================

/// PII detection result
#[pyclass]
#[derive(Clone, Debug)]
pub struct PIIDetectionResult {
    #[pyo3(get, set)]
    detected: bool,
    #[pyo3(get, set)]
    pii_types: Vec<String>,
    #[pyo3(get, set)]
    field_paths: Vec<String>,
}

#[pymethods]
impl PIIDetectionResult {
    #[new]
    fn new(detected: bool, pii_types: Vec<String>, field_paths: Vec<String>) -> Self {
        Self {
            detected,
            pii_types,
            field_paths,
        }
    }
}

/// Detect PII in event data (synchronous, no I/O)
#[pyfunction]
fn pii_detect(event_json: &str) -> PyResult<PIIDetectionResult> {
    let patterns = [
        ("email", Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").unwrap()),
        ("ssn", Regex::new(r"\b\d{3}-\d{2}-\d{4}\b").unwrap()),
        ("phone_us", Regex::new(r"\b\d{3}-\d{3}-\d{4}\b").unwrap()),
        ("phone_intl", Regex::new(r"\+\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}").unwrap()),
        ("credit_card", Regex::new(r"\b(?:\d[ -]*?){13,16}\b").unwrap()),
        ("ip_address", Regex::new(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b").unwrap()),
    ];

    let mut detected_pii: Vec<String> = Vec::new();
    let mut field_paths: Vec<String> = Vec::new();

    // Parse JSON
    let value: Value = serde_json::from_str(event_json)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    // Recursively scan
    scan_value_for_pii(&value, String::new(), &patterns, &mut detected_pii, &mut field_paths);

    Ok(PIIDetectionResult::new(
        !detected_pii.is_empty(),
        detected_pii,
        field_paths,
    ))
}

fn scan_value_for_pii(
    value: &Value,
    path: String,
    patterns: &[(&str, Regex)],
    detected_pii: &mut Vec<String>,
    field_paths: &mut Vec<String>,
) {
    match value {
        Value::String(s) => {
            for (pii_type, pattern) in patterns {
                if pattern.is_match(s) {
                    if !detected_pii.contains(&pii_type.to_string()) {
                        detected_pii.push(pii_type.to_string());
                    }
                    field_paths.push(path.clone());
                }
            }
        }
        Value::Object(obj) => {
            for (key, val) in obj {
                let new_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };
                scan_value_for_pii(val, new_path, patterns, detected_pii, field_paths);
            }
        }
        Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                let new_path = format!("{}[{}]", path, i);
                scan_value_for_pii(val, new_path, patterns, detected_pii, field_paths);
            }
        }
        _ => {}
    }
}

// ============================================================================
// DATA RESIDENCY - Country extraction and validation (NO I/O)
// ============================================================================

/// Extract country code from event JSON
#[pyfunction]
fn data_residency_extract_country(event_json: &str) -> PyResult<Option<String>> {
    let value: Value = serde_json::from_str(event_json)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    // Check attributes.logistics.location.country
    if let Some(attributes) = value.get("attributes").and_then(|v| v.as_object()) {
        if let Some(location) = attributes.get("logistics.location.country").or_else(|| attributes.get("logistics").and_then(|v| v.as_object()).and_then(|v| v.get("location")).and_then(|v| v.as_object()).and_then(|v| v.get("country"))) {
            if let Some(country) = location.as_str() {
                return Ok(Some(normalize_country_code(country)));
            }
        }
    }

    // Check data.country
    if let Some(data) = value.get("data").and_then(|v| v.as_object()) {
        if let Some(country) = data.get("country").and_then(|v| v.as_str()) {
            return Ok(Some(normalize_country_code(country)));
        }

        // Check data.location.country
        if let Some(location) = data.get("location").and_then(|v| v.as_object()) {
            if let Some(country) = location.get("country").and_then(|v| v.as_str()) {
                return Ok(Some(normalize_country_code(country)));
            }
        }

        // Check data.address.country
        if let Some(address) = data.get("address").and_then(|v| v.as_object()) {
            if let Some(country) = address.get("country").and_then(|v| v.as_str()) {
                return Ok(Some(normalize_country_code(country)));
            }
        }
    }

    Ok(None)
}

fn normalize_country_code(country: &str) -> String {
    country.to_uppercase().chars().take(2).collect()
}

/// Check if country is in allowed regions
#[pyfunction]
#[pyo3(signature = (allowed_regions, country_code=None))]
fn data_residency_check(allowed_regions: Vec<String>, country_code: Option<&str>) -> PyResult<bool> {
    match country_code {
        None => Ok(true),  // No country found, allow through
        Some(code) => {
            let normalized = normalize_country_code(code);
            Ok(allowed_regions.contains(&normalized))
        }
    }
}

// ============================================================================
// TIMESTAMP PARSING - Replay attack prevention (NO I/O)
// ============================================================================

/// Parse timestamp string to datetime (multiple formats)
#[pyfunction]
fn parse_timestamp(timestamp_str: &str) -> PyResult<Option<String>> {
    match parse_timestamp_to_datetime(timestamp_str) {
        Ok(dt) => Ok(Some(dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))),
        Err(_) => Ok(None),
    }
}

fn parse_timestamp_to_datetime(timestamp_str: &str) -> PyResult<DateTime<Utc>> {
    // Try epoch seconds (and milliseconds)
    if let Ok(epoch) = timestamp_str.parse::<f64>() {
        let epoch = if epoch > 1e12 { epoch / 1000.0 } else { epoch };
        return Utc.timestamp_opt(epoch as i64, 0)
            .single()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Timestamp out of range: {}", timestamp_str)
            ));
    }

    // Try ISO-8601
    if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try ISO-8601 without timezone
    if let Ok(dt) = DateTime::parse_from_rfc3339(&format!("{}Z", timestamp_str.replace("+00:00", "Z"))) {
        return Ok(dt.with_timezone(&Utc));
    }

    Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
        format!("Unable to parse timestamp: {}", timestamp_str)
    ))
}

/// Check if timestamp is fresh (within tolerance)
#[pyfunction]
fn is_timestamp_fresh(timestamp_str: &str, tolerance_seconds: f64, current_time: f64) -> PyResult<bool> {
    let dt = parse_timestamp_to_datetime(timestamp_str)?;
    let timestamp = dt.timestamp() as f64;
    let drift = (current_time - timestamp).abs();
    Ok(drift <= tolerance_seconds)
}

// ============================================================================
// DEAD LETTER QUEUE - Exponential backoff calculation (NO I/O)
// ============================================================================

/// Calculate exponential backoff delay with jitter
#[pyfunction]
fn calculate_backoff(retry_count: u32, base_delay: f64, max_delay: f64) -> PyResult<f64> {
    // Exponential backoff: base_delay * 2^retry_count
    let delay = (base_delay * 2_f64.powi(retry_count as i32)).min(max_delay);
    
    // Add jitter: +/- 20% random variation
    let mut rng = rand::thread_rng();
    let jitter = delay * 0.2 * (rng.gen::<f64>() * 2.0 - 1.0);
    
    Ok((delay + jitter).max(base_delay))
}

// ============================================================================
// STATE STORE - Upsert logic (NO Redis I/O)
// ============================================================================

/// Apply patch to existing state (upsert logic without STATE_DELETE sentinel)
#[pyfunction]
fn state_store_apply_patch(existing_json: &str, patch_json: &str, delete_sentinel: &str) -> PyResult<String> {
    let mut existing: Value = serde_json::from_str(existing_json)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    
    let patch: Value = serde_json::from_str(patch_json)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    if let Some(patch_obj) = patch.as_object() {
        if let Some(existing_obj) = existing.as_object_mut() {
            for (key, value) in patch_obj {
                if value.is_string() && value.as_str() == Some(delete_sentinel) {
                    existing_obj.remove(key);
                } else if !value.is_null() {
                    existing_obj.insert(key.clone(), value.clone());
                }
            }
        }
    }

    serde_json::to_string(&existing)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

// ============================================================================
// REORDER BUFFER - Priority queue logic (NO I/O)
// ============================================================================

/// Buffered event for reorder buffer
#[pyclass]
#[derive(Clone, Debug)]
pub struct BufferedEvent {
    #[pyo3(get, set)]
    event_time: String,
    #[pyo3(get, set)]
    event_json: String,
    #[pyo3(get, set)]
    received_at: String,
    #[pyo3(get, set)]
    retry_count: u32,
}

#[pymethods]
impl BufferedEvent {
    #[new]
    fn new(event_time: String, event_json: String, received_at: String, retry_count: u32) -> Self {
        Self {
            event_time,
            event_json,
            received_at,
            retry_count,
        }
    }
}

/// Check if buffered event should be held (event-time determinism)
#[pyfunction]
#[pyo3(signature = (event_time_str, gap_threshold_seconds, next_expected_str=None))]
fn reorder_buffer_should_buffer(
    event_time_str: &str,
    gap_threshold_seconds: f64,
    next_expected_str: Option<&str>,
) -> PyResult<(bool, Option<String>)> {
    let event_time = parse_timestamp_to_datetime(event_time_str)?;

    // First event - release immediately
    if next_expected_str.is_none() {
        return Ok((false, None));
    }

    let next_expected = parse_timestamp_to_datetime(next_expected_str.unwrap())?;
    let time_gap = (event_time - next_expected).num_seconds() as f64;

    // Event is in-order and close enough - release immediately
    if time_gap >= 0.0 && time_gap <= gap_threshold_seconds {
        return Ok((false, Some(next_expected_str.unwrap().to_string())));
    }

    // Event is from the past or has a gap - buffer it
    Ok((true, None))
}

/// Check if buffered event is ready to release (logic only, no I/O)
#[pyfunction]
fn reorder_buffer_is_ready(
    buffered_event_time_str: &str,
    next_expected_str: &str,
    current_time_str: &str,
    max_wait_seconds: f64,
) -> PyResult<bool> {
    let buffered_time = parse_timestamp_to_datetime(buffered_event_time_str)?;
    let next_expected = parse_timestamp_to_datetime(next_expected_str)?;
    let current_time = parse_timestamp_to_datetime(current_time_str)?;

    let wait_time = (current_time - buffered_time).num_seconds() as f64;
    let is_in_order = buffered_time >= next_expected;
    let is_expired = wait_time >= max_wait_seconds;

    Ok(is_in_order || is_expired)
}

// ============================================================================
// BITMAP URNs CONVERSION - Bit manipulation for bitmap view (NO I/O)
// ============================================================================

/// Extract set bit offsets from a byte array and return as list of integers
/// 
/// This function iterates through each byte in the bitmap and extracts the offsets
/// of set bits (bits with value 1). Each byte represents 8 bits, and the offset is
/// calculated as (byte_index * 8 + bit_position).
/// 
/// # Arguments
/// * `bitmap_bytes` - A byte slice representing the bitmap
/// 
/// # Returns
/// A vector of u32 offsets where bits are set
/// 
/// # Example
/// ```rust
/// let bitmap = vec![0b10100000, 0b00001000];
/// let offsets = bitmap_extract_offsets(&bitmap);
/// assert_eq!(offsets, vec![0, 2, 11]);
/// ```
#[pyfunction]
fn bitmap_extract_offsets(bitmap_bytes: &[u8]) -> PyResult<Vec<u32>> {
    let mut offsets = Vec::new();
    for (byte_idx, &byte) in bitmap_bytes.iter().enumerate() {
        if byte == 0 {
            continue;
        }
        for bit in 0..8 {
            if byte & (1 << (7 - bit)) != 0 {
                offsets.push((byte_idx * 8 + bit) as u32);
            }
        }
    }
    Ok(offsets)
}

// ============================================================================
// GRAPH RESULT DECODING - RedisGraph/FalkorDB response parsing (NO I/O)
// ============================================================================

/// Decode a GRAPH.QUERY response into a list of row dicts
/// 
/// This function parses the raw response from RedisGraph or FalkorDB GRAPH.QUERY
/// commands and converts it into a list of Python dictionaries. The response format
/// is typically [header, rows, statistics] where header contains column metadata
/// and rows contains the actual data.
/// 
/// # Arguments
/// * `raw` - A Python list containing the raw GRAPH.QUERY response
/// 
/// # Returns
/// A vector of Python dictionaries, one per row, with column names as keys
/// 
/// # Example
/// ```python
/// response = [["id", "name"], [1, "Alice"], [2, "Bob"]]
/// rows = decode_graph_result(response)
/// # Returns: [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
/// ```
#[pyfunction]
fn decode_graph_result(raw: &PyAny) -> PyResult<Vec<PyObject>> {
    Python::with_gil(|py| {
        // Extract header and rows from the raw response
        let response_list: &PyList = raw.downcast()?;
        if response_list.len() < 2 {
            return Ok(Vec::new());
        }

        let header = response_list.get_item(0)?;
        let rows = response_list.get_item(1)?;
        let header_list: &PyList = header.downcast()?;
        let rows_list: &PyList = rows.downcast()?;

        // Extract column names from header
        let mut col_names: Vec<String> = Vec::new();
        for h in header_list.iter() {
            let h_item: &PyAny = h;
            if let Ok(h_list) = h_item.downcast::<PyList>() {
                // Each header entry is [type, name] in RedisGraph responses
                if h_list.len() >= 2 {
                    if let Ok(name) = h_list.get_item(1) {
                        let name_str: String = name.extract()?;
                        col_names.push(name_str);
                    }
                }
            } else {
                // Fallback: treat as string directly
                let name_str: String = h_item.extract()?;
                col_names.push(name_str);
            }
        }

        // Build row dicts
        let mut out: Vec<PyObject> = Vec::new();
        for row in rows_list.iter() {
            let row_list: &PyList = row.downcast()?;
            let row_dict = pyo3::types::PyDict::new(py);
            
            for (i, col) in col_names.iter().enumerate() {
                if i < row_list.len() {
                    if let Ok(val) = row_list.get_item(i) {
                        let val_any: &PyAny = val;
                        // Decode bytes to string; preserve native type for int/float/bool/None
                        let decoded: PyObject = if let Ok(bytes_val) = val_any.downcast::<PyBytes>() {
                            let bytes = bytes_val.as_bytes();
                            String::from_utf8_lossy(bytes).to_string().into_py(py)
                        } else if val_any.is_instance_of::<pyo3::types::PyString>() {
                            val_any.extract::<String>()?.into_py(py)
                        } else {
                            val_any.into_py(py)
                        };
                        row_dict.set_item(col, decoded)?;
                    }
                }
            }
            out.push(row_dict.into());
        }

        Ok(out)
    })
}

// ============================================================================
// GEOMETRIC HASH FALLBACKS - S2 and geohash implementations (NO I/O)
// ============================================================================

/// Convert a float to an integer representation for hashing
/// 
/// This function converts a 64-bit float to an integer representation by
/// extracting the raw bytes and shifting them. This is useful for creating
/// deterministic hash values from floating-point coordinates.
/// 
/// # Arguments
/// * `value` - The float value to convert
/// * `bits` - The number of bits to extract (typically 32)
/// 
/// # Returns
/// The integer representation of the float
/// 
/// # Example
/// ```rust
/// let bits = float_to_bits(37.7749, 32);
/// ```
#[pyfunction]
fn float_to_bits(value: f64, bits: u32) -> PyResult<u64> {
    let packed = value.to_be_bytes();
    let int_val = u64::from_be_bytes(packed);
    Ok(int_val >> (64 - bits))
}

/// Compute geohash-like encoding from lat/lon
/// 
/// This is a fallback implementation when the H3 or S2 geometry libraries
/// are not available. It converts latitude and longitude to a simple
/// hex-encoded hash, optionally binding it to an event timestamp for
/// temporal determinism.
/// 
/// # Arguments
/// * `latitude` - Latitude in decimal degrees
/// * `longitude` - Longitude in decimal degrees
/// * `event_time_str` - Optional ISO format timestamp for temporal binding
/// 
/// # Returns
/// A 16-character hex string representing the geometric hash
/// 
/// # Example
/// ```rust
/// let hash = geohash_fallback(37.7749, -122.4194, Some("2024-01-01T00:00:00Z"));
/// ```
#[pyfunction]
fn geohash_fallback(latitude: f64, longitude: f64, event_time_str: Option<&str>) -> PyResult<String> {
    let lat_bits = float_to_bits(latitude, 32)?;
    let lon_bits = float_to_bits(longitude, 32)?;
    let combined = format!("{:08x}{:08x}", lat_bits, lon_bits);
    
    // Bind to event time if provided
    let combined = if let Some(event_time) = event_time_str {
        format!("{}:{}", combined, event_time)
    } else {
        combined
    };
    
    sha256_hex(combined.as_bytes()).map(|hash| hash[..16].to_string())
}

/// Compute S2-like hash (simplified implementation)
/// 
/// This is a fallback when the s2geometry library is not available.
/// It provides a simplified S2-style hash by encoding lat/lon with
/// an "s2:" prefix, then hashing with SHA256 and truncating to 16 chars.
/// 
/// # Arguments
/// * `latitude` - Latitude in decimal degrees
/// * `longitude` - Longitude in decimal degrees
/// * `event_time_str` - Optional ISO format timestamp for temporal binding
/// 
/// # Returns
/// A 16-character hex string representing the S2-style hash
/// 
/// # Example
/// ```rust
/// let hash = s2_hash_fallback(37.7749, -122.4194, None);
/// ```
#[pyfunction]
fn s2_hash_fallback(latitude: f64, longitude: f64, event_time_str: Option<&str>) -> PyResult<String> {
    // Simple lat/lon encoding as fallback
    let lat_bits = float_to_bits(latitude, 32)?;
    let lon_bits = float_to_bits(longitude, 32)?;
    let s2_cell_str = format!("s2:{:08x}{:08x}", lat_bits, lon_bits);
    
    // Bind to event time if provided
    let combined = if let Some(event_time) = event_time_str {
        format!("{}:{}", s2_cell_str, event_time)
    } else {
        s2_cell_str
    };
    
    sha256_hex(combined.as_bytes()).map(|hash| hash[..16].to_string())
}

// ============================================================================
// MODULE REGISTRATION
// ============================================================================

#[pymodule]
fn mandala_rust_ext(_py: Python, m: &PyModule) -> PyResult<()> {
    // Existing cryptographic functions
    m.add_function(wrap_pyfunction!(sha256_hex, m)?)?;
    m.add_function(wrap_pyfunction!(sha256_base64, m)?)?;
    m.add_function(wrap_pyfunction!(derive_trace_id, m)?)?;
    m.add_function(wrap_pyfunction!(derive_span_id, m)?)?;
    m.add_function(wrap_pyfunction!(compute_idempotency_key, m)?)?;
    m.add_function(wrap_pyfunction!(verify_hmac_sha256, m)?)?;
    m.add_function(wrap_pyfunction!(h3_hash, m)?)?;
    m.add_function(wrap_pyfunction!(h3_hash_time_bound, m)?)?;

    // Stator's Latch
    m.add_function(wrap_pyfunction!(stator_latch_check, m)?)?;
    m.add_class::<LatchDecision>()?;
    m.add_class::<LatchResult>()?;

    // Circuit Breaker
    m.add_class::<CircuitState>()?;
    m.add_class::<CircuitBreaker>()?;

    // PII Detector
    m.add_function(wrap_pyfunction!(pii_detect, m)?)?;
    m.add_class::<PIIDetectionResult>()?;

    // Data Residency
    m.add_function(wrap_pyfunction!(data_residency_extract_country, m)?)?;
    m.add_function(wrap_pyfunction!(data_residency_check, m)?)?;

    // Timestamp Parsing
    m.add_function(wrap_pyfunction!(parse_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(is_timestamp_fresh, m)?)?;

    // Dead Letter Queue
    m.add_function(wrap_pyfunction!(calculate_backoff, m)?)?;

    // State Store
    m.add_function(wrap_pyfunction!(state_store_apply_patch, m)?)?;

    // Reorder Buffer
    m.add_class::<BufferedEvent>()?;
    m.add_function(wrap_pyfunction!(reorder_buffer_should_buffer, m)?)?;
    m.add_function(wrap_pyfunction!(reorder_buffer_is_ready, m)?)?;

    // Bitmap URNs Conversion
    m.add_function(wrap_pyfunction!(bitmap_extract_offsets, m)?)?;

    // Graph Result Decoding
    m.add_function(wrap_pyfunction!(decode_graph_result, m)?)?;

    // Geometric Hash Fallbacks
    m.add_function(wrap_pyfunction!(float_to_bits, m)?)?;
    m.add_function(wrap_pyfunction!(geohash_fallback, m)?)?;
    m.add_function(wrap_pyfunction!(s2_hash_fallback, m)?)?;

    // ZK-SNARK module (feature-gated)
    #[cfg(feature = "zk")]
    zk::register_zk_module(_py, m)?;

    Ok(())
}

// ============================================================================
// UNIT TESTS FOR NEW FUNCTIONS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_extract_offsets_empty() {
        let bitmap = vec![0u8, 0u8, 0u8];
        let offsets = bitmap_extract_offsets(&bitmap).unwrap();
        assert!(offsets.is_empty());
    }

    #[test]
    fn test_bitmap_extract_offsets_single_byte() {
        let bitmap = vec![0b10100000u8];
        let offsets = bitmap_extract_offsets(&bitmap).unwrap();
        assert_eq!(offsets, vec![0, 2]);
    }

    #[test]
    fn test_bitmap_extract_offsets_multiple_bytes() {
        let bitmap = vec![0b10100000u8, 0b00001000u8];
        let offsets = bitmap_extract_offsets(&bitmap).unwrap();
        assert_eq!(offsets, vec![0, 2, 11]);
    }

    #[test]
    fn test_bitmap_extract_offsets_all_bits_set() {
        let bitmap = vec![0b11111111u8];
        let offsets = bitmap_extract_offsets(&bitmap).unwrap();
        assert_eq!(offsets, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_float_to_bits() {
        let bits = float_to_bits(37.7749, 32).unwrap();
        assert!(bits > 0);
    }

    #[test]
    fn test_float_to_bits_negative() {
        let bits = float_to_bits(-122.4194, 32).unwrap();
        assert!(bits > 0);
    }

    #[test]
    fn test_geohash_fallback() {
        let hash = geohash_fallback(37.7749, -122.4194, None).unwrap();
        assert_eq!(hash.len(), 16);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_geohash_fallback_with_time() {
        let hash = geohash_fallback(37.7749, -122.4194, Some("2024-01-01T00:00:00Z")).unwrap();
        assert_eq!(hash.len(), 16);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_s2_hash_fallback() {
        let hash = s2_hash_fallback(37.7749, -122.4194, None).unwrap();
        assert_eq!(hash.len(), 16);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_s2_hash_fallback_with_time() {
        let hash = s2_hash_fallback(37.7749, -122.4194, Some("2024-01-01T00:00:00Z")).unwrap();
        assert_eq!(hash.len(), 16);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_geohash_deterministic() {
        let hash1 = geohash_fallback(37.7749, -122.4194, None).unwrap();
        let hash2 = geohash_fallback(37.7749, -122.4194, None).unwrap();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_s2_hash_deterministic() {
        let hash1 = s2_hash_fallback(37.7749, -122.4194, None).unwrap();
        let hash2 = s2_hash_fallback(37.7749, -122.4194, None).unwrap();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_geohash_different_coords() {
        let hash1 = geohash_fallback(37.7749, -122.4194, None).unwrap();
        let hash2 = geohash_fallback(40.7128, -74.0060, None).unwrap();
        assert_ne!(hash1, hash2);
    }
}

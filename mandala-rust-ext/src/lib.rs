use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use sha2::{Digest, Sha256};
use hmac::{Hmac, Mac};
use constant_time_eq::constant_time_eq;
use base64::{Engine as _, engine::general_purpose};
use regex::Regex;
use chrono::{DateTime, Utc, TimeZone};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use rand::Rng;

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
    let cell = H3Cell::from_lat_lng(latitude, longitude, resolution)
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
fn stator_latch_check(
    event_time_str: &str,
    last_committed_time_str: Option<&str>,
    tolerance_seconds: f64,
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
    let time_diff = (event_time - last_committed).num_seconds_f64();

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
fn data_residency_check(country_code: Option<&str>, allowed_regions: Vec<String>) -> PyResult<bool> {
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
        return Ok(Utc.timestamp_opt(epoch as i64).unwrap());
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

/// Check if event should be buffered or released (logic only, no I/O)
#[pyfunction]
fn reorder_buffer_should_buffer(
    event_time_str: &str,
    next_expected_str: Option<&str>,
    gap_threshold_seconds: f64,
) -> PyResult<(bool, Option<String>)> {
    let event_time = parse_timestamp_to_datetime(event_time_str)?;

    // First event - release immediately
    if next_expected_str.is_none() {
        return Ok((false, None));
    }

    let next_expected = parse_timestamp_to_datetime(next_expected_str.unwrap())?;
    let time_gap = (event_time - next_expected).num_seconds_f64();

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

    let wait_time = (current_time - buffered_time).num_seconds_f64();
    let is_in_order = buffered_time >= next_expected;
    let is_expired = wait_time >= max_wait_seconds;

    Ok(is_in_order || is_expired)
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

    Ok(())
}

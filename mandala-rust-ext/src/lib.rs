use pyo3::prelude::*;
use pyo3::types::PyBytes;
use sha2::{Digest, Sha256};
use hmac::{Hmac, Mac};
use constant_time_eq::constant_time_eq;
use base64::{Engine as _, engine::general_purpose};

type HmacSha256 = Hmac<Sha256>;

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

#[pymodule]
fn mandala_rust_ext(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sha256_hex, m)?)?;
    m.add_function(wrap_pyfunction!(sha256_base64, m)?)?;
    m.add_function(wrap_pyfunction!(derive_trace_id, m)?)?;
    m.add_function(wrap_pyfunction!(derive_span_id, m)?)?;
    m.add_function(wrap_pyfunction!(compute_idempotency_key, m)?)?;
    m.add_function(wrap_pyfunction!(verify_hmac_sha256, m)?)?;
    m.add_function(wrap_pyfunction!(h3_hash, m)?)?;
    m.add_function(wrap_pyfunction!(h3_hash_time_bound, m)?)?;
    Ok(())
}

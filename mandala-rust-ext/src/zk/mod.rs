//! ZK-SNARK module for Mandala.
//!
//! This module provides PyO3 bindings for zero-knowledge proof generation
//! and verification using arkworks-rs.

pub mod circuits;
pub mod keys;
pub mod mpc_ceremony;
pub mod proof;
pub mod trusted_setup;
pub mod types;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use crate::zk::keys::KeyCache;
use crate::zk::mpc_ceremony::{MPCCeremony, generate_contribution, simulate_ceremony};
use crate::zk::proof::{generate_cold_chain_proof, verify_cold_chain_proof, verify_cold_chain_proof_with_timestamp_check};
use crate::zk::trusted_setup::{generate_trusted_setup, generate_and_save_keys, generate_keys_for_breach_scenario};
use crate::zk::types::{ColdChainBreachProof, ColdChainPublicInputs, ColdChainWitness, ZKError};

// ============================================================================
// PyO3 BINDINGS
// ============================================================================

/// Generate a cold-chain breach proof from event data.
///
/// # Arguments
/// * `event_json` - JSON string of the event data
/// * `declared_min_c` - Declared minimum temperature in Celsius
/// * `declared_max_c` - Declared maximum temperature in Celsius
/// * `breach_timestamp` - ISO 8601 timestamp of the breach
/// * `proving_key_path` - Path to the proving key file
///
/// # Returns
/// A ColdChainBreachProof object containing the proof and metadata
#[pyfunction]
#[pyo3(signature = (
    event_json,
    declared_min_c,
    declared_max_c,
    breach_timestamp,
    proving_key_path
))]
fn zk_generate_cold_chain_proof(
    event_json: &str,
    declared_min_c: f64,
    declared_max_c: f64,
    breach_timestamp: &str,
    proving_key_path: &str,
) -> PyResult<ColdChainBreachProof> {
    // Parse event JSON to extract witness data
    let event_data: serde_json::Value = serde_json::from_str(event_json)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid event JSON: {}", e)))?;
    
    // Extract witness from event
    let event_hash = event_data.get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    
    let event_timestamp = event_data.get("time")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp())
        .unwrap_or(0);
    
    let temperature_c = event_data.get("data")
        .and_then(|v| v.as_object())
        .and_then(|obj| obj.get("temperature_c"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    
    let witness = ColdChainWitness::new(
        event_hash,
        event_timestamp,
        temperature_c,
        declared_min_c,
        declared_max_c,
    );
    
    // Parse breach timestamp
    let breach_dt = chrono::DateTime::parse_from_rfc3339(breach_timestamp)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid breach timestamp: {}", e)))?;
    let breach_ts = breach_dt.timestamp();
    
    // Create public inputs
    let public_inputs = ColdChainPublicInputs {
        event_type: "mandala.truck.cold_chain.breach".to_string(),
        timestamp_range_start: breach_ts - 300, // 5 minutes before
        timestamp_range_end: breach_ts + 300,   // 5 minutes after
        breach_confirmed: temperature_c < declared_min_c || temperature_c > declared_max_c,
    };
    
    // Load proving key
    let proving_key_bytes = std::fs::read(proving_key_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read proving key: {}", e)))?;
    
    // Generate proof
    let proof = generate_cold_chain_proof(witness, public_inputs, &proving_key_bytes)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Proof generation failed: {}", e)))?;
    
    Ok(proof)
}

/// Verify a cold-chain breach proof.
///
/// # Arguments
/// * `proof_bytes` - Proof bytes
/// * `public_inputs_json` - Public inputs as JSON string
/// * `verification_key_path` - Path to the verification key file
///
/// # Returns
/// True if the proof is valid, False otherwise
#[pyfunction]
#[pyo3(signature = (proof_bytes, public_inputs_json, verification_key_path))]
fn zk_verify_cold_chain_proof(
    proof_bytes: &[u8],
    public_inputs_json: &str,
    verification_key_path: &str,
) -> PyResult<bool> {
    // Load verification key
    let verification_key_bytes = std::fs::read(verification_key_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read verification key: {}", e)))?;
    
    // Verify proof
    let is_valid = verify_cold_chain_proof(proof_bytes, public_inputs_json, &verification_key_bytes)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Verification failed: {}", e)))?;
    
    Ok(is_valid)
}

/// Verify a cold-chain breach proof with timestamp range validation.
///
/// # Arguments
/// * `proof_bytes` - Proof bytes
/// * `public_inputs_json` - Public inputs as JSON string
/// * `verification_key_path` - Path to the verification key file
/// * `expected_timestamp_start` - Expected start timestamp (ISO 8601)
/// * `expected_timestamp_end` - Expected end timestamp (ISO 8601)
///
/// # Returns
/// True if the proof is valid and timestamp is in range, False otherwise
#[pyfunction]
#[pyo3(signature = (
    proof_bytes,
    public_inputs_json,
    verification_key_path,
    expected_timestamp_start=None,
    expected_timestamp_end=None
))]
fn zk_verify_cold_chain_proof_with_timestamp_check(
    proof_bytes: &[u8],
    public_inputs_json: &str,
    verification_key_path: &str,
    expected_timestamp_start: Option<&str>,
    expected_timestamp_end: Option<&str>,
) -> PyResult<bool> {
    // Load verification key
    let verification_key_bytes = std::fs::read(verification_key_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read verification key: {}", e)))?;
    
    // Parse expected timestamp range if provided
    let expected_range = if let (Some(start), Some(end)) = (expected_timestamp_start, expected_timestamp_end) {
        let start_dt = chrono::DateTime::parse_from_rfc3339(start)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid start timestamp: {}", e)))?;
        let end_dt = chrono::DateTime::parse_from_rfc3339(end)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid end timestamp: {}", e)))?;
        Some((start_dt.with_timezone(&chrono::Utc), end_dt.with_timezone(&chrono::Utc)))
    } else {
        None
    };
    
    // Verify proof with timestamp check
    let is_valid = verify_cold_chain_proof_with_timestamp_check(
        proof_bytes,
        public_inputs_json,
        &verification_key_bytes,
        expected_range,
    ).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Verification failed: {}", e)))?;
    
    Ok(is_valid)
}

/// Load a verification key from file.
#[pyfunction]
fn zk_load_verification_key(path: &str) -> PyResult<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read verification key: {}", e)))
}

/// Load a proving key from file.
#[pyfunction]
fn zk_load_proving_key(path: &str) -> PyResult<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to read proving key: {}", e)))
}

/// Generate proving and verification keys (development only).
///
/// WARNING: This is for development/testing only. In production, use a
/// secure multi-party computation (MPC) ceremony to generate keys.
///
/// # Arguments
/// * `event_json` - JSON string of example event data
/// * `declared_min_c` - Declared minimum temperature in Celsius
/// * `declared_max_c` - Declared maximum temperature in Celsius
/// * `breach_timestamp` - ISO 8601 timestamp of the breach
/// * `pk_path` - Path to save the proving key
/// * `vk_path` - Path to save the verification key
#[pyfunction]
#[pyo3(signature = (
    event_json,
    declared_min_c,
    declared_max_c,
    breach_timestamp,
    pk_path,
    vk_path
))]
fn zk_generate_keys(
    event_json: &str,
    declared_min_c: f64,
    declared_max_c: f64,
    breach_timestamp: &str,
    pk_path: &str,
    vk_path: &str,
) -> PyResult<()> {
    // Parse event JSON to extract witness data
    let event_data: serde_json::Value = serde_json::from_str(event_json)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid event JSON: {}", e)))?;
    
    // Extract witness from event
    let event_hash = event_data.get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    
    let event_timestamp = event_data.get("time")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp())
        .unwrap_or(0);
    
    let temperature_c = event_data.get("data")
        .and_then(|v| v.as_object())
        .and_then(|obj| obj.get("temperature_c"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    
    let witness = ColdChainWitness::new(
        event_hash,
        event_timestamp,
        temperature_c,
        declared_min_c,
        declared_max_c,
    );
    
    // Parse breach timestamp
    let breach_dt = chrono::DateTime::parse_from_rfc3339(breach_timestamp)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid breach timestamp: {}", e)))?;
    let breach_ts = breach_dt.timestamp();
    
    // Create public inputs
    let public_inputs = ColdChainPublicInputs {
        event_type: "mandala.truck.cold_chain.breach".to_string(),
        timestamp_range_start: breach_ts - 300,
        timestamp_range_end: breach_ts + 300,
        breach_confirmed: temperature_c < declared_min_c || temperature_c > declared_max_c,
    };
    
    // Generate and save keys
    generate_and_save_keys(witness, public_inputs, pk_path, vk_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Key generation failed: {}", e)))?;
    
    Ok(())
}

/// Generate keys for a breach scenario (convenience function).
///
/// WARNING: This is for development/testing only.
#[pyfunction]
#[pyo3(signature = (pk_path, vk_path))]
fn zk_generate_keys_breach_scenario(pk_path: &str, vk_path: &str) -> PyResult<()> {
    generate_keys_for_breach_scenario(pk_path, vk_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Key generation failed: {}", e)))?;
    Ok(())
}

/// Create a new MPC ceremony.
///
/// # Arguments
/// * `required_participants` - Minimum number of participants required
#[pyfunction]
#[pyo3(signature = (required_participants))]
fn zk_mpc_ceremony_new(required_participants: usize) -> PyResult<MPCCeremonyWrapper> {
    Ok(MPCCeremonyWrapper {
        inner: MPCCeremony::new(required_participants),
    })
}

/// Generate a random contribution for MPC ceremony participant.
#[pyfunction]
fn zk_mpc_generate_contribution() -> PyResult<Vec<u8>> {
    Ok(generate_contribution())
}

/// Simulate a full MPC ceremony (for testing only).
///
/// WARNING: This is for testing only. In production, run a real ceremony
/// with multiple independent participants.
#[pyfunction]
#[pyo3(signature = (num_participants, event_json, declared_min_c, declared_max_c, breach_timestamp, pk_path, vk_path))]
fn zk_mpc_simulate_ceremony(
    num_participants: usize,
    event_json: &str,
    declared_min_c: f64,
    declared_max_c: f64,
    breach_timestamp: &str,
    pk_path: &str,
    vk_path: &str,
) -> PyResult<()> {
    // Parse event JSON to extract witness data
    let event_data: serde_json::Value = serde_json::from_str(event_json)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid event JSON: {}", e)))?;
    
    let event_hash = event_data.get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    
    let event_timestamp = event_data.get("time")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp())
        .unwrap_or(0);
    
    let temperature_c = event_data.get("data")
        .and_then(|v| v.as_object())
        .and_then(|obj| obj.get("temperature_c"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    
    let witness = ColdChainWitness::new(
        event_hash,
        event_timestamp,
        temperature_c,
        declared_min_c,
        declared_max_c,
    );
    
    let breach_dt = chrono::DateTime::parse_from_rfc3339(breach_timestamp)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid breach timestamp: {}", e)))?;
    let breach_ts = breach_dt.timestamp();
    
    let public_inputs = ColdChainPublicInputs {
        event_type: "mandala.truck.cold_chain.breach".to_string(),
        timestamp_range_start: breach_ts - 300,
        timestamp_range_end: breach_ts + 300,
        breach_confirmed: temperature_c < declared_min_c || temperature_c > declared_max_c,
    };
    
    let (pk_bytes, vk_bytes, _state) = simulate_ceremony(num_participants, witness, public_inputs)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("MPC ceremony failed: {}", e)))?;
    
    std::fs::write(pk_path, pk_bytes)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write proving key: {}", e)))?;
    
    std::fs::write(vk_path, vk_bytes)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write verification key: {}", e)))?;
    
    Ok(())
}

/// Wrapper for MPCCeremony to expose it to Python.
#[pyclass]
pub struct MPCCeremonyWrapper {
    inner: MPCCeremony,
}

#[pymethods]
impl MPCCeremonyWrapper {
    /// Add a participant contribution.
    #[pyo3(signature = (participant_id, randomness))]
    fn add_contribution(&mut self, participant_id: String, randomness: Vec<u8>) -> PyResult<String> {
        self.inner.add_contribution(participant_id, randomness)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to add contribution: {}", e)))?;
        Ok("Contribution added successfully".to_string())
    }
    
    /// Generate keys from the ceremony.
    #[pyo3(signature = (event_json, declared_min_c, declared_max_c, breach_timestamp, pk_path, vk_path))]
    fn generate_keys(
        &self,
        event_json: &str,
        declared_min_c: f64,
        declared_max_c: f64,
        breach_timestamp: &str,
        pk_path: &str,
        vk_path: &str,
    ) -> PyResult<()> {
        // Parse event JSON to extract witness data
        let event_data: serde_json::Value = serde_json::from_str(event_json)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid event JSON: {}", e)))?;
        
        let event_hash = event_data.get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        
        let event_timestamp = event_data.get("time")
            .and_then(|v| v.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.timestamp())
            .unwrap_or(0);
        
        let temperature_c = event_data.get("data")
            .and_then(|v| v.as_object())
            .and_then(|obj| obj.get("temperature_c"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        
        let witness = ColdChainWitness::new(
            event_hash,
            event_timestamp,
            temperature_c,
            declared_min_c,
            declared_max_c,
        );
        
        let breach_dt = chrono::DateTime::parse_from_rfc3339(breach_timestamp)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid breach timestamp: {}", e)))?;
        let breach_ts = breach_dt.timestamp();
        
        let public_inputs = ColdChainPublicInputs {
            event_type: "mandala.truck.cold_chain.breach".to_string(),
            timestamp_range_start: breach_ts - 300,
            timestamp_range_end: breach_ts + 300,
            breach_confirmed: temperature_c < declared_min_c || temperature_c > declared_max_c,
        };
        
        let (pk_bytes, vk_bytes) = self.inner.generate_keys(witness, public_inputs)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Key generation failed: {}", e)))?;
        
        std::fs::write(pk_path, pk_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write proving key: {}", e)))?;
        
        std::fs::write(vk_path, vk_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to write verification key: {}", e)))?;
        
        Ok(())
    }
    
    /// Check if the ceremony is complete.
    fn is_complete(&self) -> bool {
        self.inner.is_complete()
    }
    
    /// Get the number of required participants.
    fn required_participants(&self) -> usize {
        self.inner.required_participants()
    }
    
    /// Get the number of current participants.
    fn current_participants(&self) -> usize {
        self.inner.current_participants()
    }
}

/// Key cache for in-memory key storage.
#[pyclass]
pub struct ZKKeyCache {
    inner: KeyCache,
}

#[pymethods]
impl ZKKeyCache {
    /// Create a new key cache.
    #[new]
    fn new() -> Self {
        Self {
            inner: KeyCache::new(),
        }
    }
    
    /// Load a verification key from file and cache it.
    fn load_verification_key(&self, path: &str) -> PyResult<Vec<u8>> {
        self.inner.load_verification_key(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to load verification key: {}", e)))
    }
    
    /// Load a proving key from file and cache it.
    fn load_proving_key(&self, path: &str) -> PyResult<Vec<u8>> {
        self.inner.load_proving_key(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to load proving key: {}", e)))
    }
    
    /// Load verification key from bytes and cache it.
    fn load_verification_key_bytes(&self, bytes: Vec<u8>, cache_key: &str) -> PyResult<Vec<u8>> {
        self.inner.load_verification_key_bytes(bytes, cache_key)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to load verification key bytes: {}", e)))
    }
    
    /// Load proving key from bytes and cache it.
    fn load_proving_key_bytes(&self, bytes: Vec<u8>, cache_key: &str) -> PyResult<Vec<u8>> {
        self.inner.load_proving_key_bytes(bytes, cache_key)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to load proving key bytes: {}", e)))
    }
    
    /// Clear the cache.
    fn clear(&self) -> PyResult<()> {
        self.inner.clear()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to clear cache: {}", e)))
    }
    
    /// Get cache statistics (vk_count, pk_count).
    fn stats(&self) -> PyResult<(usize, usize)> {
        self.inner.stats()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get stats: {}", e)))
    }
}

// ============================================================================
// PYTHON CLASS BINDINGS
// ============================================================================

#[pymethods]
impl ColdChainBreachProof {
    /// Create a new ColdChainBreachProof.
    #[new]
    #[pyo3(signature = (proof, public_inputs, verification_key, proof_id, generated_at))]
    fn new(
        proof: Vec<u8>,
        public_inputs: String,
        verification_key: Vec<u8>,
        proof_id: String,
        generated_at: String,
    ) -> Self {
        Self::new(proof, public_inputs, verification_key, proof_id, generated_at)
    }
    
    /// Get the proof as a hex string.
    fn proof_hex(&self) -> String {
        hex::encode(&self.proof)
    }
    
    /// Get the verification key as a hex string.
    fn verification_key_hex(&self) -> String {
        hex::encode(&self.verification_key)
    }
    
    /// Convert to dictionary representation.
    fn to_dict(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let dict = pyo3::types::PyDict::new(py);
            dict.set_item("proof", self.proof_hex())?;
            dict.set_item("public_inputs", self.public_inputs.clone())?;
            dict.set_item("verification_key", self.verification_key_hex())?;
            dict.set_item("proof_id", self.proof_id.clone())?;
            dict.set_item("generated_at", self.generated_at.clone())?;
            Ok(dict.into())
        })
    }
}

/// Register the ZK module with PyO3.
pub fn register_zk_module(py: Python, m: &PyModule) -> PyResult<()> {
    let zk_module = PyModule::new(py, "zk")?;
    
    // Register functions
    zk_module.add_function(wrap_pyfunction!(zk_generate_cold_chain_proof, zk_module)?)?;
    zk_module.add_function(wrap_pyfunction!(zk_verify_cold_chain_proof, zk_module)?)?;
    zk_module.add_function(wrap_pyfunction!(zk_verify_cold_chain_proof_with_timestamp_check, zk_module)?)?;
    zk_module.add_function(wrap_pyfunction!(zk_load_verification_key, zk_module)?)?;
    zk_module.add_function(wrap_pyfunction!(zk_load_proving_key, zk_module)?)?;
    zk_module.add_function(wrap_pyfunction!(zk_generate_keys, zk_module)?)?;
    zk_module.add_function(wrap_pyfunction!(zk_generate_keys_breach_scenario, zk_module)?)?;
    zk_module.add_function(wrap_pyfunction!(zk_mpc_ceremony_new, zk_module)?)?;
    zk_module.add_function(wrap_pyfunction!(zk_mpc_generate_contribution, zk_module)?)?;
    zk_module.add_function(wrap_pyfunction!(zk_mpc_simulate_ceremony, zk_module)?)?;
    
    // Register classes
    zk_module.add_class::<ColdChainBreachProof>()?;
    zk_module.add_class::<ZKKeyCache>()?;
    zk_module.add_class::<MPCCeremonyWrapper>()?;
    
    m.add_submodule(zk_module)?;
    Ok(())
}

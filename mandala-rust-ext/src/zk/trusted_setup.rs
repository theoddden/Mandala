//! Trusted setup ceremony for ZK-SNARK key generation.
//!
//! This module provides functions for generating proving and verification keys
//! for the cold-chain breach circuit. In production, this should be done via
//! a secure multi-party computation (MPC) ceremony to ensure the toxic waste
//! is destroyed.

use ark_bls12_381::Bls12_381;
use ark_groth16::{generate_random_parameters, ProvingKey, VerifyingKey};
use ark_relations::r1cs::ConstraintSynthesizer;
use ark_std::rand::rngs::OsRng;
use crate::zk::circuits::ColdChainBreachCircuit;
use crate::zk::keys::serialize_proving_key, serialize_verifying_key;
use crate::zk::types::{ColdChainPublicInputs, ColdChainWitness, ZKError};

/// Generate proving and verification keys for the cold-chain breach circuit.
///
/// This is a simplified trusted setup for development/testing. In production,
/// this should be done via a secure multi-party computation (MPC) ceremony
/// to ensure the toxic waste (randomness used in key generation) is destroyed.
///
/// # Arguments
/// * `witness` - Example witness for the circuit
/// * `public_inputs` - Example public inputs for the circuit
///
/// # Returns
/// A tuple of (proving_key_bytes, verification_key_bytes)
pub fn generate_trusted_setup(
    witness: ColdChainWitness,
    public_inputs: ColdChainPublicInputs,
) -> Result<(Vec<u8>, Vec<u8>), ZKError> {
    // Build circuit
    let circuit = ColdChainBreachCircuit::<ark_bls12_381::Fr>::new(witness, public_inputs)?;
    
    // Generate random parameters (trusted setup)
    let rng = &mut OsRng;
    let params = generate_random_parameters::<Bls12_381, _, _>(circuit, rng)
        .map_err(|e| ZKError::ProofGenerationError(format!("Trusted setup failed: {}", e)))?;
    
    // Serialize proving key
    let pk_bytes = serialize_proving_key(&params.pk)?;
    
    // Serialize verification key
    let vk_bytes = serialize_verifying_key(&params.vk)?;
    
    Ok((pk_bytes, vk_bytes))
}

/// Generate proving and verification keys and save to files.
///
/// # Arguments
/// * `witness` - Example witness for the circuit
/// * `public_inputs` - Example public inputs for the circuit
/// * `pk_path` - Path to save the proving key
/// * `vk_path` - Path to save the verification key
pub fn generate_and_save_keys(
    witness: ColdChainWitness,
    public_inputs: ColdChainPublicInputs,
    pk_path: &str,
    vk_path: &str,
) -> Result<(), ZKError> {
    let (pk_bytes, vk_bytes) = generate_trusted_setup(witness, public_inputs)?;
    
    // Save proving key
    std::fs::write(pk_path, pk_bytes)
        .map_err(|e| ZKError::KeyLoadError(format!("Failed to write proving key: {}", e)))?;
    
    // Save verification key
    std::fs::write(vk_path, vk_bytes)
        .map_err(|e| ZKError::KeyLoadError(format!("Failed to write verification key: {}", e)))?;
    
    Ok(())
}

/// Generate keys for a cold-chain breach scenario.
///
/// This is a convenience function that creates example witness and public inputs
/// for a cold-chain breach scenario and generates keys for it.
pub fn generate_keys_for_breach_scenario(
    pk_path: &str,
    vk_path: &str,
) -> Result<(), ZKError> {
    // Example breach scenario
    let witness = ColdChainWitness::new(
        "event_hash_example_123".to_string(),
        1704067200, // 2024-01-01 00:00:00 UTC
        -5.0,       // Temperature breach (below 0°C)
        0.0,
        10.0,
    );
    
    let public_inputs = ColdChainPublicInputs {
        event_type: "mandala.truck.cold_chain.breach".to_string(),
        timestamp_range_start: 1704066900,
        timestamp_range_end: 1704067500,
        breach_confirmed: true,
    };
    
    generate_and_save_keys(witness, public_inputs, pk_path, vk_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_trusted_setup_generation() {
        let witness = ColdChainWitness::new(
            "test_hash".to_string(),
            1704067200,
            -5.0,
            0.0,
            10.0,
        );
        
        let public_inputs = ColdChainPublicInputs {
            event_type: "mandala.truck.cold_chain.breach".to_string(),
            timestamp_range_start: 1704066900,
            timestamp_range_end: 1704067500,
            breach_confirmed: true,
        };
        
        let result = generate_trusted_setup(witness, public_inputs);
        assert!(result.is_ok());
        
        let (pk_bytes, vk_bytes) = result.unwrap();
        assert!(!pk_bytes.is_empty());
        assert!(!vk_bytes.is_empty());
    }
}

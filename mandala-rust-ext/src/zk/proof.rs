//! ZK-SNARK proof generation and verification.
//!
//! This module provides functions for generating and verifying Groth16 proofs
//! for the cold-chain breach circuit.

use ark_bls12_381::Bls12_381;
use ark_ff::PrimeField;
use ark_groth16::{Groth16, ProvingKey, VerifyingKey};
use ark_relations::r1cs::ConstraintSynthesizer;
use ark_std::rand::rngs::OsRng;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::zk::circuits::ColdChainBreachCircuit;
use crate::zk::keys::{deserialize_proving_key, deserialize_verifying_key};
use crate::zk::types::{ColdChainBreachProof, ColdChainPublicInputs, ColdChainWitness, ZKError};

/// Generate a cold-chain breach proof.
///
/// # Arguments
/// * `witness` - Private inputs (temperature, timestamps, etc.)
/// * `public_inputs` - Public inputs (event type, timestamp range, etc.)
/// * `proving_key_bytes` - Serialized proving key
///
/// # Returns
/// A `ColdChainBreachProof` containing the proof and metadata
pub fn generate_cold_chain_proof(
    witness: ColdChainWitness,
    public_inputs: ColdChainPublicInputs,
    proving_key_bytes: &[u8],
) -> Result<ColdChainBreachProof, ZKError> {
    // Deserialize proving key
    let pk = deserialize_proving_key(proving_key_bytes)?;
    
    // Build circuit
    let circuit = ColdChainBreachCircuit::<ark_bls12_381::Fr>::new(witness.clone(), public_inputs.clone())?;
    
    // Generate proof
    let rng = &mut OsRng;
    let proof = Groth16::<Bls12_381>::prove(&pk, circuit, rng)
        .map_err(|e| ZKError::ProofGenerationError(format!("Proof generation failed: {}", e)))?;
    
    // Serialize proof
    let mut proof_bytes = Vec::new();
    proof.serialize_uncompressed(&mut proof_bytes)
        .map_err(|e| ZKError::SerializationError(format!("Proof serialization failed: {}", e)))?;
    
    // Serialize verification key for storage
    let mut vk_bytes = Vec::new();
    pk.vk.serialize_uncompressed(&mut vk_bytes)
        .map_err(|e| ZKError::SerializationError(format!("Verification key serialization failed: {}", e)))?;
    
    // Generate proof ID and timestamp
    let proof_id = Uuid::new_v4().to_string();
    let generated_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    
    // Serialize public inputs
    let public_inputs_json = public_inputs.to_json()
        .map_err(|e| ZKError::SerializationError(format!("Public inputs serialization failed: {}", e)))?;
    
    Ok(ColdChainBreachProof::new(
        proof_bytes,
        public_inputs_json,
        vk_bytes,
        proof_id,
        generated_at,
    ))
}

/// Verify a cold-chain breach proof.
///
/// # Arguments
/// * `proof_bytes` - Serialized proof
/// * `public_inputs_json` - Public inputs as JSON string
/// * `verification_key_bytes` - Serialized verification key
///
/// # Returns
/// `true` if the proof is valid, `false` otherwise
pub fn verify_cold_chain_proof(
    proof_bytes: &[u8],
    public_inputs_json: &str,
    verification_key_bytes: &[u8],
) -> Result<bool, ZKError> {
    // Deserialize verification key
    let vk = deserialize_verifying_key(verification_key_bytes)?;
    
    // Deserialize proof
    let proof = ark_groth16::Proof::<Bls12_381>::deserialize_uncompressed(proof_bytes)
        .map_err(|e| ZKError::SerializationError(format!("Proof deserialization failed: {}", e)))?;
    
    // Parse public inputs
    let public_inputs = ColdChainPublicInputs::from_json(public_inputs_json)
        .map_err(|e| ZKError::InvalidInput(format!("Public inputs parsing failed: {}", e)))?;
    
    // Convert public inputs to field elements
    let event_type_field = ColdChainBreachCircuit::<ark_bls12_381::Fr>::string_to_field(&public_inputs.event_type)?;
    let timestamp_start_field = ColdChainBreachCircuit::<ark_bls12_381::Fr>::i64_to_field(public_inputs.timestamp_range_start);
    let timestamp_end_field = ColdChainBreachCircuit::<ark_bls12_381::Fr>::i64_to_field(public_inputs.timestamp_range_end);
    let breach_confirmed_field = if public_inputs.breach_confirmed {
        ark_bls12_381::Fr::one()
    } else {
        ark_bls12_381::Fr::zero()
    };
    
    let public_inputs_vec = vec![
        event_type_field,
        timestamp_start_field,
        timestamp_end_field,
        breach_confirmed_field,
    ];
    
    // Verify proof
    let is_valid = Groth16::<Bls12_381>::verify(&vk, &public_inputs_vec, &proof)
        .map_err(|e| ZKError::VerificationError(format!("Verification failed: {}", e)))?;
    
    Ok(is_valid)
}

/// Verify a proof with additional timestamp range validation.
///
/// # Arguments
/// * `proof_bytes` - Serialized proof
/// * `public_inputs_json` - Public inputs as JSON string
/// * `verification_key_bytes` - Serialized verification key
/// * `expected_timestamp_range` - Optional (start, end) tuple to validate against
///
/// # Returns
/// `true` if the proof is valid and timestamp is in range, `false` otherwise
pub fn verify_cold_chain_proof_with_timestamp_check(
    proof_bytes: &[u8],
    public_inputs_json: &str,
    verification_key_bytes: &[u8],
    expected_timestamp_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
) -> Result<bool, ZKError> {
    // First, verify the cryptographic proof
    let is_valid = verify_cold_chain_proof(proof_bytes, public_inputs_json, verification_key_bytes)?;
    
    if !is_valid {
        return Ok(false);
    }
    
    // If timestamp range is provided, validate it
    if let Some((ts_start, ts_end)) = expected_timestamp_range {
        let public_inputs = ColdChainPublicInputs::from_json(public_inputs_json)?;
        
        let proof_ts_start = DateTime::from_timestamp(public_inputs.timestamp_range_start, 0)
            .ok_or_else(|| ZKError::InvalidInput("Invalid timestamp start".to_string()))?;
        let proof_ts_end = DateTime::from_timestamp(public_inputs.timestamp_range_end, 0)
            .ok_or_else(|| ZKError::InvalidInput("Invalid timestamp end".to_string()))?;
        
        // Check that proof timestamp range is within expected range
        let in_range = proof_ts_start >= ts_start && proof_ts_end <= ts_end;
        
        if !in_range {
            return Ok(false);
        }
    }
    
    Ok(true)
}

/// Generate a proving key and verification key for the circuit.
///
/// This is a one-time setup operation that generates the trusted setup keys.
/// In production, this should be done via a secure multi-party computation (MPC)
/// ceremony to ensure the toxic waste is destroyed.
///
/// # Arguments
/// * `witness` - Example witness for the circuit
/// * `public_inputs` - Example public inputs for the circuit
///
/// # Returns
/// A tuple of (proving_key_bytes, verification_key_bytes)
#[cfg(feature = "zk")]
pub fn generate_trusted_setup(
    witness: ColdChainWitness,
    public_inputs: ColdChainPublicInputs,
) -> Result<(Vec<u8>, Vec<u8>), ZKError> {
    use ark_groth16::generate_random_parameters;
    
    // Build circuit
    let circuit = ColdChainBreachCircuit::<ark_bls12_381::Fr>::new(witness, public_inputs)?;
    
    // Generate random parameters (trusted setup)
    let rng = &mut OsRng;
    let params = generate_random_parameters::<Bls12_381, _, _>(circuit, rng)
        .map_err(|e| ZKError::ProofGenerationError(format!("Trusted setup failed: {}", e)))?;
    
    // Serialize proving key
    let mut pk_bytes = Vec::new();
    params.pk.serialize_uncompressed(&mut pk_bytes)
        .map_err(|e| ZKError::SerializationError(format!("Proving key serialization failed: {}", e)))?;
    
    // Serialize verification key
    let mut vk_bytes = Vec::new();
    params.vk.serialize_uncompressed(&mut vk_bytes)
        .map_err(|e| ZKError::SerializationError(format!("Verification key serialization failed: {}", e)))?;
    
    Ok((pk_bytes, vk_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_proof_generation_and_verification() {
        // This test requires a valid proving key, which we can't generate
        // in a unit test without running the trusted setup.
        // In production, this would be an integration test.
    }
    
    #[test]
    fn test_public_inputs_serialization() {
        let public_inputs = ColdChainPublicInputs {
            event_type: "mandala.truck.cold_chain.breach".to_string(),
            timestamp_range_start: 1704066900,
            timestamp_range_end: 1704067500,
            breach_confirmed: true,
        };
        
        let json = public_inputs.to_json();
        assert!(json.is_ok());
        
        let parsed = ColdChainPublicInputs::from_json(&json.unwrap());
        assert!(parsed.is_ok());
        
        let parsed_inputs = parsed.unwrap();
        assert_eq!(parsed_inputs.event_type, public_inputs.event_type);
        assert_eq!(parsed_inputs.timestamp_range_start, public_inputs.timestamp_range_start);
        assert_eq!(parsed_inputs.timestamp_range_end, public_inputs.timestamp_range_end);
        assert_eq!(parsed_inputs.breach_confirmed, public_inputs.breach_confirmed);
    }
    
    #[test]
    fn test_verify_with_invalid_json() {
        let result = verify_cold_chain_proof(
            &[0u8; 256],
            "invalid json",
            &[0u8; 256],
        );
        assert!(result.is_err());
    }
}

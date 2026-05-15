//! ZK-SNARK type definitions for Mandala.
//!
//! This module defines the core types used for zero-knowledge proof generation
//! and verification for logistics events.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Zero-knowledge proof for cold-chain breach verification.
///
/// Contains the cryptographic proof, public inputs, and metadata needed
/// for verification without revealing sensitive witness data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColdChainBreachProof {
    /// The Groth16 proof bytes (256 bytes for BLS12-381)
    pub proof: Vec<u8>,
    
    /// Public inputs revealed to the verifier (JSON string)
    pub public_inputs: String,
    
    /// Verification key used to generate this proof
    pub verification_key: Vec<u8>,
    
    /// Unique identifier for tracking this proof
    pub proof_id: String,
    
    /// ISO 8601 timestamp when proof was generated
    pub generated_at: String,
}

impl ColdChainBreachProof {
    /// Create a new cold-chain breach proof.
    pub fn new(
        proof: Vec<u8>,
        public_inputs: String,
        verification_key: Vec<u8>,
        proof_id: String,
        generated_at: String,
    ) -> Self {
        Self {
            proof,
            public_inputs,
            verification_key,
            proof_id,
            generated_at,
        }
    }
}

impl fmt::Display for ColdChainBreachProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ColdChainBreachProof(id={}, generated_at={}, proof_size={} bytes)",
            self.proof_id,
            self.generated_at,
            self.proof.len()
        )
    }
}

/// Public inputs for the cold-chain breach circuit.
///
/// These values are revealed to the verifier and do not contain sensitive data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColdChainPublicInputs {
    /// Event type (e.g., "mandala.truck.cold_chain.breach")
    pub event_type: String,
    
    /// Start of timestamp range (Unix timestamp in seconds)
    pub timestamp_range_start: i64,
    
    /// End of timestamp range (Unix timestamp in seconds)
    pub timestamp_range_end: i64,
    
    /// Whether a breach was confirmed
    pub breach_confirmed: bool,
}

impl ColdChainPublicInputs {
    /// Convert to JSON string.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
    
    /// Parse from JSON string.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Private inputs (witness) for the cold-chain breach circuit.
///
/// These values are kept secret and used only for proof generation.
#[derive(Clone, Debug)]
pub struct ColdChainWitness {
    /// Hash of the event (for commitment)
    pub event_hash: String,
    
    /// Event timestamp (Unix timestamp in seconds)
    pub event_timestamp: i64,
    
    /// Temperature reading in Celsius
    pub temperature_c: f64,
    
    /// Declared minimum temperature in Celsius
    pub declared_min_c: f64,
    
    /// Declared maximum temperature in Celsius
    pub declared_max_c: f64,
}

impl ColdChainWitness {
    /// Create a new witness.
    pub fn new(
        event_hash: String,
        event_timestamp: i64,
        temperature_c: f64,
        declared_min_c: f64,
        declared_max_c: f64,
    ) -> Self {
        Self {
            event_hash,
            event_timestamp,
            temperature_c,
            declared_min_c,
            declared_max_c,
        }
    }
}

/// Error types for ZK operations.
#[derive(Debug)]
pub enum ZKError {
    /// Circuit constraint synthesis failed
    SynthesisError(String),
    
    /// Proof generation failed
    ProofGenerationError(String),
    
    /// Proof verification failed
    VerificationError(String),
    
    /// Key loading failed
    KeyLoadError(String),
    
    /// Serialization error
    SerializationError(String),
    
    /// Invalid input
    InvalidInput(String),
}

impl fmt::Display for ZKError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZKError::SynthesisError(msg) => write!(f, "Synthesis error: {}", msg),
            ZKError::ProofGenerationError(msg) => write!(f, "Proof generation error: {}", msg),
            ZKError::VerificationError(msg) => write!(f, "Verification error: {}", msg),
            ZKError::KeyLoadError(msg) => write!(f, "Key load error: {}", msg),
            ZKError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            ZKError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
        }
    }
}

impl std::error::Error for ZKError {}

/// Convert arkworks serialization errors to ZKError.
impl From<ark_serialize::SerializationError> for ZKError {
    fn from(err: ark_serialize::SerializationError) -> Self {
        ZKError::SerializationError(err.to_string())
    }
}

/// Convert arkworks synthesis errors to ZKError.
impl From<ark_relations::r1cs::SynthesisError> for ZKError {
    fn from(err: ark_relations::r1cs::SynthesisError) -> Self {
        ZKError::SynthesisError(err.to_string())
    }
}

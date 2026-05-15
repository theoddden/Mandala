//! Multi-Party Computation (MPC) ceremony for trusted setup.
//!
//! This module implements an MPC ceremony protocol for generating proving and
//! verification keys without any single party knowing the toxic waste (random seed).
//!
//! For production use, consider using established frameworks like:
//! - Perpetual Powers of Tau (https://github.com/privacy-scaling-explorations/perpetualpowersoftau)
//! - Arkworks MPC (https://github.com/arkworks-rs/mpc)
//!
//! This implementation provides a simplified ceremony for demonstration purposes.

use ark_bls12_381::Bls12_381;
use ark_groth16::{ProvingKey, VerifyingKey};
use ark_relations::r1cs::ConstraintSynthesizer;
use ark_std::rand::rngs::OsRng;
use ark_std::rand::Rng;
use ark_serialize::{CanonicalDeserialize, CanonicalSerialize};
use crate::zk::circuits::ColdChainBreachCircuit;
use crate::zk::keys::serialize_proving_key, serialize_verifying_key;
use crate::zk::types::{ColdChainPublicInputs, ColdChainWitness, ZKError};

/// Participant in the MPC ceremony.
#[derive(Clone, Debug)]
pub struct Participant {
    /// Unique identifier for the participant
    pub id: String,
    /// Public contribution (to be shared)
    pub contribution: Vec<u8>,
}

/// State of the MPC ceremony.
#[derive(Clone, Debug)]
pub struct CeremonyState {
    /// Current round number
    pub round: u32,
    /// Accumulated randomness from all participants
    pub accumulated_randomness: Vec<u8>,
    /// List of participants who have contributed
    pub participants: Vec<Participant>,
    /// Whether the ceremony is complete
    pub complete: bool,
}

/// MPC ceremony coordinator.
pub struct MPCCeremony {
    state: CeremonyState,
    required_participants: usize,
}

impl MPCCeremony {
    /// Create a new MPC ceremony.
    ///
    /// # Arguments
    /// * `required_participants` - Minimum number of participants required
    pub fn new(required_participants: usize) -> Self {
        Self {
            state: CeremonyState {
                round: 0,
                accumulated_randomness: Vec::new(),
                participants: Vec::new(),
                complete: false,
            },
            required_participants,
        }
    }

    /// Add a participant contribution to the ceremony.
    ///
    /// # Arguments
    /// * `participant_id` - Unique identifier for the participant
    /// * `randomness` - Random contribution from the participant
    ///
    /// # Returns
    /// Updated ceremony state
    pub fn add_contribution(
        &mut self,
        participant_id: String,
        randomness: Vec<u8>,
    ) -> Result<CeremonyState, ZKError> {
        if self.state.complete {
            return Err(ZKError::ProofGenerationError("Ceremony is already complete".to_string()));
        }

        // Check if participant already contributed
        if self.state.participants.iter().any(|p| p.id == participant_id) {
            return Err(ZKError::InvalidInput("Participant already contributed".to_string()));
        }

        // Add contribution
        let participant = Participant {
            id: participant_id,
            contribution: randomness.clone(),
        };
        self.state.participants.push(participant);
        
        // Accumulate randomness (XOR for simplicity)
        for (i, byte) in randomness.iter().enumerate() {
            if i < self.state.accumulated_randomness.len() {
                self.state.accumulated_randomness[i] ^= byte;
            } else {
                self.state.accumulated_randomness.push(*byte);
            }
        }

        self.state.round += 1;

        // Check if ceremony is complete
        if self.state.participants.len() >= self.required_participants {
            self.state.complete = true;
        }

        Ok(self.state.clone())
    }

    /// Generate proving and verification keys using the accumulated randomness.
    ///
    /// # Arguments
    /// * `witness` - Example witness for the circuit
    /// * `public_inputs` - Example public inputs for the circuit
    ///
    /// # Returns
    /// A tuple of (proving_key_bytes, verification_key_bytes)
    pub fn generate_keys(
        &self,
        witness: ColdChainWitness,
        public_inputs: ColdChainPublicInputs,
    ) -> Result<(Vec<u8>, Vec<u8>), ZKError> {
        if !self.state.complete {
            return Err(ZKError::ProofGenerationError(
                "Ceremony is not complete. Need more participants.".to_string(),
            ));
        }

        if self.state.participants.len() < self.required_participants {
            return Err(ZKError::ProofGenerationError(format!(
                "Not enough participants. Required: {}, Got: {}",
                self.required_participants,
                self.state.participants.len()
            )));
        }

        // Use accumulated randomness as seed for key generation
        let seed: [u8; 32] = self
            .state
            .accumulated_randomness
            .get(..32)
            .and_then(|bytes| bytes.try_into().ok())
            .unwrap_or_else(|| {
                // Fallback if not enough bytes
                let mut seed = [0u8; 32];
                let bytes = &self.state.accumulated_randomness;
                let len = bytes.len().min(32);
                seed[..len].copy_from_slice(&bytes[..len]);
                seed
            });

        // Build circuit
        let circuit = ColdChainBreachCircuit::<ark_bls12_381::Fr>::new(witness, public_inputs)?;

        // Generate parameters using the seed
        let rng = &mut seed_rng(seed);
        let params = ark_groth16::generate_random_parameters::<Bls12_381, _, _>(circuit, rng)
            .map_err(|e| ZKError::ProofGenerationError(format!("Key generation failed: {}", e)))?;

        // Serialize keys
        let pk_bytes = serialize_proving_key(&params.pk)?;
        let vk_bytes = serialize_verifying_key(&params.vk)?;

        Ok((pk_bytes, vk_bytes))
    }

    /// Get the current ceremony state.
    pub fn get_state(&self) -> &CeremonyState {
        &self.state
    }

    /// Check if the ceremony is complete.
    pub fn is_complete(&self) -> bool {
        self.state.complete
    }

    /// Get the number of required participants.
    pub fn required_participants(&self) -> usize {
        self.required_participants
    }

    /// Get the number of current participants.
    pub fn current_participants(&self) -> usize {
        self.state.participants.len()
    }
}

/// Create a seeded RNG from bytes.
fn seed_rng(seed: [u8; 32]) -> impl rand::Rng {
    use rand::SeedableRng;
    rand::rngs::StdRng::from_seed(seed)
}

/// Generate a random contribution for a participant.
pub fn generate_contribution() -> Vec<u8> {
    let mut rng = OsRng;
    let mut contribution = vec![0u8; 32];
    rng.fill(&mut contribution[..]);
    contribution
}

/// Simulate a full MPC ceremony with the given number of participants.
///
/// # Arguments
/// * `num_participants` - Number of participants to simulate
/// * `witness` - Example witness for the circuit
/// * `public_inputs` - Example public inputs for the circuit
///
/// # Returns
/// A tuple of (proving_key_bytes, verification_key_bytes, ceremony_state)
pub fn simulate_ceremony(
    num_participants: usize,
    witness: ColdChainWitness,
    public_inputs: ColdChainPublicInputs,
) -> Result<(Vec<u8>, Vec<u8>, CeremonyState), ZKError> {
    let mut ceremony = MPCCeremony::new(num_participants);

    // Simulate participants contributing
    for i in 0..num_participants {
        let participant_id = format!("participant_{}", i);
        let contribution = generate_contribution();
        ceremony.add_contribution(participant_id, contribution)?;
    }

    // Generate keys
    let (pk_bytes, vk_bytes) = ceremony.generate_keys(witness, public_inputs)?;
    let state = ceremony.get_state().clone();

    Ok((pk_bytes, vk_bytes, state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ceremony_creation() {
        let ceremony = MPCCeremony::new(3);
        assert!(!ceremony.is_complete());
        assert_eq!(ceremony.required_participants(), 3);
        assert_eq!(ceremony.current_participants(), 0);
    }

    #[test]
    fn test_add_contribution() {
        let mut ceremony = MPCCeremony::new(2);
        let contribution = generate_contribution();
        
        let state = ceremony.add_contribution("participant_1".to_string(), contribution.clone()).unwrap();
        assert_eq!(state.participants.len(), 1);
        assert!(!state.complete);
    }

    #[test]
    fn test_ceremony_completion() {
        let mut ceremony = MPCCeremony::new(2);
        
        ceremony.add_contribution("participant_1".to_string(), generate_contribution()).unwrap();
        assert!(!ceremony.is_complete());
        
        ceremony.add_contribution("participant_2".to_string(), generate_contribution()).unwrap();
        assert!(ceremony.is_complete());
    }

    #[test]
    fn test_duplicate_participant() {
        let mut ceremony = MPCCeremony::new(2);
        let contribution = generate_contribution();
        
        ceremony.add_contribution("participant_1".to_string(), contribution.clone()).unwrap();
        let result = ceremony.add_contribution("participant_1".to_string(), contribution);
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_keys_before_completion() {
        let ceremony = MPCCeremony::new(3);
        let witness = ColdChainWitness::new("test".to_string(), 0, 0.0, 0.0, 10.0);
        let public_inputs = ColdChainPublicInputs {
            event_type: "test".to_string(),
            timestamp_range_start: 0,
            timestamp_range_end: 100,
            breach_confirmed: false,
        };
        
        let result = ceremony.generate_keys(witness, public_inputs);
        assert!(result.is_err());
    }

    #[test]
    fn test_simulate_ceremony() {
        let witness = ColdChainWitness::new("test".to_string(), 0, 0.0, 0.0, 10.0);
        let public_inputs = ColdChainPublicInputs {
            event_type: "test".to_string(),
            timestamp_range_start: 0,
            timestamp_range_end: 100,
            breach_confirmed: false,
        };
        
        let result = simulate_ceremony(3, witness, public_inputs);
        assert!(result.is_ok());
        
        let (pk_bytes, vk_bytes, state) = result.unwrap();
        assert!(!pk_bytes.is_empty());
        assert!(!vk_bytes.is_empty());
        assert!(state.complete);
        assert_eq!(state.participants.len(), 3);
    }
}

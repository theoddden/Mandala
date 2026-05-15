//! ZK-SNARK key loading and caching.
//!
//! This module provides efficient loading and caching of proving and verification keys
//! for Groth16 circuits.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use ark_bls12_381::Bls12_381;
use ark_groth16::{ProvingKey, VerifyingKey};
use ark_serialize::{CanonicalDeserialize, CanonicalSerialize};
use crate::zk::types::ZKError;

/// In-memory cache for ZK keys to avoid repeated disk I/O.
#[derive(Clone)]
pub struct KeyCache {
    verification_keys: Arc<std::sync::Mutex<HashMap<String, Vec<u8>>>>,
    proving_keys: Arc<std::sync::Mutex<HashMap<String, Vec<u8>>>>,
}

impl KeyCache {
    /// Create a new empty key cache.
    pub fn new() -> Self {
        Self {
            verification_keys: Arc::new(std::sync::Mutex::new(HashMap::new())),
            proving_keys: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
    
    /// Load a verification key from file, with caching.
    pub fn load_verification_key(&self, path: &str) -> Result<Vec<u8>, ZKError> {
        // Check cache first
        {
            let cache = self.verification_keys.lock()
                .map_err(|e| ZKError::KeyLoadError(format!("Cache lock error: {}", e)))?;
            if let Some(bytes) = cache.get(path) {
                return Ok(bytes.clone());
            }
        }
        
        // Load from disk
        let bytes = std::fs::read(path)
            .map_err(|e| ZKError::KeyLoadError(format!("Failed to read verification key: {}", e)))?;
        
        // Cache the result
        {
            let mut cache = self.verification_keys.lock()
                .map_err(|e| ZKError::KeyLoadError(format!("Cache lock error: {}", e)))?;
            cache.insert(path.to_string(), bytes.clone());
        }
        
        Ok(bytes)
    }
    
    /// Load a proving key from file, with caching.
    pub fn load_proving_key(&self, path: &str) -> Result<Vec<u8>, ZKError> {
        // Check cache first
        {
            let cache = self.proving_keys.lock()
                .map_err(|e| ZKError::KeyLoadError(format!("Cache lock error: {}", e)))?;
            if let Some(bytes) = cache.get(path) {
                return Ok(bytes.clone());
            }
        }
        
        // Load from disk
        let bytes = std::fs::read(path)
            .map_err(|e| ZKError::KeyLoadError(format!("Failed to read proving key: {}", e)))?;
        
        // Cache the result
        {
            let mut cache = self.proving_keys.lock()
                .map_err(|e| ZKError::KeyLoadError(format!("Cache lock error: {}", e)))?;
            cache.insert(path.to_string(), bytes.clone());
        }
        
        Ok(bytes)
    }
    
    /// Load verification key from bytes directly (for in-memory keys).
    pub fn load_verification_key_bytes(&self, bytes: Vec<u8>, cache_key: &str) -> Result<Vec<u8>, ZKError> {
        let mut cache = self.verification_keys.lock()
            .map_err(|e| ZKError::KeyLoadError(format!("Cache lock error: {}", e)))?;
        cache.insert(cache_key.to_string(), bytes.clone());
        Ok(bytes)
    }
    
    /// Load proving key from bytes directly (for in-memory keys).
    pub fn load_proving_key_bytes(&self, bytes: Vec<u8>, cache_key: &str) -> Result<Vec<u8>, ZKError> {
        let mut cache = self.proving_keys.lock()
            .map_err(|e| ZKError::KeyLoadError(format!("Cache lock error: {}", e)))?;
        cache.insert(cache_key.to_string(), bytes.clone());
        Ok(bytes)
    }
    
    /// Clear the cache.
    pub fn clear(&self) -> Result<(), ZKError> {
        self.verification_keys.lock()
            .map_err(|e| ZKError::KeyLoadError(format!("Cache lock error: {}", e)))?
            .clear();
        self.proving_keys.lock()
            .map_err(|e| ZKError::KeyLoadError(format!("Cache lock error: {}", e)))?
            .clear();
        Ok(())
    }
    
    /// Get cache statistics.
    pub fn stats(&self) -> Result<(usize, usize), ZKError> {
        let vk_count = self.verification_keys.lock()
            .map_err(|e| ZKError::KeyLoadError(format!("Cache lock error: {}", e)))?
            .len();
        let pk_count = self.proving_keys.lock()
            .map_err(|e| ZKError::KeyLoadError(format!("Cache lock error: {}", e)))?
            .len();
        Ok((vk_count, pk_count))
    }
}

impl Default for KeyCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Deserialize a verifying key from bytes.
pub fn deserialize_verifying_key(bytes: &[u8]) -> Result<VerifyingKey<Bls12_381>, ZKError> {
    VerifyingKey::<Bls12_381>::deserialize_uncompressed(bytes)
        .map_err(|e| ZKError::SerializationError(format!("Failed to deserialize verifying key: {}", e)))
}

/// Deserialize a proving key from bytes.
pub fn deserialize_proving_key(bytes: &[u8]) -> Result<ProvingKey<Bls12_381>, ZKError> {
    ProvingKey::<Bls12_381>::deserialize_uncompressed(bytes)
        .map_err(|e| ZKError::SerializationError(format!("Failed to deserialize proving key: {}", e)))
}

/// Serialize a verifying key to bytes.
pub fn serialize_verifying_key(vk: &VerifyingKey<Bls12_381>) -> Result<Vec<u8>, ZKError> {
    let mut bytes = Vec::new();
    vk.serialize_uncompressed(&mut bytes)
        .map_err(|e| ZKError::SerializationError(format!("Failed to serialize verifying key: {}", e)))?;
    Ok(bytes)
}

/// Serialize a proving key to bytes.
pub fn serialize_proving_key(pk: &ProvingKey<Bls12_381>) -> Result<Vec<u8>, ZKError> {
    let mut bytes = Vec::new();
    pk.serialize_uncompressed(&mut bytes)
        .map_err(|e| ZKError::SerializationError(format!("Failed to serialize proving key: {}", e)))?;
    Ok(bytes)
}

/// Load a verifying key from file and deserialize it.
pub fn load_and_deserialize_vk(path: &str) -> Result<VerifyingKey<Bls12_381>, ZKError> {
    let bytes = std::fs::read(path)
        .map_err(|e| ZKError::KeyLoadError(format!("Failed to read verification key: {}", e)))?;
    deserialize_verifying_key(&bytes)
}

/// Load a proving key from file and deserialize it.
pub fn load_and_deserialize_pk(path: &str) -> Result<ProvingKey<Bls12_381>, ZKError> {
    let bytes = std::fs::read(path)
        .map_err(|e| ZKError::KeyLoadError(format!("Failed to read proving key: {}", e)))?;
    deserialize_proving_key(&bytes)
}

/// Check if a key file exists.
pub fn key_file_exists(path: &str) -> bool {
    Path::new(path).exists()
}

/// Get the size of a key file in bytes.
pub fn key_file_size(path: &str) -> Result<u64, ZKError> {
    std::fs::metadata(path)
        .map(|m| m.len())
        .map_err(|e| ZKError::KeyLoadError(format!("Failed to get key file size: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_key_cache_new() {
        let cache = KeyCache::new();
        assert_eq!(cache.stats().unwrap(), (0, 0));
    }
    
    #[test]
    fn test_key_cache_clear() {
        let cache = KeyCache::new();
        cache.load_verification_key_bytes(vec![1, 2, 3], "test_vk").unwrap();
        cache.load_proving_key_bytes(vec![4, 5, 6], "test_pk").unwrap();
        assert_eq!(cache.stats().unwrap(), (1, 1));
        cache.clear().unwrap();
        assert_eq!(cache.stats().unwrap(), (0, 0));
    }
    
    #[test]
    fn test_key_cache_stats() {
        let cache = KeyCache::new();
        cache.load_verification_key_bytes(vec![1, 2, 3], "vk1").unwrap();
        cache.load_verification_key_bytes(vec![4, 5, 6], "vk2").unwrap();
        cache.load_proving_key_bytes(vec![7, 8, 9], "pk1").unwrap();
        assert_eq!(cache.stats().unwrap(), (2, 1));
    }
    
    #[test]
    fn test_key_file_exists_nonexistent() {
        assert!(!key_file_exists("/nonexistent/path.vk"));
    }
}

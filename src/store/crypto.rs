use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use hkdf::Hkdf;
use sha2::Sha256;
use blake3;

pub struct SpaceKey {
    key: [u8; 32],
}

impl SpaceKey {
    pub fn from_passphrase(passphrase: &str, space_id: &[u8]) -> Self {
        let hk = Hkdf::<Sha256>::new(None, passphrase.as_bytes());
        let mut okm = [0u8; 32];
        hk.expand(space_id, &mut okm).expect("HKDF expansion failed");
        Self { key: okm }
    }

    pub fn encrypt_blob(&self, plaintext: &[u8], blob_hash: &str) -> Vec<u8> {
        let cipher = Aes256Gcm::new_from_slice(&self.key).unwrap();
        
        // Deterministic nonce based on blob hash to preserve deduplication across encrypted space
        let mut hasher = blake3::Hasher::new();
        hasher.update(blob_hash.as_bytes());
        let hash = hasher.finalize();
        let nonce = Nonce::from_slice(&hash.as_bytes()[..12]);

        cipher.encrypt(nonce, plaintext).expect("Encryption failed")
    }

    pub fn decrypt_blob(&self, ciphertext: &[u8], blob_hash: &str) -> Result<Vec<u8>, String> {
        let cipher = Aes256Gcm::new_from_slice(&self.key).unwrap();
        
        let mut hasher = blake3::Hasher::new();
        hasher.update(blob_hash.as_bytes());
        let hash = hasher.finalize();
        let nonce = Nonce::from_slice(&hash.as_bytes()[..12]);

        cipher.decrypt(nonce, ciphertext).map_err(|e| e.to_string())
    }

    pub fn encrypt_metadata(&self, plaintext: &[u8]) -> Vec<u8> {
        let cipher = Aes256Gcm::new_from_slice(&self.key).unwrap();
        
        // Random nonce for metadata since it changes frequently and isn't deduplicated
        use rand::{RngCore, thread_rng};
        let mut nonce_bytes = [0u8; 12];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let mut encrypted = cipher.encrypt(nonce, plaintext).expect("Metadata encryption failed");
        // Prepend nonce to the ciphertext
        let mut result = nonce_bytes.to_vec();
        result.append(&mut encrypted);
        result
    }

    pub fn decrypt_metadata(&self, ciphertext: &[u8]) -> Result<Vec<u8>, String> {
        if ciphertext.len() < 12 {
            return Err("Invalid metadata ciphertext".to_string());
        }
        let cipher = Aes256Gcm::new_from_slice(&self.key).unwrap();
        let nonce = Nonce::from_slice(&ciphertext[..12]);
        cipher.decrypt(nonce, &ciphertext[12..]).map_err(|e| e.to_string())
    }
}

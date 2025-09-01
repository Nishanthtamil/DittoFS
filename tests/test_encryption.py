"""
Tests for the encryption module.
"""

import os
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch

from dittofs.encryption import (
    KeyManager, ChunkEncryption, EncryptedChunk,
    create_encryption_system, load_encryption_system,
    AES_KEY_SIZE, SALT_SIZE, AES_IV_SIZE, HMAC_SIZE
)
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.backends import default_backend


class TestEncryptedChunk:
    """Test EncryptedChunk serialization and deserialization."""
    
    def test_encrypted_chunk_roundtrip(self):
        """Test serialization and deserialization of EncryptedChunk."""
        original = EncryptedChunk(
            encrypted_data=b"encrypted_test_data",
            iv=os.urandom(AES_IV_SIZE),
            salt=os.urandom(SALT_SIZE),
            hmac_tag=os.urandom(HMAC_SIZE),
            key_id="test_key_123"
        )
        
        # Serialize to bytes
        serialized = original.to_bytes()
        
        # Deserialize back
        deserialized = EncryptedChunk.from_bytes(serialized)
        
        # Verify all fields match
        assert deserialized.encrypted_data == original.encrypted_data
        assert deserialized.iv == original.iv
        assert deserialized.salt == original.salt
        assert deserialized.hmac_tag == original.hmac_tag
        assert deserialized.key_id == original.key_id
    
    def test_encrypted_chunk_invalid_data(self):
        """Test handling of invalid serialized data."""
        # Too short data
        with pytest.raises(ValueError, match="too short"):
            EncryptedChunk.from_bytes(b"short")
        
        # Invalid key_id length
        invalid_data = os.urandom(SALT_SIZE + AES_IV_SIZE + HMAC_SIZE) + b"\x00\xFF"  # Invalid length
        with pytest.raises(ValueError, match="key_id length mismatch"):
            EncryptedChunk.from_bytes(invalid_data)


class TestKeyManager:
    """Test KeyManager functionality."""
    
    def test_key_manager_initialization(self):
        """Test KeyManager initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_store_path = Path(temp_dir) / "keys"
            key_manager = KeyManager(key_store_path)
            
            assert key_manager.key_store_path == key_store_path
            assert key_store_path.exists()
    
    def test_master_key_derivation(self):
        """Test master key derivation from password."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            
            password = "test_password_123"
            salt = key_manager.set_master_key(password)
            
            assert len(salt) == SALT_SIZE
            assert key_manager._master_key is not None
            assert len(key_manager._master_key) == AES_KEY_SIZE
            
            # Salt file should be created
            salt_file = key_manager.key_store_path / "master.salt"
            assert salt_file.exists()
            assert salt_file.read_bytes() == salt
    
    def test_master_key_loading(self):
        """Test loading master key from stored salt."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            
            password = "test_password_123"
            original_salt = key_manager.set_master_key(password)
            original_key = key_manager._master_key
            
            # Create new key manager and load the key
            key_manager2 = KeyManager(Path(temp_dir) / "keys")
            success = key_manager2.load_master_key(password)
            
            assert success
            assert key_manager2._master_key == original_key
    
    def test_master_key_loading_wrong_password(self):
        """Test loading master key with wrong password."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            
            # Set key with one password
            key_manager.set_master_key("correct_password")
            
            # Try to load with wrong password
            key_manager2 = KeyManager(Path(temp_dir) / "keys")
            success = key_manager2.load_master_key("wrong_password")
            
            assert success  # Loading will succeed but key will be different
            assert key_manager2._master_key != key_manager._master_key
    
    def test_master_key_loading_no_salt_file(self):
        """Test loading master key when no salt file exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            
            success = key_manager.load_master_key("any_password")
            assert not success
    
    def test_chunk_key_derivation(self):
        """Test chunk key derivation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            key_manager.set_master_key("test_password")
            
            chunk_hash = "abcdef123456789"
            chunk_key = key_manager.derive_chunk_key(chunk_hash)
            
            assert len(chunk_key) == AES_KEY_SIZE
            
            # Same hash should produce same key
            chunk_key2 = key_manager.derive_chunk_key(chunk_hash)
            assert chunk_key == chunk_key2
            
            # Different hash should produce different key
            different_key = key_manager.derive_chunk_key("different_hash")
            assert different_key != chunk_key
    
    def test_chunk_key_derivation_no_master_key(self):
        """Test chunk key derivation without master key."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            
            with pytest.raises(ValueError, match="Master key not set"):
                key_manager.derive_chunk_key("test_hash")
    
    def test_key_cache_management(self):
        """Test key cache functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            key_manager.set_master_key("test_password")
            
            chunk_hash = "test_hash_123"
            
            # First derivation should cache the key
            key1 = key_manager.derive_chunk_key(chunk_hash)
            assert chunk_hash in key_manager._derived_keys
            
            # Second derivation should use cache
            key2 = key_manager.derive_chunk_key(chunk_hash)
            assert key1 == key2
            
            # Clear cache
            key_manager.clear_key_cache()
            assert len(key_manager._derived_keys) == 0
            
            # Should still work after cache clear
            key3 = key_manager.derive_chunk_key(chunk_hash)
            assert key3 == key1  # Should be same key


class TestChunkEncryption:
    """Test ChunkEncryption functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.key_manager = KeyManager(Path(self.temp_dir) / "keys")
        self.key_manager.set_master_key("test_password_123")
        self.chunk_encryption = ChunkEncryption(self.key_manager)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_chunk_encryption_roundtrip(self):
        """Test encrypting and decrypting a chunk."""
        chunk_data = b"This is test chunk data for encryption testing"
        chunk_hash = "test_chunk_hash_123"
        
        # Encrypt the chunk
        encrypted_chunk = self.chunk_encryption.encrypt_chunk(chunk_data, chunk_hash)
        
        # Verify encrypted chunk structure
        assert isinstance(encrypted_chunk, EncryptedChunk)
        assert len(encrypted_chunk.iv) == AES_IV_SIZE
        assert len(encrypted_chunk.salt) == SALT_SIZE
        assert len(encrypted_chunk.hmac_tag) == HMAC_SIZE
        assert encrypted_chunk.key_id == chunk_hash[:16]
        assert encrypted_chunk.encrypted_data != chunk_data  # Should be different
        
        # Decrypt the chunk
        decrypted_data = self.chunk_encryption.decrypt_chunk(encrypted_chunk, chunk_hash)
        
        # Verify decryption
        assert decrypted_data == chunk_data
    
    def test_chunk_encryption_empty_data(self):
        """Test encrypting empty chunk data."""
        with pytest.raises(ValueError, match="Cannot encrypt empty chunk data"):
            self.chunk_encryption.encrypt_chunk(b"", "test_hash")
    
    def test_chunk_encryption_different_hashes(self):
        """Test that different chunk hashes produce different encrypted data."""
        chunk_data = b"Same data for different hashes"
        
        encrypted1 = self.chunk_encryption.encrypt_chunk(chunk_data, "hash1")
        encrypted2 = self.chunk_encryption.encrypt_chunk(chunk_data, "hash2")
        
        # Should produce different encrypted data due to different keys
        assert encrypted1.encrypted_data != encrypted2.encrypted_data
        assert encrypted1.key_id != encrypted2.key_id
    
    def test_chunk_decryption_wrong_hash(self):
        """Test decrypting with wrong chunk hash."""
        chunk_data = b"Test data for wrong hash test"
        chunk_hash = "correct_hash"
        wrong_hash = "wrong_hash"
        
        encrypted_chunk = self.chunk_encryption.encrypt_chunk(chunk_data, chunk_hash)
        
        # Should fail with wrong hash (different key)
        with pytest.raises(ValueError, match="HMAC verification failed"):
            self.chunk_encryption.decrypt_chunk(encrypted_chunk, wrong_hash)
    
    def test_chunk_integrity_verification(self):
        """Test chunk integrity verification."""
        chunk_data = b"Test data for integrity verification"
        chunk_hash = "integrity_test_hash"
        
        encrypted_chunk = self.chunk_encryption.encrypt_chunk(chunk_data, chunk_hash)
        
        # Should verify successfully
        assert self.chunk_encryption.verify_chunk_integrity(encrypted_chunk, chunk_hash)
        
        # Should fail with wrong hash
        assert not self.chunk_encryption.verify_chunk_integrity(encrypted_chunk, "wrong_hash")
        
        # Should fail with corrupted data
        corrupted_chunk = EncryptedChunk(
            encrypted_data=encrypted_chunk.encrypted_data[:-1] + b"X",  # Corrupt last byte
            iv=encrypted_chunk.iv,
            salt=encrypted_chunk.salt,
            hmac_tag=encrypted_chunk.hmac_tag,
            key_id=encrypted_chunk.key_id
        )
        assert not self.chunk_encryption.verify_chunk_integrity(corrupted_chunk, chunk_hash)
    
    def test_chunk_decryption_corrupted_data(self):
        """Test decryption of corrupted encrypted data."""
        chunk_data = b"Test data for corruption test"
        chunk_hash = "corruption_test_hash"
        
        encrypted_chunk = self.chunk_encryption.encrypt_chunk(chunk_data, chunk_hash)
        
        # Corrupt the encrypted data
        corrupted_chunk = EncryptedChunk(
            encrypted_data=encrypted_chunk.encrypted_data[:-1] + b"X",
            iv=encrypted_chunk.iv,
            salt=encrypted_chunk.salt,
            hmac_tag=encrypted_chunk.hmac_tag,
            key_id=encrypted_chunk.key_id
        )
        
        with pytest.raises(ValueError, match="HMAC verification failed"):
            self.chunk_encryption.decrypt_chunk(corrupted_chunk, chunk_hash)
    
    def test_chunk_decryption_invalid_encrypted_data(self):
        """Test decryption with invalid encrypted data format."""
        chunk_hash = "test_hash"
        
        # Create a valid encrypted chunk first
        valid_data = b"test data for HMAC generation"
        valid_encrypted = self.chunk_encryption.encrypt_chunk(valid_data, chunk_hash)
        
        # Create chunk with short encrypted data but compute correct HMAC for it
        short_encrypted_data = b"short"
        chunk_key = self.key_manager.derive_chunk_key(chunk_hash)
        
        # Compute correct HMAC for the short data
        h = hmac.HMAC(chunk_key, hashes.SHA256(), backend=default_backend())
        h.update(valid_encrypted.salt + valid_encrypted.iv + short_encrypted_data)
        correct_hmac = h.finalize()
        
        invalid_chunk = EncryptedChunk(
            encrypted_data=short_encrypted_data,  # Too short for GCM tag
            iv=valid_encrypted.iv,
            salt=valid_encrypted.salt,
            hmac_tag=correct_hmac,  # Correct HMAC for the short data
            key_id="test_key"
        )
        
        # This should now pass HMAC verification but fail at GCM tag length check
        with pytest.raises(ValueError, match="too short for GCM tag"):
            self.chunk_encryption.decrypt_chunk(invalid_chunk, chunk_hash)


class TestEncryptionSystemIntegration:
    """Test complete encryption system integration."""
    
    def test_create_encryption_system(self):
        """Test creating a new encryption system."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_store_path = Path(temp_dir) / "keys"
            password = "integration_test_password"
            
            key_manager, chunk_encryption = create_encryption_system(password, key_store_path)
            
            assert isinstance(key_manager, KeyManager)
            assert isinstance(chunk_encryption, ChunkEncryption)
            assert key_manager.key_store_path == key_store_path
            assert key_manager._master_key is not None
    
    def test_load_encryption_system(self):
        """Test loading an existing encryption system."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_store_path = Path(temp_dir) / "keys"
            password = "load_test_password"
            
            # Create system first
            key_manager1, chunk_encryption1 = create_encryption_system(password, key_store_path)
            original_key = key_manager1._master_key
            
            # Load the system
            result = load_encryption_system(password, key_store_path)
            assert result is not None
            
            key_manager2, chunk_encryption2 = result
            assert key_manager2._master_key == original_key
    
    def test_load_encryption_system_no_existing(self):
        """Test loading encryption system when none exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_store_path = Path(temp_dir) / "keys"
            password = "nonexistent_test"
            
            result = load_encryption_system(password, key_store_path)
            assert result is None
    
    def test_encryption_system_end_to_end(self):
        """Test complete end-to-end encryption workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_store_path = Path(temp_dir) / "keys"
            password = "end_to_end_test_password"
            
            # Create encryption system
            key_manager, chunk_encryption = create_encryption_system(password, key_store_path)
            
            # Test data
            test_chunks = [
                (b"First chunk data", "hash1"),
                (b"Second chunk with different content", "hash2"),
                (b"Third chunk for comprehensive testing", "hash3")
            ]
            
            encrypted_chunks = []
            
            # Encrypt all chunks
            for chunk_data, chunk_hash in test_chunks:
                encrypted_chunk = chunk_encryption.encrypt_chunk(chunk_data, chunk_hash)
                encrypted_chunks.append((encrypted_chunk, chunk_hash))
                
                # Verify integrity
                assert chunk_encryption.verify_chunk_integrity(encrypted_chunk, chunk_hash)
            
            # Create new encryption system (simulating restart)
            result = load_encryption_system(password, key_store_path)
            assert result is not None
            key_manager2, chunk_encryption2 = result
            
            # Decrypt all chunks with new system
            for i, (encrypted_chunk, chunk_hash) in enumerate(encrypted_chunks):
                decrypted_data = chunk_encryption2.decrypt_chunk(encrypted_chunk, chunk_hash)
                original_data = test_chunks[i][0]
                assert decrypted_data == original_data


@pytest.mark.crypto
class TestEncryptionPerformance:
    """Test encryption performance characteristics."""
    
    def test_large_chunk_encryption(self):
        """Test encryption of large chunks."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            key_manager.set_master_key("performance_test_password")
            chunk_encryption = ChunkEncryption(key_manager)
            
            # Create 1MB chunk
            large_chunk = os.urandom(1024 * 1024)
            chunk_hash = "large_chunk_hash"
            
            # Should handle large chunks without issues
            encrypted_chunk = chunk_encryption.encrypt_chunk(large_chunk, chunk_hash)
            decrypted_data = chunk_encryption.decrypt_chunk(encrypted_chunk, chunk_hash)
            
            assert decrypted_data == large_chunk
    
    def test_key_derivation_caching(self):
        """Test that key derivation caching improves performance."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            key_manager.set_master_key("caching_test_password")
            
            chunk_hash = "cache_test_hash"
            
            # First derivation (should be slower)
            import time
            start_time = time.time()
            key1 = key_manager.derive_chunk_key(chunk_hash)
            first_time = time.time() - start_time
            
            # Second derivation (should be faster due to caching)
            start_time = time.time()
            key2 = key_manager.derive_chunk_key(chunk_hash)
            second_time = time.time() - start_time
            
            assert key1 == key2
            # Second call should be significantly faster (cached)
            assert second_time < first_time / 2  # At least 2x faster


@pytest.mark.security
class TestEncryptionSecurity:
    """Test security aspects of encryption implementation."""
    
    def test_different_passwords_different_keys(self):
        """Test that different passwords produce different keys."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_store_path = Path(temp_dir) / "keys"
            
            # Create two systems with different passwords
            km1, _ = create_encryption_system("password1", key_store_path / "sys1")
            km2, _ = create_encryption_system("password2", key_store_path / "sys2")
            
            # Should produce different master keys
            assert km1._master_key != km2._master_key
            
            # Should produce different chunk keys for same hash
            chunk_hash = "same_hash"
            key1 = km1.derive_chunk_key(chunk_hash)
            key2 = km2.derive_chunk_key(chunk_hash)
            assert key1 != key2
    
    def test_salt_randomness(self):
        """Test that salts are properly randomized."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            key_manager.set_master_key("salt_test_password")
            chunk_encryption = ChunkEncryption(key_manager)
            
            chunk_data = b"Same data for salt test"
            chunk_hash = "salt_test_hash"
            
            # Encrypt same data multiple times
            encrypted1 = chunk_encryption.encrypt_chunk(chunk_data, chunk_hash)
            encrypted2 = chunk_encryption.encrypt_chunk(chunk_data, chunk_hash)
            
            # Should have different salts and IVs
            assert encrypted1.salt != encrypted2.salt
            assert encrypted1.iv != encrypted2.iv
            # But same key_id (derived from hash)
            assert encrypted1.key_id == encrypted2.key_id
    
    def test_iv_uniqueness(self):
        """Test that IVs are unique for each encryption."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            key_manager.set_master_key("iv_test_password")
            chunk_encryption = ChunkEncryption(key_manager)
            
            chunk_data = b"IV uniqueness test data"
            chunk_hash = "iv_test_hash"
            
            ivs = set()
            for _ in range(10):
                encrypted_chunk = chunk_encryption.encrypt_chunk(chunk_data, chunk_hash)
                ivs.add(encrypted_chunk.iv)
            
            # All IVs should be unique
            assert len(ivs) == 10
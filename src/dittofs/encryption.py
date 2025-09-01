"""
Encryption module for DittoFS chunk encryption at rest.

This module provides AES-256 encryption for individual chunks with unique keys,
secure key derivation using Argon2, and key management for chunk encryption keys.
"""

import hashlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

# Encryption configuration
AES_KEY_SIZE = 32  # 256 bits
AES_IV_SIZE = 16  # 128 bits for AES-GCM
SALT_SIZE = 32  # 256 bits
HMAC_SIZE = 32  # 256 bits for SHA-256
KDF_ITERATIONS = 100000  # PBKDF2 iterations

# Key derivation configuration
SCRYPT_N = 2**14  # CPU/memory cost parameter (16384)
SCRYPT_R = 8  # Block size parameter
SCRYPT_P = 1  # Parallelization parameter


@dataclass
class EncryptedChunk:
    """Represents an encrypted chunk with metadata."""

    encrypted_data: bytes
    iv: bytes
    salt: bytes
    hmac_tag: bytes
    key_id: str

    def to_bytes(self) -> bytes:
        """Serialize encrypted chunk to bytes for storage."""
        # Format: [salt][iv][hmac_tag][key_id_len][key_id][encrypted_data]
        key_id_bytes = self.key_id.encode("utf-8")
        key_id_len = len(key_id_bytes).to_bytes(2, "big")

        return (
            self.salt
            + self.iv
            + self.hmac_tag
            + key_id_len
            + key_id_bytes
            + self.encrypted_data
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "EncryptedChunk":
        """Deserialize encrypted chunk from bytes."""
        if len(data) < SALT_SIZE + AES_IV_SIZE + HMAC_SIZE + 2:
            raise ValueError("Invalid encrypted chunk data: too short")

        offset = 0
        salt = data[offset : offset + SALT_SIZE]
        offset += SALT_SIZE

        iv = data[offset : offset + AES_IV_SIZE]
        offset += AES_IV_SIZE

        hmac_tag = data[offset : offset + HMAC_SIZE]
        offset += HMAC_SIZE

        key_id_len = int.from_bytes(data[offset : offset + 2], "big")
        offset += 2

        if len(data) < offset + key_id_len:
            raise ValueError("Invalid encrypted chunk data: key_id length mismatch")

        key_id = data[offset : offset + key_id_len].decode("utf-8")
        offset += key_id_len

        encrypted_data = data[offset:]

        return cls(
            encrypted_data=encrypted_data,
            iv=iv,
            salt=salt,
            hmac_tag=hmac_tag,
            key_id=key_id,
        )


class KeyManager:
    """Manages encryption keys for chunks."""

    def __init__(self, key_store_path: Optional[Path] = None):
        """Initialize key manager with optional key store path."""
        self.key_store_path = key_store_path or (Path.home() / ".dittofs" / "keys")
        self.key_store_path.mkdir(parents=True, exist_ok=True)
        self._master_key: Optional[bytes] = None
        self._derived_keys: Dict[str, bytes] = {}

    def set_master_key(self, password: str, salt: Optional[bytes] = None) -> bytes:
        """Set master key derived from password using Scrypt KDF."""
        if salt is None:
            salt = os.urandom(SALT_SIZE)

        # Use Scrypt for master key derivation (more secure than PBKDF2)
        kdf = Scrypt(
            length=AES_KEY_SIZE,
            salt=salt,
            n=SCRYPT_N,
            r=SCRYPT_R,
            p=SCRYPT_P,
            backend=default_backend(),
        )

        self._master_key = kdf.derive(password.encode("utf-8"))

        # Store salt for future key derivation
        salt_file = self.key_store_path / "master.salt"
        salt_file.write_bytes(salt)

        logging.info("Master key derived and set")
        return salt

    def load_master_key(self, password: str) -> bool:
        """Load master key from stored salt and password."""
        salt_file = self.key_store_path / "master.salt"
        if not salt_file.exists():
            logging.error("Master key salt file not found")
            return False

        try:
            salt = salt_file.read_bytes()
            self.set_master_key(password, salt)
            return True
        except Exception as e:
            logging.error(f"Failed to load master key: {e}")
            return False

    def derive_chunk_key(self, chunk_hash: str) -> bytes:
        """Derive a unique key for a specific chunk."""
        if self._master_key is None:
            raise ValueError("Master key not set")

        # Check cache first
        if chunk_hash in self._derived_keys:
            return self._derived_keys[chunk_hash]

        # Derive key using PBKDF2 with chunk hash as salt
        chunk_salt = hashlib.sha256(chunk_hash.encode("utf-8")).digest()

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=AES_KEY_SIZE,
            salt=chunk_salt,
            iterations=KDF_ITERATIONS,
            backend=default_backend(),
        )

        derived_key = kdf.derive(self._master_key)

        # Cache the derived key
        self._derived_keys[chunk_hash] = derived_key

        return derived_key

    def clear_key_cache(self) -> None:
        """Clear the derived key cache."""
        self._derived_keys.clear()
        logging.info("Key cache cleared")


class ChunkEncryption:
    """Handles encryption and decryption of individual chunks."""

    def __init__(self, key_manager: KeyManager):
        """Initialize with a key manager."""
        self.key_manager = key_manager

    def encrypt_chunk(self, chunk_data: bytes, chunk_hash: str) -> EncryptedChunk:
        """Encrypt a chunk with AES-256-GCM."""
        if not chunk_data:
            raise ValueError("Cannot encrypt empty chunk data")

        # Derive unique key for this chunk
        chunk_key = self.key_manager.derive_chunk_key(chunk_hash)

        # Generate random IV and salt for this encryption
        iv = os.urandom(AES_IV_SIZE)
        salt = os.urandom(SALT_SIZE)

        # Create cipher
        cipher = Cipher(
            algorithms.AES(chunk_key), modes.GCM(iv), backend=default_backend()
        )
        encryptor = cipher.encryptor()

        # Encrypt the data
        encrypted_data = encryptor.update(chunk_data) + encryptor.finalize()

        # Get the authentication tag from GCM mode
        auth_tag = encryptor.tag

        # Create HMAC for additional integrity verification
        h = hmac.HMAC(chunk_key, hashes.SHA256(), backend=default_backend())
        h.update(salt + iv + encrypted_data + auth_tag)
        hmac_tag = h.finalize()

        return EncryptedChunk(
            encrypted_data=encrypted_data + auth_tag,  # Include GCM auth tag
            iv=iv,
            salt=salt,
            hmac_tag=hmac_tag,
            key_id=chunk_hash[:16],  # Use first 16 chars of hash as key ID
        )

    def decrypt_chunk(self, encrypted_chunk: EncryptedChunk, chunk_hash: str) -> bytes:
        """Decrypt a chunk and verify integrity."""
        # Derive the same key used for encryption
        chunk_key = self.key_manager.derive_chunk_key(chunk_hash)

        # Verify HMAC first
        h = hmac.HMAC(chunk_key, hashes.SHA256(), backend=default_backend())
        h.update(
            encrypted_chunk.salt + encrypted_chunk.iv + encrypted_chunk.encrypted_data
        )

        try:
            h.verify(encrypted_chunk.hmac_tag)
        except InvalidSignature:
            raise ValueError(
                "HMAC verification failed - chunk may be corrupted or tampered"
            )

        # Split encrypted data and GCM auth tag
        if len(encrypted_chunk.encrypted_data) < 16:
            raise ValueError("Invalid encrypted data: too short for GCM tag")

        encrypted_data = encrypted_chunk.encrypted_data[:-16]
        auth_tag = encrypted_chunk.encrypted_data[-16:]

        # Create cipher for decryption
        cipher = Cipher(
            algorithms.AES(chunk_key),
            modes.GCM(encrypted_chunk.iv, auth_tag),
            backend=default_backend(),
        )
        decryptor = cipher.decryptor()

        try:
            # Decrypt and verify
            decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
            return decrypted_data
        except Exception as e:
            raise ValueError(f"Decryption failed: {e}")

    def verify_chunk_integrity(
        self, encrypted_chunk: EncryptedChunk, chunk_hash: str
    ) -> bool:
        """Verify chunk integrity without full decryption."""
        try:
            chunk_key = self.key_manager.derive_chunk_key(chunk_hash)

            # Verify HMAC
            h = hmac.HMAC(chunk_key, hashes.SHA256(), backend=default_backend())
            h.update(
                encrypted_chunk.salt
                + encrypted_chunk.iv
                + encrypted_chunk.encrypted_data
            )
            h.verify(encrypted_chunk.hmac_tag)

            return True
        except (InvalidSignature, Exception):
            return False


def create_encryption_system(
    password: str, key_store_path: Optional[Path] = None
) -> Tuple[KeyManager, ChunkEncryption]:
    """Create a complete encryption system with key manager and chunk encryption."""
    key_manager = KeyManager(key_store_path)
    key_manager.set_master_key(password)
    chunk_encryption = ChunkEncryption(key_manager)

    return key_manager, chunk_encryption


def load_encryption_system(
    password: str, key_store_path: Optional[Path] = None
) -> Optional[Tuple[KeyManager, ChunkEncryption]]:
    """Load an existing encryption system from stored keys."""
    key_manager = KeyManager(key_store_path)

    if not key_manager.load_master_key(password):
        return None

    chunk_encryption = ChunkEncryption(key_manager)
    return key_manager, chunk_encryption

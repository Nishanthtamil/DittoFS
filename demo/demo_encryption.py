#!/usr/bin/env python3
"""
Demo script showing DittoFS chunk encryption at rest functionality.

This script demonstrates:
1. AES-256 encryption for individual chunks with unique keys
2. Secure key derivation using Scrypt and PBKDF2
3. Key management system for chunk encryption keys
4. Encrypted chunk storage with integrity verification
"""

import os

# Add src to path for imports
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "src")

from dittofs.chunker import (
    disable_encryption,
    enable_encryption,
    get_chunk_stats,
    join,
    split,
)
from dittofs.encryption import create_encryption_system, load_encryption_system


def demo_encryption():
    """Demonstrate encryption functionality."""
    print("=== DittoFS Chunk Encryption Demo ===\n")

    # Create a test file
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
        test_content = """This is a test file for demonstrating DittoFS encryption.
It contains multiple lines of text that will be split into chunks.
Each chunk will be encrypted with AES-256 using unique keys.
The encryption uses Scrypt for master key derivation and PBKDF2 for chunk keys.
This provides strong security for data at rest."""
        f.write(test_content)
        test_file = Path(f.name)

    try:
        print(f"Created test file: {test_file}")
        print(f"File size: {test_file.stat().st_size} bytes")
        print(f"Content preview: {test_content[:100]}...\n")

        # Enable encryption first
        print("1. Testing with encryption enabled:")
        password = "demo_encryption_password_123"
        success = enable_encryption(password)
        print(f"   Encryption enabled: {success}")

        # Split file with encryption
        encrypted_hashes = split(test_file)
        encrypted_stats = get_chunk_stats(encrypted_hashes)
        print(f"   Chunks created: {encrypted_stats['count']}")
        print(f"   Total size: {encrypted_stats['total_size']} bytes")
        print(f"   Encrypted chunks: {encrypted_stats['encrypted_count']}")
        print()

        # Test without encryption on a different file
        print("2. Testing without encryption (for comparison):")
        disable_encryption()

        # Create another test file
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f2:
            f2.write(test_content)
            test_file2 = Path(f2.name)

        unencrypted_hashes = split(test_file2)
        unencrypted_stats = get_chunk_stats(unencrypted_hashes)
        print(f"   Chunks created: {unencrypted_stats['count']}")
        print(f"   Total size: {unencrypted_stats['total_size']} bytes")
        print(f"   Encrypted chunks: {unencrypted_stats['encrypted_count']}")
        print()

        # Re-enable encryption for remaining tests
        enable_encryption(password)

        # Test reconstruction
        print("3. Testing encrypted file reconstruction:")
        reconstructed_file = test_file.parent / "reconstructed.txt"
        success = join(encrypted_hashes, reconstructed_file)
        print(f"   Reconstruction successful: {success}")

        if success:
            reconstructed_content = reconstructed_file.read_text()
            print(f"   Content matches: {reconstructed_content == test_content}")
            print(f"   Reconstructed size: {reconstructed_file.stat().st_size} bytes")
        print()

        # Test encryption system persistence
        print("4. Testing encryption system persistence:")
        disable_encryption()

        # Try to load the encryption system
        success = enable_encryption(password)
        print(f"   Encryption reloaded: {success}")

        # Test that we can still decrypt the chunks
        if success:
            reconstructed_file2 = test_file.parent / "reconstructed2.txt"
            success = join(encrypted_hashes, reconstructed_file2)
            print(f"   Decryption after reload: {success}")

            if success:
                content_matches = reconstructed_file2.read_text() == test_content
                print(f"   Content still matches: {content_matches}")
        print()

        # Demonstrate key derivation
        print("5. Demonstrating key derivation:")
        from dittofs.encryption import KeyManager

        with tempfile.TemporaryDirectory() as temp_dir:
            key_manager = KeyManager(Path(temp_dir) / "keys")
            key_manager.set_master_key("demo_password")

            # Show that same chunk hash produces same key
            chunk_hash = "example_chunk_hash_123"
            key1 = key_manager.derive_chunk_key(chunk_hash)
            key2 = key_manager.derive_chunk_key(chunk_hash)
            print(f"   Same hash produces same key: {key1 == key2}")

            # Show that different hashes produce different keys
            different_key = key_manager.derive_chunk_key("different_hash")
            print(f"   Different hash produces different key: {key1 != different_key}")
            print(f"   Key length: {len(key1)} bytes (AES-256)")

        print("\n=== Demo completed successfully! ===")

    finally:
        # Cleanup
        disable_encryption()
        if test_file.exists():
            test_file.unlink()
        if "test_file2" in locals() and test_file2.exists():
            test_file2.unlink()
        reconstructed = test_file.parent / "reconstructed.txt"
        if reconstructed.exists():
            reconstructed.unlink()
        reconstructed2 = test_file.parent / "reconstructed2.txt"
        if reconstructed2.exists():
            reconstructed2.unlink()


if __name__ == "__main__":
    demo_encryption()

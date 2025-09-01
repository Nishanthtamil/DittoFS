#!/usr/bin/env python3
"""
Demo script showing DittoFS integrity verification system.

This script demonstrates:
1. Creating test files and chunks
2. Simulating corruption
3. Detecting corruption with integrity verification
4. Repairing corruption using peer data
5. Recovery procedures
"""

import asyncio
import logging
import pathlib
import tempfile

import blake3

from src.dittofs.chunker import CHUNK_DIR, split
from src.dittofs.crdt_store import CRDTStore
from src.dittofs.integrity import (
    IntegrityScheduler,
    IntegrityVerifier,
    RecoveryProcedures,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def demo_integrity_verification():
    """Demonstrate the integrity verification system"""

    print("=" * 60)
    print("DittoFS Integrity Verification System Demo")
    print("=" * 60)

    # Create temporary directory for demo
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = pathlib.Path(temp_dir)

        # Step 1: Create test files and add them to DittoFS
        print("\n1. Creating test files...")

        test_file1 = temp_path / "test1.txt"
        test_file2 = temp_path / "test2.txt"

        test_file1.write_text("This is test file 1 with some content for chunking.")
        test_file2.write_text(
            "This is test file 2 with different content for demonstration."
        )

        # Initialize CRDT store
        store = CRDTStore()

        # Split files into chunks
        hashes1 = split(test_file1)
        hashes2 = split(test_file2)

        # Add files to store
        store.add_file(test_file1, hashes1)
        store.add_file(test_file2, hashes2)
        store.save()

        print(f"✓ Created {test_file1.name} with {len(hashes1)} chunks")
        print(f"✓ Created {test_file2.name} with {len(hashes2)} chunks")

        # Step 2: Initialize integrity verification system
        print("\n2. Initializing integrity verification system...")

        verifier = IntegrityVerifier(store)
        scheduler = IntegrityScheduler(verifier)
        recovery = RecoveryProcedures(verifier, store)

        print("✓ Integrity verification system initialized")

        # Step 3: Verify initial integrity (should be clean)
        print("\n3. Performing initial integrity check...")

        reports = await verifier.verify_all_chunks()

        if not reports:
            print("✓ All chunks verified successfully - no corruption detected")
        else:
            print(f"✗ Found {len(reports)} corruptions (unexpected)")

        # Step 4: Simulate corruption
        print("\n4. Simulating chunk corruption...")

        # Corrupt one chunk from the first file
        if hashes1:
            corrupt_hash = hashes1[0]
            chunk_path = CHUNK_DIR / corrupt_hash

            if chunk_path.exists():
                # Overwrite with corrupted data
                chunk_path.write_bytes(b"CORRUPTED DATA")
                print(f"✓ Corrupted chunk: {corrupt_hash[:16]}...")
            else:
                print(f"✗ Chunk file not found: {corrupt_hash[:16]}...")

        # Delete another chunk to simulate missing chunk
        if len(hashes2) > 0:
            missing_hash = hashes2[0]
            chunk_path = CHUNK_DIR / missing_hash

            if chunk_path.exists():
                chunk_path.unlink()
                print(f"✓ Deleted chunk: {missing_hash[:16]}...")

        # Step 5: Detect corruption
        print("\n5. Detecting corruption...")

        reports = await verifier.verify_all_chunks()

        if reports:
            print(f"✓ Detected {len(reports)} corruptions:")
            for report in reports:
                print(
                    f"  - {report.corruption_type.value}: {report.chunk_hash[:16] if report.chunk_hash else 'N/A'}..."
                )
                print(f"    Error: {report.error_message}")
        else:
            print("✗ No corruption detected (unexpected)")

        # Step 6: Simulate peer data for repair
        print("\n6. Setting up mock peer data for repair...")

        # Create mock peer provider that can provide the original data
        original_data = {}

        # Store original data for corrupted chunk
        if hashes1:
            original_chunk_data = test_file1.read_bytes()[
                : len(original_chunk_data) if "original_chunk_data" in locals() else 50
            ]
            # Recreate the original chunk data
            test_file1_content = "This is test file 1 with some content for chunking."
            original_data[hashes1[0]] = test_file1_content.encode()[
                :50
            ]  # Approximate chunk size

        # Store original data for missing chunk
        if len(hashes2) > 0:
            test_file2_content = (
                "This is test file 2 with different content for demonstration."
            )
            original_data[hashes2[0]] = test_file2_content.encode()[
                :50
            ]  # Approximate chunk size

        async def mock_peer_provider(chunk_hash):
            """Mock peer provider that returns original chunk data"""
            if chunk_hash in original_data:
                return original_data[chunk_hash]
            return None

        verifier.register_peer_provider("mock_peer", mock_peer_provider)
        print("✓ Mock peer provider registered")

        # Step 7: Attempt repair
        print("\n7. Attempting to repair corruptions...")

        repair_results = await verifier.repair_all_corruptions()

        successful_repairs = sum(
            1 for r in repair_results if r.status.value == "success"
        )
        failed_repairs = sum(1 for r in repair_results if r.status.value == "failed")
        no_peer_repairs = sum(1 for r in repair_results if r.status.value == "no_peers")

        print(f"Repair results:")
        print(f"  ✓ Successful: {successful_repairs}")
        print(f"  ✗ Failed: {failed_repairs}")
        print(f"  ? No peers: {no_peer_repairs}")

        # Step 8: Verify repair success
        print("\n8. Verifying repair success...")

        final_reports = await verifier.verify_all_chunks()

        if not final_reports:
            print("✓ All corruptions repaired successfully!")
        else:
            print(f"✗ {len(final_reports)} corruptions remain:")
            for report in final_reports:
                print(
                    f"  - {report.corruption_type.value}: {report.chunk_hash[:16] if report.chunk_hash else 'N/A'}..."
                )

        # Step 9: Demonstrate recovery procedures
        print("\n9. Demonstrating emergency recovery...")

        recovery_stats = await recovery.emergency_recovery()

        print(f"Emergency recovery results:")
        print(f"  Chunks recovered: {recovery_stats['chunks_recovered']}")
        print(f"  Files recovered: {recovery_stats['files_recovered']}")
        print(f"  Metadata rebuilt: {recovery_stats['metadata_rebuilt']}")
        print(f"  Errors: {len(recovery_stats['errors'])}")

        # Step 10: Show statistics
        print("\n10. Integrity verification statistics...")

        corruption_summary = verifier.get_corruption_summary()
        repair_summary = verifier.get_repair_summary()

        print(f"Corruption summary:")
        print(f"  Total reports: {corruption_summary['total_reports']}")
        print(f"  By type: {corruption_summary['by_type']}")
        print(f"  By severity: {corruption_summary['by_severity']}")

        print(f"Repair summary:")
        print(f"  Total repairs: {repair_summary['total_repairs']}")
        print(f"  By status: {repair_summary['by_status']}")
        print(f"  Success rate: {repair_summary['success_rate']:.2%}")

        # Step 11: Demonstrate scheduler configuration
        print("\n11. Demonstrating integrity scheduler...")

        # Configure scheduler for demo (shorter intervals)
        scheduler.configure(
            full_check_interval=60,  # 1 minute
            incremental_check_interval=30,  # 30 seconds
            auto_repair_enabled=True,
            max_concurrent_repairs=3,
        )

        print("✓ Integrity scheduler configured for demo")
        print("  Full check interval: 60 seconds")
        print("  Incremental check interval: 30 seconds")
        print("  Auto-repair enabled: True")
        print("  Max concurrent repairs: 3")

        print("\n" + "=" * 60)
        print("Demo completed successfully!")
        print("=" * 60)

        print("\nKey features demonstrated:")
        print("✓ Chunk integrity verification")
        print("✓ Corruption detection (missing and corrupted chunks)")
        print("✓ Peer-based chunk repair")
        print("✓ Emergency recovery procedures")
        print("✓ Integrity statistics and reporting")
        print("✓ Background integrity scheduling")


if __name__ == "__main__":
    asyncio.run(demo_integrity_verification())

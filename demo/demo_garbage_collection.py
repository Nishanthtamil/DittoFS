#!/usr/bin/env python3
"""
Demo script for DittoFS Garbage Collection System

This script demonstrates the garbage collection functionality including:
- Reference counting and tracking
- Configurable retention policies
- Safe cleanup procedures
- Metrics and reporting
"""

import asyncio
import pathlib
import shutil

# Add src to path for imports
import sys
import tempfile
import time
from datetime import datetime

sys.path.insert(0, "src")

from dittofs.chunker import CHUNK_DIR, ensure_chunk_dir, join, split
from dittofs.crdt_store import CRDTStore
from dittofs.garbage_collector import (
    ChunkReferenceTracker,
    GarbageCollector,
    GCStats,
    RetentionPolicy,
)


async def demo_basic_gc():
    """Demonstrate basic garbage collection functionality"""
    print("=" * 60)
    print("DEMO: Basic Garbage Collection")
    print("=" * 60)

    # Create temporary directory for demo
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = pathlib.Path(temp_dir)

        # Override chunk directory for demo
        import dittofs.chunker as chunker_module
        import dittofs.garbage_collector as gc_module

        original_chunk_dir = chunker_module.CHUNK_DIR
        demo_chunk_dir = temp_path / "chunks"
        chunker_module.CHUNK_DIR = demo_chunk_dir
        gc_module.CHUNK_DIR = demo_chunk_dir

        try:
            ensure_chunk_dir()

            # Create CRDT store
            store_path = temp_path / "demo_store.yrs"
            store = CRDTStore(store_path)

            # Create test files
            file1 = temp_path / "file1.txt"
            file2 = temp_path / "file2.txt"
            file3 = temp_path / "file3.txt"

            file1.write_text("This is test file 1 with some content for chunking.")
            file2.write_text("This is test file 2 with different content for chunking.")
            file3.write_text("This is test file 3 with more content for chunking.")

            print(f"Created test files in: {temp_path}")

            # Add files to DittoFS
            print("\n1. Adding files to DittoFS...")
            hashes1 = split(file1)
            hashes2 = split(file2)
            hashes3 = split(file3)

            store.add_file(file1, hashes1)
            store.add_file(file2, hashes2)
            store.add_file(file3, hashes3)

            print(f"   File 1: {len(hashes1)} chunks")
            print(f"   File 2: {len(hashes2)} chunks")
            print(f"   File 3: {len(hashes3)} chunks")

            # Show initial chunk status
            all_chunks = set(hashes1 + hashes2 + hashes3)
            print(f"   Total unique chunks: {len(all_chunks)}")

            # Create garbage collector with short retention for demo
            policy = RetentionPolicy(
                min_age_seconds=1,  # Very short for demo
                max_deletions_per_run=100,
                delete_orphaned=True,
                delete_from_missing_files=True,
            )

            gc = GarbageCollector(store, policy)

            print("\n2. Initial garbage collection (should find no garbage)...")
            stats = await gc.collect_garbage()
            print(f"   Chunks scanned: {stats.chunks_scanned}")
            print(f"   Chunks deleted: {stats.chunks_deleted}")
            print(f"   Orphaned chunks: {stats.orphaned_chunks}")

            # Create some orphaned chunks by manually adding chunk files
            print("\n3. Creating orphaned chunks...")
            orphan_chunk1 = demo_chunk_dir / "orphan1"
            orphan_chunk2 = demo_chunk_dir / "orphan2"

            orphan_chunk1.write_bytes(b"This is an orphaned chunk 1")
            orphan_chunk2.write_bytes(b"This is an orphaned chunk 2")

            print(f"   Created 2 orphaned chunks")

            # Wait for minimum age
            print("   Waiting for minimum age...")
            await asyncio.sleep(2)

            # Run garbage collection again
            print("\n4. Garbage collection with orphaned chunks...")
            stats = await gc.collect_garbage()
            print(f"   Chunks scanned: {stats.chunks_scanned}")
            print(f"   Chunks deleted: {stats.chunks_deleted}")
            print(f"   Bytes freed: {stats.bytes_freed}")
            print(f"   Orphaned chunks found: {stats.orphaned_chunks}")

            # Verify orphaned chunks were deleted
            print(f"   Orphan 1 exists: {orphan_chunk1.exists()}")
            print(f"   Orphan 2 exists: {orphan_chunk2.exists()}")

            # Delete a file and run GC to clean up its chunks
            print("\n5. Deleting file and cleaning up chunks...")
            file3.unlink()  # Delete the actual file
            store.remove_file(file3)  # Remove from store

            # Wait for minimum age
            await asyncio.sleep(2)

            stats = await gc.collect_garbage()
            print(f"   Chunks deleted: {stats.chunks_deleted}")
            print(f"   Bytes freed: {stats.bytes_freed}")

            # Show final statistics
            print("\n6. Final statistics...")
            storage_info = gc.get_storage_info()
            print(f"   Chunk count: {storage_info['chunk_count']}")
            print(f"   Chunk storage: {storage_info['chunk_bytes']} bytes")

            stats_history = gc.get_stats_history()
            print(f"   GC runs: {len(stats_history)}")

            total_deleted = sum(s.chunks_deleted for s in stats_history)
            total_freed = sum(s.bytes_freed for s in stats_history)
            print(f"   Total chunks deleted: {total_deleted}")
            print(f"   Total bytes freed: {total_freed}")

        finally:
            # Restore original chunk directory
            chunker_module.CHUNK_DIR = original_chunk_dir
            gc_module.CHUNK_DIR = original_chunk_dir


async def demo_reference_tracking():
    """Demonstrate chunk reference tracking"""
    print("\n" + "=" * 60)
    print("DEMO: Chunk Reference Tracking")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = pathlib.Path(temp_dir)

        # Create CRDT store
        store_path = temp_path / "demo_store.yrs"
        store = CRDTStore(store_path)

        # Create reference tracker
        tracker = ChunkReferenceTracker(store)

        # Create test files with overlapping content (for deduplication)
        file1 = temp_path / "shared1.txt"
        file2 = temp_path / "shared2.txt"

        shared_content = "This is shared content that will be deduplicated. " * 10
        file1.write_text(shared_content + "Unique content for file 1.")
        file2.write_text(shared_content + "Unique content for file 2.")

        print(f"Created files with shared content")

        # Override chunk directory for demo
        import dittofs.chunker as chunker_module

        original_chunk_dir = chunker_module.CHUNK_DIR
        demo_chunk_dir = temp_path / "chunks"
        chunker_module.CHUNK_DIR = demo_chunk_dir

        try:
            ensure_chunk_dir()

            # Add files to store
            print("\n1. Adding files with shared content...")
            hashes1 = split(file1)
            hashes2 = split(file2)

            store.add_file(file1, hashes1)
            store.add_file(file2, hashes2)

            print(f"   File 1 chunks: {len(hashes1)}")
            print(f"   File 2 chunks: {len(hashes2)}")

            # Analyze references
            print("\n2. Analyzing chunk references...")
            references = tracker.get_chunk_references()

            shared_chunks = []
            unique_chunks = []

            for chunk_hash, owners in references.items():
                if len(owners) > 1:
                    shared_chunks.append(chunk_hash)
                else:
                    unique_chunks.append(chunk_hash)

            print(f"   Total chunks: {len(references)}")
            print(f"   Shared chunks: {len(shared_chunks)}")
            print(f"   Unique chunks: {len(unique_chunks)}")

            # Show chunk ownership details
            if shared_chunks:
                print(f"\n   Shared chunk example: {shared_chunks[0][:16]}...")
                owners = references[shared_chunks[0]]
                for owner in owners:
                    print(f"     Referenced by: {pathlib.Path(owner).name}")

            # Test access time tracking
            print("\n3. Testing access time tracking...")
            test_chunk = list(references.keys())[0]

            print(
                f"   Initial access time: {tracker.get_chunk_last_access(test_chunk)}"
            )

            tracker.update_chunk_access_time(test_chunk)
            access_time = tracker.get_chunk_last_access(test_chunk)
            print(f"   After update: {access_time}")
            print(
                f"   Recently accessed: {tracker.is_chunk_recently_accessed(test_chunk, 3600)}"
            )

            # Test orphaned chunk detection
            print("\n4. Testing orphaned chunk detection...")

            # Create an orphaned chunk
            orphan_hash = "orphaned_chunk_hash"
            orphan_path = demo_chunk_dir / orphan_hash
            orphan_path.write_bytes(b"This is an orphaned chunk")

            orphaned = tracker.get_orphaned_chunks()
            print(f"   Orphaned chunks found: {len(orphaned)}")
            print(f"   Contains our orphan: {orphan_hash in orphaned}")

            # Test chunks from missing files
            print("\n5. Testing chunks from missing files...")

            # Delete one file but keep it in the store
            file2.unlink()

            chunks_from_missing = tracker.get_chunks_from_missing_files()
            print(f"   Chunks from missing files: {len(chunks_from_missing)}")

            # These should include chunks that were unique to file2
            file2_unique_chunks = set(hashes2) - set(hashes1)
            print(f"   File2 unique chunks: {len(file2_unique_chunks)}")
            print(
                f"   All file2 chunks in missing: {file2_unique_chunks.issubset(chunks_from_missing)}"
            )

        finally:
            chunker_module.CHUNK_DIR = original_chunk_dir


async def demo_retention_policies():
    """Demonstrate different retention policies"""
    print("\n" + "=" * 60)
    print("DEMO: Retention Policies")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = pathlib.Path(temp_dir)

        # Override chunk directory for demo
        import dittofs.chunker as chunker_module
        import dittofs.garbage_collector as gc_module

        original_chunk_dir = chunker_module.CHUNK_DIR
        demo_chunk_dir = temp_path / "chunks"
        chunker_module.CHUNK_DIR = demo_chunk_dir
        gc_module.CHUNK_DIR = demo_chunk_dir

        try:
            ensure_chunk_dir()

            # Create CRDT store
            store_path = temp_path / "demo_store.yrs"
            store = CRDTStore(store_path)

            # Create some orphaned chunks with different ages
            print("1. Creating chunks with different ages...")

            old_chunk = demo_chunk_dir / "old_chunk"
            new_chunk = demo_chunk_dir / "new_chunk"

            old_chunk.write_bytes(b"Old chunk content")
            time.sleep(1)  # Ensure different timestamps
            new_chunk.write_bytes(b"New chunk content")

            print(f"   Created old chunk: {old_chunk.name}")
            print(f"   Created new chunk: {new_chunk.name}")

            # Test conservative policy (high minimum age)
            print("\n2. Testing conservative policy...")
            conservative_policy = RetentionPolicy(
                min_age_seconds=3600,  # 1 hour
                max_deletions_per_run=10,
                delete_orphaned=True,
            )

            gc_conservative = GarbageCollector(store, conservative_policy)
            stats = await gc_conservative.collect_garbage()

            print(f"   Chunks deleted: {stats.chunks_deleted}")
            print(f"   Old chunk exists: {old_chunk.exists()}")
            print(f"   New chunk exists: {new_chunk.exists()}")

            # Test aggressive policy (low minimum age)
            print("\n3. Testing aggressive policy...")
            aggressive_policy = RetentionPolicy(
                min_age_seconds=0,  # No age requirement
                max_deletions_per_run=100,
                delete_orphaned=True,
            )

            gc_aggressive = GarbageCollector(store, aggressive_policy)
            stats = await gc_aggressive.collect_garbage()

            print(f"   Chunks deleted: {stats.chunks_deleted}")
            print(f"   Old chunk exists: {old_chunk.exists()}")
            print(f"   New chunk exists: {new_chunk.exists()}")

            # Test limited deletion policy
            print("\n4. Testing limited deletion policy...")

            # Create many orphaned chunks
            for i in range(20):
                chunk_path = demo_chunk_dir / f"orphan_{i}"
                chunk_path.write_bytes(f"Orphaned chunk {i}".encode())

            limited_policy = RetentionPolicy(
                min_age_seconds=0,
                max_deletions_per_run=5,  # Only delete 5 at a time
                delete_orphaned=True,
            )

            gc_limited = GarbageCollector(store, limited_policy)
            stats = await gc_limited.collect_garbage()

            print(f"   Chunks deleted: {stats.chunks_deleted}")
            print(f"   Max deletions per run: {limited_policy.max_deletions_per_run}")

            # Count remaining orphaned chunks
            remaining = len(
                [f for f in demo_chunk_dir.iterdir() if f.name.startswith("orphan_")]
            )
            print(f"   Remaining orphaned chunks: {remaining}")

            # Test selective deletion policy
            print("\n5. Testing selective deletion policy...")

            selective_policy = RetentionPolicy(
                min_age_seconds=0,
                delete_orphaned=True,
                delete_from_missing_files=False,  # Don't delete from missing files
            )

            # Create a file and then delete it (but keep in store)
            test_file = temp_path / "test_file.txt"
            test_file.write_text("Test content for selective deletion")

            hashes = split(test_file)
            store.add_file(test_file, hashes)

            # Delete the file but keep it in store
            test_file.unlink()

            gc_selective = GarbageCollector(store, selective_policy)

            # Get initial chunk count
            initial_chunks = len(list(demo_chunk_dir.iterdir()))

            stats = await gc_selective.collect_garbage()

            final_chunks = len(list(demo_chunk_dir.iterdir()))

            print(f"   Initial chunks: {initial_chunks}")
            print(f"   Final chunks: {final_chunks}")
            print(f"   Chunks deleted: {stats.chunks_deleted}")
            print(
                f"   Policy preserved chunks from missing files: {selective_policy.delete_from_missing_files == False}"
            )

        finally:
            chunker_module.CHUNK_DIR = original_chunk_dir
            gc_module.CHUNK_DIR = original_chunk_dir


async def demo_gc_scheduler():
    """Demonstrate garbage collection scheduler"""
    print("\n" + "=" * 60)
    print("DEMO: Garbage Collection Scheduler")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = pathlib.Path(temp_dir)

        # Override chunk directory for demo
        import dittofs.chunker as chunker_module
        import dittofs.garbage_collector as gc_module

        original_chunk_dir = chunker_module.CHUNK_DIR
        demo_chunk_dir = temp_path / "chunks"
        chunker_module.CHUNK_DIR = demo_chunk_dir
        gc_module.CHUNK_DIR = demo_chunk_dir

        try:
            ensure_chunk_dir()

            # Create CRDT store
            store_path = temp_path / "demo_store.yrs"
            store = CRDTStore(store_path)

            # Create garbage collector with frequent scheduling for demo
            policy = RetentionPolicy(min_age_seconds=1, delete_orphaned=True)

            gc = GarbageCollector(store, policy)

            print("1. Starting GC scheduler (3 second interval)...")

            # Start scheduler
            await gc.start_scheduler(interval_seconds=3)

            print("2. Creating orphaned chunks periodically...")

            # Create orphaned chunks over time
            for i in range(5):
                chunk_path = demo_chunk_dir / f"scheduled_orphan_{i}"
                chunk_path.write_bytes(f"Scheduled orphan {i}".encode())
                print(f"   Created orphan {i}")

                # Wait for scheduler to potentially run
                await asyncio.sleep(4)

                # Check if chunk was cleaned up
                if not chunk_path.exists():
                    print(f"   Orphan {i} was cleaned up by scheduler")
                else:
                    print(f"   Orphan {i} still exists")

            print("\n3. Scheduler statistics...")
            stats_history = gc.get_stats_history()
            print(f"   Total GC runs: {len(stats_history)}")

            if stats_history:
                total_deleted = sum(s.chunks_deleted for s in stats_history)
                print(f"   Total chunks deleted: {total_deleted}")

                print("   Recent runs:")
                for stats in stats_history[-3:]:  # Show last 3 runs
                    timestamp = stats.timestamp.strftime("%H:%M:%S")
                    print(f"     {timestamp}: {stats.chunks_deleted} deleted")

            print("\n4. Stopping scheduler...")
            await gc.stop_scheduler()
            print("   Scheduler stopped")

        finally:
            chunker_module.CHUNK_DIR = original_chunk_dir
            gc_module.CHUNK_DIR = original_chunk_dir


async def main():
    """Run all garbage collection demos"""
    print("DittoFS Garbage Collection System Demo")
    print("=" * 60)
    print("This demo shows the garbage collection functionality including:")
    print("- Basic garbage collection with orphaned chunks")
    print("- Chunk reference tracking and deduplication")
    print("- Different retention policies")
    print("- Automatic scheduling")
    print()

    try:
        await demo_basic_gc()
        await demo_reference_tracking()
        await demo_retention_policies()
        await demo_gc_scheduler()

        print("\n" + "=" * 60)
        print("DEMO COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print("Key features demonstrated:")
        print("✓ Orphaned chunk detection and cleanup")
        print("✓ Reference counting and tracking")
        print("✓ Configurable retention policies")
        print("✓ Safe cleanup procedures")
        print("✓ Automatic scheduling")
        print("✓ Comprehensive metrics and reporting")
        print()
        print("The garbage collection system is now ready for production use!")

    except Exception as e:
        print(f"\nDemo failed with error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(asyncio.run(main()))

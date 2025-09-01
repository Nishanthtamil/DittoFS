#!/usr/bin/env python3
"""
Demo script for DittoFS Snapshot-based Backup System

This script demonstrates the comprehensive snapshot capabilities including:
- Point-in-time snapshots of entire file system state
- Incremental snapshot system for efficient storage usage
- Snapshot restoration capabilities with selective file recovery
- Snapshot scheduling and automatic cleanup policies
"""

import json
import logging
import pathlib
import tempfile
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_demo_files(demo_dir: pathlib.Path):
    """Create demo files for snapshot testing"""
    print("\n=== Creating Demo Files ===")

    # Create various types of files
    files = {
        "document.txt": "This is a text document.\nIt has multiple lines.\nFor testing purposes.",
        "config.json": json.dumps(
            {"setting1": "value1", "setting2": 42, "enabled": True}, indent=2
        ),
        "script.py": "#!/usr/bin/env python3\nprint('Hello, World!')\n",
        "data.csv": "name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCharlie,35,Chicago\n",
    }

    # Create subdirectories
    (demo_dir / "docs").mkdir(exist_ok=True)
    (demo_dir / "config").mkdir(exist_ok=True)
    (demo_dir / "scripts").mkdir(exist_ok=True)

    # Write files
    for filename, content in files.items():
        if filename.endswith(".txt"):
            file_path = demo_dir / "docs" / filename
        elif filename.endswith(".json"):
            file_path = demo_dir / "config" / filename
        elif filename.endswith(".py"):
            file_path = demo_dir / "scripts" / filename
        else:
            file_path = demo_dir / filename

        file_path.write_text(content)
        print(f"Created: {file_path}")

    return list(files.keys())


def modify_demo_files(demo_dir: pathlib.Path):
    """Modify some demo files to test incremental snapshots"""
    print("\n=== Modifying Demo Files ===")

    # Modify existing files
    doc_file = demo_dir / "docs" / "document.txt"
    if doc_file.exists():
        content = doc_file.read_text()
        content += "\nThis line was added later."
        doc_file.write_text(content)
        print(f"Modified: {doc_file}")

    # Add new file
    new_file = demo_dir / "docs" / "new_document.txt"
    new_file.write_text("This is a new document added after the first snapshot.")
    print(f"Created: {new_file}")

    # Modify config
    config_file = demo_dir / "config" / "config.json"
    if config_file.exists():
        config = json.loads(config_file.read_text())
        config["setting3"] = "new_value"
        config["modified_at"] = datetime.now().isoformat()
        config_file.write_text(json.dumps(config, indent=2))
        print(f"Modified: {config_file}")


def demo_basic_snapshots():
    """Demonstrate basic snapshot creation and restoration"""
    print("\n" + "=" * 60)
    print("DEMO: Basic Snapshot Operations")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        demo_dir = pathlib.Path(temp_dir) / "demo_files"
        demo_dir.mkdir()

        snapshot_storage = pathlib.Path(temp_dir) / "snapshots"

        # Import required modules
        from src.dittofs.chunker import split
        from src.dittofs.crdt_store import CRDTStore
        from src.dittofs.snapshot_system import (
            RestoreOptions,
            RestoreScope,
            SnapshotSystem,
            SnapshotType,
        )

        # Initialize CRDT store and snapshot system
        crdt_store = CRDTStore(pathlib.Path(temp_dir) / "crdt_store.yrs")
        snapshot_system = SnapshotSystem(snapshot_storage, crdt_store, "demo_peer")

        # Create demo files
        create_demo_files(demo_dir)

        # Add files to CRDT store
        print("\n--- Adding Files to CRDT Store ---")
        for file_path in demo_dir.rglob("*"):
            if file_path.is_file():
                hashes = split(file_path)
                if hashes:
                    crdt_store.add_file(file_path, hashes)
                    print(f"Added to CRDT: {file_path}")

        # Create first full snapshot
        print("\n--- Creating Full Snapshot ---")
        snapshot1_id = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Initial full snapshot",
            tags=["demo", "full"],
        )

        if snapshot1_id:
            print(f"Created full snapshot: {snapshot1_id}")

            # Get snapshot info
            info = snapshot_system.get_snapshot_info(snapshot1_id)
            if info:
                print(f"Snapshot contains {len(info['files'])} files")
                print(f"Total size: {info['metadata']['total_size']} bytes")

        # Wait a moment and modify files
        time.sleep(1)
        modify_demo_files(demo_dir)

        # Update CRDT store with modified files
        print("\n--- Updating CRDT Store with Changes ---")
        for file_path in demo_dir.rglob("*"):
            if file_path.is_file():
                hashes = split(file_path)
                if hashes:
                    crdt_store.add_file(file_path, hashes)
                    print(f"Updated in CRDT: {file_path}")

        # Create incremental snapshot
        print("\n--- Creating Incremental Snapshot ---")
        snapshot2_id = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.INCREMENTAL,
            description="Incremental snapshot after modifications",
            tags=["demo", "incremental"],
            parent_snapshot=snapshot1_id,
        )

        if snapshot2_id:
            print(f"Created incremental snapshot: {snapshot2_id}")

            info = snapshot_system.get_snapshot_info(snapshot2_id)
            if info:
                print(f"Incremental snapshot contains {len(info['files'])} files")
                print(f"Total size: {info['metadata']['total_size']} bytes")

        # List all snapshots
        print("\n--- Listing All Snapshots ---")
        snapshots = snapshot_system.list_snapshots()
        for snapshot in snapshots:
            print(f"Snapshot {snapshot.snapshot_id}:")
            print(f"  Type: {snapshot.snapshot_type.value}")
            print(f"  Created: {datetime.fromtimestamp(snapshot.created_at)}")
            print(f"  Files: {snapshot.file_count}")
            print(f"  Size: {snapshot.total_size} bytes")
            print(f"  Description: {snapshot.description}")
            print(f"  Tags: {snapshot.tags}")

        # Test restoration
        print("\n--- Testing Snapshot Restoration ---")
        restore_dir = pathlib.Path(temp_dir) / "restored_files"
        restore_dir.mkdir()

        restore_options = RestoreOptions(
            scope=RestoreScope.FULL,
            target_path=restore_dir,
            overwrite_existing=True,
            verify_integrity=True,
        )

        if snapshot_system.restore_snapshot(snapshot1_id, restore_options):
            print(f"Successfully restored snapshot {snapshot1_id} to {restore_dir}")

            # List restored files
            restored_files = list(restore_dir.rglob("*"))
            print(f"Restored {len([f for f in restored_files if f.is_file()])} files")
            for file_path in restored_files:
                if file_path.is_file():
                    print(f"  {file_path.name} ({file_path.stat().st_size} bytes)")

        # Test selective restoration
        print("\n--- Testing Selective Restoration ---")
        selective_dir = pathlib.Path(temp_dir) / "selective_restore"
        selective_dir.mkdir()

        selective_options = RestoreOptions(
            scope=RestoreScope.SELECTIVE,
            target_path=selective_dir,
            file_patterns=["*.txt"],  # Only restore text files
            overwrite_existing=True,
        )

        if snapshot_system.restore_snapshot(snapshot2_id, selective_options):
            print(f"Successfully restored text files from snapshot {snapshot2_id}")

            restored_files = list(selective_dir.rglob("*.txt"))
            print(f"Restored {len(restored_files)} text files")
            for file_path in restored_files:
                print(f"  {file_path.name}")


def demo_snapshot_policies():
    """Demonstrate snapshot policies and scheduling"""
    print("\n" + "=" * 60)
    print("DEMO: Snapshot Policies and Scheduling")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        snapshot_storage = pathlib.Path(temp_dir) / "snapshots"

        from src.dittofs.snapshot_system import (
            SnapshotPolicy,
            SnapshotSystem,
            SnapshotType,
        )

        # Initialize snapshot system
        snapshot_system = SnapshotSystem(snapshot_storage, None, "demo_peer")

        # Create snapshot policies
        print("\n--- Creating Snapshot Policies ---")

        # Daily backup policy
        daily_policy = SnapshotPolicy(
            name="daily_backup",
            enabled=True,
            schedule_cron="0 2 * * *",  # Daily at 2 AM
            snapshot_type=SnapshotType.INCREMENTAL,
            max_snapshots=30,
            max_age_days=90,
            full_snapshot_interval=7,  # Full snapshot every 7 incremental
            tags=["daily", "auto"],
        )

        # Weekly full backup policy
        weekly_policy = SnapshotPolicy(
            name="weekly_full",
            enabled=True,
            schedule_cron="0 3 * * 0",  # Weekly on Sunday at 3 AM
            snapshot_type=SnapshotType.FULL,
            max_snapshots=12,  # Keep 12 weekly snapshots
            max_age_days=365,
            tags=["weekly", "full", "auto"],
        )

        # Create policies
        snapshot_system.create_policy(daily_policy)
        snapshot_system.create_policy(weekly_policy)

        print(f"Created policy: {daily_policy.name}")
        print(f"  Schedule: {daily_policy.schedule_cron}")
        print(f"  Type: {daily_policy.snapshot_type.value}")
        print(f"  Max snapshots: {daily_policy.max_snapshots}")
        print(f"  Max age: {daily_policy.max_age_days} days")

        print(f"Created policy: {weekly_policy.name}")
        print(f"  Schedule: {weekly_policy.schedule_cron}")
        print(f"  Type: {weekly_policy.snapshot_type.value}")
        print(f"  Max snapshots: {weekly_policy.max_snapshots}")
        print(f"  Max age: {weekly_policy.max_age_days} days")

        # Create some test snapshots with different ages
        print("\n--- Creating Test Snapshots for Cleanup Demo ---")

        current_time = time.time()
        test_snapshots = []

        # Create snapshots with different ages
        for i in range(15):
            # Create snapshot with artificial timestamp
            snapshot_id = f"test_snapshot_{i:02d}"

            from src.dittofs.snapshot_system import SnapshotMetadata, SnapshotStatus

            # Create old snapshots (simulate different ages)
            age_days = i * 7  # 0, 7, 14, 21, ... days old
            created_at = current_time - (age_days * 24 * 3600)

            metadata = SnapshotMetadata(
                snapshot_id=snapshot_id,
                snapshot_type=SnapshotType.INCREMENTAL,
                created_at=created_at,
                created_by="demo_peer",
                description=f"Test snapshot {i} ({age_days} days old)",
                file_count=10,
                total_size=1024 * (i + 1),
                status=SnapshotStatus.COMPLETED,
                tags=["daily", "auto", "test"],
            )

            snapshot_system.snapshot_cache[snapshot_id] = metadata
            test_snapshots.append(snapshot_id)

        snapshot_system._save_metadata()

        print(f"Created {len(test_snapshots)} test snapshots")

        # List snapshots before cleanup
        print("\n--- Snapshots Before Cleanup ---")
        snapshots = snapshot_system.list_snapshots(tags=["test"])
        for snapshot in snapshots[:5]:  # Show first 5
            age_days = (current_time - snapshot.created_at) / (24 * 3600)
            print(f"  {snapshot.snapshot_id}: {age_days:.0f} days old")
        print(f"  ... and {len(snapshots) - 5} more")

        # Test cleanup
        print("\n--- Testing Cleanup Policies ---")
        deleted_count = snapshot_system.cleanup_snapshots("daily_backup")
        print(f"Cleanup deleted {deleted_count} snapshots")

        # List snapshots after cleanup
        print("\n--- Snapshots After Cleanup ---")
        remaining_snapshots = snapshot_system.list_snapshots(tags=["test"])
        print(f"Remaining snapshots: {len(remaining_snapshots)}")
        for snapshot in remaining_snapshots:
            age_days = (current_time - snapshot.created_at) / (24 * 3600)
            print(f"  {snapshot.snapshot_id}: {age_days:.0f} days old")

        # Stop scheduler
        snapshot_system.stop_scheduler()


def demo_advanced_features():
    """Demonstrate advanced snapshot features"""
    print("\n" + "=" * 60)
    print("DEMO: Advanced Snapshot Features")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as temp_dir:
        demo_dir = pathlib.Path(temp_dir) / "demo_files"
        demo_dir.mkdir()

        snapshot_storage = pathlib.Path(temp_dir) / "snapshots"

        from src.dittofs.chunker import split
        from src.dittofs.crdt_store import CRDTStore
        from src.dittofs.snapshot_system import (
            RestoreOptions,
            RestoreScope,
            SnapshotSystem,
            SnapshotType,
        )

        # Initialize systems
        crdt_store = CRDTStore(pathlib.Path(temp_dir) / "crdt_store.yrs")
        snapshot_system = SnapshotSystem(snapshot_storage, crdt_store, "demo_peer")

        # Create a larger set of demo files
        print("\n--- Creating Diverse File Set ---")

        # Create different types of files
        file_types = {
            "documents": ["report.txt", "notes.md", "readme.txt"],
            "config": ["settings.json", "config.yaml", "database.conf"],
            "scripts": ["backup.py", "deploy.sh", "test.js"],
            "data": ["users.csv", "logs.txt", "metrics.json"],
        }

        all_files = []
        for category, files in file_types.items():
            category_dir = demo_dir / category
            category_dir.mkdir()

            for filename in files:
                file_path = category_dir / filename
                content = f"Content for {filename}\nCategory: {category}\nCreated at: {datetime.now()}\n"

                # Add some variety to file sizes
                if filename.endswith(".txt") or filename.endswith(".md"):
                    content += (
                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * 10
                    )
                elif filename.endswith(".json"):
                    content = json.dumps(
                        {
                            "filename": filename,
                            "category": category,
                            "data": list(range(20)),
                            "timestamp": time.time(),
                        },
                        indent=2,
                    )

                file_path.write_text(content)
                all_files.append(file_path)
                print(f"Created: {file_path}")

        # Add files to CRDT store
        print("\n--- Adding Files to CRDT Store ---")
        for file_path in all_files:
            hashes = split(file_path)
            if hashes:
                crdt_store.add_file(file_path, hashes)

        # Create full snapshot
        print("\n--- Creating Full Snapshot ---")
        full_snapshot = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Full snapshot of diverse file set",
            tags=["demo", "full", "diverse"],
        )

        # Simulate file changes over time
        print("\n--- Simulating File Changes Over Time ---")

        snapshots_created = [full_snapshot]

        for iteration in range(3):
            print(f"\n  Iteration {iteration + 1}:")

            # Modify some files
            files_to_modify = all_files[iteration * 2 : (iteration + 1) * 2 + 2]
            for file_path in files_to_modify:
                if file_path.exists():
                    content = file_path.read_text()
                    content += f"\nModification {iteration + 1} at {datetime.now()}\n"
                    file_path.write_text(content)

                    # Update in CRDT store
                    hashes = split(file_path)
                    if hashes:
                        crdt_store.add_file(file_path, hashes)

                    print(f"    Modified: {file_path.name}")

            # Create incremental snapshot
            parent_snapshot = snapshots_created[-1]
            incremental_snapshot = snapshot_system.create_snapshot(
                snapshot_type=SnapshotType.INCREMENTAL,
                description=f"Incremental snapshot - iteration {iteration + 1}",
                tags=["demo", "incremental", f"iter_{iteration + 1}"],
                parent_snapshot=parent_snapshot,
            )

            if incremental_snapshot:
                snapshots_created.append(incremental_snapshot)
                print(f"    Created incremental snapshot: {incremental_snapshot}")

        # Analyze snapshot chain
        print("\n--- Analyzing Snapshot Chain ---")
        for i, snapshot_id in enumerate(snapshots_created):
            info = snapshot_system.get_snapshot_info(snapshot_id)
            if info:
                metadata = info["metadata"]
                print(f"Snapshot {i + 1}: {snapshot_id}")
                print(f"  Type: {metadata['snapshot_type']}")
                print(f"  Files: {metadata['file_count']}")
                print(f"  Size: {metadata['total_size']} bytes")
                print(f"  Compressed: {metadata['compressed_size']} bytes")
                if metadata["compressed_size"] > 0:
                    ratio = (
                        1 - metadata["compressed_size"] / metadata["total_size"]
                    ) * 100
                    print(f"  Compression: {ratio:.1f}%")

        # Test differential snapshot
        print("\n--- Creating Differential Snapshot ---")

        # Make more changes
        for file_path in all_files[-3:]:  # Modify last 3 files
            if file_path.exists():
                content = file_path.read_text()
                content += f"\nDifferential change at {datetime.now()}\n"
                file_path.write_text(content)

                hashes = split(file_path)
                if hashes:
                    crdt_store.add_file(file_path, hashes)

        differential_snapshot = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.DIFFERENTIAL,
            description="Differential snapshot from full snapshot",
            tags=["demo", "differential"],
        )

        if differential_snapshot:
            print(f"Created differential snapshot: {differential_snapshot}")
            info = snapshot_system.get_snapshot_info(differential_snapshot)
            if info:
                print(f"  Contains {len(info['files'])} changed files")

        # Test advanced restore options
        print("\n--- Testing Advanced Restore Options ---")

        # Restore only specific file types
        restore_dir = pathlib.Path(temp_dir) / "advanced_restore"
        restore_dir.mkdir()

        # Restore only Python and JSON files
        selective_options = RestoreOptions(
            scope=RestoreScope.SELECTIVE,
            target_path=restore_dir,
            file_patterns=["*.py", "*.json"],
            exclude_patterns=["*test*"],
            overwrite_existing=True,
            verify_integrity=True,
        )

        if snapshot_system.restore_snapshot(full_snapshot, selective_options):
            print("Successfully restored Python and JSON files")

            restored_files = list(restore_dir.rglob("*"))
            for file_path in restored_files:
                if file_path.is_file():
                    print(f"  Restored: {file_path.name}")

        # Clean up
        snapshot_system.stop_scheduler()


def main():
    """Run all snapshot system demos"""
    print("DittoFS Snapshot-based Backup System Demo")
    print("=" * 60)

    try:
        # Run demos
        demo_basic_snapshots()
        demo_snapshot_policies()
        demo_advanced_features()

        print("\n" + "=" * 60)
        print("DEMO COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print("\nThe snapshot system provides:")
        print("✓ Point-in-time snapshots of entire file system state")
        print("✓ Incremental and differential snapshots for efficiency")
        print("✓ Flexible restoration with selective file recovery")
        print("✓ Automated scheduling and cleanup policies")
        print("✓ Compression and integrity verification")
        print("✓ Tag-based organization and filtering")

    except Exception as e:
        print(f"\nDemo failed with error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())

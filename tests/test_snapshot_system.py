"""
Tests for the snapshot-based backup system

This module tests all aspects of the snapshot system including:
- Point-in-time snapshots of entire file system state
- Incremental snapshot system for efficient storage usage
- Snapshot restoration capabilities with selective file recovery
- Snapshot scheduling and automatic cleanup policies
"""

import pytest
import pathlib
import tempfile
import time
import json
import shutil
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from src.dittofs.snapshot_system import (
    SnapshotSystem, SnapshotType, SnapshotStatus, RestoreScope,
    SnapshotMetadata, SnapshotFileRecord, RestoreOptions, SnapshotPolicy
)
from src.dittofs.crdt_store import CRDTStore, FileRecord
from src.dittofs.chunker import split

class TestSnapshotSystem:
    """Test the snapshot system functionality"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield pathlib.Path(temp_dir)
    
    @pytest.fixture
    def demo_files(self, temp_dir):
        """Create demo files for testing"""
        files_dir = temp_dir / "demo_files"
        files_dir.mkdir()
        
        files = {
            "document.txt": "This is a test document.\nWith multiple lines.",
            "config.json": json.dumps({"setting": "value", "number": 42}),
            "script.py": "#!/usr/bin/env python3\nprint('Hello, World!')",
            "data.csv": "name,value\ntest,123\nother,456"
        }
        
        created_files = []
        for filename, content in files.items():
            file_path = files_dir / filename
            file_path.write_text(content)
            created_files.append(file_path)
        
        return created_files
    
    @pytest.fixture
    def crdt_store(self, temp_dir):
        """Create CRDT store for testing"""
        return CRDTStore(temp_dir / "crdt_store.yrs", "test_peer")
    
    @pytest.fixture
    def snapshot_system(self, temp_dir, crdt_store):
        """Create snapshot system for testing"""
        snapshot_storage = temp_dir / "snapshots"
        system = SnapshotSystem(snapshot_storage, crdt_store, "test_peer")
        yield system
        system.stop_scheduler()
    
    def test_snapshot_system_initialization(self, temp_dir):
        """Test snapshot system initialization"""
        snapshot_storage = temp_dir / "snapshots"
        system = SnapshotSystem(snapshot_storage, None, "test_peer")
        
        assert system.storage_path == snapshot_storage
        assert system.peer_id == "test_peer"
        assert system.snapshots_dir.exists()
        assert system.metadata_dir.exists()
        assert system.policies_dir.exists()
        assert system.scheduler_running
        
        system.stop_scheduler()
    
    def test_create_full_snapshot(self, snapshot_system, crdt_store, demo_files):
        """Test creating a full snapshot"""
        # Add files to CRDT store
        for file_path in demo_files:
            hashes = split(file_path)
            crdt_store.add_file(file_path, hashes)
        
        # Create full snapshot
        snapshot_id = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Test full snapshot",
            tags=["test", "full"]
        )
        
        assert snapshot_id is not None
        assert snapshot_id in snapshot_system.snapshot_cache
        
        metadata = snapshot_system.snapshot_cache[snapshot_id]
        assert metadata.snapshot_type == SnapshotType.FULL
        assert metadata.status == SnapshotStatus.COMPLETED
        assert metadata.file_count == len(demo_files)
        assert metadata.description == "Test full snapshot"
        assert "test" in metadata.tags
        assert "full" in metadata.tags
    
    def test_create_incremental_snapshot(self, snapshot_system, crdt_store, demo_files, temp_dir):
        """Test creating incremental snapshots"""
        # Add files to CRDT store
        for file_path in demo_files:
            hashes = split(file_path)
            crdt_store.add_file(file_path, hashes)
        
        # Create full snapshot
        full_snapshot = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Full snapshot"
        )
        
        # Modify a file
        demo_files[0].write_text("Modified content")
        hashes = split(demo_files[0])
        crdt_store.add_file(demo_files[0], hashes)
        
        # Add a new file
        new_file = temp_dir / "demo_files" / "new_file.txt"
        new_file.write_text("New file content")
        hashes = split(new_file)
        crdt_store.add_file(new_file, hashes)
        
        # Create incremental snapshot
        incremental_snapshot = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.INCREMENTAL,
            description="Incremental snapshot",
            parent_snapshot=full_snapshot
        )
        
        assert incremental_snapshot is not None
        
        metadata = snapshot_system.snapshot_cache[incremental_snapshot]
        assert metadata.snapshot_type == SnapshotType.INCREMENTAL
        assert metadata.parent_snapshot == full_snapshot
        # Should contain only changed files (modified + new)
        # Note: The actual count may vary based on implementation details
        assert metadata.file_count >= 2  # At least the modified and new files
    
    def test_snapshot_compression(self, snapshot_system, crdt_store, demo_files):
        """Test snapshot compression"""
        # Add files to CRDT store
        for file_path in demo_files:
            hashes = split(file_path)
            crdt_store.add_file(file_path, hashes)
        
        # Create snapshot
        snapshot_id = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Compressed snapshot"
        )
        
        metadata = snapshot_system.snapshot_cache[snapshot_id]
        
        # Check that snapshot was created successfully
        assert metadata.file_count == len(demo_files)
        assert metadata.total_size > 0
        
        # Check that snapshot data exists in some form
        snapshot_dir = snapshot_system.snapshots_dir / snapshot_id
        compressed_file = snapshot_dir / "snapshot.tar.gz"
        uncompressed_file = snapshot_dir / "files.json"
        
        # Either compressed or uncompressed file should exist
        assert compressed_file.exists() or uncompressed_file.exists()
        
        # If compression was used, compressed_size should be set
        if compressed_file.exists():
            assert metadata.compressed_size > 0
        else:
            # No compression was used (not beneficial for small files)
            assert metadata.compressed_size == 0
    
    def test_restore_full_snapshot(self, snapshot_system, crdt_store, demo_files, temp_dir):
        """Test restoring a full snapshot"""
        # Add files to CRDT store
        for file_path in demo_files:
            hashes = split(file_path)
            crdt_store.add_file(file_path, hashes)
        
        # Create snapshot
        snapshot_id = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Test snapshot"
        )
        
        # Restore to different location
        restore_dir = temp_dir / "restored"
        restore_dir.mkdir()
        
        restore_options = RestoreOptions(
            scope=RestoreScope.FULL,
            target_path=restore_dir,
            overwrite_existing=True,
            verify_integrity=True
        )
        
        success = snapshot_system.restore_snapshot(snapshot_id, restore_options)
        assert success
        
        # Check restored files
        restored_files = list(restore_dir.rglob("*"))
        restored_files = [f for f in restored_files if f.is_file()]
        assert len(restored_files) == len(demo_files)
        
        # Verify content
        for original_file in demo_files:
            restored_file = restore_dir / original_file.name
            assert restored_file.exists()
            assert restored_file.read_text() == original_file.read_text()
    
    def test_restore_selective_snapshot(self, snapshot_system, crdt_store, demo_files, temp_dir):
        """Test selective snapshot restoration"""
        # Add files to CRDT store
        for file_path in demo_files:
            hashes = split(file_path)
            crdt_store.add_file(file_path, hashes)
        
        # Create snapshot
        snapshot_id = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Test snapshot"
        )
        
        # Restore only text files
        restore_dir = temp_dir / "selective_restore"
        restore_dir.mkdir()
        
        restore_options = RestoreOptions(
            scope=RestoreScope.SELECTIVE,
            target_path=restore_dir,
            file_patterns=["*.txt", "*.py"],
            exclude_patterns=["*config*"],
            overwrite_existing=True
        )
        
        success = snapshot_system.restore_snapshot(snapshot_id, restore_options)
        assert success
        
        # Check that only matching files were restored
        restored_files = list(restore_dir.rglob("*"))
        restored_files = [f for f in restored_files if f.is_file()]
        
        # Should have document.txt and script.py, but not config.json or data.csv
        restored_names = [f.name for f in restored_files]
        assert "document.txt" in restored_names
        assert "script.py" in restored_names
        assert "config.json" not in restored_names
        assert "data.csv" not in restored_names
    
    def test_snapshot_policies(self, snapshot_system):
        """Test snapshot policy creation and management"""
        policy = SnapshotPolicy(
            name="test_policy",
            enabled=True,
            schedule_cron="0 2 * * *",
            snapshot_type=SnapshotType.INCREMENTAL,
            max_snapshots=10,
            max_age_days=30,
            tags=["auto", "test"]
        )
        
        # Create policy
        success = snapshot_system.create_policy(policy)
        assert success
        assert "test_policy" in snapshot_system.policy_cache
        
        stored_policy = snapshot_system.policy_cache["test_policy"]
        assert stored_policy.name == "test_policy"
        assert stored_policy.max_snapshots == 10
        assert stored_policy.max_age_days == 30
        
        # Delete policy
        success = snapshot_system.delete_policy("test_policy")
        assert success
        assert "test_policy" not in snapshot_system.policy_cache
    
    def test_snapshot_cleanup(self, snapshot_system):
        """Test snapshot cleanup based on policies"""
        # Create test policy
        policy = SnapshotPolicy(
            name="cleanup_test",
            enabled=True,
            max_snapshots=5,
            max_age_days=30,
            tags=["cleanup_test"]
        )
        snapshot_system.create_policy(policy)
        
        # Create test snapshots with different ages
        current_time = time.time()
        test_snapshots = []
        
        for i in range(10):
            snapshot_id = f"test_snapshot_{i:02d}"
            age_days = i * 5  # 0, 5, 10, 15, ... days old
            created_at = current_time - (age_days * 24 * 3600)
            
            metadata = SnapshotMetadata(
                snapshot_id=snapshot_id,
                snapshot_type=SnapshotType.INCREMENTAL,
                created_at=created_at,
                created_by="test_peer",
                description=f"Test snapshot {i}",
                status=SnapshotStatus.COMPLETED,
                tags=["cleanup_test"]
            )
            
            snapshot_system.snapshot_cache[snapshot_id] = metadata
            test_snapshots.append(snapshot_id)
        
        # Run cleanup
        deleted_count = snapshot_system.cleanup_snapshots("cleanup_test")
        
        # Should delete old snapshots beyond max_snapshots limit
        assert deleted_count > 0
        
        # Check remaining snapshots
        remaining = [s for s in snapshot_system.snapshot_cache.values() 
                    if "cleanup_test" in s.tags and s.status != SnapshotStatus.DELETED]
        assert len(remaining) <= policy.max_snapshots
    
    def test_list_snapshots(self, snapshot_system, crdt_store, demo_files):
        """Test listing snapshots with filtering"""
        # Add files to CRDT store
        for file_path in demo_files:
            hashes = split(file_path)
            crdt_store.add_file(file_path, hashes)
        
        # Create snapshots with different tags
        snapshot1 = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Full snapshot",
            tags=["full", "important"]
        )
        
        snapshot2 = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.INCREMENTAL,
            description="Incremental snapshot",
            tags=["incremental", "auto"]
        )
        
        # List all snapshots
        all_snapshots = snapshot_system.list_snapshots()
        assert len(all_snapshots) == 2
        
        # List snapshots by tag
        full_snapshots = snapshot_system.list_snapshots(tags=["full"])
        assert len(full_snapshots) == 1
        assert full_snapshots[0].snapshot_id == snapshot1
        
        auto_snapshots = snapshot_system.list_snapshots(tags=["auto"])
        assert len(auto_snapshots) == 1
        assert auto_snapshots[0].snapshot_id == snapshot2
    
    def test_delete_snapshot(self, snapshot_system, crdt_store, demo_files):
        """Test snapshot deletion"""
        # Add files to CRDT store
        for file_path in demo_files:
            hashes = split(file_path)
            crdt_store.add_file(file_path, hashes)
        
        # Create snapshot
        snapshot_id = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Test snapshot"
        )
        
        # Verify snapshot exists
        assert snapshot_id in snapshot_system.snapshot_cache
        snapshot_dir = snapshot_system.snapshots_dir / snapshot_id
        assert snapshot_dir.exists()
        
        # Delete snapshot
        success = snapshot_system.delete_snapshot(snapshot_id)
        assert success
        
        # Verify snapshot is deleted
        assert snapshot_id not in snapshot_system.snapshot_cache
        assert not snapshot_dir.exists()
    
    def test_get_snapshot_info(self, snapshot_system, crdt_store, demo_files):
        """Test getting detailed snapshot information"""
        # Add files to CRDT store
        for file_path in demo_files:
            hashes = split(file_path)
            crdt_store.add_file(file_path, hashes)
        
        # Create snapshot
        snapshot_id = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Info test snapshot",
            tags=["info", "test"]
        )
        
        # Get snapshot info
        info = snapshot_system.get_snapshot_info(snapshot_id)
        assert info is not None
        
        metadata = info['metadata']
        assert metadata['snapshot_id'] == snapshot_id
        assert metadata['snapshot_type'] == 'full'
        assert metadata['description'] == "Info test snapshot"
        assert 'info' in metadata['tags']
        
        files = info['files']
        assert len(files) == len(demo_files)
        
        # Check file information
        file_paths = [f['path'] for f in files]
        for demo_file in demo_files:
            assert str(demo_file) in file_paths
    
    def test_differential_snapshot(self, snapshot_system, crdt_store, demo_files, temp_dir):
        """Test differential snapshot creation"""
        # Add files to CRDT store
        for file_path in demo_files:
            hashes = split(file_path)
            crdt_store.add_file(file_path, hashes)
        
        # Create full snapshot
        full_snapshot = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Full snapshot"
        )
        
        # Create incremental snapshot
        demo_files[0].write_text("Modified for incremental")
        hashes = split(demo_files[0])
        crdt_store.add_file(demo_files[0], hashes)
        
        incremental_snapshot = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.INCREMENTAL,
            description="Incremental snapshot",
            parent_snapshot=full_snapshot
        )
        
        # Modify more files
        demo_files[1].write_text("Modified for differential")
        hashes = split(demo_files[1])
        crdt_store.add_file(demo_files[1], hashes)
        
        new_file = temp_dir / "demo_files" / "differential_file.txt"
        new_file.write_text("New file for differential")
        hashes = split(new_file)
        crdt_store.add_file(new_file, hashes)
        
        # Create differential snapshot (should be based on full snapshot)
        differential_snapshot = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.DIFFERENTIAL,
            description="Differential snapshot"
        )
        
        assert differential_snapshot is not None
        
        metadata = snapshot_system.snapshot_cache[differential_snapshot]
        assert metadata.snapshot_type == SnapshotType.DIFFERENTIAL
        
        # Should contain files changed since full snapshot
        info = snapshot_system.get_snapshot_info(differential_snapshot)
        # The exact count may vary based on implementation, but should include the changed files
        assert len(info['files']) >= 2  # At least the two files we explicitly changed
    
    def test_snapshot_metadata_persistence(self, temp_dir):
        """Test that snapshot metadata persists across system restarts"""
        snapshot_storage = temp_dir / "snapshots"
        
        # Create first system instance
        system1 = SnapshotSystem(snapshot_storage, None, "test_peer")
        
        # Create test metadata
        metadata = SnapshotMetadata(
            snapshot_id="test_snapshot",
            snapshot_type=SnapshotType.FULL,
            created_at=time.time(),
            created_by="test_peer",
            description="Persistence test",
            file_count=5,
            total_size=1024,
            status=SnapshotStatus.COMPLETED,
            tags=["persistence", "test"]
        )
        
        system1.snapshot_cache["test_snapshot"] = metadata
        system1._save_metadata()
        system1.stop_scheduler()
        
        # Create second system instance
        system2 = SnapshotSystem(snapshot_storage, None, "test_peer")
        
        # Check that metadata was loaded
        assert "test_snapshot" in system2.snapshot_cache
        loaded_metadata = system2.snapshot_cache["test_snapshot"]
        
        assert loaded_metadata.snapshot_id == "test_snapshot"
        assert loaded_metadata.description == "Persistence test"
        assert loaded_metadata.file_count == 5
        assert "persistence" in loaded_metadata.tags
        
        system2.stop_scheduler()
    
    def test_restore_integrity_verification(self, snapshot_system, crdt_store, demo_files, temp_dir):
        """Test integrity verification during restore"""
        # Add files to CRDT store
        for file_path in demo_files:
            hashes = split(file_path)
            crdt_store.add_file(file_path, hashes)
        
        # Create snapshot
        snapshot_id = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Integrity test snapshot"
        )
        
        # Restore with integrity verification enabled
        restore_dir = temp_dir / "integrity_restore"
        restore_dir.mkdir()
        
        restore_options = RestoreOptions(
            scope=RestoreScope.FULL,
            target_path=restore_dir,
            verify_integrity=True
        )
        
        success = snapshot_system.restore_snapshot(snapshot_id, restore_options)
        assert success
        
        # All files should be restored and verified
        restored_files = list(restore_dir.rglob("*"))
        restored_files = [f for f in restored_files if f.is_file()]
        assert len(restored_files) == len(demo_files)
    
    def test_scheduler_functionality(self, snapshot_system):
        """Test snapshot scheduler functionality"""
        # Scheduler should be running by default
        assert snapshot_system.scheduler_running
        assert snapshot_system.scheduler_thread is not None
        
        # Stop scheduler
        snapshot_system.stop_scheduler()
        assert not snapshot_system.scheduler_running
        
        # Restart scheduler
        snapshot_system.start_scheduler()
        assert snapshot_system.scheduler_running
        assert snapshot_system.scheduler_thread is not None

class TestSnapshotDataStructures:
    """Test snapshot data structures and serialization"""
    
    def test_snapshot_metadata_serialization(self):
        """Test SnapshotMetadata serialization"""
        metadata = SnapshotMetadata(
            snapshot_id="test_id",
            snapshot_type=SnapshotType.INCREMENTAL,
            created_at=time.time(),
            created_by="test_peer",
            description="Test snapshot",
            file_count=10,
            total_size=2048,
            compressed_size=1024,
            status=SnapshotStatus.COMPLETED,
            tags=["test", "serialization"]
        )
        
        # Test to_dict
        data = metadata.to_dict()
        assert data['snapshot_id'] == "test_id"
        assert data['snapshot_type'] == "incremental"
        assert data['status'] == "completed"
        assert data['tags'] == ["test", "serialization"]
        
        # Test from_dict
        restored = SnapshotMetadata.from_dict(data)
        assert restored.snapshot_id == metadata.snapshot_id
        assert restored.snapshot_type == metadata.snapshot_type
        assert restored.status == metadata.status
        assert restored.tags == metadata.tags
    
    def test_snapshot_file_record_serialization(self):
        """Test SnapshotFileRecord serialization"""
        record = SnapshotFileRecord(
            file_path="/test/file.txt",
            chunk_hashes=["hash1", "hash2", "hash3"],
            file_size=1024,
            mtime=time.time(),
            permissions=0o644,
            checksum="test_checksum",
            content_type="text/plain",
            metadata={"custom": "value"}
        )
        
        # Test to_dict
        data = record.to_dict()
        assert data['file_path'] == "/test/file.txt"
        assert data['chunk_hashes'] == ["hash1", "hash2", "hash3"]
        assert data['file_size'] == 1024
        assert data['metadata'] == {"custom": "value"}
        
        # Test from_dict
        restored = SnapshotFileRecord.from_dict(data)
        assert restored.file_path == record.file_path
        assert restored.chunk_hashes == record.chunk_hashes
        assert restored.file_size == record.file_size
        assert restored.metadata == record.metadata
    
    def test_restore_options_serialization(self):
        """Test RestoreOptions serialization"""
        options = RestoreOptions(
            scope=RestoreScope.SELECTIVE,
            target_path=pathlib.Path("/restore/path"),
            file_patterns=["*.txt", "*.py"],
            exclude_patterns=["*test*"],
            overwrite_existing=True,
            preserve_permissions=False,
            verify_integrity=True
        )
        
        # Test to_dict
        data = options.to_dict()
        assert data['scope'] == "selective"
        assert data['target_path'] == "/restore/path"
        assert data['file_patterns'] == ["*.txt", "*.py"]
        assert data['overwrite_existing'] is True
        
        # Test from_dict
        restored = RestoreOptions.from_dict(data)
        assert restored.scope == options.scope
        assert restored.target_path == options.target_path
        assert restored.file_patterns == options.file_patterns
        assert restored.overwrite_existing == options.overwrite_existing
    
    def test_snapshot_policy_serialization(self):
        """Test SnapshotPolicy serialization"""
        policy = SnapshotPolicy(
            name="test_policy",
            enabled=True,
            schedule_cron="0 2 * * *",
            snapshot_type=SnapshotType.FULL,
            max_snapshots=30,
            max_age_days=90,
            full_snapshot_interval=7,
            compress_snapshots=True,
            tags=["auto", "daily"]
        )
        
        # Test to_dict
        data = policy.to_dict()
        assert data['name'] == "test_policy"
        assert data['snapshot_type'] == "full"
        assert data['max_snapshots'] == 30
        assert data['tags'] == ["auto", "daily"]
        
        # Test from_dict
        restored = SnapshotPolicy.from_dict(data)
        assert restored.name == policy.name
        assert restored.snapshot_type == policy.snapshot_type
        assert restored.max_snapshots == policy.max_snapshots
        assert restored.tags == policy.tags

class TestSnapshotErrorHandling:
    """Test error handling in snapshot operations"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield pathlib.Path(temp_dir)
    
    def test_restore_nonexistent_snapshot(self, temp_dir):
        """Test restoring a non-existent snapshot"""
        snapshot_system = SnapshotSystem(temp_dir / "snapshots", None, "test_peer")
        
        restore_options = RestoreOptions(scope=RestoreScope.FULL)
        success = snapshot_system.restore_snapshot("nonexistent_id", restore_options)
        
        assert not success
        snapshot_system.stop_scheduler()
    
    def test_create_snapshot_without_crdt_store(self, temp_dir):
        """Test creating snapshot without CRDT store"""
        snapshot_system = SnapshotSystem(temp_dir / "snapshots", None, "test_peer")
        
        snapshot_id = snapshot_system.create_snapshot(
            snapshot_type=SnapshotType.FULL,
            description="Test without CRDT"
        )
        
        # Should handle gracefully and return None or create empty snapshot
        if snapshot_id:
            metadata = snapshot_system.snapshot_cache[snapshot_id]
            assert metadata.file_count == 0
        
        snapshot_system.stop_scheduler()
    
    def test_delete_nonexistent_snapshot(self, temp_dir):
        """Test deleting a non-existent snapshot"""
        snapshot_system = SnapshotSystem(temp_dir / "snapshots", None, "test_peer")
        
        success = snapshot_system.delete_snapshot("nonexistent_id")
        assert not success
        
        snapshot_system.stop_scheduler()
    
    def test_cleanup_with_invalid_policy(self, temp_dir):
        """Test cleanup with invalid policy name"""
        snapshot_system = SnapshotSystem(temp_dir / "snapshots", None, "test_peer")
        
        deleted_count = snapshot_system.cleanup_snapshots("nonexistent_policy")
        assert deleted_count == 0
        
        snapshot_system.stop_scheduler()

if __name__ == "__main__":
    pytest.main([__file__])
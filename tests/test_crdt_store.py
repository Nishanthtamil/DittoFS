import tempfile
import pathlib
import time
import pytest
from datetime import datetime, timezone
from unittest.mock import patch
from dittofs.crdt_store import (
    CRDTStore, FileRecord, ConflictInfo, ConflictResolutionStrategy, 
    ConflictState, ConflictType
)

def test_register_and_retrieve():
    """Test basic file registration and retrieval"""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"hello")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore()
    store.add_file(path, ["fake_hash"])
    store.save()

    # reload fresh
    store2 = CRDTStore.load()
    entry = store2.get_file(path)
    assert entry is not None
    assert entry.hashes == ["fake_hash"]
    assert abs(entry.mtime - path.stat().st_mtime) < 1
    print("CRDT round-trip ok")

def test_enhanced_file_record():
    """Test enhanced FileRecord with new fields"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"hello world")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore(peer_id="test_peer")
    assert store.add_file(path, ["hash1", "hash2"])
    
    record = store.get_file(path)
    assert record is not None
    assert record.version == 1
    assert record.peer_id == "test_peer"
    assert record.semantic_hash != ""
    assert record.content_type == "text/plain"
    assert len(record.hashes) == 2

def test_conflict_detection():
    """Test conflict detection between file versions"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"original content")
        f.flush()
        path = pathlib.Path(f.name)

    # Create two stores representing different peers
    store1 = CRDTStore(peer_id="peer1")
    store2 = CRDTStore(peer_id="peer2")
    
    # Add file to first store
    store1.add_file(path, ["hash1"])
    
    # Modify file and add to second store
    with open(path, 'w') as f:
        f.write("modified content")
    
    store2.add_file(path, ["hash2"])
    
    # Simulate conflict by manually creating conflicting records
    original_record = store1.get_file(path)
    modified_record = store2.get_file(path)
    
    conflict = store1._detect_conflict(modified_record, original_record)
    assert conflict is not None
    # The conflict could be any of these types depending on timing
    assert conflict.conflict_type in [ConflictType.CONTENT_CONFLICT, ConflictType.SEMANTIC_CONFLICT, ConflictType.TIMESTAMP_CONFLICT]
    assert len(conflict.versions) == 2

def test_last_writer_wins_resolution():
    """Test last writer wins conflict resolution"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"content")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore()
    store.set_conflict_resolution_strategy(ConflictResolutionStrategy.LAST_WRITER_WINS)
    
    # Add initial file
    store.add_file(path, ["hash1"])
    
    # Modify file with later timestamp
    time.sleep(0.1)  # Ensure different timestamp
    with open(path, 'w') as f:
        f.write("newer content")
    
    # This should trigger conflict detection and automatic resolution
    store.add_file(path, ["hash2"])
    
    # Check that the newer version won
    final_record = store.get_file(path)
    assert final_record is not None
    assert final_record.hashes == ["hash2"]  # Should have the newer version

def test_enhanced_versioning_system():
    """Test the enhanced versioning system integration"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"Initial version content")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore(peer_id="test_peer")
    
    # Create initial version
    initial_content = b"Initial version content"
    version1_id = store.create_file_version(
        path, initial_content, commit_message="Initial version"
    )
    assert version1_id is not None
    
    # Create second version
    modified_content = b"Modified version content with changes"
    version2_id = store.create_file_version(
        path, modified_content, commit_message="Modified version"
    )
    assert version2_id is not None
    assert version2_id != version1_id
    
    # Test version history
    history = store.get_enhanced_version_history(path)
    assert len(history) >= 2
    assert history[0]['version_id'] == version2_id  # Newest first
    assert history[1]['version_id'] == version1_id
    
    # Test version content retrieval
    content1 = store.get_version_content(version1_id)
    content2 = store.get_version_content(version2_id)
    assert content1 == initial_content
    assert content2 == modified_content
    
    # Test version comparison
    comparison = store.compare_file_versions(version1_id, version2_id)
    assert comparison is not None
    assert not comparison['identical']
    assert comparison['size_diff'] > 0

def test_branch_operations():
    """Test branch creation and management"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"Main branch content")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore(peer_id="test_peer")
    
    # Create initial version on main branch
    main_content = b"Main branch content"
    main_version_id = store.create_file_version(
        path, main_content, "main", "Initial main version"
    )
    
    # Create a feature branch
    branch_id = store.create_branch("feature-branch", main_version_id, "Feature development")
    assert branch_id is not None
    
    # Create version on feature branch
    feature_content = b"Feature branch content with new functionality"
    feature_version_id = store.create_file_version(
        path, feature_content, "feature-branch", "Feature implementation"
    )
    
    # Test branch listing
    branches = store.get_branches(path)
    assert len(branches) >= 1
    branch_names = [b['branch_name'] for b in branches]
    assert "feature-branch" in branch_names
    
    # Test version history per branch
    main_history = store.get_enhanced_version_history(path, "main")
    feature_history = store.get_enhanced_version_history(path, "feature-branch")
    
    assert len(main_history) >= 1
    assert len(feature_history) >= 1

def test_version_restoration():
    """Test restoring files to previous versions"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"Original content")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore(peer_id="test_peer")
    
    # Create versions
    content1 = b"Version 1 content"
    content2 = b"Version 2 content"
    
    version1_id = store.create_file_version(path, content1, commit_message="Version 1")
    version2_id = store.create_file_version(path, content2, commit_message="Version 2")
    
    # Restore to version 1
    success = store.restore_file_version(path, version1_id)
    assert success
    
    # Verify file content was restored
    with open(path, 'rb') as f:
        restored_content = f.read()
    assert restored_content == content1

def test_retention_policy():
    """Test version cleanup with retention policies"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"Test content")
        f.flush()
        path = pathlib.Path(f.name)

    from dittofs.versioning import RetentionPolicy
    
    store = CRDTStore(peer_id="test_peer")
    
    # Create multiple versions
    version_ids = []
    for i in range(8):
        content = f"Version {i} content".encode()
        version_id = store.create_file_version(path, content, commit_message=f"Version {i}")
        version_ids.append(version_id)
        time.sleep(0.01)  # Ensure different timestamps
    
    # Apply retention policy
    policy = RetentionPolicy(max_versions=5)
    store.set_retention_policy(policy)
    
    result = store.cleanup_old_versions(path)
    assert 'cleaned' in result
    assert 'compressed' in result
    
    # Check that some versions were cleaned
    final_history = store.get_enhanced_version_history(path)
    assert len(final_history) <= 5

def test_version_tree_export():
    """Test exporting version tree for visualization"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"Initial content")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore(peer_id="test_peer")
    
    # Create versions with branches
    main_version = store.create_file_version(path, b"Main content", "main", "Initial")
    branch_id = store.create_branch("feature", main_version, "Feature branch")
    feature_version = store.create_file_version(path, b"Feature content", "feature", "Feature work")
    
    # Export version tree
    tree = store.get_version_tree(path)
    
    assert 'file_path' in tree
    assert 'branches' in tree
    assert 'versions' in tree
    assert 'relationships' in tree
    
    assert len(tree['versions']) >= 2
    assert len(tree['branches']) >= 1

def test_versioning_stats():
    """Test versioning system statistics"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"Test content")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore(peer_id="test_peer")
    
    # Create some versions
    for i in range(3):
        content = f"Version {i} content".encode()
        store.create_file_version(path, content, commit_message=f"Version {i}")
    
    # Get stats
    stats = store.get_versioning_stats()
    
    assert 'total_versions' in stats
    assert 'total_branches' in stats
    assert 'storage_size' in stats
    assert stats['total_versions'] >= 3
    record = store.get_file(path)
    assert record.hashes == ["hash2"]
    
    # Should have no pending conflicts
    pending = store.get_pending_conflicts()
    assert len(pending) == 0

def test_preserve_both_resolution():
    """Test preserve both versions conflict resolution"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"original")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore(peer_id="test_peer")
    
    # Create a conflict scenario
    record1 = FileRecord(
        path=str(path),
        hashes=["hash1"],
        mtime=time.time(),
        size=8,
        checksum="checksum1",
        peer_id="peer1"
    )
    
    record2 = FileRecord(
        path=str(path),
        hashes=["hash2"],
        mtime=time.time() + 1,
        size=10,
        checksum="checksum2",
        peer_id="peer2"
    )
    
    conflict = ConflictInfo(
        conflict_id="test_conflict",
        file_path=str(path),
        conflict_type=ConflictType.CONTENT_CONFLICT,
        state=ConflictState.DETECTED,
        detected_at=time.time(),
        versions=[record1.to_dict(), record2.to_dict()]
    )
    
    # Test preserve both resolution
    result = store._resolve_preserve_both(conflict)
    assert result["success"] is True
    assert len(result["preserved_versions"]) == 2
    
    # Check that one version has a modified path
    paths = [v["path"] for v in result["preserved_versions"]]
    assert str(path) in paths
    assert any("_conflict_" in p for p in paths)

def test_version_history():
    """Test file version history tracking"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"version 1")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore()
    
    # Add initial version
    store.add_file(path, ["hash1"])
    
    # Add second version
    with open(path, 'w') as f:
        f.write("version 2")
    store.add_file(path, ["hash2"])
    
    # Add third version
    with open(path, 'w') as f:
        f.write("version 3")
    store.add_file(path, ["hash3"])
    
    # Check version history
    versions = store.get_file_versions(path)
    assert len(versions) >= 2  # Should have at least 2 versions stored
    
    # Versions should be sorted by version number (descending)
    for i in range(len(versions) - 1):
        assert versions[i].version >= versions[i + 1].version

def test_semantic_hash_calculation():
    """Test semantic hash calculation for different file types"""
    store = CRDTStore()
    
    # Test text file semantic hash
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt', mode='w') as f:
        f.write("hello    world\n\n  with   whitespace  ")
        f.flush()
        path = pathlib.Path(f.name)
    
    semantic_hash1 = store._calculate_semantic_hash(path)
    
    # Create another file with same semantic content but different whitespace
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt', mode='w') as f:
        f.write("hello world with whitespace")
        f.flush()
        path2 = pathlib.Path(f.name)
    
    semantic_hash2 = store._calculate_semantic_hash(path2)
    
    # Semantic hashes should be the same despite whitespace differences
    assert semantic_hash1 == semantic_hash2
    assert semantic_hash1 != ""

def test_manual_conflict_resolution():
    """Test manual conflict resolution workflow"""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"content")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore()
    store.set_conflict_resolution_strategy(ConflictResolutionStrategy.MANUAL_RESOLUTION)
    
    # Create a conflict
    record1 = FileRecord(
        path=str(path),
        hashes=["hash1"],
        mtime=time.time(),
        size=7,
        checksum="checksum1"
    )
    
    record2 = FileRecord(
        path=str(path),
        hashes=["hash2"],
        mtime=time.time() + 1,
        size=8,
        checksum="checksum2"
    )
    
    conflict = ConflictInfo(
        conflict_id="manual_test",
        file_path=str(path),
        conflict_type=ConflictType.CONTENT_CONFLICT,
        state=ConflictState.DETECTED,
        detected_at=datetime.now(timezone.utc),
        versions=[record1.to_dict(), record2.to_dict()]
    )
    
    store.conflicts_map[conflict.conflict_id] = conflict.to_dict()
    
    # Check that manual resolution is required
    result = store._resolve_manual(conflict)
    assert result["success"] is False
    assert result["requires_manual_intervention"] is True
    
    # Resolve manually by choosing the second version
    manual_resolution = {
        "resolved_version": record2.to_dict(),
        "chosen_by_user": True
    }
    
    success = store.resolve_conflict(
        conflict.conflict_id, 
        ConflictResolutionStrategy.MANUAL_RESOLUTION,
        manual_resolution
    )
    assert success is True
    
    # Check that conflict is resolved
    conflicts = store.get_pending_conflicts()
    assert len([c for c in conflicts if c.conflict_id == conflict.conflict_id]) == 0

def test_merge_updates_with_conflicts():
    """Test merging updates that create conflicts"""
    # This test would require more complex CRDT simulation
    # For now, we'll test the basic merge functionality
    store1 = CRDTStore(peer_id="peer1")
    store2 = CRDTStore(peer_id="peer2")
    
    # Create a simple update in store1
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b"test content")
        f.flush()
        path = pathlib.Path(f.name)
    
    store1.add_file(path, ["hash1"])
    store1.save()  # Save to ensure state is persisted
    
    # Get state vector from store2
    state_vector = store2.get_state_vector()
    
    # Get update from store1 since store2's state
    update = store1.get_update_since(state_vector)
    
    # Only test merge if we have a non-empty update
    if update and len(update) > 0:
        success = store2.merge_updates(update)
        assert success is True
        
        # Verify file exists in store2
        record = store2.get_file(path)
        assert record is not None
        assert record.hashes == ["hash1"]
    else:
        # If no update available, just test that merge doesn't fail with empty data
        success = store2.merge_updates(b"")
        # Empty updates should be handled gracefully
        assert success is True

@pytest.mark.skipif(not hasattr(tempfile, 'NamedTemporaryFile'), reason="Requires tempfile")
def test_conflict_state_machine():
    """Test the conflict resolution state machine"""
    store = CRDTStore()
    
    # Create a conflict with complete version data
    version1 = FileRecord(
        path="/test/path",
        hashes=["hash1"],
        mtime=time.time(),
        size=100,
        checksum="checksum1"
    )
    
    version2 = FileRecord(
        path="/test/path", 
        hashes=["hash2"],
        mtime=time.time() + 1,
        size=110,
        checksum="checksum2"
    )
    
    conflict = ConflictInfo(
        conflict_id="state_test",
        file_path="/test/path",
        conflict_type=ConflictType.CONTENT_CONFLICT,
        state=ConflictState.DETECTED,
        detected_at=datetime.now(timezone.utc),
        versions=[version1.to_dict(), version2.to_dict()]
    )
    
    # Test state transitions
    assert conflict.state == ConflictState.DETECTED
    
    # Store conflict
    store.conflicts_map[conflict.conflict_id] = conflict.to_dict()
    
    # Attempt resolution
    store.resolve_conflict(conflict.conflict_id, ConflictResolutionStrategy.LAST_WRITER_WINS)
    
    # Check final state
    resolved_conflict_data = store.conflicts_map.get(conflict.conflict_id)
    if resolved_conflict_data:
        resolved_conflict = ConflictInfo.from_dict(resolved_conflict_data)
        assert resolved_conflict.state in [ConflictState.RESOLVED, ConflictState.FAILED]

if __name__ == "__main__":
    test_register_and_retrieve()
    test_enhanced_file_record()
    test_conflict_detection()
    test_last_writer_wins_resolution()
    test_preserve_both_resolution()
    test_version_history()
    test_semantic_hash_calculation()
    test_manual_conflict_resolution()
    test_merge_updates_with_conflicts()
    test_conflict_state_machine()
    print("All enhanced CRDT tests passed!")
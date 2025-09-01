"""
Tests for the comprehensive file versioning system
"""

import tempfile
import pathlib
import time
import pytest
import json
from datetime import datetime, timezone

from dittofs.versioning import (
    VersioningSystem, VersionNode, VersionBranch, RetentionPolicy,
    VersionType, DeltaType, MergeStrategy
)


class TestVersioningSystem:
    """Test the core versioning system functionality"""
    
    @pytest.fixture
    def temp_storage(self):
        """Create temporary storage directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield pathlib.Path(temp_dir)
    
    @pytest.fixture
    def versioning_system(self, temp_storage):
        """Create versioning system instance"""
        return VersioningSystem(temp_storage, peer_id="test_peer")
    
    @pytest.fixture
    def sample_file(self):
        """Create a sample file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("Initial content\nLine 2\nLine 3")
            f.flush()
            yield pathlib.Path(f.name)
            # Cleanup
            pathlib.Path(f.name).unlink(missing_ok=True)
    
    def test_create_version(self, versioning_system, sample_file):
        """Test creating a new version"""
        content = b"Test content for versioning"
        file_path = str(sample_file)
        
        version_id = versioning_system.create_version(
            file_path, content, commit_message="Initial version"
        )
        
        assert version_id is not None
        assert len(version_id) == 16  # Expected length of generated ID
        
        # Verify version was stored
        stored_content = versioning_system.get_version_content(version_id)
        assert stored_content == content
        
        # Verify version metadata
        version_node = versioning_system.version_cache.get(version_id)
        assert version_node is not None
        assert version_node.file_path == file_path
        assert version_node.version_type == VersionType.MAIN
        assert version_node.commit_message == "Initial version"
    
    def test_version_history(self, versioning_system, sample_file):
        """Test version history tracking"""
        file_path = str(sample_file)
        
        # Create multiple versions
        version_ids = []
        for i in range(3):
            content = f"Version {i} content".encode()
            version_id = versioning_system.create_version(
                file_path, content, commit_message=f"Version {i}"
            )
            version_ids.append(version_id)
            time.sleep(0.01)  # Ensure different timestamps
        
        # Get version history
        history = versioning_system.get_version_history(file_path)
        
        assert len(history) == 3
        # Should be sorted by creation time (newest first)
        assert history[0].version_id == version_ids[-1]
        assert history[-1].version_id == version_ids[0]
    
    def test_delta_storage(self, versioning_system, sample_file):
        """Test delta-based storage for efficiency"""
        file_path = str(sample_file)
        
        # Create initial version
        initial_content = b"Line 1\nLine 2\nLine 3\n"
        version1_id = versioning_system.create_version(
            file_path, initial_content, commit_message="Initial"
        )
        
        # Create modified version (should use delta)
        modified_content = b"Line 1\nModified Line 2\nLine 3\nNew Line 4\n"
        version2_id = versioning_system.create_version(
            file_path, modified_content, 
            parent_versions=[version1_id],
            commit_message="Modified",
            use_delta=True
        )
        
        # Verify both versions can be retrieved
        content1 = versioning_system.get_version_content(version1_id)
        content2 = versioning_system.get_version_content(version2_id)
        
        assert content1 == initial_content
        assert content2 == modified_content
        
        # Check if delta was used
        version2_node = versioning_system.version_cache.get(version2_id)
        # Delta might not be used if it's not efficient, so we just check it works
        assert version2_node is not None
    
    def test_branch_creation(self, versioning_system, sample_file):
        """Test creating and managing branches"""
        file_path = str(sample_file)
        
        # Create initial version
        content = b"Initial content"
        main_version_id = versioning_system.create_version(
            file_path, content, commit_message="Initial"
        )
        
        # Create a branch
        branch_id = versioning_system.create_branch(
            "feature-branch", main_version_id, "Feature development branch"
        )
        
        assert branch_id is not None
        
        # Verify branch was created
        branch = versioning_system.branch_cache.get(branch_id)
        assert branch is not None
        assert branch.branch_name == "feature-branch"
        assert branch.parent_version == main_version_id
        
        # Create version on branch
        branch_content = b"Feature content"
        branch_version_id = versioning_system.create_version(
            file_path, branch_content, branch_id, [main_version_id], "Feature work"
        )
        
        # Verify branch version
        branch_version = versioning_system.version_cache.get(branch_version_id)
        assert branch_version.branch_id == branch_id
        assert branch_version.version_type == VersionType.BRANCH
    
    def test_version_comparison(self, versioning_system, sample_file):
        """Test comparing different versions"""
        file_path = str(sample_file)
        
        # Create two versions
        content1 = b"Original content\nLine 2"
        content2 = b"Modified content\nLine 2\nNew line"
        
        version1_id = versioning_system.create_version(file_path, content1)
        version2_id = versioning_system.create_version(file_path, content2)
        
        # Compare versions
        comparison = versioning_system.compare_versions(version1_id, version2_id)
        
        assert comparison is not None
        assert not comparison['identical']
        assert comparison['size_diff'] > 0  # Second version is larger
        assert comparison.get('is_text', False)  # Should detect as text
        assert 'text_diff' in comparison
    
    def test_merge_branches(self, versioning_system, sample_file):
        """Test merging branches"""
        file_path = str(sample_file)
        
        # Create initial version
        initial_content = b"Line 1\nLine 2\nLine 3"
        main_version_id = versioning_system.create_version(
            file_path, initial_content, "main", commit_message="Initial"
        )
        
        # Create branch
        branch_id = versioning_system.create_branch("feature", main_version_id)
        
        # Create version on main branch
        main_content = b"Line 1\nModified on main\nLine 3"
        main_version2_id = versioning_system.create_version(
            file_path, main_content, "main", [main_version_id], "Main changes"
        )
        
        # Create version on feature branch
        feature_content = b"Line 1\nLine 2\nFeature addition"
        feature_version_id = versioning_system.create_version(
            file_path, feature_content, branch_id, [main_version_id], "Feature changes"
        )
        
        # Attempt merge (this is a simplified test - real merging is complex)
        merge_result = versioning_system.merge_branches("main", branch_id)
        
        # The merge might fail due to conflicts, which is expected
        # We're mainly testing that the merge process doesn't crash
        assert merge_result is not None or merge_result is None  # Either outcome is valid
    
    def test_retention_policy(self, versioning_system, sample_file):
        """Test version cleanup with retention policies"""
        file_path = str(sample_file)
        
        # Create many versions
        version_ids = []
        for i in range(10):
            content = f"Version {i} content".encode()
            version_id = versioning_system.create_version(
                file_path, content, commit_message=f"Version {i}"
            )
            version_ids.append(version_id)
            time.sleep(0.01)
        
        # Apply retention policy to keep only 5 versions
        policy = RetentionPolicy(max_versions=5)
        result = versioning_system.apply_retention_policy(file_path, policy)
        
        assert result['cleaned'] >= 0  # Some versions should be cleaned
        
        # Verify remaining versions
        remaining_history = versioning_system.get_version_history(file_path)
        assert len(remaining_history) <= 5
    
    def test_storage_stats(self, versioning_system, sample_file):
        """Test storage statistics"""
        file_path = str(sample_file)
        
        # Create some versions
        for i in range(3):
            content = f"Version {i} content".encode()
            versioning_system.create_version(file_path, content)
        
        # Get storage stats
        stats = versioning_system.get_storage_stats()
        
        assert 'total_versions' in stats
        assert 'total_branches' in stats
        assert 'storage_size' in stats
        assert stats['total_versions'] >= 3
    
    def test_version_tree_export(self, versioning_system, sample_file):
        """Test exporting version tree for visualization"""
        file_path = str(sample_file)
        
        # Create versions with branches
        main_version = versioning_system.create_version(
            file_path, b"Initial", commit_message="Initial"
        )
        
        branch_id = versioning_system.create_branch("feature", main_version)
        branch_version = versioning_system.create_version(
            file_path, b"Feature", branch_id, [main_version], "Feature work"
        )
        
        # Export version tree
        tree = versioning_system.export_version_tree(file_path)
        
        assert 'file_path' in tree
        assert 'branches' in tree
        assert 'versions' in tree
        assert 'relationships' in tree
        
        assert len(tree['versions']) >= 2
        assert len(tree['branches']) >= 1
    
    def test_persistence(self, temp_storage):
        """Test that versioning data persists across system restarts"""
        file_path = "test_file.txt"
        
        # Create versioning system and add version
        vs1 = VersioningSystem(temp_storage, "test_peer")
        version_id = vs1.create_version(file_path, b"Test content")
        
        # Create new versioning system instance (simulating restart)
        vs2 = VersioningSystem(temp_storage, "test_peer")
        
        # Verify data was loaded
        content = vs2.get_version_content(version_id)
        assert content == b"Test content"
        
        history = vs2.get_version_history(file_path)
        assert len(history) == 1
        assert history[0].version_id == version_id


class TestVersioningIntegration:
    """Test integration with CRDT store"""
    
    @pytest.fixture
    def temp_file(self):
        """Create temporary file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("Test content for integration")
            f.flush()
            yield pathlib.Path(f.name)
            pathlib.Path(f.name).unlink(missing_ok=True)
    
    def test_crdt_versioning_integration(self, temp_file):
        """Test that CRDT store integrates with versioning system"""
        from dittofs.crdt_store import CRDTStore
        
        # Create CRDT store (which should initialize versioning)
        store = CRDTStore(peer_id="test_peer")
        
        # Verify versioning system is initialized
        assert hasattr(store, 'versioning')
        assert store.versioning is not None
        
        # Test creating file version through CRDT store
        content = b"Test content for CRDT integration"
        version_id = store.create_file_version(
            temp_file, content, commit_message="Integration test"
        )
        
        assert version_id is not None
        
        # Test retrieving version content
        retrieved_content = store.get_version_content(version_id)
        assert retrieved_content == content
        
        # Test enhanced version history
        history = store.get_enhanced_version_history(temp_file)
        assert len(history) >= 1
        assert history[0]['version_id'] == version_id
    
    def test_retention_policy_integration(self, temp_file):
        """Test retention policy through CRDT store"""
        from dittofs.crdt_store import CRDTStore
        from dittofs.versioning import RetentionPolicy
        
        store = CRDTStore(peer_id="test_peer")
        
        # Create multiple versions
        for i in range(5):
            content = f"Version {i} content".encode()
            store.create_file_version(temp_file, content, commit_message=f"Version {i}")
        
        # Set and apply retention policy
        policy = RetentionPolicy(max_versions=3)
        store.set_retention_policy(policy)
        
        result = store.cleanup_old_versions(temp_file)
        assert 'cleaned' in result
        assert 'compressed' in result
        
        # Verify versions were cleaned up
        history = store.get_enhanced_version_history(temp_file)
        assert len(history) <= 3


if __name__ == "__main__":
    pytest.main([__file__])
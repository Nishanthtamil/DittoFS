"""
Tests for conflict resolution workflows

This module tests the comprehensive conflict resolution capabilities including:
- User-friendly conflict resolution interface
- Automatic conflict resolution for simple cases
- Manual conflict resolution tools with side-by-side comparison
- Conflict resolution history and audit trail
"""

import pytest
import tempfile
import pathlib
import time
import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch, AsyncMock

from dittofs.conflict_resolution import (
    ConflictResolutionWorkflow, ResolutionAction, ResolutionResult,
    ResolutionEntry, ConflictDiff
)
from dittofs.crdt_store import (
    CRDTStore, FileRecord, ConflictInfo, ConflictState, ConflictType,
    ConflictResolutionStrategy
)


@pytest.fixture
def temp_store_path():
    """Create a temporary store path for testing"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir) / "test_store.yrs"


@pytest.fixture
def mock_crdt_store(temp_store_path):
    """Create a mock CRDT store for testing"""
    store = Mock(spec=CRDTStore)
    store.store_path = temp_store_path
    store.peer_id = "test_peer_123"
    store.files_map = {}
    store.conflicts_map = {}
    store.active_conflicts = {}
    
    # Mock methods
    store.get_pending_conflicts = Mock(return_value=[])
    
    return store


@pytest.fixture
def conflict_workflow(mock_crdt_store):
    """Create a conflict resolution workflow for testing"""
    return ConflictResolutionWorkflow(mock_crdt_store)


@pytest.fixture
def sample_conflict():
    """Create a sample conflict for testing"""
    return ConflictInfo(
        conflict_id="conflict_123",
        file_path="/test/file.txt",
        conflict_type=ConflictType.CONTENT_CONFLICT,
        state=ConflictState.DETECTED,
        detected_at=datetime.now(timezone.utc),
        versions=[
            {
                'path': '/test/file.txt',
                'hashes': ['chunk1', 'chunk2'],
                'checksum': 'hash1',
                'mtime': time.time() - 100,
                'size': 1000,
                'version': 1,
                'peer_id': 'peer1',
                'metadata': {'author': 'user1'}
            },
            {
                'path': '/test/file.txt',
                'hashes': ['chunk3', 'chunk4'],
                'checksum': 'hash2',
                'mtime': time.time() - 50,
                'size': 1200,
                'version': 2,
                'peer_id': 'peer2',
                'metadata': {'author': 'user2'}
            }
        ]
    )


class TestConflictResolutionWorkflow:
    """Test the main conflict resolution workflow"""
    
    def test_initialization(self, conflict_workflow, mock_crdt_store):
        """Test workflow initialization"""
        assert conflict_workflow.crdt_store == mock_crdt_store
        assert conflict_workflow.peer_id == "test_peer_123"
        assert len(conflict_workflow.auto_resolution_rules) > 0
        assert isinstance(conflict_workflow.resolution_history, list)
    
    def test_resolution_history_persistence(self, temp_store_path):
        """Test that resolution history is persisted to disk"""
        # Create workflow and add some history
        store = Mock(spec=CRDTStore)
        store.store_path = temp_store_path
        store.peer_id = "test_peer"
        
        workflow = ConflictResolutionWorkflow(store)
        
        # Add a resolution entry
        workflow._add_resolution_entry(
            "conflict_123", "/test/file.txt", ResolutionAction.KEEP_LOCAL,
            ResolutionResult.SUCCESS, ConflictResolutionStrategy.AUTOMATIC_MERGE
        )
        
        # Create new workflow instance and verify history is loaded
        workflow2 = ConflictResolutionWorkflow(store)
        assert len(workflow2.resolution_history) == 1
        assert workflow2.resolution_history[0].conflict_id == "conflict_123"
    
    def test_auto_resolution_rules(self, conflict_workflow, sample_conflict):
        """Test automatic resolution rules"""
        # Test non-overlapping metadata changes
        sample_conflict.conflict_type = ConflictType.METADATA_CONFLICT
        sample_conflict.versions[0]['metadata'] = {'author': 'user1'}
        sample_conflict.versions[1]['metadata'] = {'editor': 'user2'}
        
        can_resolve, action = conflict_workflow.can_auto_resolve(sample_conflict)
        assert can_resolve
        assert action == ResolutionAction.MERGE_AUTOMATIC
    
    def test_metadata_only_conflicts(self, conflict_workflow, sample_conflict):
        """Test auto-resolution of metadata-only conflicts"""
        # Same content, different metadata
        sample_conflict.conflict_type = ConflictType.METADATA_CONFLICT
        sample_conflict.versions[0]['checksum'] = 'same_hash'
        sample_conflict.versions[1]['checksum'] = 'same_hash'
        
        can_resolve, action = conflict_workflow.can_auto_resolve(sample_conflict)
        assert can_resolve
        assert action == ResolutionAction.MERGE_AUTOMATIC
    
    def test_permission_conflicts(self, conflict_workflow, sample_conflict):
        """Test auto-resolution of permission conflicts"""
        sample_conflict.conflict_type = ConflictType.PERMISSION_CONFLICT
        sample_conflict.versions[0]['permissions'] = 0o644
        sample_conflict.versions[1]['permissions'] = 0o755
        
        can_resolve, action = conflict_workflow.can_auto_resolve(sample_conflict)
        assert can_resolve
        assert action == ResolutionAction.KEEP_REMOTE  # More permissive
    
    def test_timestamp_tolerance(self, conflict_workflow, sample_conflict):
        """Test auto-resolution within timestamp tolerance"""
        sample_conflict.conflict_type = ConflictType.TIMESTAMP_CONFLICT
        base_time = time.time()
        sample_conflict.versions[0]['mtime'] = base_time
        sample_conflict.versions[1]['mtime'] = base_time + 1  # Within tolerance
        
        can_resolve, action = conflict_workflow.can_auto_resolve(sample_conflict)
        assert can_resolve
        assert action == ResolutionAction.KEEP_REMOTE  # Newer timestamp
    
    @pytest.mark.asyncio
    async def test_auto_resolve_conflict_success(self, conflict_workflow):
        """Test successful automatic conflict resolution"""
        # Create a conflict that can be auto-resolved (metadata-only with same content)
        auto_resolvable_conflict = ConflictInfo(
            conflict_id="auto_conflict_123",
            file_path="/test/file.txt",
            conflict_type=ConflictType.METADATA_CONFLICT,
            state=ConflictState.DETECTED,
            detected_at=datetime.now(timezone.utc),
            versions=[
                {
                    'path': '/test/file.txt',
                    'hashes': ['chunk1', 'chunk2'],
                    'checksum': 'same_hash',  # Same content
                    'mtime': time.time() - 100,
                    'size': 1000,
                    'version': 1,
                    'peer_id': 'peer1',
                    'metadata': {'author': 'user1'}
                },
                {
                    'path': '/test/file.txt',
                    'hashes': ['chunk1', 'chunk2'],
                    'checksum': 'same_hash',  # Same content
                    'mtime': time.time() - 50,
                    'size': 1000,
                    'version': 2,
                    'peer_id': 'peer2',
                    'metadata': {'editor': 'user2'}
                }
            ]
        )
        
        # Setup mock store
        conflict_workflow.crdt_store.conflicts_map = {
            "auto_conflict_123": auto_resolvable_conflict.to_dict()
        }
        
        # Mock the execution method to return success
        conflict_workflow._execute_resolution_action = AsyncMock(return_value=True)
        
        # Test auto-resolution
        result = await conflict_workflow.auto_resolve_conflict("auto_conflict_123")
        assert result is True
        
        # Verify resolution was recorded
        assert len(conflict_workflow.resolution_history) == 1
        entry = conflict_workflow.resolution_history[0]
        assert entry.conflict_id == "auto_conflict_123"
        assert entry.result == ResolutionResult.SUCCESS
    
    @pytest.mark.asyncio
    async def test_auto_resolve_conflict_failure(self, conflict_workflow, sample_conflict):
        """Test failed automatic conflict resolution"""
        # Use a content conflict that can't be auto-resolved
        conflict_workflow.crdt_store.conflicts_map = {
            "conflict_123": sample_conflict.to_dict()
        }
        
        # Test auto-resolution - should fail because content conflicts can't be auto-resolved
        result = await conflict_workflow.auto_resolve_conflict("conflict_123")
        assert result is False
        
        # No resolution should be recorded for conflicts that can't be auto-resolved
        assert len(conflict_workflow.resolution_history) == 0
    
    @pytest.mark.asyncio
    async def test_manual_resolve_conflict(self, conflict_workflow, sample_conflict):
        """Test manual conflict resolution"""
        # Setup mock store
        conflict_workflow.crdt_store.conflicts_map = {
            "conflict_123": sample_conflict.to_dict()
        }
        
        # Mock the execution method
        conflict_workflow._execute_resolution_action = AsyncMock(return_value=True)
        
        # Test manual resolution
        result = await conflict_workflow.manual_resolve_conflict(
            "conflict_123", ResolutionAction.KEEP_LOCAL
        )
        assert result is True
        
        # Verify resolution was recorded
        assert len(conflict_workflow.resolution_history) == 1
        entry = conflict_workflow.resolution_history[0]
        assert entry.action_taken == ResolutionAction.KEEP_LOCAL
        assert entry.strategy_used == ConflictResolutionStrategy.MANUAL_RESOLUTION
    
    def test_generate_conflict_diff(self, conflict_workflow, sample_conflict):
        """Test conflict diff generation"""
        # Setup mock store
        conflict_workflow.crdt_store.conflicts_map = {
            "conflict_123": sample_conflict.to_dict()
        }
        
        # Generate diff
        diff = conflict_workflow.generate_conflict_diff("conflict_123")
        
        assert diff is not None
        assert diff.file_path == "/test/file.txt"
        assert diff.local_version.checksum == "hash1"
        assert diff.remote_version.checksum == "hash2"
        assert len(diff.metadata_diff) > 0
    
    def test_text_file_detection(self, conflict_workflow):
        """Test text file detection for diff generation"""
        assert conflict_workflow._is_text_file(pathlib.Path("test.txt"))
        assert conflict_workflow._is_text_file(pathlib.Path("test.py"))
        assert conflict_workflow._is_text_file(pathlib.Path("test.json"))
        assert not conflict_workflow._is_text_file(pathlib.Path("test.jpg"))
        assert not conflict_workflow._is_text_file(pathlib.Path("test.bin"))
    
    def test_metadata_diff_generation(self, conflict_workflow):
        """Test metadata diff generation"""
        local_record = FileRecord(
            path="/test/file.txt",
            hashes=["hash1"],
            mtime=1000,
            size=100,
            metadata={"author": "user1", "tags": ["work"]}
        )
        
        remote_record = FileRecord(
            path="/test/file.txt",
            hashes=["hash2"],
            mtime=2000,
            size=200,
            metadata={"author": "user2", "category": "docs"}
        )
        
        diff = conflict_workflow._generate_metadata_diff(local_record, remote_record)
        
        assert "mtime" in diff
        assert "size" in diff
        assert "metadata" in diff
        assert diff["metadata"]["author"]["local"] == "user1"
        assert diff["metadata"]["author"]["remote"] == "user2"
    
    @pytest.mark.asyncio
    async def test_batch_auto_resolve(self, conflict_workflow):
        """Test batch auto-resolution of conflicts"""
        # Create mock conflicts
        conflicts = [
            ConflictInfo(
                conflict_id=f"conflict_{i}",
                file_path=f"/test/file{i}.txt",
                conflict_type=ConflictType.METADATA_CONFLICT,
                state=ConflictState.DETECTED,
                detected_at=datetime.now(timezone.utc),
                versions=[
                    {'checksum': 'same_hash', 'metadata': {'author': f'user{i}'}},
                    {'checksum': 'same_hash', 'metadata': {'editor': f'editor{i}'}}
                ]
            )
            for i in range(3)
        ]
        
        # Mock the store method
        conflict_workflow.crdt_store.get_pending_conflicts = Mock(return_value=conflicts)
        conflict_workflow.auto_resolve_conflict = AsyncMock(return_value=True)
        
        # Test batch resolution
        results = await conflict_workflow.batch_auto_resolve()
        
        assert results['total_conflicts'] == 3
        assert results['auto_resolved'] == 3
        assert results['manual_required'] == 0
        assert results['failed'] == 0
    
    def test_resolution_stats(self, conflict_workflow):
        """Test resolution statistics generation"""
        # Add some resolution history
        for i in range(5):
            conflict_workflow._add_resolution_entry(
                f"conflict_{i}", f"/test/file{i}.txt",
                ResolutionAction.KEEP_LOCAL if i % 2 == 0 else ResolutionAction.MERGE_AUTOMATIC,
                ResolutionResult.SUCCESS,
                ConflictResolutionStrategy.AUTOMATIC_MERGE
            )
        
        stats = conflict_workflow.get_resolution_stats()
        
        assert stats['total_resolutions'] == 5
        assert stats['success_rate'] == 1.0
        assert stats['auto_resolution_rate'] == 1.0
        assert 'resolution_actions' in stats
        assert 'resolution_strategies' in stats
    
    def test_custom_handler_registration(self, conflict_workflow):
        """Test custom conflict resolution handler registration"""
        def custom_handler(conflict):
            return ResolutionAction.PRESERVE_BOTH
        
        conflict_workflow.register_custom_handler("custom_test", custom_handler)
        
        assert "custom_test" in conflict_workflow.custom_handlers
        assert conflict_workflow.custom_handlers["custom_test"] == custom_handler


class TestResolutionEntry:
    """Test the ResolutionEntry data class"""
    
    def test_to_dict_conversion(self):
        """Test conversion to dictionary"""
        entry = ResolutionEntry(
            resolution_id="res_123",
            conflict_id="conflict_123",
            file_path="/test/file.txt",
            resolved_at=datetime.now(timezone.utc),
            resolved_by="test_peer",
            action_taken=ResolutionAction.KEEP_LOCAL,
            result=ResolutionResult.SUCCESS,
            strategy_used=ConflictResolutionStrategy.AUTOMATIC_MERGE
        )
        
        data = entry.to_dict()
        
        assert data['resolution_id'] == "res_123"
        assert data['action_taken'] == "keep_local"
        assert data['result'] == "success"
        assert data['strategy_used'] == "automatic_merge"
        assert isinstance(data['resolved_at'], str)
    
    def test_from_dict_conversion(self):
        """Test conversion from dictionary"""
        data = {
            'resolution_id': "res_123",
            'conflict_id': "conflict_123",
            'file_path': "/test/file.txt",
            'resolved_at': datetime.now(timezone.utc).isoformat(),
            'resolved_by': "test_peer",
            'action_taken': "keep_local",
            'result': "success",
            'strategy_used': "automatic_merge",
            'details': {}
        }
        
        entry = ResolutionEntry.from_dict(data)
        
        assert entry.resolution_id == "res_123"
        assert entry.action_taken == ResolutionAction.KEEP_LOCAL
        assert entry.result == ResolutionResult.SUCCESS
        assert entry.strategy_used == ConflictResolutionStrategy.AUTOMATIC_MERGE
        assert isinstance(entry.resolved_at, datetime)


class TestConflictDiff:
    """Test the ConflictDiff functionality"""
    
    def test_conflict_diff_creation(self):
        """Test creation of conflict diff"""
        local_record = FileRecord(
            path="/test/file.txt",
            hashes=["hash1"],
            mtime=1000,
            size=100
        )
        
        remote_record = FileRecord(
            path="/test/file.txt",
            hashes=["hash2"],
            mtime=2000,
            size=200
        )
        
        diff = ConflictDiff(
            file_path="/test/file.txt",
            local_version=local_record,
            remote_version=remote_record,
            binary_file=False
        )
        
        assert diff.file_path == "/test/file.txt"
        assert diff.local_version == local_record
        assert diff.remote_version == remote_record
        assert not diff.binary_file


@pytest.mark.integration
class TestConflictResolutionIntegration:
    """Integration tests for conflict resolution workflows"""
    
    @pytest.mark.asyncio
    async def test_end_to_end_auto_resolution(self, temp_store_path):
        """Test end-to-end automatic conflict resolution"""
        # This would require a real CRDT store, so we'll mock it for now
        # In a real integration test, you'd use actual file operations
        
        store = Mock(spec=CRDTStore)
        store.store_path = temp_store_path
        store.peer_id = "integration_test_peer"
        store.files_map = {}
        store.conflicts_map = {}
        store.active_conflicts = {}
        
        workflow = ConflictResolutionWorkflow(store)
        
        # Create a conflict that can be auto-resolved
        conflict = ConflictInfo(
            conflict_id="integration_conflict",
            file_path="/test/integration.txt",
            conflict_type=ConflictType.METADATA_CONFLICT,
            state=ConflictState.DETECTED,
            detected_at=datetime.now(timezone.utc),
            versions=[
                {'checksum': 'same_hash', 'metadata': {'author': 'user1'}},
                {'checksum': 'same_hash', 'metadata': {'editor': 'user2'}}
            ]
        )
        
        store.conflicts_map["integration_conflict"] = conflict.to_dict()
        
        # Mock the execution to simulate success
        workflow._execute_resolution_action = AsyncMock(return_value=True)
        
        # Test auto-resolution
        result = await workflow.auto_resolve_conflict("integration_conflict")
        
        assert result is True
        assert len(workflow.resolution_history) == 1
        
        # Verify the resolution was properly recorded
        entry = workflow.resolution_history[0]
        assert entry.conflict_id == "integration_conflict"
        assert entry.result == ResolutionResult.SUCCESS


if __name__ == "__main__":
    pytest.main([__file__])
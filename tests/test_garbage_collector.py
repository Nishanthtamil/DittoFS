"""
Tests for the garbage collection system
"""

import asyncio
import json
import pathlib
import tempfile
import time
from unittest.mock import MagicMock, Mock, patch

import pytest

from src.dittofs.chunker import CHUNK_DIR, ensure_chunk_dir
from src.dittofs.crdt_store import CRDTStore, FileRecord
from src.dittofs.garbage_collector import (
    ChunkReferenceTracker,
    GarbageCollector,
    GCStats,
    RetentionPolicy,
    get_garbage_collector,
    run_garbage_collection,
)


class TestRetentionPolicy:
    """Test retention policy configuration"""

    def test_default_policy(self):
        """Test default retention policy values"""
        policy = RetentionPolicy()

        assert policy.min_age_seconds == 3600  # 1 hour
        assert policy.max_deletions_per_run == 1000
        assert policy.delete_orphaned is True
        assert policy.delete_from_missing_files is True
        assert policy.access_grace_period == 86400  # 24 hours
        assert policy.min_free_space_bytes == 1024 * 1024 * 1024  # 1GB
        assert policy.emergency_mode_threshold == 100 * 1024 * 1024  # 100MB

    def test_custom_policy(self):
        """Test custom retention policy"""
        policy = RetentionPolicy(
            min_age_seconds=7200,  # 2 hours
            max_deletions_per_run=500,
            delete_orphaned=False,
            emergency_mode_threshold=50 * 1024 * 1024,  # 50MB
        )

        assert policy.min_age_seconds == 7200
        assert policy.max_deletions_per_run == 500
        assert policy.delete_orphaned is False
        assert policy.emergency_mode_threshold == 50 * 1024 * 1024

    def test_policy_serialization(self):
        """Test policy serialization to/from dict"""
        policy = RetentionPolicy(min_age_seconds=1800, max_deletions_per_run=200)

        policy_dict = policy.to_dict()
        assert policy_dict["min_age_seconds"] == 1800
        assert policy_dict["max_deletions_per_run"] == 200

        restored_policy = RetentionPolicy.from_dict(policy_dict)
        assert restored_policy.min_age_seconds == 1800
        assert restored_policy.max_deletions_per_run == 200


class TestGCStats:
    """Test garbage collection statistics"""

    def test_default_stats(self):
        """Test default GC stats"""
        stats = GCStats()

        assert stats.chunks_scanned == 0
        assert stats.chunks_deleted == 0
        assert stats.bytes_freed == 0
        assert stats.orphaned_chunks == 0
        assert stats.referenced_chunks == 0
        assert stats.errors == 0
        assert stats.duration_seconds == 0.0
        assert stats.timestamp is not None

    def test_stats_serialization(self):
        """Test stats serialization to/from dict"""
        stats = GCStats(
            chunks_scanned=100,
            chunks_deleted=10,
            bytes_freed=1024,
            orphaned_chunks=5,
            referenced_chunks=95,
            errors=1,
            duration_seconds=5.5,
        )

        stats_dict = stats.to_dict()
        assert stats_dict["chunks_scanned"] == 100
        assert stats_dict["chunks_deleted"] == 10
        assert stats_dict["bytes_freed"] == 1024
        assert "timestamp" in stats_dict

        restored_stats = GCStats.from_dict(stats_dict)
        assert restored_stats.chunks_scanned == 100
        assert restored_stats.chunks_deleted == 10
        assert restored_stats.bytes_freed == 1024


class TestChunkReferenceTracker:
    """Test chunk reference tracking"""

    @pytest.fixture
    def mock_store(self):
        """Create a mock CRDT store"""
        store = Mock(spec=CRDTStore)
        return store

    @pytest.fixture
    def tracker(self, mock_store):
        """Create a chunk reference tracker"""
        return ChunkReferenceTracker(mock_store)

    def test_cache_invalidation(self, tracker):
        """Test cache invalidation"""
        # Initially cache should be invalid
        assert not tracker._cache_valid

        # Simulate building cache
        tracker._cache_valid = True
        tracker._reference_cache = {"chunk1": {"file1"}}

        # Invalidate cache
        tracker.invalidate_cache()
        assert not tracker._cache_valid
        assert len(tracker._reference_cache) == 0

    def test_chunk_access_tracking(self, tracker):
        """Test chunk access time tracking"""
        chunk_hash = "test_chunk"

        # Initially no access time
        assert tracker.get_chunk_last_access(chunk_hash) == 0.0

        # Update access time
        tracker.update_chunk_access_time(chunk_hash)
        access_time = tracker.get_chunk_last_access(chunk_hash)
        assert access_time > 0
        assert abs(access_time - time.time()) < 1.0  # Within 1 second

    def test_get_chunk_references(self, tracker, mock_store):
        """Test getting chunk references from store"""
        # Mock file records
        file1 = FileRecord(
            path="/test/file1.txt",
            hashes=["chunk1", "chunk2"],
            mtime=time.time(),
            size=1024,
        )
        file2 = FileRecord(
            path="/test/file2.txt",
            hashes=["chunk2", "chunk3"],
            mtime=time.time(),
            size=2048,
        )

        mock_store.list_files.return_value = [file1, file2]

        references = tracker.get_chunk_references()

        assert "chunk1" in references
        assert "chunk2" in references
        assert "chunk3" in references

        assert "/test/file1.txt" in references["chunk1"]
        assert "/test/file1.txt" in references["chunk2"]
        assert "/test/file2.txt" in references["chunk2"]
        assert "/test/file2.txt" in references["chunk3"]

        # Cache should be valid now
        assert tracker._cache_valid

    def test_recently_accessed_check(self, tracker):
        """Test recently accessed chunk detection"""
        chunk_hash = "test_chunk"
        grace_period = 3600  # 1 hour

        # Not recently accessed initially
        assert not tracker.is_chunk_recently_accessed(chunk_hash, grace_period)

        # Update access time
        tracker.update_chunk_access_time(chunk_hash)

        # Should be recently accessed now
        assert tracker.is_chunk_recently_accessed(chunk_hash, grace_period)

        # Simulate old access time
        tracker._access_times[chunk_hash] = time.time() - 7200  # 2 hours ago
        assert not tracker.is_chunk_recently_accessed(chunk_hash, grace_period)

    @patch("src.dittofs.garbage_collector.CHUNK_DIR")
    def test_get_orphaned_chunks(self, mock_chunk_dir, tracker, mock_store):
        """Test finding orphaned chunks"""
        # Mock chunk directory
        mock_chunk_dir.exists.return_value = True

        # Create proper mock objects with name attributes
        chunk1_mock = Mock()
        chunk1_mock.name = "chunk1"
        chunk1_mock.is_file.return_value = True

        chunk2_mock = Mock()
        chunk2_mock.name = "chunk2"
        chunk2_mock.is_file.return_value = True

        chunk3_mock = Mock()
        chunk3_mock.name = "chunk3"
        chunk3_mock.is_file.return_value = True

        not_chunk_mock = Mock()
        not_chunk_mock.name = "not_a_chunk"
        not_chunk_mock.is_file.return_value = False

        mock_chunk_dir.iterdir.return_value = [
            chunk1_mock,
            chunk2_mock,
            chunk3_mock,
            not_chunk_mock,
        ]

        # Mock file records (only chunk1 and chunk2 are referenced)
        file1 = FileRecord(
            path="/test/file1.txt",
            hashes=["chunk1", "chunk2"],
            mtime=time.time(),
            size=1024,
        )
        mock_store.list_files.return_value = [file1]

        orphaned = tracker.get_orphaned_chunks()

        # chunk3 should be orphaned
        assert "chunk3" in orphaned
        assert "chunk1" not in orphaned
        assert "chunk2" not in orphaned

    def test_get_chunks_from_missing_files(self, tracker, mock_store):
        """Test finding chunks from missing files"""
        # Mock file records with some missing files
        file1 = FileRecord(
            path="/nonexistent/file1.txt",  # This file doesn't exist
            hashes=["chunk1", "chunk2"],
            mtime=time.time(),
            size=1024,
        )

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            existing_file = tmp.name

        file2 = FileRecord(
            path=existing_file,  # This file exists
            hashes=["chunk3", "chunk4"],
            mtime=time.time(),
            size=2048,
        )

        mock_store.list_files.return_value = [file1, file2]

        try:
            chunks_from_missing = tracker.get_chunks_from_missing_files()

            # Chunks from missing file should be included
            assert "chunk1" in chunks_from_missing
            assert "chunk2" in chunks_from_missing
            # Chunks from existing file should not be included
            assert "chunk3" not in chunks_from_missing
            assert "chunk4" not in chunks_from_missing
        finally:
            pathlib.Path(existing_file).unlink()


class TestGarbageCollector:
    """Test garbage collector main functionality"""

    @pytest.fixture
    def mock_store(self):
        """Create a mock CRDT store"""
        return Mock(spec=CRDTStore)

    @pytest.fixture
    def policy(self):
        """Create a test retention policy"""
        return RetentionPolicy(
            min_age_seconds=1,  # Very short for testing
            max_deletions_per_run=10,
            min_free_space_bytes=1024 * 1024,  # 1MB
            emergency_mode_threshold=512 * 1024,  # 512KB
        )

    @pytest.fixture
    def gc(self, mock_store, policy):
        """Create a garbage collector"""
        return GarbageCollector(mock_store, policy)

    def test_initialization(self, gc, mock_store, policy):
        """Test garbage collector initialization"""
        assert gc.store == mock_store
        assert gc.policy == policy
        assert isinstance(gc.tracker, ChunkReferenceTracker)
        assert not gc._running
        assert gc._scheduler_task is None

    @patch("src.dittofs.garbage_collector.CHUNK_DIR")
    @patch("shutil.disk_usage")
    def test_get_storage_info(self, mock_disk_usage, mock_chunk_dir, gc):
        """Test storage information retrieval"""
        # Mock disk usage
        mock_disk_usage.return_value = (1000000, 500000, 500000)  # total, used, free

        # Mock chunk directory without statvfs to force fallback to shutil.disk_usage
        mock_chunk_dir.exists.return_value = True
        mock_chunk_dir.statvfs = None  # Force fallback

        # Create proper mock objects for chunks
        chunk1_mock = Mock()
        chunk1_mock.is_file.return_value = True
        chunk1_mock.stat.return_value = Mock(st_size=1024)

        chunk2_mock = Mock()
        chunk2_mock.is_file.return_value = True
        chunk2_mock.stat.return_value = Mock(st_size=2048)

        mock_chunk_dir.iterdir.return_value = [chunk1_mock, chunk2_mock]

        storage_info = gc.get_storage_info()

        assert storage_info["free_bytes"] == 500000
        assert storage_info["total_bytes"] == 1000000
        assert storage_info["chunk_bytes"] == 3072  # 1024 + 2048
        assert storage_info["chunk_count"] == 2

    @patch("shutil.disk_usage")
    def test_should_run_emergency_gc(self, mock_disk_usage, gc):
        """Test emergency GC trigger detection"""
        # Mock low free space (below emergency threshold)
        mock_disk_usage.return_value = (1000000, 900000, 100000)  # 100KB free

        assert gc.should_run_emergency_gc() is True

        # Mock sufficient free space
        mock_disk_usage.return_value = (1000000, 400000, 600000)  # 600KB free

        assert gc.should_run_emergency_gc() is False

    @patch("shutil.disk_usage")
    def test_should_run_regular_gc(self, mock_disk_usage, gc):
        """Test regular GC trigger detection"""
        # Mock low free space (below regular threshold but above emergency)
        mock_disk_usage.return_value = (2000000, 1200000, 800000)  # 800KB free

        assert gc.should_run_regular_gc() is True

        # Mock sufficient free space
        mock_disk_usage.return_value = (2000000, 500000, 1500000)  # 1.5MB free

        assert gc.should_run_regular_gc() is False

    @pytest.mark.asyncio
    async def test_collect_garbage_basic(self, gc):
        """Test basic garbage collection run"""

        # Mock async methods to avoid actual file operations
        async def mock_find_candidates(emergency_mode):
            return ["chunk1", "chunk2"]

        async def mock_prioritize_candidates(candidates):
            return candidates

        async def mock_delete_chunks(candidates):
            return (2, 3072, 0)  # deleted, bytes, errors

        async def mock_get_orphaned_chunks():
            return {"chunk3"}

        gc._find_deletion_candidates = mock_find_candidates
        gc._prioritize_candidates = mock_prioritize_candidates
        gc._delete_chunks = mock_delete_chunks
        gc._get_orphaned_chunks = mock_get_orphaned_chunks
        gc.tracker.get_chunk_references = Mock(return_value={"chunk4": {"/test/file"}})

        stats = await gc.collect_garbage()

        assert stats.chunks_deleted == 2
        assert stats.bytes_freed == 3072
        assert stats.errors == 0
        assert stats.orphaned_chunks == 1
        assert stats.referenced_chunks == 1
        assert stats.duration_seconds > 0

    @pytest.mark.asyncio
    async def test_collect_garbage_already_running(self, gc):
        """Test garbage collection when already running"""
        gc._running = True

        stats = await gc.collect_garbage()

        # Should return empty stats
        assert stats.chunks_deleted == 0
        assert stats.bytes_freed == 0

    @pytest.mark.asyncio
    async def test_collect_garbage_force_when_running(self, gc):
        """Test forced garbage collection when already running"""
        gc._running = True

        # Mock methods
        gc._find_deletion_candidates = Mock(return_value=[])
        gc._prioritize_candidates = Mock(return_value=[])
        gc._delete_chunks = Mock(return_value=(0, 0, 0))
        gc._get_orphaned_chunks = Mock(return_value=set())
        gc.tracker.get_chunk_references = Mock(return_value={})

        stats = await gc.collect_garbage(force=True)

        # Should run even when already running
        assert stats.chunks_scanned == 0
        assert stats.duration_seconds > 0

    def test_stats_persistence(self, gc, tmp_path):
        """Test GC stats persistence"""
        # Mock the stats file path
        with patch.object(
            gc, "get_stats_file_path", return_value=tmp_path / "gc_stats.json"
        ):
            # Add some stats
            stats1 = GCStats(chunks_deleted=5, bytes_freed=1024)
            stats2 = GCStats(chunks_deleted=3, bytes_freed=512)

            gc._stats_history = [stats1, stats2]
            gc._save_stats_history()

            # Verify file was created
            stats_file = tmp_path / "gc_stats.json"
            assert stats_file.exists()

            # Load stats and verify
            gc._stats_history = []
            gc._load_stats_history()

            assert len(gc._stats_history) == 2
            assert gc._stats_history[0].chunks_deleted == 5
            assert gc._stats_history[1].chunks_deleted == 3

    @pytest.mark.asyncio
    async def test_scheduler(self, gc):
        """Test garbage collection scheduler"""
        # Mock GC methods
        gc.should_run_emergency_gc = Mock(return_value=False)
        gc.should_run_regular_gc = Mock(return_value=True)
        gc.collect_garbage = Mock(return_value=GCStats())

        # Start scheduler with very short interval
        await gc.start_scheduler(interval_seconds=0.1)

        # Wait a bit for scheduler to run
        await asyncio.sleep(0.2)

        # Stop scheduler
        await gc.stop_scheduler()

        # Verify GC was called
        gc.collect_garbage.assert_called()

    @pytest.mark.asyncio
    async def test_is_safe_to_delete(self, gc):
        """Test chunk deletion safety check"""
        chunk_hash = "test_chunk"

        # Mock tracker methods
        gc.tracker.get_chunk_references = Mock(return_value={})
        gc.tracker.is_chunk_recently_accessed = Mock(return_value=False)

        # Should be safe to delete (no references, not recently accessed)
        assert await gc._is_safe_to_delete(chunk_hash) is True

        # Mock chunk with references to existing file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            existing_file = tmp.name

        try:
            gc.tracker.get_chunk_references = Mock(
                return_value={chunk_hash: {existing_file}}
            )

            # Should not be safe to delete (has references to existing file)
            assert await gc._is_safe_to_delete(chunk_hash) is False
        finally:
            pathlib.Path(existing_file).unlink()

        # Mock recently accessed chunk
        gc.tracker.get_chunk_references = Mock(return_value={})
        gc.tracker.is_chunk_recently_accessed = Mock(return_value=True)

        # Should not be safe to delete (recently accessed)
        assert await gc._is_safe_to_delete(chunk_hash) is False


class TestGlobalFunctions:
    """Test global garbage collection functions"""

    def test_get_garbage_collector_singleton(self):
        """Test global garbage collector singleton"""
        # Clear any existing instance
        import src.dittofs.garbage_collector as gc_module

        gc_module._gc_instance = None

        # Get first instance
        gc1 = get_garbage_collector()
        assert gc1 is not None

        # Get second instance - should be the same
        gc2 = get_garbage_collector()
        assert gc1 is gc2

    def test_set_garbage_collector(self):
        """Test setting global garbage collector"""
        import src.dittofs.garbage_collector as gc_module

        mock_store = Mock(spec=CRDTStore)
        custom_gc = GarbageCollector(mock_store)

        # Set custom instance
        gc_module.set_garbage_collector(custom_gc)

        # Verify it's returned by get_garbage_collector
        retrieved_gc = get_garbage_collector()
        assert retrieved_gc is custom_gc

    @pytest.mark.asyncio
    async def test_run_garbage_collection(self):
        """Test global garbage collection function"""
        import src.dittofs.garbage_collector as gc_module

        # Mock the global instance with async method
        mock_gc = Mock()

        async def mock_collect_garbage(force=False):
            return GCStats(chunks_deleted=5)

        mock_gc.collect_garbage = mock_collect_garbage
        gc_module._gc_instance = mock_gc

        stats = await run_garbage_collection(force=True)

        assert stats.chunks_deleted == 5


class TestIntegration:
    """Integration tests with real components"""

    @pytest.mark.asyncio
    async def test_integration_with_real_store(self, tmp_path):
        """Test garbage collection with real CRDT store"""
        # Skip if pycrdt not available
        pytest.importorskip("pycrdt")

        # Create temporary store
        store_path = tmp_path / "test_store.yrs"
        store = CRDTStore(store_path)

        # Create temporary chunk directory
        chunk_dir = tmp_path / "chunks"
        chunk_dir.mkdir()

        with patch("src.dittofs.garbage_collector.CHUNK_DIR", chunk_dir):
            # Create some test chunks
            (chunk_dir / "chunk1").write_bytes(b"test data 1")
            (chunk_dir / "chunk2").write_bytes(b"test data 2")
            (chunk_dir / "chunk3").write_bytes(b"orphaned chunk")

            # Add file record that references chunk1 and chunk2
            test_file = tmp_path / "test.txt"
            test_file.write_text("test content")

            store.add_file(test_file, ["chunk1", "chunk2"])

            # Create garbage collector
            policy = RetentionPolicy(min_age_seconds=0)  # No age requirement
            gc = GarbageCollector(store, policy)

            # Run garbage collection
            stats = await gc.collect_garbage()

            # chunk3 should be deleted (orphaned)
            assert not (chunk_dir / "chunk3").exists()
            # chunk1 and chunk2 should remain (referenced)
            assert (chunk_dir / "chunk1").exists()
            assert (chunk_dir / "chunk2").exists()

            assert stats.chunks_deleted >= 1
            assert stats.orphaned_chunks >= 0

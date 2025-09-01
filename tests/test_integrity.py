"""
Tests for the integrity verification system.
"""

import asyncio
import pytest
import tempfile
import pathlib
import time
import blake3
from unittest.mock import Mock, AsyncMock, patch

from src.dittofs.integrity import (
    IntegrityVerifier, IntegrityScheduler, RecoveryProcedures,
    CorruptionType, RepairStatus, CorruptionReport, RepairResult
)
from src.dittofs.crdt_store import CRDTStore, FileRecord
from src.dittofs.chunker import CHUNK_DIR, _store_chunk


class TestIntegrityVerifier:
    """Test the IntegrityVerifier class"""
    
    @pytest.fixture
    def temp_chunk_dir(self):
        """Create temporary chunk directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            chunk_dir = pathlib.Path(temp_dir) / "chunks"
            chunk_dir.mkdir(parents=True, exist_ok=True)
            yield chunk_dir
    
    @pytest.fixture
    def mock_store(self):
        """Create mock CRDT store"""
        store = Mock(spec=CRDTStore)
        store.list_files.return_value = []
        store.get_file.return_value = None
        return store
    
    @pytest.fixture
    def verifier(self, mock_store, temp_chunk_dir):
        """Create IntegrityVerifier instance"""
        return IntegrityVerifier(mock_store, temp_chunk_dir)
    
    def test_init(self, verifier, temp_chunk_dir):
        """Test verifier initialization"""
        assert verifier.chunk_dir == temp_chunk_dir
        assert verifier.corruption_reports == []
        assert verifier.repair_results == []
        assert verifier.stats.total_chunks_checked == 0
        assert not verifier.is_checking
    
    @pytest.mark.asyncio
    async def test_verify_chunk_missing(self, verifier):
        """Test verification of missing chunk"""
        chunk_hash = "nonexistent_chunk_hash"
        
        is_valid, report = await verifier.verify_chunk(chunk_hash)
        
        assert not is_valid
        assert report is not None
        assert report.corruption_type == CorruptionType.CHUNK_MISSING
        assert report.chunk_hash == chunk_hash
        assert "missing" in report.error_message.lower()
    
    @pytest.mark.asyncio
    async def test_verify_chunk_valid(self, verifier, temp_chunk_dir):
        """Test verification of valid chunk"""
        # Create a valid chunk
        chunk_data = b"test chunk data"
        chunk_hash = blake3.blake3(chunk_data).hexdigest()
        chunk_path = temp_chunk_dir / chunk_hash
        chunk_path.write_bytes(chunk_data)
        
        is_valid, report = await verifier.verify_chunk(chunk_hash)
        
        assert is_valid
        assert report is None
    
    @pytest.mark.asyncio
    async def test_verify_chunk_corrupted(self, verifier, temp_chunk_dir):
        """Test verification of corrupted chunk"""
        # Create a chunk with wrong content
        chunk_data = b"test chunk data"
        chunk_hash = blake3.blake3(chunk_data).hexdigest()
        corrupted_data = b"corrupted data"
        
        chunk_path = temp_chunk_dir / chunk_hash
        chunk_path.write_bytes(corrupted_data)
        
        is_valid, report = await verifier.verify_chunk(chunk_hash)
        
        assert not is_valid
        assert report is not None
        assert report.corruption_type == CorruptionType.HASH_MISMATCH
        assert report.chunk_hash == chunk_hash
    
    @pytest.mark.asyncio
    async def test_verify_file_integrity(self, verifier, temp_chunk_dir):
        """Test file integrity verification"""
        # Create test chunks
        chunk1_data = b"chunk 1 data"
        chunk1_hash = blake3.blake3(chunk1_data).hexdigest()
        chunk1_path = temp_chunk_dir / chunk1_hash
        chunk1_path.write_bytes(chunk1_data)
        
        # Missing chunk
        chunk2_hash = "missing_chunk_hash"
        
        # Create file record
        file_record = FileRecord(
            path="/test/file.txt",
            hashes=[chunk1_hash, chunk2_hash],
            mtime=time.time(),
            size=100
        )
        
        reports = await verifier.verify_file_integrity(file_record)
        
        assert len(reports) == 1
        assert reports[0].corruption_type == CorruptionType.CHUNK_MISSING
        assert reports[0].chunk_hash == chunk2_hash
        assert reports[0].file_path == file_record.path
    
    @pytest.mark.asyncio
    async def test_verify_all_chunks(self, verifier, mock_store, temp_chunk_dir):
        """Test verification of all chunks"""
        # Create test file records
        chunk1_data = b"chunk 1 data"
        chunk1_hash = blake3.blake3(chunk1_data).hexdigest()
        chunk1_path = temp_chunk_dir / chunk1_hash
        chunk1_path.write_bytes(chunk1_data)
        
        chunk2_hash = "missing_chunk_hash"
        
        file_record = FileRecord(
            path="/test/file.txt",
            hashes=[chunk1_hash, chunk2_hash],
            mtime=time.time(),
            size=100
        )
        
        mock_store.list_files.return_value = [file_record]
        
        reports = await verifier.verify_all_chunks()
        
        assert len(reports) == 1
        assert reports[0].corruption_type == CorruptionType.CHUNK_MISSING
        assert verifier.stats.total_chunks_checked == 2
        assert verifier.stats.corrupted_chunks_found == 1
    
    @pytest.mark.asyncio
    async def test_repair_chunk_success(self, verifier):
        """Test successful chunk repair"""
        chunk_data = b"test chunk data"
        chunk_hash = blake3.blake3(chunk_data).hexdigest()
        
        # Mock peer provider
        async def mock_provider(hash_val):
            if hash_val == chunk_hash:
                return chunk_data
            return None
        
        verifier.register_peer_provider("test_peer", mock_provider)
        
        result = await verifier.repair_chunk(chunk_hash)
        
        assert result.status == RepairStatus.SUCCESS
        assert result.chunk_hash == chunk_hash
        assert result.peer_source == "test_peer"
        
        # Verify chunk was stored
        chunk_path = verifier.chunk_dir / chunk_hash
        assert chunk_path.exists()
        assert chunk_path.read_bytes() == chunk_data
    
    @pytest.mark.asyncio
    async def test_repair_chunk_no_peers(self, verifier):
        """Test chunk repair with no peers"""
        chunk_hash = "test_chunk_hash"
        
        result = await verifier.repair_chunk(chunk_hash)
        
        assert result.status == RepairStatus.NO_PEERS
        assert result.chunk_hash == chunk_hash
    
    @pytest.mark.asyncio
    async def test_repair_chunk_invalid_data(self, verifier):
        """Test chunk repair with invalid peer data"""
        chunk_hash = "test_chunk_hash"
        
        # Mock peer provider that returns invalid data
        async def mock_provider(hash_val):
            return b"invalid data"
        
        verifier.register_peer_provider("test_peer", mock_provider)
        
        result = await verifier.repair_chunk(chunk_hash)
        
        assert result.status == RepairStatus.NO_PEERS
        assert result.chunk_hash == chunk_hash
    
    @pytest.mark.asyncio
    async def test_recover_file(self, verifier, mock_store, temp_chunk_dir):
        """Test file recovery"""
        # Create one valid chunk and one missing chunk
        chunk1_data = b"chunk 1 data"
        chunk1_hash = blake3.blake3(chunk1_data).hexdigest()
        chunk1_path = temp_chunk_dir / chunk1_hash
        chunk1_path.write_bytes(chunk1_data)
        
        chunk2_data = b"chunk 2 data"
        chunk2_hash = blake3.blake3(chunk2_data).hexdigest()
        
        file_record = FileRecord(
            path="/test/file.txt",
            hashes=[chunk1_hash, chunk2_hash],
            mtime=time.time(),
            size=100
        )
        
        mock_store.get_file.return_value = file_record
        
        # Mock peer provider for missing chunk
        async def mock_provider(hash_val):
            if hash_val == chunk2_hash:
                return chunk2_data
            return None
        
        verifier.register_peer_provider("test_peer", mock_provider)
        
        success = await verifier.recover_file("/test/file.txt")
        
        assert success
        
        # Verify both chunks exist
        assert (temp_chunk_dir / chunk1_hash).exists()
        assert (temp_chunk_dir / chunk2_hash).exists()
    
    def test_get_corruption_summary(self, verifier):
        """Test corruption summary generation"""
        # Add some corruption reports
        report1 = CorruptionReport(
            corruption_type=CorruptionType.CHUNK_MISSING,
            chunk_hash="hash1",
            severity="high"
        )
        report2 = CorruptionReport(
            corruption_type=CorruptionType.CHUNK_CORRUPTED,
            chunk_hash="hash2",
            severity="medium"
        )
        
        verifier.corruption_reports = [report1, report2]
        
        summary = verifier.get_corruption_summary()
        
        assert summary['total_reports'] == 2
        assert summary['by_type']['chunk_missing'] == 1
        assert summary['by_type']['chunk_corrupted'] == 1
        assert summary['by_severity']['high'] == 1
        assert summary['by_severity']['medium'] == 1
    
    def test_get_repair_summary(self, verifier):
        """Test repair summary generation"""
        # Add some repair results
        result1 = RepairResult(
            status=RepairStatus.SUCCESS,
            chunk_hash="hash1"
        )
        result2 = RepairResult(
            status=RepairStatus.FAILED,
            chunk_hash="hash2"
        )
        
        verifier.repair_results = [result1, result2]
        
        summary = verifier.get_repair_summary()
        
        assert summary['total_repairs'] == 2
        assert summary['by_status']['success'] == 1
        assert summary['by_status']['failed'] == 1
        assert summary['success_rate'] == 0.5
    
    def test_clear_old_reports(self, verifier):
        """Test clearing old reports"""
        # Add old and new reports
        old_time = time.time() - (200 * 60 * 60)  # 200 hours ago
        new_time = time.time() - (1 * 60 * 60)    # 1 hour ago
        
        old_report = CorruptionReport(
            corruption_type=CorruptionType.CHUNK_MISSING,
            chunk_hash="old_hash",
            detected_at=old_time
        )
        new_report = CorruptionReport(
            corruption_type=CorruptionType.CHUNK_MISSING,
            chunk_hash="new_hash",
            detected_at=new_time
        )
        
        old_result = RepairResult(
            status=RepairStatus.SUCCESS,
            chunk_hash="old_hash",
            repair_time=old_time
        )
        new_result = RepairResult(
            status=RepairStatus.SUCCESS,
            chunk_hash="new_hash",
            repair_time=new_time
        )
        
        verifier.corruption_reports = [old_report, new_report]
        verifier.repair_results = [old_result, new_result]
        
        verifier.clear_old_reports(max_age_hours=168)  # 1 week
        
        assert len(verifier.corruption_reports) == 1
        assert verifier.corruption_reports[0].chunk_hash == "new_hash"
        assert len(verifier.repair_results) == 1
        assert verifier.repair_results[0].chunk_hash == "new_hash"


class TestIntegrityScheduler:
    """Test the IntegrityScheduler class"""
    
    @pytest.fixture
    def mock_verifier(self):
        """Create mock IntegrityVerifier"""
        verifier = Mock(spec=IntegrityVerifier)
        verifier.verify_all_chunks = AsyncMock(return_value=[])
        verifier.repair_all_corruptions = AsyncMock(return_value=[])
        verifier.corruption_reports = []
        verifier.repair_results = []
        verifier.stats = Mock()
        verifier.stats.last_full_check = 0.0
        verifier.stats.last_incremental_check = 0.0
        verifier.clear_old_reports = Mock()
        return verifier
    
    @pytest.fixture
    def scheduler(self, mock_verifier):
        """Create IntegrityScheduler instance"""
        return IntegrityScheduler(mock_verifier)
    
    def test_init(self, scheduler):
        """Test scheduler initialization"""
        assert not scheduler.is_running
        assert scheduler.scheduler_task is None
        assert scheduler.full_check_interval == 24 * 60 * 60
        assert scheduler.incremental_check_interval == 60 * 60
        assert scheduler.auto_repair_enabled
        assert scheduler.max_concurrent_repairs == 5
    
    @pytest.mark.asyncio
    async def test_start_stop(self, scheduler):
        """Test scheduler start and stop"""
        # Start scheduler
        await scheduler.start()
        assert scheduler.is_running
        assert scheduler.scheduler_task is not None
        
        # Stop scheduler
        await scheduler.stop()
        assert not scheduler.is_running
    
    def test_configure(self, scheduler):
        """Test scheduler configuration"""
        scheduler.configure(
            full_check_interval=12 * 60 * 60,
            incremental_check_interval=30 * 60,
            auto_repair_enabled=False,
            max_concurrent_repairs=10
        )
        
        assert scheduler.full_check_interval == 12 * 60 * 60
        assert scheduler.incremental_check_interval == 30 * 60
        assert not scheduler.auto_repair_enabled
        assert scheduler.max_concurrent_repairs == 10


class TestRecoveryProcedures:
    """Test the RecoveryProcedures class"""
    
    @pytest.fixture
    def mock_verifier(self):
        """Create mock IntegrityVerifier"""
        verifier = Mock(spec=IntegrityVerifier)
        verifier.verify_all_chunks = AsyncMock(return_value=[])
        verifier.repair_all_corruptions = AsyncMock(return_value=[])
        verifier.verify_file_integrity = AsyncMock(return_value=[])
        verifier.recover_file = AsyncMock(return_value=True)
        verifier.chunk_dir = pathlib.Path("/tmp/chunks")
        return verifier
    
    @pytest.fixture
    def mock_store(self):
        """Create mock CRDT store"""
        store = Mock(spec=CRDTStore)
        store.list_files.return_value = []
        return store
    
    @pytest.fixture
    def recovery(self, mock_verifier, mock_store):
        """Create RecoveryProcedures instance"""
        return RecoveryProcedures(mock_verifier, mock_store)
    
    @pytest.mark.asyncio
    async def test_emergency_recovery(self, recovery, mock_verifier, mock_store):
        """Test emergency recovery procedure"""
        # Setup mock data
        file_record = FileRecord(
            path="/test/file.txt",
            hashes=["hash1", "hash2"],
            mtime=time.time(),
            size=100
        )
        mock_store.list_files.return_value = [file_record]
        
        # Mock corruption reports
        corruption_report = CorruptionReport(
            corruption_type=CorruptionType.CHUNK_MISSING,
            chunk_hash="hash1"
        )
        mock_verifier.verify_all_chunks.return_value = [corruption_report]
        
        # Mock repair results
        repair_result = RepairResult(
            status=RepairStatus.SUCCESS,
            chunk_hash="hash1"
        )
        mock_verifier.repair_all_corruptions.return_value = [repair_result]
        
        # Mock file integrity verification to return no reports (file is OK after repair)
        mock_verifier.verify_file_integrity.return_value = []
        
        stats = await recovery.emergency_recovery()
        
        assert stats['chunks_recovered'] == 1
        assert stats['files_recovered'] == 1
        assert len(stats['errors']) == 0
        
        # Verify methods were called
        mock_verifier.verify_all_chunks.assert_called_once()
        mock_verifier.repair_all_corruptions.assert_called_once()
        # recover_file is not called when file integrity verification returns no reports
        # but the file is still counted as recovered because its chunks were repaired
    
    @pytest.mark.asyncio
    async def test_selective_recovery(self, recovery, mock_verifier):
        """Test selective file recovery"""
        file_paths = ["/test/file1.txt", "/test/file2.txt"]
        
        # Mock successful recovery for first file, failure for second
        mock_verifier.recover_file.side_effect = [True, False]
        
        stats = await recovery.selective_recovery(file_paths)
        
        assert stats['files_attempted'] == 2
        assert stats['files_recovered'] == 1
        assert len(stats['errors']) == 1
        
        # Verify recover_file was called for both files
        assert mock_verifier.recover_file.call_count == 2


class TestCorruptionReport:
    """Test the CorruptionReport dataclass"""
    
    def test_init_with_defaults(self):
        """Test CorruptionReport initialization with defaults"""
        report = CorruptionReport(
            corruption_type=CorruptionType.CHUNK_MISSING,
            chunk_hash="test_hash"
        )
        
        assert report.corruption_type == CorruptionType.CHUNK_MISSING
        assert report.chunk_hash == "test_hash"
        assert report.file_path is None
        assert report.error_message == ""
        assert report.detected_at > 0
        assert report.severity == "medium"
    
    def test_to_dict(self):
        """Test CorruptionReport to_dict conversion"""
        report = CorruptionReport(
            corruption_type=CorruptionType.CHUNK_CORRUPTED,
            chunk_hash="test_hash",
            file_path="/test/file.txt",
            error_message="Test error",
            severity="high"
        )
        
        data = report.to_dict()
        
        assert data['corruption_type'] == "chunk_corrupted"
        assert data['chunk_hash'] == "test_hash"
        assert data['file_path'] == "/test/file.txt"
        assert data['error_message'] == "Test error"
        assert data['severity'] == "high"
        assert 'detected_at' in data


class TestRepairResult:
    """Test the RepairResult dataclass"""
    
    def test_init_with_defaults(self):
        """Test RepairResult initialization with defaults"""
        result = RepairResult(
            status=RepairStatus.SUCCESS,
            chunk_hash="test_hash"
        )
        
        assert result.status == RepairStatus.SUCCESS
        assert result.chunk_hash == "test_hash"
        assert result.file_path is None
        assert result.error_message == ""
        assert result.repair_time > 0
        assert result.peer_source is None
    
    def test_to_dict(self):
        """Test RepairResult to_dict conversion"""
        result = RepairResult(
            status=RepairStatus.FAILED,
            chunk_hash="test_hash",
            error_message="Test error",
            peer_source="peer1"
        )
        
        data = result.to_dict()
        
        assert data['status'] == "failed"
        assert data['chunk_hash'] == "test_hash"
        assert data['error_message'] == "Test error"
        assert data['peer_source'] == "peer1"
        assert 'repair_time' in data


if __name__ == "__main__":
    pytest.main([__file__])
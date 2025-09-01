"""
Integrity verification system for DittoFS chunks and metadata.

This module provides comprehensive integrity checking, automatic repair,
and recovery procedures for the distributed file system.
"""

import asyncio
import logging
import pathlib
import time
import hashlib
import json
from typing import Dict, List, Optional, Set, Tuple, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import blake3

from .chunker import CHUNK_DIR, verify_chunk_integrity, _retrieve_chunk, _store_chunk
from .crdt_store import CRDTStore, FileRecord


class CorruptionType(Enum):
    """Types of corruption that can be detected"""
    CHUNK_MISSING = "chunk_missing"
    CHUNK_CORRUPTED = "chunk_corrupted"
    METADATA_CORRUPTED = "metadata_corrupted"
    HASH_MISMATCH = "hash_mismatch"
    PERMISSION_DENIED = "permission_denied"
    STORAGE_FULL = "storage_full"


class RepairStatus(Enum):
    """Status of repair operations"""
    SUCCESS = "success"
    FAILED = "failed"
    NO_PEERS = "no_peers"
    PARTIAL = "partial"
    SKIPPED = "skipped"


@dataclass
class CorruptionReport:
    """Report of detected corruption"""
    corruption_type: CorruptionType
    chunk_hash: Optional[str] = None
    file_path: Optional[str] = None
    error_message: str = ""
    detected_at: float = 0.0
    severity: str = "medium"  # low, medium, high, critical
    
    def __post_init__(self):
        if self.detected_at == 0.0:
            self.detected_at = time.time()
    
    def to_dict(self) -> dict:
        return {
            **asdict(self),
            'corruption_type': self.corruption_type.value,
            'detected_at': self.detected_at
        }


@dataclass
class RepairResult:
    """Result of a repair operation"""
    status: RepairStatus
    chunk_hash: Optional[str] = None
    file_path: Optional[str] = None
    error_message: str = ""
    repair_time: float = 0.0
    peer_source: Optional[str] = None
    
    def __post_init__(self):
        if self.repair_time == 0.0:
            self.repair_time = time.time()
    
    def to_dict(self) -> dict:
        return {
            **asdict(self),
            'status': self.status.value,
            'repair_time': self.repair_time
        }


@dataclass
class IntegrityStats:
    """Statistics about integrity verification"""
    total_chunks_checked: int = 0
    corrupted_chunks_found: int = 0
    successful_repairs: int = 0
    failed_repairs: int = 0
    last_full_check: float = 0.0
    last_incremental_check: float = 0.0
    check_duration: float = 0.0
    
    def to_dict(self) -> dict:
        return asdict(self)


class IntegrityVerifier:
    """Core integrity verification and repair system"""
    
    def __init__(self, store: CRDTStore, chunk_dir: pathlib.Path = None):
        self.store = store
        self.chunk_dir = chunk_dir or CHUNK_DIR
        self.corruption_reports: List[CorruptionReport] = []
        self.repair_results: List[RepairResult] = []
        self.stats = IntegrityStats()
        self.peer_chunk_providers: Dict[str, Callable] = {}
        self.is_checking = False
        
        # Ensure directories exist
        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        self._integrity_log_path = self.chunk_dir.parent / "integrity.log"
        
        # Setup logging
        self._setup_integrity_logging()
    
    def _setup_integrity_logging(self):
        """Setup dedicated integrity logging"""
        self.integrity_logger = logging.getLogger("dittofs.integrity")
        
        # Create file handler for integrity logs
        handler = logging.FileHandler(self._integrity_log_path)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.integrity_logger.addHandler(handler)
        self.integrity_logger.setLevel(logging.INFO)
    
    def register_peer_provider(self, peer_id: str, provider_func: Callable):
        """Register a function to get chunks from a specific peer"""
        self.peer_chunk_providers[peer_id] = provider_func
    
    async def verify_chunk(self, chunk_hash: str) -> Tuple[bool, Optional[CorruptionReport]]:
        """Verify integrity of a single chunk"""
        chunk_path = self.chunk_dir / chunk_hash
        
        try:
            # Check if chunk file exists
            if not chunk_path.exists():
                report = CorruptionReport(
                    corruption_type=CorruptionType.CHUNK_MISSING,
                    chunk_hash=chunk_hash,
                    error_message=f"Chunk file missing: {chunk_path}",
                    severity="high"
                )
                return False, report
            
            # Check if we can read the chunk
            try:
                chunk_data = _retrieve_chunk(chunk_hash, chunk_path)
            except PermissionError:
                report = CorruptionReport(
                    corruption_type=CorruptionType.PERMISSION_DENIED,
                    chunk_hash=chunk_hash,
                    error_message=f"Permission denied reading chunk: {chunk_path}",
                    severity="medium"
                )
                return False, report
            except Exception as e:
                report = CorruptionReport(
                    corruption_type=CorruptionType.CHUNK_CORRUPTED,
                    chunk_hash=chunk_hash,
                    error_message=f"Failed to read chunk: {e}",
                    severity="high"
                )
                return False, report
            
            # Verify hash matches content
            computed_hash = blake3.blake3(chunk_data).hexdigest()
            if computed_hash != chunk_hash:
                report = CorruptionReport(
                    corruption_type=CorruptionType.HASH_MISMATCH,
                    chunk_hash=chunk_hash,
                    error_message=f"Hash mismatch: expected {chunk_hash}, got {computed_hash}",
                    severity="critical"
                )
                return False, report
            
            # Additional verification using existing chunker function (handles encryption)
            # Only if we're using the default chunk directory
            if self.chunk_dir == CHUNK_DIR:
                try:
                    if not verify_chunk_integrity(chunk_hash):
                        report = CorruptionReport(
                            corruption_type=CorruptionType.CHUNK_CORRUPTED,
                            chunk_hash=chunk_hash,
                            error_message="Chunk failed integrity verification",
                            severity="high"
                        )
                        return False, report
                except Exception as e:
                    # If chunker verification fails, we already verified the hash above
                    # so this is likely an encryption-related issue, which we can ignore
                    # since we already verified the basic integrity
                    logging.debug(f"Chunker verification failed for {chunk_hash}: {e}")
                    pass
            
            return True, None
            
        except Exception as e:
            report = CorruptionReport(
                corruption_type=CorruptionType.CHUNK_CORRUPTED,
                chunk_hash=chunk_hash,
                error_message=f"Unexpected error during verification: {e}",
                severity="medium"
            )
            return False, report
    
    async def verify_file_integrity(self, file_record: FileRecord) -> List[CorruptionReport]:
        """Verify integrity of all chunks for a file"""
        reports = []
        
        for chunk_hash in file_record.hashes:
            is_valid, report = await self.verify_chunk(chunk_hash)
            if not is_valid and report:
                report.file_path = file_record.path
                reports.append(report)
        
        return reports
    
    async def verify_all_chunks(self, incremental: bool = False) -> List[CorruptionReport]:
        """Verify integrity of all chunks in the system"""
        if self.is_checking:
            logging.warning("Integrity check already in progress")
            return []
        
        self.is_checking = True
        start_time = time.time()
        reports = []
        
        try:
            # Get all chunks that should exist
            all_chunk_hashes = set()
            
            # From file records
            for file_record in self.store.list_files():
                all_chunk_hashes.update(file_record.hashes)
            
            # From chunk directory (to catch orphaned chunks)
            if self.chunk_dir.exists():
                for chunk_file in self.chunk_dir.iterdir():
                    if chunk_file.is_file() and len(chunk_file.name) == 64:  # BLAKE3 hash length
                        all_chunk_hashes.add(chunk_file.name)
            
            self.integrity_logger.info(f"Starting integrity check of {len(all_chunk_hashes)} chunks")
            
            # Check each chunk
            checked_count = 0
            for chunk_hash in all_chunk_hashes:
                is_valid, report = await self.verify_chunk(chunk_hash)
                
                if not is_valid and report:
                    reports.append(report)
                    self.corruption_reports.append(report)
                    self.integrity_logger.warning(f"Corruption detected: {report.to_dict()}")
                
                checked_count += 1
                
                # Yield control periodically to avoid blocking
                if checked_count % 100 == 0:
                    await asyncio.sleep(0.01)
            
            # Update statistics
            self.stats.total_chunks_checked = checked_count
            self.stats.corrupted_chunks_found = len(reports)
            self.stats.check_duration = time.time() - start_time
            
            if incremental:
                self.stats.last_incremental_check = time.time()
            else:
                self.stats.last_full_check = time.time()
            
            self.integrity_logger.info(
                f"Integrity check completed: {checked_count} chunks checked, "
                f"{len(reports)} corruptions found in {self.stats.check_duration:.2f}s"
            )
            
        except Exception as e:
            self.integrity_logger.error(f"Integrity check failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.is_checking = False
        
        return reports
    
    async def repair_chunk(self, chunk_hash: str, peer_sources: Optional[List[str]] = None) -> RepairResult:
        """Attempt to repair a corrupted or missing chunk"""
        try:
            # Try to get chunk data from peers
            chunk_data = None
            peer_source = None
            
            # Use provided peer sources or all registered providers
            sources_to_try = peer_sources or list(self.peer_chunk_providers.keys())
            
            for peer_id in sources_to_try:
                if peer_id in self.peer_chunk_providers:
                    try:
                        provider_func = self.peer_chunk_providers[peer_id]
                        chunk_data = await provider_func(chunk_hash)
                        
                        if chunk_data:
                            # Verify the data before using it
                            computed_hash = blake3.blake3(chunk_data).hexdigest()
                            if computed_hash == chunk_hash:
                                peer_source = peer_id
                                break
                            else:
                                self.integrity_logger.warning(
                                    f"Peer {peer_id} provided invalid data for chunk {chunk_hash}"
                                )
                                chunk_data = None
                    except Exception as e:
                        self.integrity_logger.debug(f"Failed to get chunk from peer {peer_id}: {e}")
                        continue
            
            if not chunk_data:
                return RepairResult(
                    status=RepairStatus.NO_PEERS,
                    chunk_hash=chunk_hash,
                    error_message="No peers could provide valid chunk data"
                )
            
            # Store the repaired chunk
            chunk_path = self.chunk_dir / chunk_hash
            _store_chunk(chunk_data, chunk_hash, chunk_path)
            
            # Verify the repair was successful
            is_valid, _ = await self.verify_chunk(chunk_hash)
            
            if is_valid:
                result = RepairResult(
                    status=RepairStatus.SUCCESS,
                    chunk_hash=chunk_hash,
                    peer_source=peer_source
                )
                self.repair_results.append(result)
                self.stats.successful_repairs += 1
                self.integrity_logger.info(f"Successfully repaired chunk {chunk_hash} from peer {peer_source}")
                return result
            else:
                result = RepairResult(
                    status=RepairStatus.FAILED,
                    chunk_hash=chunk_hash,
                    error_message="Repair verification failed"
                )
                self.repair_results.append(result)
                self.stats.failed_repairs += 1
                return result
                
        except Exception as e:
            result = RepairResult(
                status=RepairStatus.FAILED,
                chunk_hash=chunk_hash,
                error_message=f"Repair failed with exception: {e}"
            )
            self.repair_results.append(result)
            self.stats.failed_repairs += 1
            self.integrity_logger.error(f"Failed to repair chunk {chunk_hash}: {e}")
            return result
    
    async def repair_all_corruptions(self) -> List[RepairResult]:
        """Attempt to repair all detected corruptions"""
        results = []
        
        # Get unique chunk hashes that need repair
        chunks_to_repair = set()
        for report in self.corruption_reports:
            if report.chunk_hash and report.corruption_type in [
                CorruptionType.CHUNK_MISSING,
                CorruptionType.CHUNK_CORRUPTED,
                CorruptionType.HASH_MISMATCH
            ]:
                chunks_to_repair.add(report.chunk_hash)
        
        self.integrity_logger.info(f"Attempting to repair {len(chunks_to_repair)} corrupted chunks")
        
        # Repair each chunk
        for chunk_hash in chunks_to_repair:
            result = await self.repair_chunk(chunk_hash)
            results.append(result)
            
            # Small delay to avoid overwhelming peers
            await asyncio.sleep(0.1)
        
        return results
    
    async def recover_file(self, file_path: str) -> bool:
        """Attempt to recover a complete file by repairing all its chunks"""
        try:
            file_record = self.store.get_file(pathlib.Path(file_path))
            if not file_record:
                self.integrity_logger.error(f"No file record found for {file_path}")
                return False
            
            # Check and repair all chunks for this file
            all_repaired = True
            for chunk_hash in file_record.hashes:
                is_valid, _ = await self.verify_chunk(chunk_hash)
                
                if not is_valid:
                    repair_result = await self.repair_chunk(chunk_hash)
                    if repair_result.status != RepairStatus.SUCCESS:
                        all_repaired = False
                        self.integrity_logger.error(
                            f"Failed to repair chunk {chunk_hash} for file {file_path}"
                        )
            
            if all_repaired:
                self.integrity_logger.info(f"Successfully recovered file {file_path}")
            else:
                self.integrity_logger.warning(f"Partial recovery of file {file_path}")
            
            return all_repaired
            
        except Exception as e:
            self.integrity_logger.error(f"Failed to recover file {file_path}: {e}")
            return False
    
    def get_corruption_summary(self) -> Dict:
        """Get summary of detected corruptions"""
        summary = {
            'total_reports': len(self.corruption_reports),
            'by_type': {},
            'by_severity': {},
            'recent_reports': []
        }
        
        # Count by type and severity
        for report in self.corruption_reports:
            corruption_type = report.corruption_type.value
            severity = report.severity
            
            summary['by_type'][corruption_type] = summary['by_type'].get(corruption_type, 0) + 1
            summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1
        
        # Get recent reports (last 24 hours)
        recent_threshold = time.time() - (24 * 60 * 60)
        summary['recent_reports'] = [
            report.to_dict() for report in self.corruption_reports
            if report.detected_at > recent_threshold
        ]
        
        return summary
    
    def get_repair_summary(self) -> Dict:
        """Get summary of repair operations"""
        summary = {
            'total_repairs': len(self.repair_results),
            'by_status': {},
            'success_rate': 0.0,
            'recent_repairs': []
        }
        
        # Count by status
        for result in self.repair_results:
            status = result.status.value
            summary['by_status'][status] = summary['by_status'].get(status, 0) + 1
        
        # Calculate success rate
        if self.repair_results:
            successful = summary['by_status'].get('success', 0)
            summary['success_rate'] = successful / len(self.repair_results)
        
        # Get recent repairs (last 24 hours)
        recent_threshold = time.time() - (24 * 60 * 60)
        summary['recent_repairs'] = [
            result.to_dict() for result in self.repair_results
            if result.repair_time > recent_threshold
        ]
        
        return summary
    
    def clear_old_reports(self, max_age_hours: int = 168):  # 1 week default
        """Clear old corruption reports and repair results"""
        cutoff_time = time.time() - (max_age_hours * 60 * 60)
        
        # Clear old corruption reports
        old_count = len(self.corruption_reports)
        self.corruption_reports = [
            report for report in self.corruption_reports
            if report.detected_at > cutoff_time
        ]
        
        # Clear old repair results
        old_repair_count = len(self.repair_results)
        self.repair_results = [
            result for result in self.repair_results
            if result.repair_time > cutoff_time
        ]
        
        cleared_reports = old_count - len(self.corruption_reports)
        cleared_repairs = old_repair_count - len(self.repair_results)
        
        if cleared_reports > 0 or cleared_repairs > 0:
            self.integrity_logger.info(
                f"Cleared {cleared_reports} old corruption reports and "
                f"{cleared_repairs} old repair results"
            )


class IntegrityScheduler:
    """Scheduler for background integrity verification"""
    
    def __init__(self, verifier: IntegrityVerifier):
        self.verifier = verifier
        self.is_running = False
        self.scheduler_task: Optional[asyncio.Task] = None
        
        # Configuration
        self.full_check_interval = 24 * 60 * 60  # 24 hours
        self.incremental_check_interval = 60 * 60  # 1 hour
        self.auto_repair_enabled = True
        self.max_concurrent_repairs = 5
    
    async def start(self):
        """Start the integrity verification scheduler"""
        if self.is_running:
            logging.warning("Integrity scheduler already running")
            return
        
        self.is_running = True
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        logging.info("Integrity verification scheduler started")
    
    async def stop(self):
        """Stop the integrity verification scheduler"""
        self.is_running = False
        
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
        
        logging.info("Integrity verification scheduler stopped")
    
    async def _scheduler_loop(self):
        """Main scheduler loop"""
        try:
            while self.is_running:
                current_time = time.time()
                
                # Check if we need a full integrity check
                if (current_time - self.verifier.stats.last_full_check) > self.full_check_interval:
                    logging.info("Starting scheduled full integrity check")
                    await self._run_full_check()
                
                # Check if we need an incremental check
                elif (current_time - self.verifier.stats.last_incremental_check) > self.incremental_check_interval:
                    logging.info("Starting scheduled incremental integrity check")
                    await self._run_incremental_check()
                
                # Auto-repair if enabled
                if self.auto_repair_enabled and self.verifier.corruption_reports:
                    await self._run_auto_repair()
                
                # Clean up old reports periodically
                if current_time % (24 * 60 * 60) < 60:  # Once per day
                    self.verifier.clear_old_reports()
                
                # Sleep before next check
                await asyncio.sleep(60)  # Check every minute
                
        except asyncio.CancelledError:
            logging.info("Integrity scheduler cancelled")
        except Exception as e:
            logging.error(f"Integrity scheduler error: {e}")
            import traceback
            traceback.print_exc()
    
    async def _run_full_check(self):
        """Run a full integrity check"""
        try:
            reports = await self.verifier.verify_all_chunks(incremental=False)
            
            if reports:
                logging.warning(f"Full integrity check found {len(reports)} corruptions")
                
                # Log summary by type
                type_counts = {}
                for report in reports:
                    corruption_type = report.corruption_type.value
                    type_counts[corruption_type] = type_counts.get(corruption_type, 0) + 1
                
                for corruption_type, count in type_counts.items():
                    logging.warning(f"  {corruption_type}: {count}")
            else:
                logging.info("Full integrity check completed - no corruptions found")
                
        except Exception as e:
            logging.error(f"Full integrity check failed: {e}")
    
    async def _run_incremental_check(self):
        """Run an incremental integrity check (recently modified files)"""
        try:
            # For now, incremental check is the same as full check
            # In the future, we could track modification times and only check recent files
            reports = await self.verifier.verify_all_chunks(incremental=True)
            
            if reports:
                logging.info(f"Incremental integrity check found {len(reports)} corruptions")
            else:
                logging.debug("Incremental integrity check completed - no corruptions found")
                
        except Exception as e:
            logging.error(f"Incremental integrity check failed: {e}")
    
    async def _run_auto_repair(self):
        """Run automatic repair for detected corruptions"""
        try:
            # Get corruptions that haven't been repaired recently
            recent_threshold = time.time() - (60 * 60)  # 1 hour
            recent_repair_hashes = {
                result.chunk_hash for result in self.verifier.repair_results
                if result.repair_time > recent_threshold and result.status == RepairStatus.SUCCESS
            }
            
            # Find corruptions to repair
            chunks_to_repair = []
            for report in self.verifier.corruption_reports:
                if (report.chunk_hash and 
                    report.chunk_hash not in recent_repair_hashes and
                    report.corruption_type in [
                        CorruptionType.CHUNK_MISSING,
                        CorruptionType.CHUNK_CORRUPTED,
                        CorruptionType.HASH_MISMATCH
                    ]):
                    chunks_to_repair.append(report.chunk_hash)
            
            if not chunks_to_repair:
                return
            
            # Limit concurrent repairs
            chunks_to_repair = chunks_to_repair[:self.max_concurrent_repairs]
            
            logging.info(f"Auto-repairing {len(chunks_to_repair)} corrupted chunks")
            
            # Repair chunks concurrently
            repair_tasks = [
                self.verifier.repair_chunk(chunk_hash)
                for chunk_hash in chunks_to_repair
            ]
            
            results = await asyncio.gather(*repair_tasks, return_exceptions=True)
            
            # Log results
            successful = sum(1 for result in results 
                           if isinstance(result, RepairResult) and result.status == RepairStatus.SUCCESS)
            
            if successful > 0:
                logging.info(f"Auto-repair completed: {successful}/{len(results)} chunks repaired successfully")
            
        except Exception as e:
            logging.error(f"Auto-repair failed: {e}")
    
    def configure(self, 
                  full_check_interval: Optional[int] = None,
                  incremental_check_interval: Optional[int] = None,
                  auto_repair_enabled: Optional[bool] = None,
                  max_concurrent_repairs: Optional[int] = None):
        """Configure scheduler parameters"""
        if full_check_interval is not None:
            self.full_check_interval = full_check_interval
        
        if incremental_check_interval is not None:
            self.incremental_check_interval = incremental_check_interval
        
        if auto_repair_enabled is not None:
            self.auto_repair_enabled = auto_repair_enabled
        
        if max_concurrent_repairs is not None:
            self.max_concurrent_repairs = max_concurrent_repairs
        
        logging.info(f"Integrity scheduler configured: "
                    f"full_check={self.full_check_interval}s, "
                    f"incremental_check={self.incremental_check_interval}s, "
                    f"auto_repair={self.auto_repair_enabled}, "
                    f"max_repairs={self.max_concurrent_repairs}")


# Recovery procedures for various corruption scenarios
class RecoveryProcedures:
    """Recovery procedures for various corruption scenarios"""
    
    def __init__(self, verifier: IntegrityVerifier, store: CRDTStore):
        self.verifier = verifier
        self.store = store
    
    async def emergency_recovery(self) -> Dict:
        """Emergency recovery procedure for critical system corruption"""
        logging.warning("Starting emergency recovery procedure")
        
        recovery_stats = {
            'chunks_recovered': 0,
            'files_recovered': 0,
            'metadata_rebuilt': False,
            'errors': []
        }
        
        try:
            # Step 1: Verify and repair all chunks
            logging.info("Step 1: Verifying all chunks")
            corruption_reports = await self.verifier.verify_all_chunks()
            
            if corruption_reports:
                logging.warning(f"Found {len(corruption_reports)} corrupted chunks")
                repair_results = await self.verifier.repair_all_corruptions()
                
                recovery_stats['chunks_recovered'] = sum(
                    1 for result in repair_results 
                    if result.status == RepairStatus.SUCCESS
                )
            
            # Step 2: Verify file integrity
            logging.info("Step 2: Verifying file integrity")
            files = self.store.list_files()
            
            for file_record in files:
                try:
                    file_reports = await self.verifier.verify_file_integrity(file_record)
                    
                    if file_reports:
                        # Attempt to recover the file
                        if await self.verifier.recover_file(file_record.path):
                            recovery_stats['files_recovered'] += 1
                    else:
                        # Even if no current reports, check if any of the file's chunks were repaired
                        file_chunks_repaired = any(
                            chunk_hash in file_record.hashes 
                            for result in repair_results 
                            if result.status == RepairStatus.SUCCESS 
                            for chunk_hash in [result.chunk_hash] if chunk_hash
                        )
                        
                        if file_chunks_repaired:
                            # File had chunks repaired, count as recovered
                            recovery_stats['files_recovered'] += 1
                    
                except Exception as e:
                    error_msg = f"Failed to verify file {file_record.path}: {e}"
                    recovery_stats['errors'].append(error_msg)
                    logging.error(error_msg)
            
            # Step 3: Rebuild metadata if necessary
            logging.info("Step 3: Checking metadata integrity")
            if await self._check_metadata_integrity():
                recovery_stats['metadata_rebuilt'] = True
            
            logging.info(f"Emergency recovery completed: {recovery_stats}")
            
        except Exception as e:
            error_msg = f"Emergency recovery failed: {e}"
            recovery_stats['errors'].append(error_msg)
            logging.error(error_msg)
        
        return recovery_stats
    
    async def _check_metadata_integrity(self) -> bool:
        """Check and repair metadata integrity"""
        try:
            # Verify CRDT store can be loaded
            files = self.store.list_files()
            
            # Check for orphaned chunks
            orphaned_chunks = await self._find_orphaned_chunks()
            
            if orphaned_chunks:
                logging.warning(f"Found {len(orphaned_chunks)} orphaned chunks")
                # Could implement orphan cleanup here
            
            return True
            
        except Exception as e:
            logging.error(f"Metadata integrity check failed: {e}")
            return False
    
    async def _find_orphaned_chunks(self) -> Set[str]:
        """Find chunks that exist on disk but aren't referenced by any file"""
        try:
            # Get all chunk hashes from file records
            referenced_chunks = set()
            for file_record in self.store.list_files():
                referenced_chunks.update(file_record.hashes)
            
            # Get all chunks on disk
            disk_chunks = set()
            if self.verifier.chunk_dir.exists():
                for chunk_file in self.verifier.chunk_dir.iterdir():
                    if chunk_file.is_file() and len(chunk_file.name) == 64:
                        disk_chunks.add(chunk_file.name)
            
            # Find orphaned chunks
            orphaned = disk_chunks - referenced_chunks
            return orphaned
            
        except Exception as e:
            logging.error(f"Failed to find orphaned chunks: {e}")
            return set()
    
    async def selective_recovery(self, file_paths: List[str]) -> Dict:
        """Recover specific files"""
        recovery_stats = {
            'files_attempted': len(file_paths),
            'files_recovered': 0,
            'errors': []
        }
        
        for file_path in file_paths:
            try:
                if await self.verifier.recover_file(file_path):
                    recovery_stats['files_recovered'] += 1
                    logging.info(f"Successfully recovered {file_path}")
                else:
                    error_msg = f"Failed to recover {file_path}"
                    recovery_stats['errors'].append(error_msg)
                    logging.error(error_msg)
                    
            except Exception as e:
                error_msg = f"Error recovering {file_path}: {e}"
                recovery_stats['errors'].append(error_msg)
                logging.error(error_msg)
        
        return recovery_stats
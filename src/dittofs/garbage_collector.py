"""
Garbage Collection System for DittoFS

This module implements efficient garbage collection for unreferenced chunks,
including reference counting, configurable retention policies, and safe cleanup
procedures that verify chunk references before deletion.
"""

import asyncio
import json
import logging
import pathlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from .chunker import CHUNK_DIR, ensure_chunk_dir
from .crdt_store import CRDTStore


@dataclass
class GCStats:
    """Statistics from garbage collection run"""

    chunks_scanned: int = 0
    chunks_deleted: int = 0
    bytes_freed: int = 0
    orphaned_chunks: int = 0
    referenced_chunks: int = 0
    errors: int = 0
    duration_seconds: float = 0.0
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def to_dict(self) -> dict:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat()
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "GCStats":
        if "timestamp" in data and isinstance(data["timestamp"], str):
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        return cls(**data)


@dataclass
class RetentionPolicy:
    """Configurable retention policy for garbage collection"""

    # Minimum age before a chunk can be deleted (in seconds)
    min_age_seconds: int = 3600  # 1 hour default

    # Maximum number of chunks to delete in a single run
    max_deletions_per_run: int = 1000

    # Whether to delete chunks that are not referenced by any files
    delete_orphaned: bool = True

    # Whether to delete chunks from files that no longer exist
    delete_from_missing_files: bool = True

    # Grace period for recently accessed chunks (in seconds)
    access_grace_period: int = 86400  # 24 hours

    # Minimum free space threshold (bytes) - if below this, be more aggressive
    min_free_space_bytes: int = 1024 * 1024 * 1024  # 1GB

    # Emergency mode - more aggressive deletion when space is critically low
    emergency_mode_threshold: int = 100 * 1024 * 1024  # 100MB

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "RetentionPolicy":
        return cls(**data)


class ChunkReferenceTracker:
    """Tracks chunk references and usage for garbage collection"""

    def __init__(self, store: CRDTStore):
        self.store = store
        self._reference_cache: Dict[str, Set[str]] = {}
        self._access_times: Dict[str, float] = {}
        self._cache_valid = False
        self._lock = threading.RLock()

    def invalidate_cache(self):
        """Invalidate the reference cache"""
        with self._lock:
            self._cache_valid = False
            self._reference_cache.clear()

    def update_chunk_access_time(self, chunk_hash: str):
        """Update the last access time for a chunk"""
        with self._lock:
            self._access_times[chunk_hash] = time.time()

    def get_chunk_references(self) -> Dict[str, Set[str]]:
        """Get all chunk references from the CRDT store"""
        with self._lock:
            if self._cache_valid:
                return self._reference_cache.copy()

            self._reference_cache.clear()

            # Scan all files in the CRDT store
            files = self.store.list_files()
            for file_record in files:
                file_path = file_record.path
                for chunk_hash in file_record.hashes:
                    if chunk_hash not in self._reference_cache:
                        self._reference_cache[chunk_hash] = set()
                    self._reference_cache[chunk_hash].add(file_path)

            self._cache_valid = True
            logging.debug(
                f"Built reference cache with {len(self._reference_cache)} chunks"
            )
            return self._reference_cache.copy()

    def get_orphaned_chunks(self) -> Set[str]:
        """Find chunks that exist on disk but are not referenced by any files"""
        ensure_chunk_dir()

        # Get all chunks on disk
        disk_chunks = set()
        if CHUNK_DIR.exists():
            for chunk_file in CHUNK_DIR.iterdir():
                if chunk_file.is_file():
                    disk_chunks.add(chunk_file.name)

        # Get all referenced chunks
        references = self.get_chunk_references()
        referenced_chunks = set(references.keys())

        # Find orphaned chunks
        orphaned = disk_chunks - referenced_chunks
        logging.debug(
            f"Found {len(orphaned)} orphaned chunks out of {len(disk_chunks)} total"
        )

        return orphaned

    def get_chunks_from_missing_files(self) -> Set[str]:
        """Find chunks that belong to files that no longer exist on disk"""
        chunks_from_missing = set()

        files = self.store.list_files()
        for file_record in files:
            file_path = pathlib.Path(file_record.path)
            if not file_path.exists():
                chunks_from_missing.update(file_record.hashes)

        logging.debug(f"Found {len(chunks_from_missing)} chunks from missing files")
        return chunks_from_missing

    def get_chunk_age(self, chunk_hash: str) -> float:
        """Get the age of a chunk in seconds"""
        chunk_path = CHUNK_DIR / chunk_hash
        if not chunk_path.exists():
            return 0.0

        try:
            stat = chunk_path.stat()
            return time.time() - stat.st_mtime
        except OSError:
            return 0.0

    def get_chunk_last_access(self, chunk_hash: str) -> float:
        """Get the last access time of a chunk"""
        with self._lock:
            return self._access_times.get(chunk_hash, 0.0)

    def is_chunk_recently_accessed(self, chunk_hash: str, grace_period: int) -> bool:
        """Check if a chunk was accessed recently"""
        last_access = self.get_chunk_last_access(chunk_hash)
        if last_access == 0.0:
            return False

        return (time.time() - last_access) < grace_period


class GarbageCollector:
    """Main garbage collection system"""

    def __init__(self, store: CRDTStore, policy: Optional[RetentionPolicy] = None):
        self.store = store
        self.policy = policy or RetentionPolicy()
        self.tracker = ChunkReferenceTracker(store)
        self._stats_history: List[GCStats] = []
        self._running = False
        self._scheduler_task: Optional[asyncio.Task] = None
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="gc")

        # Load stats history
        self._load_stats_history()

    def get_stats_file_path(self) -> pathlib.Path:
        """Get the path to the GC stats file"""
        return pathlib.Path.home() / ".dittofs" / "gc_stats.json"

    def _load_stats_history(self):
        """Load GC stats history from disk"""
        stats_file = self.get_stats_file_path()
        if stats_file.exists():
            try:
                with open(stats_file, "r") as f:
                    data = json.load(f)
                    self._stats_history = [GCStats.from_dict(item) for item in data]
                logging.debug(f"Loaded {len(self._stats_history)} GC stats records")
            except Exception as e:
                logging.warning(f"Failed to load GC stats history: {e}")
                self._stats_history = []

    def _save_stats_history(self):
        """Save GC stats history to disk"""
        stats_file = self.get_stats_file_path()
        stats_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Keep only the last 100 records
            recent_stats = self._stats_history[-100:]
            data = [stats.to_dict() for stats in recent_stats]

            with open(stats_file, "w") as f:
                json.dump(data, f, indent=2)

            self._stats_history = recent_stats
            logging.debug(f"Saved {len(recent_stats)} GC stats records")
        except Exception as e:
            logging.error(f"Failed to save GC stats history: {e}")

    def get_storage_info(self) -> Dict[str, int]:
        """Get storage information for the chunk directory"""
        ensure_chunk_dir()

        try:
            stat = CHUNK_DIR.stat()
            statvfs = CHUNK_DIR.statvfs() if hasattr(CHUNK_DIR, "statvfs") else None

            if statvfs:
                free_bytes = statvfs.f_bavail * statvfs.f_frsize
                total_bytes = statvfs.f_blocks * statvfs.f_frsize
            else:
                # Fallback for systems without statvfs
                import shutil

                total_bytes, used_bytes, free_bytes = shutil.disk_usage(CHUNK_DIR)

            # Calculate chunk directory size
            chunk_bytes = 0
            chunk_count = 0
            if CHUNK_DIR.exists():
                for chunk_file in CHUNK_DIR.iterdir():
                    if chunk_file.is_file():
                        try:
                            chunk_bytes += chunk_file.stat().st_size
                            chunk_count += 1
                        except OSError:
                            pass

            return {
                "free_bytes": free_bytes,
                "total_bytes": total_bytes,
                "chunk_bytes": chunk_bytes,
                "chunk_count": chunk_count,
                "used_bytes": total_bytes - free_bytes,
            }
        except Exception as e:
            logging.error(f"Failed to get storage info: {e}")
            return {
                "free_bytes": 0,
                "total_bytes": 0,
                "chunk_bytes": 0,
                "chunk_count": 0,
                "used_bytes": 0,
            }

    def should_run_emergency_gc(self) -> bool:
        """Check if emergency garbage collection should be triggered"""
        storage_info = self.get_storage_info()
        free_bytes = storage_info["free_bytes"]

        return free_bytes < self.policy.emergency_mode_threshold

    def should_run_regular_gc(self) -> bool:
        """Check if regular garbage collection should be triggered"""
        storage_info = self.get_storage_info()
        free_bytes = storage_info["free_bytes"]

        return free_bytes < self.policy.min_free_space_bytes

    async def collect_garbage(self, force: bool = False) -> GCStats:
        """Run garbage collection"""
        if self._running and not force:
            logging.warning("Garbage collection already running")
            return GCStats()

        self._running = True
        start_time = time.time()
        stats = GCStats()

        try:
            logging.info("Starting garbage collection")

            # Invalidate cache to get fresh data
            self.tracker.invalidate_cache()

            # Determine if we're in emergency mode
            emergency_mode = self.should_run_emergency_gc()
            if emergency_mode:
                logging.warning("Running garbage collection in emergency mode")

            # Get storage info
            storage_info = self.get_storage_info()
            logging.info(f"Storage info: {storage_info}")

            # Find candidates for deletion
            candidates = await self._find_deletion_candidates(emergency_mode)
            stats.chunks_scanned = len(candidates)

            # Sort candidates by priority (oldest first, least recently accessed first)
            candidates = await self._prioritize_candidates(candidates)

            # Limit deletions per run
            max_deletions = self.policy.max_deletions_per_run
            if emergency_mode:
                max_deletions *= 2  # Be more aggressive in emergency mode

            candidates = candidates[:max_deletions]

            # Delete chunks
            deleted_chunks, bytes_freed, errors = await self._delete_chunks(candidates)

            stats.chunks_deleted = deleted_chunks
            stats.bytes_freed = bytes_freed
            stats.errors = errors
            stats.orphaned_chunks = len(await self._get_orphaned_chunks())
            stats.referenced_chunks = len(self.tracker.get_chunk_references())

            # Update stats
            stats.duration_seconds = time.time() - start_time
            self._stats_history.append(stats)
            self._save_stats_history()

            logging.info(
                f"Garbage collection completed: {stats.chunks_deleted} chunks deleted, "
                f"{stats.bytes_freed} bytes freed in {stats.duration_seconds:.2f}s"
            )

            return stats

        except Exception as e:
            logging.error(f"Garbage collection failed: {e}")
            stats.errors += 1
            stats.duration_seconds = time.time() - start_time
            return stats
        finally:
            self._running = False

    async def _find_deletion_candidates(self, emergency_mode: bool) -> List[str]:
        """Find chunks that are candidates for deletion"""
        candidates = set()

        # Find orphaned chunks
        if self.policy.delete_orphaned:
            orphaned = await self._get_orphaned_chunks()
            candidates.update(orphaned)
            logging.debug(f"Found {len(orphaned)} orphaned chunks")

        # Find chunks from missing files
        if self.policy.delete_from_missing_files:
            from_missing = await self._get_chunks_from_missing_files()
            candidates.update(from_missing)
            logging.debug(f"Found {len(from_missing)} chunks from missing files")

        # In emergency mode, be more aggressive
        if emergency_mode:
            # Consider chunks that haven't been accessed recently
            all_chunks = await self._get_all_chunks()
            for chunk_hash in all_chunks:
                if not self.tracker.is_chunk_recently_accessed(
                    chunk_hash,
                    self.policy.access_grace_period // 2,  # Halve grace period
                ):
                    candidates.add(chunk_hash)

        # Filter by age
        aged_candidates = []
        for chunk_hash in candidates:
            age = self.tracker.get_chunk_age(chunk_hash)
            min_age = self.policy.min_age_seconds
            if emergency_mode:
                min_age //= 2  # Halve minimum age in emergency mode

            if age >= min_age:
                aged_candidates.append(chunk_hash)

        logging.debug(
            f"Found {len(aged_candidates)} deletion candidates after age filtering"
        )
        return aged_candidates

    async def _get_orphaned_chunks(self) -> Set[str]:
        """Get orphaned chunks (async wrapper)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, self.tracker.get_orphaned_chunks
        )

    async def _get_chunks_from_missing_files(self) -> Set[str]:
        """Get chunks from missing files (async wrapper)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, self.tracker.get_chunks_from_missing_files
        )

    async def _get_all_chunks(self) -> Set[str]:
        """Get all chunks on disk"""

        def _get_chunks():
            ensure_chunk_dir()
            chunks = set()
            if CHUNK_DIR.exists():
                for chunk_file in CHUNK_DIR.iterdir():
                    if chunk_file.is_file():
                        chunks.add(chunk_file.name)
            return chunks

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, _get_chunks)

    async def _prioritize_candidates(self, candidates: List[str]) -> List[str]:
        """Prioritize deletion candidates"""

        def _sort_key(chunk_hash: str) -> Tuple[float, float]:
            age = self.tracker.get_chunk_age(chunk_hash)
            last_access = self.tracker.get_chunk_last_access(chunk_hash)
            # Sort by age (descending) then by last access (ascending)
            return (-age, last_access)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, lambda: sorted(candidates, key=_sort_key, reverse=True)
        )

    async def _delete_chunks(self, chunk_hashes: List[str]) -> Tuple[int, int, int]:
        """Delete chunks and return (deleted_count, bytes_freed, errors)"""
        deleted_count = 0
        bytes_freed = 0
        errors = 0

        for chunk_hash in chunk_hashes:
            try:
                chunk_path = CHUNK_DIR / chunk_hash
                if chunk_path.exists():
                    # Double-check that chunk is safe to delete
                    if await self._is_safe_to_delete(chunk_hash):
                        size = chunk_path.stat().st_size
                        chunk_path.unlink()
                        deleted_count += 1
                        bytes_freed += size
                        logging.debug(f"Deleted chunk {chunk_hash} ({size} bytes)")
                    else:
                        logging.warning(
                            f"Chunk {chunk_hash} not safe to delete, skipping"
                        )

            except Exception as e:
                logging.error(f"Failed to delete chunk {chunk_hash}: {e}")
                errors += 1

        return deleted_count, bytes_freed, errors

    async def _is_safe_to_delete(self, chunk_hash: str) -> bool:
        """Verify that a chunk is safe to delete"""
        # Re-check references to avoid race conditions
        references = self.tracker.get_chunk_references()
        if chunk_hash in references:
            # Check if any of the referencing files still exist
            for file_path in references[chunk_hash]:
                if pathlib.Path(file_path).exists():
                    return False

        # Check if chunk was recently accessed
        if self.tracker.is_chunk_recently_accessed(
            chunk_hash, self.policy.access_grace_period
        ):
            return False

        return True

    def get_stats_history(self) -> List[GCStats]:
        """Get garbage collection statistics history"""
        return self._stats_history.copy()

    def get_latest_stats(self) -> Optional[GCStats]:
        """Get the latest garbage collection statistics"""
        return self._stats_history[-1] if self._stats_history else None

    async def start_scheduler(self, interval_seconds: int = 3600):
        """Start the garbage collection scheduler"""
        if self._scheduler_task and not self._scheduler_task.done():
            logging.warning("GC scheduler already running")
            return

        self._scheduler_task = asyncio.create_task(
            self._scheduler_loop(interval_seconds)
        )
        logging.info(f"Started GC scheduler with {interval_seconds}s interval")

    async def stop_scheduler(self):
        """Stop the garbage collection scheduler"""
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
            self._scheduler_task = None
            logging.info("Stopped GC scheduler")

    async def _scheduler_loop(self, interval_seconds: int):
        """Main scheduler loop"""
        while True:
            try:
                await asyncio.sleep(interval_seconds)

                # Check if GC should run
                if self.should_run_emergency_gc() or self.should_run_regular_gc():
                    await self.collect_garbage()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error in GC scheduler: {e}")
                await asyncio.sleep(60)  # Wait a minute before retrying

    def cleanup(self):
        """Cleanup resources"""
        if self._executor:
            self._executor.shutdown(wait=True)


# Global garbage collector instance
_gc_instance: Optional[GarbageCollector] = None


def get_garbage_collector(
    store: Optional[CRDTStore] = None, policy: Optional[RetentionPolicy] = None
) -> GarbageCollector:
    """Get or create the global garbage collector instance"""
    global _gc_instance

    if _gc_instance is None:
        if store is None:
            store = CRDTStore()
        _gc_instance = GarbageCollector(store, policy)

    return _gc_instance


def set_garbage_collector(gc: GarbageCollector):
    """Set the global garbage collector instance"""
    global _gc_instance
    _gc_instance = gc


async def run_garbage_collection(force: bool = False) -> GCStats:
    """Run garbage collection using the global instance"""
    gc = get_garbage_collector()
    return await gc.collect_garbage(force=force)


def get_gc_stats() -> List[GCStats]:
    """Get garbage collection statistics"""
    gc = get_garbage_collector()
    return gc.get_stats_history()

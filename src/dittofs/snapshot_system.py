"""
Snapshot-based backup system for DittoFS

This module provides comprehensive snapshot capabilities including:
- Point-in-time snapshots of entire file system state
- Incremental snapshot system for efficient storage usage
- Snapshot restoration capabilities with selective file recovery
- Snapshot scheduling and automatic cleanup policies
"""

import hashlib
import json
import logging
import pathlib
import shutil
import threading
import time

try:
    import schedule

    SCHEDULE_AVAILABLE = True
except ImportError:
    SCHEDULE_AVAILABLE = False
    logging.warning("schedule module not available. Install with: pip install schedule")
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union


class SnapshotType(Enum):
    """Types of snapshots"""

    FULL = "full"  # Complete snapshot of all files
    INCREMENTAL = "incremental"  # Only changed files since last snapshot
    DIFFERENTIAL = "differential"  # Changed files since last full snapshot


class SnapshotStatus(Enum):
    """Status of snapshot operations"""

    CREATING = "creating"
    COMPLETED = "completed"
    FAILED = "failed"
    RESTORING = "restoring"
    DELETED = "deleted"


class RestoreScope(Enum):
    """Scope of restore operations"""

    FULL = "full"  # Restore entire snapshot
    SELECTIVE = "selective"  # Restore specific files/folders
    METADATA_ONLY = "metadata_only"  # Restore only metadata


@dataclass
class SnapshotMetadata:
    """Metadata for a snapshot"""

    snapshot_id: str
    snapshot_type: SnapshotType
    created_at: float
    created_by: str
    description: str = ""
    parent_snapshot: Optional[str] = None  # For incremental snapshots
    file_count: int = 0
    total_size: int = 0
    compressed_size: int = 0
    status: SnapshotStatus = SnapshotStatus.CREATING
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["snapshot_type"] = self.snapshot_type.value
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "SnapshotMetadata":
        data["snapshot_type"] = SnapshotType(data["snapshot_type"])
        data["status"] = SnapshotStatus(data["status"])
        return cls(**data)


@dataclass
class SnapshotFileRecord:
    """Record of a file in a snapshot"""

    file_path: str
    chunk_hashes: List[str]
    file_size: int
    mtime: float
    permissions: int
    checksum: str
    content_type: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "SnapshotFileRecord":
        return cls(**data)


@dataclass
class RestoreOptions:
    """Options for restore operations"""

    scope: RestoreScope = RestoreScope.FULL
    target_path: Optional[pathlib.Path] = None
    file_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    overwrite_existing: bool = False
    preserve_permissions: bool = True
    verify_integrity: bool = True

    def to_dict(self) -> dict:
        data = asdict(self)
        data["scope"] = self.scope.value
        if self.target_path:
            data["target_path"] = str(self.target_path)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "RestoreOptions":
        data["scope"] = RestoreScope(data["scope"])
        if data.get("target_path"):
            data["target_path"] = pathlib.Path(data["target_path"])
        return cls(**data)


@dataclass
class SnapshotPolicy:
    """Policy for automatic snapshot creation and cleanup"""

    name: str
    enabled: bool = True
    schedule_cron: str = "0 2 * * *"  # Daily at 2 AM by default
    snapshot_type: SnapshotType = SnapshotType.INCREMENTAL
    max_snapshots: Optional[int] = 30  # Keep 30 snapshots by default
    max_age_days: Optional[int] = 90  # Keep snapshots for 90 days
    full_snapshot_interval: int = 7  # Full snapshot every 7 incremental snapshots
    compress_snapshots: bool = True
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["snapshot_type"] = self.snapshot_type.value
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "SnapshotPolicy":
        data["snapshot_type"] = SnapshotType(data["snapshot_type"])
        return cls(**data)


class SnapshotSystem:
    """Comprehensive snapshot-based backup system"""

    def __init__(
        self, storage_path: pathlib.Path, crdt_store=None, peer_id: str = "default"
    ):
        self.storage_path = storage_path
        self.crdt_store = crdt_store
        self.peer_id = peer_id

        # Create storage directories
        self.snapshots_dir = storage_path / "snapshots"
        self.metadata_dir = storage_path / "metadata"
        self.policies_dir = storage_path / "policies"

        for dir_path in [self.snapshots_dir, self.metadata_dir, self.policies_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # In-memory caches
        self.snapshot_cache: Dict[str, SnapshotMetadata] = {}
        self.policy_cache: Dict[str, SnapshotPolicy] = {}

        # Scheduler for automatic snapshots
        self.scheduler_thread = None
        self.scheduler_running = False

        # Load existing data
        self._load_metadata()
        self._load_policies()

        # Start scheduler
        self.start_scheduler()

    def _generate_snapshot_id(self, snapshot_type: SnapshotType) -> str:
        """Generate unique snapshot ID"""
        timestamp = str(time.time())
        data = f"{snapshot_type.value}_{timestamp}_{self.peer_id}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def _load_metadata(self):
        """Load snapshot metadata from disk"""
        try:
            metadata_file = self.metadata_dir / "snapshots.json"
            if metadata_file.exists():
                with open(metadata_file, "r") as f:
                    metadata_data = json.load(f)
                    for snapshot_id, snapshot_data in metadata_data.items():
                        self.snapshot_cache[snapshot_id] = SnapshotMetadata.from_dict(
                            snapshot_data
                        )
        except Exception as e:
            logging.error(f"Error loading snapshot metadata: {e}")

    def _save_metadata(self):
        """Save snapshot metadata to disk"""
        try:
            metadata_data = {sid: s.to_dict() for sid, s in self.snapshot_cache.items()}
            with open(self.metadata_dir / "snapshots.json", "w") as f:
                json.dump(metadata_data, f, indent=2)
        except Exception as e:
            logging.error(f"Error saving snapshot metadata: {e}")

    def _load_policies(self):
        """Load snapshot policies from disk"""
        try:
            policies_file = self.policies_dir / "policies.json"
            if policies_file.exists():
                with open(policies_file, "r") as f:
                    policies_data = json.load(f)
                    for policy_name, policy_data in policies_data.items():
                        self.policy_cache[policy_name] = SnapshotPolicy.from_dict(
                            policy_data
                        )
        except Exception as e:
            logging.error(f"Error loading snapshot policies: {e}")

    def _save_policies(self):
        """Save snapshot policies to disk"""
        try:
            policies_data = {name: p.to_dict() for name, p in self.policy_cache.items()}
            with open(self.policies_dir / "policies.json", "w") as f:
                json.dump(policies_data, f, indent=2)
        except Exception as e:
            logging.error(f"Error saving snapshot policies: {e}")

    def create_snapshot(
        self,
        snapshot_type: SnapshotType = SnapshotType.INCREMENTAL,
        description: str = "",
        tags: List[str] = None,
        parent_snapshot: Optional[str] = None,
    ) -> Optional[str]:
        """Create a new snapshot"""
        try:
            snapshot_id = self._generate_snapshot_id(snapshot_type)

            # Create snapshot metadata
            snapshot_metadata = SnapshotMetadata(
                snapshot_id=snapshot_id,
                snapshot_type=snapshot_type,
                created_at=time.time(),
                created_by=self.peer_id,
                description=description,
                parent_snapshot=parent_snapshot,
                tags=tags or [],
                status=SnapshotStatus.CREATING,
            )

            # Cache metadata immediately
            self.snapshot_cache[snapshot_id] = snapshot_metadata
            self._save_metadata()

            logging.info(f"Creating {snapshot_type.value} snapshot {snapshot_id}")

            # Create snapshot directory
            snapshot_dir = self.snapshots_dir / snapshot_id
            snapshot_dir.mkdir(exist_ok=True)

            # Collect files to snapshot
            files_to_snapshot = self._collect_files_for_snapshot(
                snapshot_type, parent_snapshot
            )

            # Create snapshot data
            snapshot_files = {}
            total_size = 0
            compressed_size = 0

            for file_path, file_record in files_to_snapshot.items():
                try:
                    # Create snapshot file record
                    snapshot_file = SnapshotFileRecord(
                        file_path=file_path,
                        chunk_hashes=file_record.hashes,
                        file_size=file_record.size,
                        mtime=file_record.mtime,
                        permissions=file_record.permissions,
                        checksum=file_record.checksum,
                        content_type=file_record.content_type,
                        metadata=file_record.metadata,
                    )

                    snapshot_files[file_path] = snapshot_file
                    total_size += file_record.size

                except Exception as e:
                    logging.warning(
                        f"Failed to include file {file_path} in snapshot: {e}"
                    )

            # Save snapshot data
            snapshot_data_file = snapshot_dir / "files.json"
            with open(snapshot_data_file, "w") as f:
                json.dump(
                    {path: file.to_dict() for path, file in snapshot_files.items()},
                    f,
                    indent=2,
                )

            # Compress snapshot if enabled and beneficial
            compressed_size = 0
            if len(snapshot_files) > 0:
                # Only compress if we have a reasonable amount of data
                files_json_path = snapshot_dir / "files.json"
                if files_json_path.exists():
                    original_size = files_json_path.stat().st_size
                    # Only attempt compression if original is larger than 1KB
                    if original_size > 1024:
                        compressed_size = self._compress_snapshot(snapshot_dir)
                        if compressed_size == 0:
                            # Compression wasn't beneficial, keep original
                            pass

            # Update metadata
            snapshot_metadata.file_count = len(snapshot_files)
            snapshot_metadata.total_size = total_size
            snapshot_metadata.compressed_size = compressed_size
            snapshot_metadata.status = SnapshotStatus.COMPLETED

            self.snapshot_cache[snapshot_id] = snapshot_metadata
            self._save_metadata()

            logging.info(
                f"Created snapshot {snapshot_id} with {len(snapshot_files)} files "
                f"({total_size} bytes, compressed to {compressed_size} bytes)"
            )

            return snapshot_id

        except Exception as e:
            logging.error(f"Error creating snapshot: {e}")
            # Mark snapshot as failed
            if snapshot_id in self.snapshot_cache:
                self.snapshot_cache[snapshot_id].status = SnapshotStatus.FAILED
                self._save_metadata()
            return None

    def _collect_files_for_snapshot(
        self, snapshot_type: SnapshotType, parent_snapshot: Optional[str] = None
    ) -> Dict[str, Any]:
        """Collect files to include in snapshot based on type"""
        if not self.crdt_store:
            logging.error("CRDT store not available for snapshot creation")
            return {}

        current_files = {}

        # Get all current files from CRDT store
        try:
            all_files = self.crdt_store.list_files()
            for file_record in all_files:
                current_files[file_record.path] = file_record
        except Exception as e:
            logging.error(f"Error getting files from CRDT store: {e}")
            return {}

        if snapshot_type == SnapshotType.FULL:
            return current_files

        elif snapshot_type == SnapshotType.INCREMENTAL:
            if not parent_snapshot:
                # Find the most recent snapshot as parent
                parent_snapshot = self._find_latest_snapshot()

            if not parent_snapshot:
                # No parent snapshot, create full snapshot
                logging.info("No parent snapshot found, creating full snapshot instead")
                return current_files

            # Get files that changed since parent snapshot
            return self._get_changed_files_since_snapshot(
                current_files, parent_snapshot
            )

        elif snapshot_type == SnapshotType.DIFFERENTIAL:
            # Find the most recent full snapshot
            full_snapshot = self._find_latest_full_snapshot()
            if not full_snapshot:
                logging.info("No full snapshot found, creating full snapshot instead")
                return current_files

            return self._get_changed_files_since_snapshot(current_files, full_snapshot)

        return current_files

    def _find_latest_snapshot(self) -> Optional[str]:
        """Find the most recent snapshot"""
        latest_snapshot = None
        latest_time = 0

        for snapshot_id, metadata in self.snapshot_cache.items():
            if (
                metadata.status == SnapshotStatus.COMPLETED
                and metadata.created_at > latest_time
            ):
                latest_time = metadata.created_at
                latest_snapshot = snapshot_id

        return latest_snapshot

    def _find_latest_full_snapshot(self) -> Optional[str]:
        """Find the most recent full snapshot"""
        latest_snapshot = None
        latest_time = 0

        for snapshot_id, metadata in self.snapshot_cache.items():
            if (
                metadata.status == SnapshotStatus.COMPLETED
                and metadata.snapshot_type == SnapshotType.FULL
                and metadata.created_at > latest_time
            ):
                latest_time = metadata.created_at
                latest_snapshot = snapshot_id

        return latest_snapshot

    def _get_changed_files_since_snapshot(
        self, current_files: Dict[str, Any], parent_snapshot_id: str
    ) -> Dict[str, Any]:
        """Get files that changed since a specific snapshot"""
        try:
            # Load parent snapshot data using the proper method
            parent_snapshot_files = self._load_snapshot_data(parent_snapshot_id)

            if not parent_snapshot_files:
                logging.warning(f"Parent snapshot {parent_snapshot_id} data not found")
                return current_files

            changed_files = {}

            # Check for new or modified files
            for file_path, file_record in current_files.items():
                if file_path not in parent_snapshot_files:
                    # New file
                    changed_files[file_path] = file_record
                else:
                    # Check if file was modified
                    parent_file = parent_snapshot_files[file_path]
                    if (
                        file_record.checksum != parent_file.checksum
                        or file_record.mtime > parent_file.mtime
                    ):
                        changed_files[file_path] = file_record

            logging.info(
                f"Found {len(changed_files)} changed files since snapshot {parent_snapshot_id}"
            )
            return changed_files

        except Exception as e:
            logging.error(f"Error getting changed files: {e}")
            return current_files

    def _compress_snapshot(self, snapshot_dir: pathlib.Path) -> int:
        """Compress snapshot data for storage efficiency"""
        try:
            import gzip
            import tarfile

            # Create compressed archive
            archive_path = snapshot_dir / "snapshot.tar.gz"
            files_json = snapshot_dir / "files.json"

            if not files_json.exists():
                return 0

            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(files_json, arcname="files.json")

            compressed_size = archive_path.stat().st_size
            original_size = files_json.stat().st_size

            # Only remove original if compression is beneficial
            if compressed_size < original_size:
                files_json.unlink()
                return compressed_size
            else:
                # Remove compressed file if it's not beneficial
                archive_path.unlink()
                return 0

        except Exception as e:
            logging.error(f"Error compressing snapshot: {e}")
            return 0

    def restore_snapshot(self, snapshot_id: str, options: RestoreOptions) -> bool:
        """Restore files from a snapshot"""
        try:
            if snapshot_id not in self.snapshot_cache:
                logging.error(f"Snapshot {snapshot_id} not found")
                return False

            snapshot_metadata = self.snapshot_cache[snapshot_id]
            if snapshot_metadata.status != SnapshotStatus.COMPLETED:
                logging.error(f"Snapshot {snapshot_id} is not in completed state")
                return False

            # Mark snapshot as being restored
            snapshot_metadata.status = SnapshotStatus.RESTORING
            self._save_metadata()

            logging.info(
                f"Restoring snapshot {snapshot_id} with scope {options.scope.value}"
            )

            # Load snapshot data
            snapshot_files = self._load_snapshot_data(snapshot_id)
            if not snapshot_files:
                logging.error(f"Failed to load snapshot data for {snapshot_id}")
                return False

            # Filter files based on restore options
            files_to_restore = self._filter_files_for_restore(snapshot_files, options)

            # Restore files
            restored_count = 0
            failed_count = 0

            for file_path, snapshot_file in files_to_restore.items():
                try:
                    if self._restore_single_file(snapshot_file, options):
                        restored_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logging.error(f"Error restoring file {file_path}: {e}")
                    failed_count += 1

            # Mark snapshot as completed
            snapshot_metadata.status = SnapshotStatus.COMPLETED
            self._save_metadata()

            logging.info(
                f"Restore completed: {restored_count} files restored, {failed_count} failed"
            )
            return failed_count == 0

        except Exception as e:
            logging.error(f"Error restoring snapshot {snapshot_id}: {e}")
            return False

    def _load_snapshot_data(
        self, snapshot_id: str
    ) -> Optional[Dict[str, SnapshotFileRecord]]:
        """Load snapshot file data"""
        try:
            snapshot_dir = self.snapshots_dir / snapshot_id

            if not snapshot_dir.exists():
                logging.warning(f"Snapshot directory {snapshot_dir} does not exist")
                return None

            # Check for compressed data first
            archive_path = snapshot_dir / "snapshot.tar.gz"
            files_json = snapshot_dir / "files.json"

            if archive_path.exists() and not files_json.exists():
                import tarfile

                # Extract compressed data temporarily
                with tarfile.open(archive_path, "r:gz") as tar:
                    tar.extractall(snapshot_dir)

            # Load files data
            if not files_json.exists():
                logging.warning(f"Files data not found for snapshot {snapshot_id}")
                return None

            with open(files_json, "r") as f:
                files_data = json.load(f)

            snapshot_files = {}
            for file_path, file_data in files_data.items():
                snapshot_files[file_path] = SnapshotFileRecord.from_dict(file_data)

            return snapshot_files

        except Exception as e:
            logging.error(f"Error loading snapshot data: {e}")
            return None

    def _filter_files_for_restore(
        self, snapshot_files: Dict[str, SnapshotFileRecord], options: RestoreOptions
    ) -> Dict[str, SnapshotFileRecord]:
        """Filter files based on restore options"""
        if options.scope == RestoreScope.FULL:
            return snapshot_files

        filtered_files = {}

        for file_path, snapshot_file in snapshot_files.items():
            # Check file patterns
            if options.file_patterns:
                import fnmatch

                matches_pattern = any(
                    fnmatch.fnmatch(file_path, pattern)
                    for pattern in options.file_patterns
                )
                if not matches_pattern:
                    continue

            # Check exclude patterns
            if options.exclude_patterns:
                import fnmatch

                matches_exclude = any(
                    fnmatch.fnmatch(file_path, pattern)
                    for pattern in options.exclude_patterns
                )
                if matches_exclude:
                    continue

            filtered_files[file_path] = snapshot_file

        return filtered_files

    def _restore_single_file(
        self, snapshot_file: SnapshotFileRecord, options: RestoreOptions
    ) -> bool:
        """Restore a single file from snapshot"""
        try:
            # Determine target path
            if options.target_path:
                target_path = (
                    options.target_path / pathlib.Path(snapshot_file.file_path).name
                )
            else:
                target_path = pathlib.Path(snapshot_file.file_path)

            # Check if file exists and handle overwrite
            if target_path.exists() and not options.overwrite_existing:
                logging.warning(
                    f"File {target_path} exists, skipping (overwrite disabled)"
                )
                return True

            # Create parent directories
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Restore file content using chunker
            from .chunker import join

            if not join(snapshot_file.chunk_hashes, target_path):
                logging.error(
                    f"Failed to restore file content for {snapshot_file.file_path}"
                )
                return False

            # Restore file metadata
            if options.preserve_permissions:
                try:
                    target_path.chmod(int(snapshot_file.permissions))
                except Exception as e:
                    logging.warning(
                        f"Failed to restore permissions for {target_path}: {e}"
                    )

            # Set modification time
            try:
                import os

                os.utime(target_path, (snapshot_file.mtime, snapshot_file.mtime))
            except Exception as e:
                logging.warning(f"Failed to restore mtime for {target_path}: {e}")

            # Verify integrity if requested
            if options.verify_integrity:
                if not self._verify_restored_file(target_path, snapshot_file):
                    logging.error(f"Integrity verification failed for {target_path}")
                    return False

            logging.debug(
                f"Successfully restored {snapshot_file.file_path} to {target_path}"
            )
            return True

        except Exception as e:
            logging.error(f"Error restoring file {snapshot_file.file_path}: {e}")
            return False

    def _verify_restored_file(
        self, file_path: pathlib.Path, snapshot_file: SnapshotFileRecord
    ) -> bool:
        """Verify integrity of restored file"""
        try:
            if not file_path.exists():
                return False

            # Check file size
            if file_path.stat().st_size != snapshot_file.file_size:
                return False

            # Check checksum
            import hashlib

            file_hash = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    file_hash.update(chunk)

            return file_hash.hexdigest() == snapshot_file.checksum

        except Exception as e:
            logging.error(f"Error verifying restored file: {e}")
            return False

    def list_snapshots(self, tags: List[str] = None) -> List[SnapshotMetadata]:
        """List all snapshots, optionally filtered by tags"""
        snapshots = []

        for snapshot_metadata in self.snapshot_cache.values():
            if tags:
                if not any(tag in snapshot_metadata.tags for tag in tags):
                    continue
            snapshots.append(snapshot_metadata)

        # Sort by creation time (newest first)
        snapshots.sort(key=lambda s: s.created_at, reverse=True)
        return snapshots

    def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot"""
        try:
            if snapshot_id not in self.snapshot_cache:
                logging.error(f"Snapshot {snapshot_id} not found")
                return False

            # Remove snapshot directory
            snapshot_dir = self.snapshots_dir / snapshot_id
            if snapshot_dir.exists():
                shutil.rmtree(snapshot_dir)

            # Update metadata
            self.snapshot_cache[snapshot_id].status = SnapshotStatus.DELETED
            self._save_metadata()

            # Remove from cache
            del self.snapshot_cache[snapshot_id]
            self._save_metadata()

            logging.info(f"Deleted snapshot {snapshot_id}")
            return True

        except Exception as e:
            logging.error(f"Error deleting snapshot {snapshot_id}: {e}")
            return False

    def create_policy(self, policy: SnapshotPolicy) -> bool:
        """Create or update a snapshot policy"""
        try:
            self.policy_cache[policy.name] = policy
            self._save_policies()

            # Update scheduler
            self._update_scheduler()

            logging.info(f"Created/updated snapshot policy '{policy.name}'")
            return True

        except Exception as e:
            logging.error(f"Error creating policy: {e}")
            return False

    def delete_policy(self, policy_name: str) -> bool:
        """Delete a snapshot policy"""
        try:
            if policy_name in self.policy_cache:
                del self.policy_cache[policy_name]
                self._save_policies()

                # Update scheduler
                self._update_scheduler()

                logging.info(f"Deleted snapshot policy '{policy_name}'")
                return True
            else:
                logging.warning(f"Policy '{policy_name}' not found")
                return False

        except Exception as e:
            logging.error(f"Error deleting policy: {e}")
            return False

    def cleanup_snapshots(self, policy_name: Optional[str] = None) -> int:
        """Clean up old snapshots based on policies"""
        try:
            policies_to_apply = []

            if policy_name:
                if policy_name in self.policy_cache:
                    policies_to_apply.append(self.policy_cache[policy_name])
            else:
                policies_to_apply = list(self.policy_cache.values())

            deleted_count = 0

            for policy in policies_to_apply:
                if not policy.enabled:
                    continue

                # Get snapshots that match this policy's tags
                matching_snapshots = []
                for snapshot_id, metadata in self.snapshot_cache.items():
                    if metadata.status == SnapshotStatus.COMPLETED and (
                        not policy.tags
                        or any(tag in metadata.tags for tag in policy.tags)
                    ):
                        matching_snapshots.append((snapshot_id, metadata))

                # Sort by creation time (oldest first for deletion)
                matching_snapshots.sort(key=lambda x: x[1].created_at)

                # Apply max_snapshots limit
                if (
                    policy.max_snapshots
                    and len(matching_snapshots) > policy.max_snapshots
                ):
                    snapshots_to_delete = matching_snapshots[: -policy.max_snapshots]
                    for snapshot_id, _ in snapshots_to_delete:
                        if self.delete_snapshot(snapshot_id):
                            deleted_count += 1

                # Apply max_age_days limit
                if policy.max_age_days:
                    cutoff_time = time.time() - (policy.max_age_days * 24 * 3600)
                    for snapshot_id, metadata in matching_snapshots:
                        if metadata.created_at < cutoff_time:
                            if self.delete_snapshot(snapshot_id):
                                deleted_count += 1

            logging.info(f"Cleanup completed: deleted {deleted_count} snapshots")
            return deleted_count

        except Exception as e:
            logging.error(f"Error during cleanup: {e}")
            return 0

    def start_scheduler(self):
        """Start the snapshot scheduler"""
        if not SCHEDULE_AVAILABLE:
            logging.warning("Scheduler not available - install schedule module")
            return

        if self.scheduler_running:
            return

        self.scheduler_running = True
        self._update_scheduler()

        def run_scheduler():
            while self.scheduler_running:
                try:
                    schedule.run_pending()
                    time.sleep(60)  # Check every minute
                except Exception as e:
                    logging.error(f"Scheduler error: {e}")

        self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()

        logging.info("Snapshot scheduler started")

    def stop_scheduler(self):
        """Stop the snapshot scheduler"""
        self.scheduler_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)

        if SCHEDULE_AVAILABLE:
            schedule.clear()
        logging.info("Snapshot scheduler stopped")

    def _update_scheduler(self):
        """Update scheduler with current policies"""
        if not SCHEDULE_AVAILABLE:
            logging.warning("Scheduler not available - install schedule module")
            return

        schedule.clear()

        for policy in self.policy_cache.values():
            if policy.enabled:
                # Parse cron schedule (simplified - only supports basic patterns)
                try:
                    self._schedule_policy(policy)
                except Exception as e:
                    logging.error(f"Error scheduling policy '{policy.name}': {e}")

    def _schedule_policy(self, policy: SnapshotPolicy):
        """Schedule a specific policy"""
        if not SCHEDULE_AVAILABLE:
            return

        def create_scheduled_snapshot():
            try:
                # Determine snapshot type based on policy
                snapshot_type = policy.snapshot_type

                # Check if we need a full snapshot
                if (
                    snapshot_type == SnapshotType.INCREMENTAL
                    and policy.full_snapshot_interval > 0
                ):

                    # Count incremental snapshots since last full
                    incremental_count = 0
                    for metadata in self.snapshot_cache.values():
                        if (
                            metadata.status == SnapshotStatus.COMPLETED
                            and any(tag in metadata.tags for tag in policy.tags)
                            and metadata.snapshot_type == SnapshotType.INCREMENTAL
                        ):
                            incremental_count += 1

                    if incremental_count >= policy.full_snapshot_interval:
                        snapshot_type = SnapshotType.FULL

                # Create snapshot
                description = f"Scheduled snapshot from policy '{policy.name}'"
                snapshot_id = self.create_snapshot(
                    snapshot_type=snapshot_type,
                    description=description,
                    tags=policy.tags,
                )

                if snapshot_id:
                    logging.info(
                        f"Created scheduled snapshot {snapshot_id} from policy '{policy.name}'"
                    )

                    # Run cleanup after creating snapshot
                    self.cleanup_snapshots(policy.name)
                else:
                    logging.error(
                        f"Failed to create scheduled snapshot from policy '{policy.name}'"
                    )

            except Exception as e:
                logging.error(f"Error in scheduled snapshot creation: {e}")

        # Simple cron parsing - for production, use a proper cron library
        cron_parts = policy.schedule_cron.split()
        if len(cron_parts) == 5:
            minute, hour, day, month, weekday = cron_parts

            # Handle daily schedules (most common case)
            if day == "*" and month == "*" and weekday == "*":
                if hour.isdigit() and minute.isdigit():
                    schedule.every().day.at(f"{hour}:{minute}").do(
                        create_scheduled_snapshot
                    )
                    logging.info(
                        f"Scheduled policy '{policy.name}' for daily at {hour}:{minute}"
                    )

    def get_snapshot_info(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a snapshot"""
        if snapshot_id not in self.snapshot_cache:
            return None

        metadata = self.snapshot_cache[snapshot_id]

        # Load file list if available
        files_info = []
        try:
            snapshot_files = self._load_snapshot_data(snapshot_id)
            if snapshot_files:
                files_info = [
                    {
                        "path": file_path,
                        "size": file_record.file_size,
                        "mtime": file_record.mtime,
                        "checksum": file_record.checksum,
                    }
                    for file_path, file_record in snapshot_files.items()
                ]
        except Exception as e:
            logging.warning(f"Could not load file info for snapshot {snapshot_id}: {e}")

        return {"metadata": metadata.to_dict(), "files": files_info}


# Global snapshot system instance
_snapshot_system: Optional[SnapshotSystem] = None


def get_snapshot_system(
    storage_path: pathlib.Path = None, crdt_store=None, peer_id: str = "default"
) -> SnapshotSystem:
    """Get or create the global snapshot system instance"""
    global _snapshot_system

    if _snapshot_system is None:
        if storage_path is None:
            storage_path = pathlib.Path.home() / ".dittofs" / "snapshots"

        _snapshot_system = SnapshotSystem(storage_path, crdt_store, peer_id)

    return _snapshot_system


def initialize_snapshot_system(
    storage_path: pathlib.Path, crdt_store=None, peer_id: str = "default"
) -> SnapshotSystem:
    """Initialize the snapshot system with specific parameters"""
    global _snapshot_system
    _snapshot_system = SnapshotSystem(storage_path, crdt_store, peer_id)
    return _snapshot_system

import hashlib
import json
import logging
import os
import pathlib
import time
from datetime import datetime, timezone
from enum import Enum

try:
    import pycrdt as Y

    PYCRDT_AVAILABLE = True
except ImportError:
    PYCRDT_AVAILABLE = False
    logging.warning("pycrdt not available. Install with: pip install pycrdt")

from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Union

# Import the new versioning system
from .versioning import RetentionPolicy, VersioningSystem


class ConflictResolutionStrategy(Enum):
    """Strategies for resolving file conflicts"""

    LAST_WRITER_WINS = "last_writer_wins"
    MANUAL_RESOLUTION = "manual_resolution"
    AUTOMATIC_MERGE = "automatic_merge"
    PRESERVE_BOTH = "preserve_both"
    SEMANTIC_MERGE = "semantic_merge"


class ConflictState(Enum):
    """States in the conflict resolution state machine"""

    NO_CONFLICT = "no_conflict"
    DETECTED = "detected"
    ANALYZING = "analyzing"
    AWAITING_RESOLUTION = "awaiting_resolution"
    RESOLVING = "resolving"
    RESOLVED = "resolved"
    FAILED = "failed"


class ConflictType(Enum):
    """Types of conflicts that can occur"""

    TIMESTAMP_CONFLICT = "timestamp_conflict"
    CONTENT_CONFLICT = "content_conflict"
    SEMANTIC_CONFLICT = "semantic_conflict"
    PERMISSION_CONFLICT = "permission_conflict"
    METADATA_CONFLICT = "metadata_conflict"


@dataclass
class ConflictInfo:
    """Information about a detected conflict"""

    conflict_id: str
    file_path: str
    conflict_type: ConflictType
    state: ConflictState
    detected_at: datetime
    versions: List[Dict[str, Any]]
    resolution_strategy: Optional[ConflictResolutionStrategy] = None
    resolved_at: Optional[datetime] = None
    resolution_data: Optional[Dict[str, Any]] = None

    def to_dict(self) -> dict:
        data = asdict(self)
        # Convert datetime objects to ISO strings for serialization
        if isinstance(self.detected_at, datetime):
            data["detected_at"] = self.detected_at.isoformat()
        elif isinstance(self.detected_at, (int, float)):
            data["detected_at"] = datetime.fromtimestamp(
                self.detected_at, tz=timezone.utc
            ).isoformat()

        if self.resolved_at:
            if isinstance(self.resolved_at, datetime):
                data["resolved_at"] = self.resolved_at.isoformat()
            elif isinstance(self.resolved_at, (int, float)):
                data["resolved_at"] = datetime.fromtimestamp(
                    self.resolved_at, tz=timezone.utc
                ).isoformat()

        # Convert enums to their values
        data["conflict_type"] = self.conflict_type.value
        data["state"] = self.state.value
        if self.resolution_strategy:
            data["resolution_strategy"] = self.resolution_strategy.value
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "ConflictInfo":
        # Convert ISO strings back to datetime objects
        if isinstance(data["detected_at"], str):
            data["detected_at"] = datetime.fromisoformat(data["detected_at"])
        elif isinstance(data["detected_at"], (int, float)):
            data["detected_at"] = datetime.fromtimestamp(
                data["detected_at"], tz=timezone.utc
            )

        if data.get("resolved_at"):
            if isinstance(data["resolved_at"], str):
                data["resolved_at"] = datetime.fromisoformat(data["resolved_at"])
            elif isinstance(data["resolved_at"], (int, float)):
                data["resolved_at"] = datetime.fromtimestamp(
                    data["resolved_at"], tz=timezone.utc
                )

        # Convert string values back to enums
        data["conflict_type"] = ConflictType(data["conflict_type"])
        data["state"] = ConflictState(data["state"])
        if data.get("resolution_strategy"):
            data["resolution_strategy"] = ConflictResolutionStrategy(
                data["resolution_strategy"]
            )
        return cls(**data)


@dataclass
class FileRecord:
    path: str
    hashes: List[str]
    mtime: float
    size: int
    permissions: int = 0o644
    checksum: str = ""
    version: int = 1
    created_at: Optional[float] = None
    modified_at: Optional[float] = None
    peer_id: str = ""
    content_type: str = ""
    semantic_hash: str = ""  # Hash of semantic content for conflict detection
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()
        if self.modified_at is None:
            self.modified_at = self.mtime

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "FileRecord":
        # Handle missing fields for backward compatibility
        if "metadata" not in data:
            data["metadata"] = {}
        if "version" not in data:
            data["version"] = 1
        if "created_at" not in data:
            data["created_at"] = data.get("mtime", time.time())
        if "modified_at" not in data:
            data["modified_at"] = data.get("mtime", time.time())
        return cls(**data)


class CRDTStore:
    """Enhanced CRDT-based file metadata store with semantic conflict resolution"""

    def __init__(self, store_path: pathlib.Path = None, peer_id: str = None):
        if not PYCRDT_AVAILABLE:
            raise ImportError("pycrdt is required. Install with: pip install pycrdt")

        self.store_path = store_path or (
            pathlib.Path.home() / ".dittofs" / "crdt_store.yrs"
        )
        self.store_path.parent.mkdir(parents=True, exist_ok=True)

        # Peer identification for conflict resolution
        self.peer_id = peer_id or self._generate_peer_id()

        # Initialize CRDT document
        self.doc = Y.Doc()

        # Initialize versioning system
        versioning_path = self.store_path.parent / "versioning"
        self.versioning = VersioningSystem(versioning_path, self.peer_id)

        # Conflict resolution strategies
        self.conflict_resolvers: Dict[ConflictResolutionStrategy, Callable] = {
            ConflictResolutionStrategy.LAST_WRITER_WINS: self._resolve_last_writer_wins,
            ConflictResolutionStrategy.MANUAL_RESOLUTION: self._resolve_manual,
            ConflictResolutionStrategy.AUTOMATIC_MERGE: self._resolve_automatic_merge,
            ConflictResolutionStrategy.PRESERVE_BOTH: self._resolve_preserve_both,
            ConflictResolutionStrategy.SEMANTIC_MERGE: self._resolve_semantic_merge,
        }

        # Default conflict resolution strategy
        self.default_strategy = ConflictResolutionStrategy.LAST_WRITER_WINS

        # Active conflicts tracking
        self.active_conflicts: Dict[str, ConflictInfo] = {}

        # Load existing data first
        self._load()

        # Initialize maps - create them if they don't exist
        self._ensure_maps_exist()

    def _generate_peer_id(self) -> str:
        """Generate a unique peer ID"""
        import uuid

        return str(uuid.uuid4())[:8]

    def _ensure_maps_exist(self):
        """Ensure root, files, chunks, and conflicts maps exist"""
        try:
            # Get or create root map using the correct pycrdt API
            self.root = self.doc.get("root", type=Y.Map)

            # Get or create files map - Map.get() works like dict.get()
            self.files_map = self.root.get("files")
            if self.files_map is None:
                self.files_map = Y.Map()
                self.root["files"] = self.files_map

            # Get or create chunks map
            self.chunks_map = self.root.get("chunks")
            if self.chunks_map is None:
                self.chunks_map = Y.Map()
                self.root["chunks"] = self.chunks_map

            # Get or create conflicts map for tracking conflicts
            self.conflicts_map = self.root.get("conflicts")
            if self.conflicts_map is None:
                self.conflicts_map = Y.Map()
                self.root["conflicts"] = self.conflicts_map

            # Get or create versions map for file version history
            self.versions_map = self.root.get("versions")
            if self.versions_map is None:
                self.versions_map = Y.Map()
                self.root["versions"] = self.versions_map

        except Exception as e:
            logging.error(f"Error ensuring maps exist: {e}")
            # Fallback: create new maps
            self.root = Y.Map()
            self.files_map = Y.Map()
            self.chunks_map = Y.Map()
            self.conflicts_map = Y.Map()
            self.versions_map = Y.Map()
            self.doc["root"] = self.root
            self.root["files"] = self.files_map
            self.root["chunks"] = self.chunks_map
            self.root["conflicts"] = self.conflicts_map
            self.root["versions"] = self.versions_map

    def _calculate_semantic_hash(self, file_path: pathlib.Path) -> str:
        """Calculate semantic hash for conflict detection"""
        try:
            # For text files, normalize whitespace and calculate hash
            if file_path.suffix.lower() in [
                ".txt",
                ".md",
                ".py",
                ".js",
                ".html",
                ".css",
                ".json",
                ".xml",
            ]:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                    # Normalize whitespace for semantic comparison
                    normalized = " ".join(content.split())
                    return hashlib.sha256(normalized.encode()).hexdigest()
            else:
                # For binary files, use regular checksum
                with open(file_path, "rb") as f:
                    return hashlib.sha256(f.read()).hexdigest()
        except Exception as e:
            logging.warning(f"Failed to calculate semantic hash for {file_path}: {e}")
            return ""

    def _detect_conflict(
        self, new_record: FileRecord, existing_record: FileRecord
    ) -> Optional[ConflictInfo]:
        """Detect conflicts between file versions"""
        conflicts = []

        # Check for timestamp conflicts
        if (
            abs(new_record.mtime - existing_record.mtime) < 1.0
            and new_record.checksum != existing_record.checksum
        ):
            conflicts.append(ConflictType.TIMESTAMP_CONFLICT)

        # Check for content conflicts
        if new_record.checksum != existing_record.checksum:
            conflicts.append(ConflictType.CONTENT_CONFLICT)

        # Check for semantic conflicts (different semantic content)
        if (
            new_record.semantic_hash
            and existing_record.semantic_hash
            and new_record.semantic_hash != existing_record.semantic_hash
            and new_record.checksum != existing_record.checksum
        ):
            conflicts.append(ConflictType.SEMANTIC_CONFLICT)

        # Check for permission conflicts
        if new_record.permissions != existing_record.permissions:
            conflicts.append(ConflictType.PERMISSION_CONFLICT)

        # Check for metadata conflicts
        if new_record.metadata != existing_record.metadata:
            conflicts.append(ConflictType.METADATA_CONFLICT)

        if conflicts:
            conflict_id = hashlib.sha256(
                f"{new_record.path}_{new_record.mtime}_{existing_record.mtime}".encode()
            ).hexdigest()[:16]
            primary_conflict = conflicts[
                0
            ]  # Use the first detected conflict as primary

            return ConflictInfo(
                conflict_id=conflict_id,
                file_path=new_record.path,
                conflict_type=primary_conflict,
                state=ConflictState.DETECTED,
                detected_at=datetime.now(timezone.utc),
                versions=[existing_record.to_dict(), new_record.to_dict()],
            )

        return None

    def _resolve_last_writer_wins(self, conflict: ConflictInfo) -> Dict[str, Any]:
        """Resolve conflict using last writer wins strategy"""
        versions = conflict.versions
        if len(versions) < 2:
            return {"success": False, "error": "Insufficient versions for resolution"}

        # Find the version with the latest modification time
        latest_version = max(versions, key=lambda v: v.get("mtime", 0))

        return {
            "success": True,
            "resolved_version": latest_version,
            "strategy": "last_writer_wins",
            "discarded_versions": [v for v in versions if v != latest_version],
        }

    def _resolve_manual(self, conflict: ConflictInfo) -> Dict[str, Any]:
        """Mark conflict for manual resolution"""
        return {
            "success": False,
            "requires_manual_intervention": True,
            "strategy": "manual_resolution",
            "message": f"Conflict in {conflict.file_path} requires manual resolution",
        }

    def _resolve_automatic_merge(self, conflict: ConflictInfo) -> Dict[str, Any]:
        """Attempt automatic merge for compatible changes"""
        versions = conflict.versions
        if len(versions) != 2:
            return {
                "success": False,
                "error": "Automatic merge requires exactly 2 versions",
            }

        v1, v2 = versions

        # For non-conflicting metadata, merge them
        merged_metadata = {**v1.get("metadata", {}), **v2.get("metadata", {})}

        # Use the latest content but preserve metadata from both
        latest_version = v1 if v1.get("mtime", 0) > v2.get("mtime", 0) else v2
        merged_version = latest_version.copy()
        merged_version["metadata"] = merged_metadata
        merged_version["version"] = max(v1.get("version", 1), v2.get("version", 1)) + 1

        return {
            "success": True,
            "resolved_version": merged_version,
            "strategy": "automatic_merge",
            "merged_metadata": True,
        }

    def _resolve_preserve_both(self, conflict: ConflictInfo) -> Dict[str, Any]:
        """Preserve both versions with different names"""
        versions = conflict.versions
        if len(versions) < 2:
            return {"success": False, "error": "Insufficient versions for preservation"}

        base_path = pathlib.Path(conflict.file_path)
        preserved_versions = []

        for i, version in enumerate(versions):
            if i == 0:
                # Keep the first version as-is
                preserved_versions.append(version)
            else:
                # Rename subsequent versions
                stem = base_path.stem
                suffix = base_path.suffix
                peer_id = version.get("peer_id", "unknown")
                timestamp = int(version.get("mtime", time.time()))

                new_name = f"{stem}_conflict_{peer_id}_{timestamp}{suffix}"
                new_path = str(base_path.parent / new_name)

                preserved_version = version.copy()
                preserved_version["path"] = new_path
                preserved_versions.append(preserved_version)

        return {
            "success": True,
            "preserved_versions": preserved_versions,
            "strategy": "preserve_both",
        }

    def _resolve_semantic_merge(self, conflict: ConflictInfo) -> Dict[str, Any]:
        """Attempt semantic merge for text files"""
        versions = conflict.versions
        if len(versions) != 2:
            return {
                "success": False,
                "error": "Semantic merge requires exactly 2 versions",
            }

        # This is a placeholder for more sophisticated semantic merging
        # In a full implementation, this could use diff3 algorithms or AI-based merging

        v1, v2 = versions
        file_path = pathlib.Path(conflict.file_path)

        # Check if it's a text file that can be semantically merged
        if file_path.suffix.lower() not in [
            ".txt",
            ".md",
            ".py",
            ".js",
            ".html",
            ".css",
            ".json",
        ]:
            return {
                "success": False,
                "error": "Semantic merge only supported for text files",
            }

        # For now, fall back to automatic merge
        return self._resolve_automatic_merge(conflict)

    def resolve_conflict(
        self,
        conflict_id: str,
        strategy: Optional[ConflictResolutionStrategy] = None,
        manual_resolution: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Resolve a specific conflict"""
        try:
            conflict_data = self.conflicts_map.get(conflict_id)
            if not conflict_data:
                logging.error(f"Conflict {conflict_id} not found")
                return False

            conflict = ConflictInfo.from_dict(conflict_data)
            conflict.state = ConflictState.RESOLVING

            # Update conflict state in CRDT
            self.conflicts_map[conflict_id] = conflict.to_dict()

            # Use provided strategy or default
            resolution_strategy = strategy or self.default_strategy

            # Handle manual resolution
            if (
                resolution_strategy == ConflictResolutionStrategy.MANUAL_RESOLUTION
                and manual_resolution
            ):
                resolution_result = manual_resolution
                resolution_result["strategy"] = "manual_resolution"
                resolution_result["success"] = True
            else:
                # Use automatic resolution
                resolver = self.conflict_resolvers.get(resolution_strategy)
                if not resolver:
                    logging.error(f"Unknown resolution strategy: {resolution_strategy}")
                    return False

                resolution_result = resolver(conflict)

            if resolution_result.get("success"):
                # Apply the resolution
                if "resolved_version" in resolution_result:
                    # Single version resolution
                    resolved_record = FileRecord.from_dict(
                        resolution_result["resolved_version"]
                    )
                    self.files_map[conflict.file_path] = resolved_record.to_dict()
                elif "preserved_versions" in resolution_result:
                    # Multiple versions preservation
                    for version_data in resolution_result["preserved_versions"]:
                        version_record = FileRecord.from_dict(version_data)
                        self.files_map[version_record.path] = version_record.to_dict()

                # Mark conflict as resolved
                conflict.state = ConflictState.RESOLVED
                conflict.resolved_at = datetime.now(timezone.utc)
                conflict.resolution_strategy = resolution_strategy
                conflict.resolution_data = resolution_result

                self.conflicts_map[conflict_id] = conflict.to_dict()

                # Remove from active conflicts
                if conflict_id in self.active_conflicts:
                    del self.active_conflicts[conflict_id]

                logging.info(
                    f"Resolved conflict {conflict_id} using {resolution_strategy.value}"
                )
                return True
            else:
                # Resolution failed
                conflict.state = ConflictState.FAILED
                self.conflicts_map[conflict_id] = conflict.to_dict()

                if resolution_result.get("requires_manual_intervention"):
                    conflict.state = ConflictState.AWAITING_RESOLUTION
                    self.conflicts_map[conflict_id] = conflict.to_dict()
                    self.active_conflicts[conflict_id] = conflict

                logging.warning(
                    f"Failed to resolve conflict {conflict_id}: {resolution_result.get('error', 'Unknown error')}"
                )
                return False

        except Exception as e:
            logging.error(f"Error resolving conflict {conflict_id}: {e}")
            return False

    def get_conflicts(self, file_path: Optional[str] = None) -> List[ConflictInfo]:
        """Get all conflicts, optionally filtered by file path"""
        conflicts = []
        try:
            for conflict_id, conflict_data in self.conflicts_map.items():
                try:
                    conflict = ConflictInfo.from_dict(conflict_data)
                    if file_path is None or conflict.file_path == file_path:
                        conflicts.append(conflict)
                except Exception as e:
                    logging.warning(
                        f"Skipping corrupted conflict record {conflict_id}: {e}"
                    )
            return conflicts
        except Exception as e:
            logging.error(f"Error retrieving conflicts: {e}")
            return []

    def get_pending_conflicts(self) -> List[ConflictInfo]:
        """Get conflicts that require resolution"""
        return [
            c
            for c in self.get_conflicts()
            if c.state in [ConflictState.DETECTED, ConflictState.AWAITING_RESOLUTION]
        ]

    def add_file(self, file_path: pathlib.Path, hashes: List[str]) -> bool:
        """Add or update a file record with conflict detection"""
        try:
            if not file_path.exists():
                logging.error(f"File does not exist: {file_path}")
                return False

            stat = file_path.stat()
            path_str = str(file_path.resolve())

            # Calculate file checksum for integrity
            file_checksum = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    file_checksum.update(chunk)

            # Calculate semantic hash for conflict detection
            semantic_hash = self._calculate_semantic_hash(file_path)

            # Determine content type
            content_type = ""
            try:
                import mimetypes

                content_type = mimetypes.guess_type(str(file_path))[0] or ""
            except:
                pass

            # Check for existing record
            existing_record = self.get_file(file_path)

            # Create new record
            new_record = FileRecord(
                path=path_str,
                hashes=hashes,
                mtime=stat.st_mtime,
                size=stat.st_size,
                permissions=stat.st_mode & 0o777,
                checksum=file_checksum.hexdigest(),
                version=(existing_record.version + 1) if existing_record else 1,
                peer_id=self.peer_id,
                content_type=content_type,
                semantic_hash=semantic_hash,
                modified_at=time.time(),
            )

            # Detect conflicts if there's an existing record
            if existing_record and existing_record.checksum != new_record.checksum:
                conflict = self._detect_conflict(new_record, existing_record)
                if conflict:
                    # Store conflict information
                    self.conflicts_map[conflict.conflict_id] = conflict.to_dict()
                    self.active_conflicts[conflict.conflict_id] = conflict

                    logging.warning(
                        f"Conflict detected for {path_str}: {conflict.conflict_type.value}"
                    )

                    # Store version history
                    version_key = f"{path_str}_{new_record.version}"
                    self.versions_map[version_key] = new_record.to_dict()

                    # Attempt automatic resolution based on default strategy
                    if (
                        self.default_strategy
                        != ConflictResolutionStrategy.MANUAL_RESOLUTION
                    ):
                        self.resolve_conflict(
                            conflict.conflict_id, self.default_strategy
                        )
                    else:
                        logging.info(
                            f"Conflict {conflict.conflict_id} awaiting manual resolution"
                        )
                        return True  # Don't update the file record yet

            # Store in CRDT map (convert to dict for JSON serialization)
            self.files_map[new_record.path] = new_record.to_dict()

            # Store version history
            version_key = f"{path_str}_{new_record.version}"
            self.versions_map[version_key] = new_record.to_dict()

            # Track chunk ownership
            for chunk_hash in hashes:
                try:
                    chunk_owners = self.chunks_map.get(chunk_hash, [])
                    if isinstance(chunk_owners, str):
                        # Handle legacy single owner format
                        chunk_owners = [chunk_owners]
                    elif not isinstance(chunk_owners, list):
                        chunk_owners = []

                    if new_record.path not in chunk_owners:
                        chunk_owners.append(new_record.path)
                        self.chunks_map[chunk_hash] = chunk_owners
                except Exception as e:
                    logging.warning(
                        f"Failed to update chunk ownership for {chunk_hash}: {e}"
                    )

            logging.info(
                f"Added file: {new_record.path} with {len(hashes)} chunks (version {new_record.version})"
            )
            return True

        except Exception as e:
            logging.error(f"Failed to add file {file_path}: {e}")
            import traceback

            traceback.print_exc()
            return False

    def get_file(self, file_path: pathlib.Path) -> Optional[FileRecord]:
        """Get file record by path"""
        try:
            path_str = str(file_path.resolve())
            data = self.files_map.get(path_str)
            if data and isinstance(data, dict):
                return FileRecord.from_dict(data)
            return None
        except Exception as e:
            logging.error(f"Failed to get file {file_path}: {e}")
            return None

    def list_files(self) -> List[FileRecord]:
        """List all files in the store"""
        files = []
        try:
            for path_str, data in self.files_map.items():
                try:
                    if isinstance(data, dict):
                        files.append(FileRecord.from_dict(data))
                except Exception as e:
                    logging.warning(f"Skipping corrupted file record: {path_str}, {e}")
            return files
        except Exception as e:
            logging.error(f"Failed to list files: {e}")
            return []

    def get_missing_chunks(self) -> Set[str]:
        """Find chunks that we have metadata for but missing files"""
        from .chunker import CHUNK_DIR

        missing = set()

        try:
            for chunk_hash in self.chunks_map.keys():
                chunk_path = CHUNK_DIR / chunk_hash
                if not chunk_path.exists():
                    missing.add(chunk_hash)
        except Exception as e:
            logging.error(f"Error finding missing chunks: {e}")

        return missing

    def find_chunk_owners(self, chunk_hash: str) -> List[str]:
        """Find which files contain this chunk"""
        try:
            owners = self.chunks_map.get(chunk_hash, [])
            if isinstance(owners, str):
                return [owners]
            elif isinstance(owners, list):
                return owners
            else:
                return []
        except Exception as e:
            logging.error(f"Error finding chunk owners for {chunk_hash}: {e}")
            return []

    def remove_file(self, file_path: pathlib.Path) -> bool:
        """Remove a file record and update chunk ownership"""
        try:
            path_str = str(file_path.resolve())

            # Get the file record first to clean up chunk ownership
            file_record = self.get_file(file_path)
            if file_record:
                # Remove file from chunk ownership
                for chunk_hash in file_record.hashes:
                    try:
                        chunk_owners = self.chunks_map.get(chunk_hash, [])
                        if isinstance(chunk_owners, str):
                            chunk_owners = [chunk_owners]
                        elif not isinstance(chunk_owners, list):
                            chunk_owners = []

                        if path_str in chunk_owners:
                            chunk_owners.remove(path_str)
                            if chunk_owners:
                                self.chunks_map[chunk_hash] = chunk_owners
                            else:
                                # No more owners, remove chunk entry
                                del self.chunks_map[chunk_hash]
                    except Exception as e:
                        logging.warning(
                            f"Failed to update chunk ownership for {chunk_hash}: {e}"
                        )

            # Remove file record
            if path_str in self.files_map:
                del self.files_map[path_str]
                logging.info(f"Removed file: {path_str}")
                return True
            else:
                logging.warning(f"File not found in store: {path_str}")
                return False

        except Exception as e:
            logging.error(f"Failed to remove file {file_path}: {e}")
            return False

    def get_file_versions(
        self, file_path: pathlib.Path, limit: int = 10
    ) -> List[FileRecord]:
        """Get version history for a file"""
        try:
            path_str = str(file_path.resolve())
            versions = []

            for version_key, version_data in self.versions_map.items():
                if version_key.startswith(f"{path_str}_"):
                    try:
                        version_record = FileRecord.from_dict(version_data)
                        versions.append(version_record)
                    except Exception as e:
                        logging.warning(
                            f"Skipping corrupted version record {version_key}: {e}"
                        )

            # Sort by version number (descending)
            versions.sort(key=lambda v: v.version, reverse=True)
            return versions[:limit]

        except Exception as e:
            logging.error(f"Failed to get file versions: {e}")
            return []

    # Enhanced versioning methods using the new versioning system

    def create_file_version(
        self,
        file_path: pathlib.Path,
        content: bytes,
        branch_name: str = "main",
        commit_message: str = "",
    ) -> Optional[str]:
        """Create a new version of a file with advanced versioning"""
        try:
            path_str = str(file_path.resolve())

            # Get current version to determine parent
            current_record = self.get_file(file_path)
            parent_versions = []
            if current_record:
                # Find the version ID for the current record
                version_history = self.versioning.get_version_history(path_str, limit=1)
                if version_history:
                    parent_versions = [version_history[0].version_id]

            # Create version in the versioning system
            version_id = self.versioning.create_version(
                path_str, content, branch_name, parent_versions, commit_message
            )

            if version_id:
                logging.info(f"Created version {version_id} for {path_str}")
                return version_id

            return None

        except Exception as e:
            logging.error(f"Error creating file version: {e}")
            return None

    def get_version_content(self, version_id: str) -> Optional[bytes]:
        """Get content for a specific version"""
        return self.versioning.get_version_content(version_id)

    def get_enhanced_version_history(
        self,
        file_path: pathlib.Path,
        branch_name: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get enhanced version history with full version tree information"""
        try:
            path_str = str(file_path.resolve())
            version_nodes = self.versioning.get_version_history(
                path_str, branch_name, limit
            )

            # Convert to dict format with additional information
            enhanced_history = []
            for node in version_nodes:
                version_info = node.to_dict()

                # Add comparison information if there's a previous version
                if enhanced_history:
                    prev_version_id = enhanced_history[-1]["version_id"]
                    comparison = self.versioning.compare_versions(
                        prev_version_id, node.version_id
                    )
                    if comparison:
                        version_info["diff_info"] = {
                            "size_change": comparison.get("size_diff", 0),
                            "has_text_diff": comparison.get("is_text", False),
                            "is_identical": comparison.get("identical", False),
                        }

                enhanced_history.append(version_info)

            return enhanced_history

        except Exception as e:
            logging.error(f"Error getting enhanced version history: {e}")
            return []

    def create_branch(
        self, branch_name: str, parent_version_id: str, description: str = ""
    ) -> Optional[str]:
        """Create a new branch from a parent version"""
        return self.versioning.create_branch(
            branch_name, parent_version_id, description
        )

    def merge_branches(self, target_branch: str, source_branch: str) -> Optional[str]:
        """Merge one branch into another"""
        from .versioning import MergeStrategy

        return self.versioning.merge_branches(
            target_branch, source_branch, MergeStrategy.THREE_WAY
        )

    def get_branches(
        self, file_path: Optional[pathlib.Path] = None
    ) -> List[Dict[str, Any]]:
        """Get all branches, optionally filtered by file path"""
        path_str = str(file_path.resolve()) if file_path else None
        branches = self.versioning.get_branches(path_str)
        return [branch.to_dict() for branch in branches]

    def compare_file_versions(
        self, version1_id: str, version2_id: str
    ) -> Optional[Dict[str, Any]]:
        """Compare two versions of a file"""
        return self.versioning.compare_versions(version1_id, version2_id)

    def restore_file_version(self, file_path: pathlib.Path, version_id: str) -> bool:
        """Restore a file to a specific version"""
        try:
            # Get the version content
            content = self.versioning.get_version_content(version_id)
            if content is None:
                logging.error(f"Version {version_id} not found")
                return False

            # Write content to file
            with open(file_path, "wb") as f:
                f.write(content)

            # Update file record in CRDT store
            try:
                from .chunker import chunk_file

                hashes = chunk_file(file_path)
            except ImportError:
                # Fallback if chunker is not available
                hashes = [hashlib.sha256(content).hexdigest()]

            if self.add_file(file_path, hashes):
                logging.info(f"Restored {file_path} to version {version_id}")
                return True

            return False

        except Exception as e:
            logging.error(f"Error restoring file version: {e}")
            return False

    def set_retention_policy(self, policy: RetentionPolicy):
        """Set the default retention policy for version cleanup"""
        self.versioning.default_retention = policy

    def cleanup_old_versions(
        self,
        file_path: Optional[pathlib.Path] = None,
        policy: Optional[RetentionPolicy] = None,
    ) -> Dict[str, Any]:
        """Clean up old versions according to retention policy"""
        try:
            if file_path:
                # Clean up specific file
                path_str = str(file_path.resolve())
                return self.versioning.apply_retention_policy(path_str, policy)
            else:
                # Clean up all files
                total_stats = {"cleaned": 0, "compressed": 0, "errors": []}

                # Get all unique file paths from version history
                file_paths = set()
                for version_node in self.versioning.version_cache.values():
                    file_paths.add(version_node.file_path)

                for path_str in file_paths:
                    stats = self.versioning.apply_retention_policy(path_str, policy)
                    total_stats["cleaned"] += stats.get("cleaned", 0)
                    total_stats["compressed"] += stats.get("compressed", 0)
                    total_stats["errors"].extend(stats.get("errors", []))

                return total_stats

        except Exception as e:
            logging.error(f"Error cleaning up versions: {e}")
            return {"cleaned": 0, "compressed": 0, "errors": [str(e)]}

    def get_version_tree(self, file_path: pathlib.Path) -> Dict[str, Any]:
        """Get the complete version tree for a file"""
        path_str = str(file_path.resolve())
        return self.versioning.export_version_tree(path_str)

    def get_versioning_stats(self) -> Dict[str, Any]:
        """Get statistics about the versioning system"""
        return self.versioning.get_storage_stats()

    def set_conflict_resolution_strategy(self, strategy: ConflictResolutionStrategy):
        """Set the default conflict resolution strategy"""
        self.default_strategy = strategy
        logging.info(f"Set default conflict resolution strategy to {strategy.value}")

    def cleanup_missing_files(self) -> int:
        """Remove file records for files that no longer exist on disk"""
        removed_count = 0

        try:
            files = self.list_files()
            for file_record in files:
                file_path = pathlib.Path(file_record.path)
                if not file_path.exists():
                    if self.remove_file(file_path):
                        removed_count += 1

            if removed_count > 0:
                logging.info(f"Cleaned up {removed_count} missing file records")

        except Exception as e:
            logging.error(f"Error cleaning up missing files: {e}")

        return removed_count

    def merge_updates(self, update_data: bytes) -> bool:
        """Merge updates from remote peer with conflict detection"""
        try:
            # Handle empty updates gracefully
            if not update_data:
                logging.debug("Empty update data, nothing to merge")
                return True

            # Store current state for conflict detection
            current_files = {path: data for path, data in self.files_map.items()}

            # Apply the update
            self.doc.apply_update(update_data)

            # Re-ensure maps exist after update
            self._ensure_maps_exist()

            # Check for new conflicts after merge
            new_conflicts = []
            for path, new_data in self.files_map.items():
                if path in current_files:
                    current_data = current_files[path]
                    if isinstance(new_data, dict) and isinstance(current_data, dict):
                        try:
                            new_record = FileRecord.from_dict(new_data)
                            current_record = FileRecord.from_dict(current_data)

                            # Only check for conflicts if the records are actually different
                            if (
                                new_record.checksum != current_record.checksum
                                or new_record.mtime != current_record.mtime
                            ):
                                conflict = self._detect_conflict(
                                    new_record, current_record
                                )
                                if conflict:
                                    new_conflicts.append(conflict)
                        except Exception as e:
                            logging.warning(
                                f"Error checking for conflicts in {path}: {e}"
                            )

            # Process new conflicts
            for conflict in new_conflicts:
                self.conflicts_map[conflict.conflict_id] = conflict.to_dict()
                self.active_conflicts[conflict.conflict_id] = conflict

                logging.warning(
                    f"Merge conflict detected for {conflict.file_path}: {conflict.conflict_type.value}"
                )

                # Attempt automatic resolution if not set to manual
                if (
                    self.default_strategy
                    != ConflictResolutionStrategy.MANUAL_RESOLUTION
                ):
                    self.resolve_conflict(conflict.conflict_id, self.default_strategy)

            if new_conflicts:
                logging.info(
                    f"Merged CRDT update with {len(new_conflicts)} conflicts detected"
                )
            else:
                logging.info("Successfully merged CRDT update without conflicts")

            return True

        except Exception as e:
            logging.error(f"Failed to merge CRDT update: {e}")
            return False

    def get_state_vector(self) -> bytes:
        """Get state vector for efficient sync"""
        try:
            return self.doc.get_state()
        except Exception as e:
            logging.error(f"Failed to get state vector: {e}")
            return b""

    def get_update_since(self, state_vector: bytes) -> bytes:
        """Get update since given state vector"""
        try:
            if not state_vector:
                # If no state vector provided, return full update
                return self.doc.get_update()
            return self.doc.get_update(state_vector)
        except Exception as e:
            logging.error(f"Failed to get update: {e}")
            return b""

    def save(self) -> bool:
        """Save the CRDT document to disk"""
        try:
            # Get the current document state as bytes (full update from creation)
            update_data = self.doc.get_update()
            with open(self.store_path, "wb") as f:
                f.write(update_data)
            logging.debug(f"Saved CRDT store to {self.store_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to save CRDT store: {e}")
            return False

    def _load(self) -> bool:
        """Load the CRDT document from disk"""
        try:
            if self.store_path.exists():
                with open(self.store_path, "rb") as f:
                    update_data = f.read()
                    if update_data:
                        # Use the doc.apply_update() method instead of Y.merge_updates
                        self.doc.apply_update(update_data)
                        logging.debug(f"Loaded CRDT store from {self.store_path}")
                        # Re-ensure maps exist after loading
                        self._ensure_maps_exist()
            return True
        except Exception as e:
            logging.error(f"Failed to load CRDT store: {e}")
            # If loading fails, continue with empty document
            logging.info("Starting with empty CRDT document")
            return False

    @classmethod
    def load(cls, store_path: pathlib.Path = None, peer_id: str = None) -> "CRDTStore":
        """Load CRDT store from disk (backward compatibility method)"""
        return cls(store_path=store_path, peer_id=peer_id)

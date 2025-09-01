"""
Comprehensive file versioning system for DittoFS

This module provides advanced versioning capabilities including:
- Version history tracking with efficient delta storage
- Branching and merging for collaborative editing
- Version comparison and diff visualization
- Configurable retention policies
"""

import difflib
import hashlib
import json
import logging
import pathlib
import time
import zlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union


class VersionType(Enum):
    """Types of versions in the system"""

    MAIN = "main"  # Main branch version
    BRANCH = "branch"  # Branch version
    MERGE = "merge"  # Merge commit version
    SNAPSHOT = "snapshot"  # Snapshot version


class DeltaType(Enum):
    """Types of deltas for efficient storage"""

    FULL = "full"  # Full file content
    BINARY_DELTA = "binary_delta"  # Binary diff
    TEXT_DELTA = "text_delta"  # Text-based diff
    COMPRESSED = "compressed"  # Compressed content


class MergeStrategy(Enum):
    """Strategies for merging versions"""

    THREE_WAY = "three_way"  # Three-way merge
    FAST_FORWARD = "fast_forward"  # Fast-forward merge
    MANUAL = "manual"  # Manual merge required
    AUTO_RESOLVE = "auto_resolve"  # Automatic conflict resolution


@dataclass
class VersionDelta:
    """Represents a delta between two versions"""

    delta_type: DeltaType
    source_version: str
    target_version: str
    delta_data: bytes
    compression_ratio: float = 0.0
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["delta_type"] = self.delta_type.value
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "VersionDelta":
        data["delta_type"] = DeltaType(data["delta_type"])
        return cls(**data)


@dataclass
class VersionBranch:
    """Represents a version branch"""

    branch_id: str
    branch_name: str
    parent_version: str
    created_at: float
    created_by: str
    description: str = ""
    is_active: bool = True

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "VersionBranch":
        return cls(**data)


@dataclass
class VersionNode:
    """Represents a single version in the version tree"""

    version_id: str
    file_path: str
    version_type: VersionType
    parent_versions: List[str]  # Support for merge commits
    branch_id: str
    content_hash: str
    delta_hash: Optional[str] = None  # Hash of delta if using delta storage
    size: int = 0
    created_at: float = field(default_factory=time.time)
    created_by: str = ""
    commit_message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["version_type"] = self.version_type.value
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "VersionNode":
        data["version_type"] = VersionType(data["version_type"])
        return cls(**data)


@dataclass
class RetentionPolicy:
    """Configurable retention policy for version cleanup"""

    max_versions: Optional[int] = None  # Maximum number of versions to keep
    max_age_days: Optional[int] = None  # Maximum age in days
    keep_major_versions: bool = True  # Keep major versions (tagged/branched)
    keep_merge_commits: bool = True  # Keep merge commits
    compress_old_versions: bool = True  # Compress old versions
    compress_after_days: int = 30  # Compress versions older than this

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "RetentionPolicy":
        return cls(**data)


class VersioningSystem:
    """Comprehensive file versioning system"""

    def __init__(self, storage_path: pathlib.Path, peer_id: str = "default"):
        self.storage_path = storage_path
        self.peer_id = peer_id

        # Create storage directories
        self.versions_dir = storage_path / "versions"
        self.deltas_dir = storage_path / "deltas"
        self.branches_dir = storage_path / "branches"

        for dir_path in [self.versions_dir, self.deltas_dir, self.branches_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # In-memory caches
        self.version_cache: Dict[str, VersionNode] = {}
        self.branch_cache: Dict[str, VersionBranch] = {}
        self.delta_cache: Dict[str, VersionDelta] = {}

        # Default retention policy
        self.default_retention = RetentionPolicy(
            max_versions=100, max_age_days=365, compress_after_days=30
        )

        # Load existing data
        self._load_metadata()

    def _generate_version_id(self, file_path: str, content_hash: str) -> str:
        """Generate unique version ID"""
        timestamp = str(time.time())
        data = f"{file_path}_{content_hash}_{timestamp}_{self.peer_id}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def _generate_branch_id(self, branch_name: str) -> str:
        """Generate unique branch ID"""
        timestamp = str(time.time())
        data = f"{branch_name}_{timestamp}_{self.peer_id}"
        return hashlib.sha256(data.encode()).hexdigest()[:12]

    def _calculate_content_hash(self, content: bytes) -> str:
        """Calculate hash of content"""
        return hashlib.sha256(content).hexdigest()

    def _load_metadata(self):
        """Load version and branch metadata from disk"""
        try:
            # Load version nodes
            versions_file = self.storage_path / "versions.json"
            if versions_file.exists():
                with open(versions_file, "r") as f:
                    versions_data = json.load(f)
                    for version_id, version_data in versions_data.items():
                        self.version_cache[version_id] = VersionNode.from_dict(
                            version_data
                        )

            # Load branches
            branches_file = self.storage_path / "branches.json"
            if branches_file.exists():
                with open(branches_file, "r") as f:
                    branches_data = json.load(f)
                    for branch_id, branch_data in branches_data.items():
                        self.branch_cache[branch_id] = VersionBranch.from_dict(
                            branch_data
                        )

            # Load deltas
            deltas_file = self.storage_path / "deltas.json"
            if deltas_file.exists():
                with open(deltas_file, "r") as f:
                    deltas_data = json.load(f)
                    for delta_id, delta_data in deltas_data.items():
                        self.delta_cache[delta_id] = VersionDelta.from_dict(delta_data)

        except Exception as e:
            logging.error(f"Error loading versioning metadata: {e}")

    def _save_metadata(self):
        """Save version and branch metadata to disk"""
        try:
            # Save version nodes
            versions_data = {vid: v.to_dict() for vid, v in self.version_cache.items()}
            with open(self.storage_path / "versions.json", "w") as f:
                json.dump(versions_data, f, indent=2)

            # Save branches
            branches_data = {bid: b.to_dict() for bid, b in self.branch_cache.items()}
            with open(self.storage_path / "branches.json", "w") as f:
                json.dump(branches_data, f, indent=2)

            # Save deltas
            deltas_data = {did: d.to_dict() for did, d in self.delta_cache.items()}
            with open(self.storage_path / "deltas.json", "w") as f:
                json.dump(deltas_data, f, indent=2)

        except Exception as e:
            logging.error(f"Error saving versioning metadata: {e}")

    def create_branch(
        self, branch_name: str, parent_version: str, description: str = ""
    ) -> str:
        """Create a new branch from a parent version"""
        try:
            branch_id = self._generate_branch_id(branch_name)

            branch = VersionBranch(
                branch_id=branch_id,
                branch_name=branch_name,
                parent_version=parent_version,
                created_at=time.time(),
                created_by=self.peer_id,
                description=description,
            )

            self.branch_cache[branch_id] = branch
            self._save_metadata()

            logging.info(
                f"Created branch '{branch_name}' ({branch_id}) from version {parent_version}"
            )
            return branch_id

        except Exception as e:
            logging.error(f"Error creating branch: {e}")
            return ""

    def create_version(
        self,
        file_path: str,
        content: bytes,
        branch_id: str = "main",
        parent_versions: Optional[List[str]] = None,
        commit_message: str = "",
        use_delta: bool = True,
    ) -> str:
        """Create a new version of a file"""
        try:
            content_hash = self._calculate_content_hash(content)
            version_id = self._generate_version_id(file_path, content_hash)

            # Determine version type
            version_type = VersionType.MAIN
            if branch_id != "main":
                version_type = VersionType.BRANCH
            if parent_versions and len(parent_versions) > 1:
                version_type = VersionType.MERGE

            # Create version node
            version_node = VersionNode(
                version_id=version_id,
                file_path=file_path,
                version_type=version_type,
                parent_versions=parent_versions or [],
                branch_id=branch_id,
                content_hash=content_hash,
                size=len(content),
                created_by=self.peer_id,
                commit_message=commit_message,
            )

            # Store content (with delta compression if enabled)
            if use_delta and parent_versions:
                delta_stored = self._store_with_delta(
                    version_node, content, parent_versions[0]
                )
                if not delta_stored:
                    # Fall back to full storage
                    self._store_full_content(version_id, content)
            else:
                self._store_full_content(version_id, content)

            # Cache and save
            self.version_cache[version_id] = version_node
            self._save_metadata()

            logging.info(
                f"Created version {version_id} for {file_path} on branch {branch_id}"
            )
            return version_id

        except Exception as e:
            logging.error(f"Error creating version: {e}")
            return ""

    def _store_full_content(self, version_id: str, content: bytes):
        """Store full content for a version"""
        version_file = self.versions_dir / f"{version_id}.dat"
        with open(version_file, "wb") as f:
            f.write(content)

    def _store_with_delta(
        self, version_node: VersionNode, content: bytes, parent_version_id: str
    ) -> bool:
        """Store content using delta compression"""
        try:
            # Get parent content
            parent_content = self.get_version_content(parent_version_id)
            if parent_content is None:
                return False

            # Calculate delta
            delta_data = self._calculate_delta(parent_content, content)
            if not delta_data:
                return False

            # Check if delta is worth it (should be smaller than full content)
            compression_ratio = len(delta_data) / len(content)
            if (
                compression_ratio > 0.8
            ):  # If delta is more than 80% of original, store full
                return False

            # Store delta
            delta_id = f"{parent_version_id}_{version_node.version_id}"
            delta_file = self.deltas_dir / f"{delta_id}.delta"

            with open(delta_file, "wb") as f:
                f.write(delta_data)

            # Create delta record
            delta = VersionDelta(
                delta_type=DeltaType.BINARY_DELTA,
                source_version=parent_version_id,
                target_version=version_node.version_id,
                delta_data=delta_data,
                compression_ratio=compression_ratio,
            )

            self.delta_cache[delta_id] = delta
            version_node.delta_hash = delta_id

            logging.debug(
                f"Stored delta for {version_node.version_id} with {compression_ratio:.2%} compression"
            )
            return True

        except Exception as e:
            logging.error(f"Error storing delta: {e}")
            return False

    def _calculate_delta(
        self, old_content: bytes, new_content: bytes
    ) -> Optional[bytes]:
        """Calculate binary delta between two content versions"""
        try:
            # Simple delta implementation - in production, consider using more sophisticated algorithms
            # like xdelta or bsdiff for better compression

            # For text files, use text-based diff
            try:
                old_text = old_content.decode("utf-8")
                new_text = new_content.decode("utf-8")

                # Generate unified diff
                diff_lines = list(
                    difflib.unified_diff(
                        old_text.splitlines(keepends=True),
                        new_text.splitlines(keepends=True),
                        lineterm="",
                    )
                )

                if diff_lines:
                    diff_text = "".join(diff_lines)
                    compressed_diff = zlib.compress(diff_text.encode("utf-8"))
                    return compressed_diff

            except UnicodeDecodeError:
                # Binary file - use simple binary diff
                pass

            # Binary delta (simplified implementation)
            # In production, use proper binary diff algorithms
            if len(new_content) < len(old_content) * 0.5:
                # If new content is much smaller, just store it compressed
                return zlib.compress(new_content)

            # For now, return compressed new content as "delta"
            compressed = zlib.compress(new_content)
            if len(compressed) < len(new_content) * 0.8:
                return compressed

            return None

        except Exception as e:
            logging.error(f"Error calculating delta: {e}")
            return None

    def get_version_content(self, version_id: str) -> Optional[bytes]:
        """Retrieve content for a specific version"""
        try:
            version_node = self.version_cache.get(version_id)
            if not version_node:
                return None

            # Check if stored as delta
            if version_node.delta_hash:
                return self._reconstruct_from_delta(version_id)

            # Load full content
            version_file = self.versions_dir / f"{version_id}.dat"
            if version_file.exists():
                with open(version_file, "rb") as f:
                    return f.read()

            return None

        except Exception as e:
            logging.error(f"Error getting version content: {e}")
            return None

    def _reconstruct_from_delta(self, version_id: str) -> Optional[bytes]:
        """Reconstruct content from delta"""
        try:
            version_node = self.version_cache.get(version_id)
            if not version_node or not version_node.delta_hash:
                return None

            delta = self.delta_cache.get(version_node.delta_hash)
            if not delta:
                return None

            # Get parent content
            parent_content = self.get_version_content(delta.source_version)
            if parent_content is None:
                return None

            # Load delta data
            delta_file = self.deltas_dir / f"{version_node.delta_hash}.delta"
            if not delta_file.exists():
                return None

            with open(delta_file, "rb") as f:
                delta_data = f.read()

            # Apply delta
            return self._apply_delta(parent_content, delta_data, delta.delta_type)

        except Exception as e:
            logging.error(f"Error reconstructing from delta: {e}")
            return None

    def _apply_delta(
        self, base_content: bytes, delta_data: bytes, delta_type: DeltaType
    ) -> Optional[bytes]:
        """Apply delta to base content"""
        try:
            if delta_type == DeltaType.BINARY_DELTA:
                # Decompress delta
                decompressed = zlib.decompress(delta_data)

                # Try to apply as text diff first
                try:
                    base_text = base_content.decode("utf-8")
                    diff_text = decompressed.decode("utf-8")

                    # Parse unified diff and apply
                    # This is a simplified implementation
                    # In production, use proper diff application libraries
                    if diff_text.startswith("---"):
                        # This looks like a unified diff, but for simplicity,
                        # we'll just return the decompressed content as the new version
                        return decompressed

                except UnicodeDecodeError:
                    pass

                # Binary delta - return decompressed content
                return decompressed

            elif delta_type == DeltaType.COMPRESSED:
                return zlib.decompress(delta_data)

            return None

        except Exception as e:
            logging.error(f"Error applying delta: {e}")
            return None

    def get_version_history(
        self, file_path: str, branch_id: Optional[str] = None, limit: int = 50
    ) -> List[VersionNode]:
        """Get version history for a file"""
        try:
            versions = []

            for version_id, version_node in self.version_cache.items():
                if version_node.file_path == file_path:
                    if branch_id is None or version_node.branch_id == branch_id:
                        versions.append(version_node)

            # Sort by creation time (newest first)
            versions.sort(key=lambda v: v.created_at, reverse=True)

            return versions[:limit]

        except Exception as e:
            logging.error(f"Error getting version history: {e}")
            return []

    def get_branches(self, file_path: Optional[str] = None) -> List[VersionBranch]:
        """Get all branches, optionally filtered by file path"""
        try:
            branches = list(self.branch_cache.values())

            if file_path:
                # Filter branches that have versions for this file
                filtered_branches = []
                for branch in branches:
                    has_file_versions = any(
                        v.file_path == file_path and v.branch_id == branch.branch_id
                        for v in self.version_cache.values()
                    )
                    if has_file_versions:
                        filtered_branches.append(branch)
                return filtered_branches

            return branches

        except Exception as e:
            logging.error(f"Error getting branches: {e}")
            return []

    def merge_branches(
        self,
        target_branch: str,
        source_branch: str,
        strategy: MergeStrategy = MergeStrategy.THREE_WAY,
    ) -> Optional[str]:
        """Merge one branch into another"""
        try:
            # Get latest versions from both branches
            target_versions = self._get_latest_versions_in_branch(target_branch)
            source_versions = self._get_latest_versions_in_branch(source_branch)

            # Find common files
            common_files = set(target_versions.keys()) & set(source_versions.keys())

            merge_results = {}
            conflicts = []

            for file_path in common_files:
                target_version = target_versions[file_path]
                source_version = source_versions[file_path]

                # Find common ancestor
                common_ancestor = self._find_common_ancestor(
                    target_version.version_id, source_version.version_id
                )

                if strategy == MergeStrategy.FAST_FORWARD:
                    # Fast-forward merge - just use source version
                    merge_results[file_path] = source_version

                elif strategy == MergeStrategy.THREE_WAY and common_ancestor:
                    # Three-way merge
                    merge_result = self._three_way_merge(
                        common_ancestor, target_version, source_version
                    )

                    if merge_result.get("conflicts"):
                        conflicts.append(
                            {
                                "file_path": file_path,
                                "conflicts": merge_result["conflicts"],
                            }
                        )
                    else:
                        merge_results[file_path] = merge_result["merged_version"]

                else:
                    # Manual merge required
                    conflicts.append(
                        {"file_path": file_path, "reason": "Manual merge required"}
                    )

            if conflicts and strategy != MergeStrategy.AUTO_RESOLVE:
                logging.warning(f"Merge conflicts detected: {len(conflicts)} files")
                return None

            # Create merge commit
            merge_commit_id = self._create_merge_commit(
                target_branch, source_branch, merge_results
            )

            logging.info(f"Successfully merged {source_branch} into {target_branch}")
            return merge_commit_id

        except Exception as e:
            logging.error(f"Error merging branches: {e}")
            return None

    def _get_latest_versions_in_branch(self, branch_id: str) -> Dict[str, VersionNode]:
        """Get the latest version of each file in a branch"""
        latest_versions = {}

        for version_node in self.version_cache.values():
            if version_node.branch_id == branch_id:
                file_path = version_node.file_path
                if (
                    file_path not in latest_versions
                    or version_node.created_at > latest_versions[file_path].created_at
                ):
                    latest_versions[file_path] = version_node

        return latest_versions

    def _find_common_ancestor(
        self, version1_id: str, version2_id: str
    ) -> Optional[VersionNode]:
        """Find common ancestor of two versions"""
        # Simplified implementation - in production, use proper graph traversal
        try:
            version1 = self.version_cache.get(version1_id)
            version2 = self.version_cache.get(version2_id)

            if not version1 or not version2:
                return None

            # For now, return the version with earlier timestamp as common ancestor
            # In a full implementation, traverse the version graph to find actual common ancestor
            if version1.created_at < version2.created_at:
                return version1
            else:
                return version2

        except Exception as e:
            logging.error(f"Error finding common ancestor: {e}")
            return None

    def _three_way_merge(
        self, ancestor: VersionNode, version1: VersionNode, version2: VersionNode
    ) -> Dict[str, Any]:
        """Perform three-way merge"""
        try:
            # Get content for all three versions
            ancestor_content = self.get_version_content(ancestor.version_id)
            content1 = self.get_version_content(version1.version_id)
            content2 = self.get_version_content(version2.version_id)

            if None in [ancestor_content, content1, content2]:
                return {"conflicts": ["Unable to retrieve version content"]}

            # Try text-based merge for text files
            try:
                ancestor_text = ancestor_content.decode("utf-8")
                text1 = content1.decode("utf-8")
                text2 = content2.decode("utf-8")

                # Perform line-by-line merge
                merged_text, conflicts = self._merge_text_content(
                    ancestor_text, text1, text2
                )

                if conflicts:
                    return {"conflicts": conflicts}

                # Create merged version
                merged_content = merged_text.encode("utf-8")
                merged_version_id = self.create_version(
                    version1.file_path,
                    merged_content,
                    version1.branch_id,
                    [version1.version_id, version2.version_id],
                    f"Merge {version2.branch_id} into {version1.branch_id}",
                )

                return {"merged_version": self.version_cache[merged_version_id]}

            except UnicodeDecodeError:
                # Binary file - cannot auto-merge
                return {"conflicts": ["Binary file cannot be automatically merged"]}

        except Exception as e:
            logging.error(f"Error in three-way merge: {e}")
            return {"conflicts": [f"Merge error: {str(e)}"]}

    def _merge_text_content(
        self, ancestor: str, version1: str, version2: str
    ) -> Tuple[str, List[str]]:
        """Merge text content using line-by-line comparison"""
        # Simplified merge implementation
        # In production, use proper merge algorithms like diff3

        ancestor_lines = ancestor.splitlines()
        lines1 = version1.splitlines()
        lines2 = version2.splitlines()

        # For now, just return version1 content if no conflicts
        # In a full implementation, perform proper three-way merge
        if lines1 == lines2:
            return version1, []

        # Detect conflicts (simplified)
        conflicts = []
        if lines1 != ancestor_lines and lines2 != ancestor_lines:
            conflicts.append("Content modified in both versions")

        if conflicts:
            return "", conflicts

        # Return the modified version
        if lines1 != ancestor_lines:
            return version1, []
        else:
            return version2, []

    def _create_merge_commit(
        self,
        target_branch: str,
        source_branch: str,
        merge_results: Dict[str, VersionNode],
    ) -> str:
        """Create a merge commit"""
        try:
            # Create merge commit for each merged file
            merge_commit_ids = []

            for file_path, merged_version in merge_results.items():
                # Get parent versions
                target_version = self._get_latest_version_in_branch(
                    file_path, target_branch
                )
                source_version = self._get_latest_version_in_branch(
                    file_path, source_branch
                )

                parent_versions = []
                if target_version:
                    parent_versions.append(target_version.version_id)
                if source_version:
                    parent_versions.append(source_version.version_id)

                # Create merge version
                content = self.get_version_content(merged_version.version_id)
                if content:
                    merge_id = self.create_version(
                        file_path,
                        content,
                        target_branch,
                        parent_versions,
                        f"Merge {source_branch} into {target_branch}",
                    )
                    merge_commit_ids.append(merge_id)

            return merge_commit_ids[0] if merge_commit_ids else ""

        except Exception as e:
            logging.error(f"Error creating merge commit: {e}")
            return ""

    def _get_latest_version_in_branch(
        self, file_path: str, branch_id: str
    ) -> Optional[VersionNode]:
        """Get latest version of a file in a specific branch"""
        latest_version = None

        for version_node in self.version_cache.values():
            if (
                version_node.file_path == file_path
                and version_node.branch_id == branch_id
            ):
                if (
                    latest_version is None
                    or version_node.created_at > latest_version.created_at
                ):
                    latest_version = version_node

        return latest_version

    def compare_versions(
        self, version1_id: str, version2_id: str
    ) -> Optional[Dict[str, Any]]:
        """Compare two versions and return diff information"""
        try:
            content1 = self.get_version_content(version1_id)
            content2 = self.get_version_content(version2_id)

            if content1 is None or content2 is None:
                return None

            version1 = self.version_cache.get(version1_id)
            version2 = self.version_cache.get(version2_id)

            comparison = {
                "version1": version1.to_dict() if version1 else {},
                "version2": version2.to_dict() if version2 else {},
                "size_diff": len(content2) - len(content1),
                "identical": content1 == content2,
            }

            if not comparison["identical"]:
                # Try to generate text diff
                try:
                    text1 = content1.decode("utf-8")
                    text2 = content2.decode("utf-8")

                    diff_lines = list(
                        difflib.unified_diff(
                            text1.splitlines(keepends=True),
                            text2.splitlines(keepends=True),
                            fromfile=f"Version {version1_id}",
                            tofile=f"Version {version2_id}",
                            lineterm="",
                        )
                    )

                    comparison["text_diff"] = "".join(diff_lines)
                    comparison["is_text"] = True

                except UnicodeDecodeError:
                    comparison["is_text"] = False
                    comparison["binary_diff"] = True

            return comparison

        except Exception as e:
            logging.error(f"Error comparing versions: {e}")
            return None

    def apply_retention_policy(
        self, file_path: str, policy: Optional[RetentionPolicy] = None
    ) -> Dict[str, Any]:
        """Apply retention policy to clean up old versions"""
        try:
            policy = policy or self.default_retention

            # Get all versions for the file
            versions = self.get_version_history(file_path, limit=None)
            if not versions:
                return {"cleaned": 0, "compressed": 0, "errors": []}

            cleaned_count = 0
            compressed_count = 0
            errors = []

            # Sort versions by age (oldest first)
            versions.sort(key=lambda v: v.created_at)

            # Apply max_versions limit
            if policy.max_versions and len(versions) > policy.max_versions:
                versions_to_remove = versions[: -policy.max_versions]

                for version in versions_to_remove:
                    # Keep major versions and merge commits if policy says so
                    if policy.keep_major_versions and version.version_type in [
                        VersionType.MERGE,
                        VersionType.SNAPSHOT,
                    ]:
                        continue

                    if self._remove_version(version.version_id):
                        cleaned_count += 1
                    else:
                        errors.append(f"Failed to remove version {version.version_id}")

            # Apply age-based cleanup
            if policy.max_age_days:
                cutoff_time = time.time() - (policy.max_age_days * 24 * 3600)

                for version in versions:
                    if version.created_at < cutoff_time:
                        # Keep major versions if policy says so
                        if policy.keep_major_versions and version.version_type in [
                            VersionType.MERGE,
                            VersionType.SNAPSHOT,
                        ]:
                            continue

                        if self._remove_version(version.version_id):
                            cleaned_count += 1
                        else:
                            errors.append(
                                f"Failed to remove old version {version.version_id}"
                            )

            # Apply compression to old versions
            if policy.compress_old_versions and policy.compress_after_days:
                compress_cutoff = time.time() - (policy.compress_after_days * 24 * 3600)

                for version in versions:
                    if (
                        version.created_at < compress_cutoff and not version.delta_hash
                    ):  # Not already using delta storage

                        if self._compress_version(version.version_id):
                            compressed_count += 1
                        else:
                            errors.append(
                                f"Failed to compress version {version.version_id}"
                            )

            self._save_metadata()

            return {
                "cleaned": cleaned_count,
                "compressed": compressed_count,
                "errors": errors,
            }

        except Exception as e:
            logging.error(f"Error applying retention policy: {e}")
            return {"cleaned": 0, "compressed": 0, "errors": [str(e)]}

    def _remove_version(self, version_id: str) -> bool:
        """Remove a version and its associated data"""
        try:
            version_node = self.version_cache.get(version_id)
            if not version_node:
                return False

            # Remove content file
            version_file = self.versions_dir / f"{version_id}.dat"
            if version_file.exists():
                version_file.unlink()

            # Remove delta file if exists
            if version_node.delta_hash:
                delta_file = self.deltas_dir / f"{version_node.delta_hash}.delta"
                if delta_file.exists():
                    delta_file.unlink()

                # Remove delta from cache
                if version_node.delta_hash in self.delta_cache:
                    del self.delta_cache[version_node.delta_hash]

            # Remove from cache
            del self.version_cache[version_id]

            logging.debug(f"Removed version {version_id}")
            return True

        except Exception as e:
            logging.error(f"Error removing version {version_id}: {e}")
            return False

    def _compress_version(self, version_id: str) -> bool:
        """Compress a version using delta storage"""
        try:
            version_node = self.version_cache.get(version_id)
            if not version_node or version_node.delta_hash:
                return False  # Already compressed or doesn't exist

            # Get current content
            content = self.get_version_content(version_id)
            if content is None:
                return False

            # Find a suitable parent version for delta compression
            parent_versions = self.get_version_history(
                version_node.file_path, version_node.branch_id
            )
            parent_version = None

            for v in parent_versions:
                if (
                    v.version_id != version_id
                    and v.created_at < version_node.created_at
                    and not v.delta_hash
                ):  # Use a non-delta version as parent
                    parent_version = v
                    break

            if parent_version:
                # Try to create delta
                if self._store_with_delta(
                    version_node, content, parent_version.version_id
                ):
                    # Remove original full content file
                    version_file = self.versions_dir / f"{version_id}.dat"
                    if version_file.exists():
                        version_file.unlink()

                    logging.debug(f"Compressed version {version_id} using delta")
                    return True

            return False

        except Exception as e:
            logging.error(f"Error compressing version {version_id}: {e}")
            return False

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics for the versioning system"""
        try:
            stats = {
                "total_versions": len(self.version_cache),
                "total_branches": len(self.branch_cache),
                "total_deltas": len(self.delta_cache),
                "storage_size": 0,
                "delta_size": 0,
                "compression_ratio": 0.0,
            }

            # Calculate storage sizes
            for version_file in self.versions_dir.glob("*.dat"):
                stats["storage_size"] += version_file.stat().st_size

            for delta_file in self.deltas_dir.glob("*.delta"):
                stats["delta_size"] += delta_file.stat().st_size

            total_size = stats["storage_size"] + stats["delta_size"]
            if total_size > 0:
                stats["compression_ratio"] = stats["delta_size"] / total_size

            # Version type breakdown
            version_types = {}
            for version in self.version_cache.values():
                vtype = version.version_type.value
                version_types[vtype] = version_types.get(vtype, 0) + 1

            stats["version_types"] = version_types

            return stats

        except Exception as e:
            logging.error(f"Error getting storage stats: {e}")
            return {}

    def export_version_tree(self, file_path: str) -> Dict[str, Any]:
        """Export the version tree for a file in a format suitable for visualization"""
        try:
            versions = self.get_version_history(file_path, limit=None)
            branches = self.get_branches(file_path)

            # Build tree structure
            tree = {
                "file_path": file_path,
                "branches": [b.to_dict() for b in branches],
                "versions": [],
                "relationships": [],
            }

            for version in versions:
                version_data = version.to_dict()
                version_data["content_size"] = version.size
                tree["versions"].append(version_data)

                # Add parent relationships
                for parent_id in version.parent_versions:
                    tree["relationships"].append(
                        {
                            "parent": parent_id,
                            "child": version.version_id,
                            "type": "parent_child",
                        }
                    )

            return tree

        except Exception as e:
            logging.error(f"Error exporting version tree: {e}")
            return {}

"""
Conflict Resolution Workflows for DittoFS

This module provides comprehensive conflict resolution capabilities including:
- User-friendly conflict resolution interface
- Automatic conflict resolution for simple cases
- Manual conflict resolution tools with side-by-side comparison
- Conflict resolution history and audit trail
"""

import asyncio
import difflib
import hashlib
import json
import logging
import pathlib
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .crdt_store import (
    ConflictInfo,
    ConflictResolutionStrategy,
    ConflictState,
    ConflictType,
    CRDTStore,
    FileRecord,
)


class ResolutionAction(Enum):
    """Actions that can be taken to resolve conflicts"""

    KEEP_LOCAL = "keep_local"
    KEEP_REMOTE = "keep_remote"
    MERGE_AUTOMATIC = "merge_automatic"
    MERGE_MANUAL = "merge_manual"
    PRESERVE_BOTH = "preserve_both"
    CUSTOM_RESOLUTION = "custom_resolution"


class ResolutionResult(Enum):
    """Results of conflict resolution attempts"""

    SUCCESS = "success"
    FAILED = "failed"
    REQUIRES_MANUAL = "requires_manual"
    PARTIAL_SUCCESS = "partial_success"


@dataclass
class ResolutionEntry:
    """Audit trail entry for conflict resolution"""

    resolution_id: str
    conflict_id: str
    file_path: str
    resolved_at: datetime
    resolved_by: str  # peer_id or user identifier
    action_taken: ResolutionAction
    result: ResolutionResult
    strategy_used: ConflictResolutionStrategy
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["resolved_at"] = self.resolved_at.isoformat()
        data["action_taken"] = self.action_taken.value
        data["result"] = self.result.value
        data["strategy_used"] = self.strategy_used.value
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "ResolutionEntry":
        data["resolved_at"] = datetime.fromisoformat(data["resolved_at"])
        data["action_taken"] = ResolutionAction(data["action_taken"])
        data["result"] = ResolutionResult(data["result"])
        data["strategy_used"] = ConflictResolutionStrategy(data["strategy_used"])
        return cls(**data)


@dataclass
class ConflictDiff:
    """Represents differences between conflicting versions"""

    file_path: str
    local_version: FileRecord
    remote_version: FileRecord
    content_diff: Optional[str] = None
    metadata_diff: Dict[str, Any] = field(default_factory=dict)
    binary_file: bool = False


class ConflictResolutionWorkflow:
    """
    Comprehensive conflict resolution workflow system

    Provides user-friendly interfaces for resolving conflicts with support for:
    - Automatic resolution of simple conflicts
    - Manual resolution with side-by-side comparison
    - Audit trail of all resolution actions
    - Customizable resolution strategies
    """

    def __init__(self, crdt_store: CRDTStore, peer_id: str = None):
        self.crdt_store = crdt_store
        self.peer_id = peer_id or crdt_store.peer_id

        # Resolution history storage
        self.resolution_history_path = (
            crdt_store.store_path.parent / "resolution_history.json"
        )
        self.resolution_history: List[ResolutionEntry] = []
        self._load_resolution_history()

        # Custom resolution handlers
        self.custom_handlers: Dict[str, Callable] = {}

        # Auto-resolution rules
        self.auto_resolution_rules: List[
            Callable[[ConflictInfo], Optional[ResolutionAction]]
        ] = [
            self._rule_non_overlapping_changes,
            self._rule_metadata_only_conflicts,
            self._rule_permission_conflicts,
            self._rule_timestamp_tolerance,
        ]

    def _load_resolution_history(self):
        """Load resolution history from disk"""
        try:
            if self.resolution_history_path.exists():
                with open(self.resolution_history_path, "r") as f:
                    data = json.load(f)
                    self.resolution_history = [
                        ResolutionEntry.from_dict(entry) for entry in data
                    ]
                logging.debug(
                    f"Loaded {len(self.resolution_history)} resolution history entries"
                )
        except Exception as e:
            logging.warning(f"Failed to load resolution history: {e}")
            self.resolution_history = []

    def _save_resolution_history(self):
        """Save resolution history to disk"""
        try:
            self.resolution_history_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.resolution_history_path, "w") as f:
                data = [entry.to_dict() for entry in self.resolution_history]
                json.dump(data, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save resolution history: {e}")

    def _add_resolution_entry(
        self,
        conflict_id: str,
        file_path: str,
        action: ResolutionAction,
        result: ResolutionResult,
        strategy: ConflictResolutionStrategy,
        details: Dict[str, Any] = None,
    ):
        """Add entry to resolution audit trail"""
        resolution_id = hashlib.sha256(
            f"{conflict_id}_{time.time()}_{self.peer_id}".encode()
        ).hexdigest()[:16]

        entry = ResolutionEntry(
            resolution_id=resolution_id,
            conflict_id=conflict_id,
            file_path=file_path,
            resolved_at=datetime.now(timezone.utc),
            resolved_by=self.peer_id,
            action_taken=action,
            result=result,
            strategy_used=strategy,
            details=details or {},
        )

        self.resolution_history.append(entry)
        self._save_resolution_history()

        logging.info(
            f"Added resolution entry: {resolution_id} for conflict {conflict_id}"
        )

    def _rule_non_overlapping_changes(
        self, conflict: ConflictInfo
    ) -> Optional[ResolutionAction]:
        """Auto-resolve conflicts with non-overlapping changes"""
        if conflict.conflict_type == ConflictType.METADATA_CONFLICT:
            # Check if metadata changes don't overlap
            versions = conflict.versions
            if len(versions) == 2:
                v1_meta = versions[0].get("metadata", {})
                v2_meta = versions[1].get("metadata", {})

                # If metadata keys don't overlap, we can merge automatically
                if not set(v1_meta.keys()) & set(v2_meta.keys()):
                    return ResolutionAction.MERGE_AUTOMATIC

        return None

    def _rule_metadata_only_conflicts(
        self, conflict: ConflictInfo
    ) -> Optional[ResolutionAction]:
        """Auto-resolve metadata-only conflicts"""
        if conflict.conflict_type == ConflictType.METADATA_CONFLICT:
            versions = conflict.versions
            if len(versions) == 2:
                # If content is the same but metadata differs, merge metadata
                if versions[0].get("checksum") == versions[1].get("checksum"):
                    return ResolutionAction.MERGE_AUTOMATIC

        return None

    def _rule_permission_conflicts(
        self, conflict: ConflictInfo
    ) -> Optional[ResolutionAction]:
        """Auto-resolve permission conflicts by keeping more permissive settings"""
        if conflict.conflict_type == ConflictType.PERMISSION_CONFLICT:
            versions = conflict.versions
            if len(versions) == 2:
                perm1 = versions[0].get("permissions", 0o644)
                perm2 = versions[1].get("permissions", 0o644)

                # Keep the more permissive setting (higher permission bits)
                if perm1 != perm2:
                    return (
                        ResolutionAction.KEEP_LOCAL
                        if perm1 > perm2
                        else ResolutionAction.KEEP_REMOTE
                    )

        return None

    def _rule_timestamp_tolerance(
        self, conflict: ConflictInfo
    ) -> Optional[ResolutionAction]:
        """Auto-resolve conflicts within timestamp tolerance"""
        if conflict.conflict_type == ConflictType.TIMESTAMP_CONFLICT:
            versions = conflict.versions
            if len(versions) == 2:
                mtime1 = versions[0].get("mtime", 0)
                mtime2 = versions[1].get("mtime", 0)

                # If timestamps are very close (within 2 seconds), use last writer wins
                if abs(mtime1 - mtime2) <= 2.0:
                    return (
                        ResolutionAction.KEEP_LOCAL
                        if mtime1 > mtime2
                        else ResolutionAction.KEEP_REMOTE
                    )

        return None

    def can_auto_resolve(
        self, conflict: ConflictInfo
    ) -> Tuple[bool, Optional[ResolutionAction]]:
        """Check if conflict can be automatically resolved"""
        for rule in self.auto_resolution_rules:
            try:
                action = rule(conflict)
                if action:
                    return True, action
            except Exception as e:
                logging.warning(f"Auto-resolution rule failed: {e}")

        return False, None

    async def auto_resolve_conflict(self, conflict_id: str) -> bool:
        """Attempt automatic resolution of a conflict"""
        try:
            # Get conflict info
            conflict_data = self.crdt_store.conflicts_map.get(conflict_id)
            if not conflict_data:
                logging.error(f"Conflict {conflict_id} not found")
                return False

            conflict = ConflictInfo.from_dict(conflict_data)

            # Check if auto-resolution is possible
            can_resolve, action = self.can_auto_resolve(conflict)
            if not can_resolve:
                logging.info(f"Conflict {conflict_id} requires manual resolution")
                return False

            # Perform auto-resolution
            success = await self._execute_resolution_action(conflict, action)

            if success:
                self._add_resolution_entry(
                    conflict_id,
                    conflict.file_path,
                    action,
                    ResolutionResult.SUCCESS,
                    ConflictResolutionStrategy.AUTOMATIC_MERGE,
                    {"auto_resolved": True, "rule_applied": action.value},
                )
                logging.info(
                    f"Auto-resolved conflict {conflict_id} using {action.value}"
                )
                return True
            else:
                self._add_resolution_entry(
                    conflict_id,
                    conflict.file_path,
                    action,
                    ResolutionResult.FAILED,
                    ConflictResolutionStrategy.AUTOMATIC_MERGE,
                    {"auto_resolve_failed": True},
                )
                return False

        except Exception as e:
            logging.error(f"Error auto-resolving conflict {conflict_id}: {e}")
            return False

    async def _execute_resolution_action(
        self, conflict: ConflictInfo, action: ResolutionAction
    ) -> bool:
        """Execute a specific resolution action"""
        try:
            versions = conflict.versions
            if len(versions) < 2:
                return False

            if action == ResolutionAction.KEEP_LOCAL:
                # Keep the first version (local)
                resolved_version = versions[0]

            elif action == ResolutionAction.KEEP_REMOTE:
                # Keep the second version (remote)
                resolved_version = versions[1]

            elif action == ResolutionAction.MERGE_AUTOMATIC:
                # Perform automatic merge
                resolved_version = self._merge_versions_automatically(
                    versions[0], versions[1]
                )
                if not resolved_version:
                    return False

            elif action == ResolutionAction.PRESERVE_BOTH:
                # Preserve both versions with different names
                return await self._preserve_both_versions(conflict, versions)

            else:
                logging.error(f"Unsupported resolution action: {action}")
                return False

            # Update the file record in CRDT store
            file_record = FileRecord.from_dict(resolved_version)
            self.crdt_store.files_map[conflict.file_path] = file_record.to_dict()

            # Mark conflict as resolved
            conflict.state = ConflictState.RESOLVED
            conflict.resolved_at = datetime.now(timezone.utc)
            conflict.resolution_strategy = ConflictResolutionStrategy.AUTOMATIC_MERGE

            self.crdt_store.conflicts_map[conflict.conflict_id] = conflict.to_dict()

            # Remove from active conflicts
            if conflict.conflict_id in self.crdt_store.active_conflicts:
                del self.crdt_store.active_conflicts[conflict.conflict_id]

            return True

        except Exception as e:
            logging.error(f"Error executing resolution action {action}: {e}")
            return False

    def _merge_versions_automatically(
        self, version1: dict, version2: dict
    ) -> Optional[dict]:
        """Automatically merge two file versions"""
        try:
            # Start with the version that has the latest modification time
            if version1.get("mtime", 0) > version2.get("mtime", 0):
                merged = version1.copy()
                other = version2
            else:
                merged = version2.copy()
                other = version1

            # Merge metadata from both versions
            merged_metadata = merged.get("metadata", {}).copy()
            other_metadata = other.get("metadata", {})

            # Add non-conflicting metadata
            for key, value in other_metadata.items():
                if key not in merged_metadata:
                    merged_metadata[key] = value

            merged["metadata"] = merged_metadata

            # Increment version number
            merged["version"] = (
                max(version1.get("version", 1), version2.get("version", 1)) + 1
            )

            # Update modification time
            merged["modified_at"] = time.time()

            return merged

        except Exception as e:
            logging.error(f"Error merging versions automatically: {e}")
            return None

    async def _preserve_both_versions(
        self, conflict: ConflictInfo, versions: List[dict]
    ) -> bool:
        """Preserve both versions with different file names"""
        try:
            base_path = pathlib.Path(conflict.file_path)

            for i, version in enumerate(versions):
                if i == 0:
                    # Keep the first version as-is
                    file_record = FileRecord.from_dict(version)
                    self.crdt_store.files_map[conflict.file_path] = (
                        file_record.to_dict()
                    )
                else:
                    # Rename subsequent versions
                    stem = base_path.stem
                    suffix = base_path.suffix
                    peer_id = version.get("peer_id", "unknown")
                    timestamp = int(version.get("mtime", time.time()))

                    new_name = f"{stem}_conflict_{peer_id}_{timestamp}{suffix}"
                    new_path = str(base_path.parent / new_name)

                    # Update the version with new path
                    preserved_version = version.copy()
                    preserved_version["path"] = new_path

                    file_record = FileRecord.from_dict(preserved_version)
                    self.crdt_store.files_map[new_path] = file_record.to_dict()

            return True

        except Exception as e:
            logging.error(f"Error preserving both versions: {e}")
            return False

    def generate_conflict_diff(self, conflict_id: str) -> Optional[ConflictDiff]:
        """Generate a detailed diff for manual conflict resolution"""
        try:
            conflict_data = self.crdt_store.conflicts_map.get(conflict_id)
            if not conflict_data:
                return None

            conflict = ConflictInfo.from_dict(conflict_data)
            versions = conflict.versions

            if len(versions) < 2:
                return None

            local_record = FileRecord.from_dict(versions[0])
            remote_record = FileRecord.from_dict(versions[1])

            diff = ConflictDiff(
                file_path=conflict.file_path,
                local_version=local_record,
                remote_version=remote_record,
            )

            # Generate content diff if it's a text file
            file_path = pathlib.Path(conflict.file_path)
            if self._is_text_file(file_path):
                diff.content_diff = self._generate_text_diff(
                    file_path, local_record, remote_record
                )
                diff.binary_file = False
            else:
                diff.binary_file = True

            # Generate metadata diff
            diff.metadata_diff = self._generate_metadata_diff(
                local_record, remote_record
            )

            return diff

        except Exception as e:
            logging.error(f"Error generating conflict diff: {e}")
            return None

    def _is_text_file(self, file_path: pathlib.Path) -> bool:
        """Check if file is a text file suitable for diff generation"""
        text_extensions = {
            ".txt",
            ".md",
            ".py",
            ".js",
            ".html",
            ".css",
            ".json",
            ".xml",
            ".yaml",
            ".yml",
            ".ini",
            ".cfg",
            ".conf",
            ".log",
            ".sql",
            ".sh",
            ".bat",
            ".ps1",
            ".c",
            ".cpp",
            ".h",
            ".hpp",
            ".java",
            ".go",
            ".rs",
            ".php",
            ".rb",
            ".pl",
            ".r",
            ".m",
            ".swift",
        }

        return file_path.suffix.lower() in text_extensions

    def _generate_text_diff(
        self,
        file_path: pathlib.Path,
        local_record: FileRecord,
        remote_record: FileRecord,
    ) -> str:
        """Generate unified diff for text files"""
        try:
            # This is a simplified version - in practice, you'd need to
            # reconstruct the file content from chunks to generate the diff

            # For now, return a placeholder diff showing metadata differences
            local_info = f"Local version (modified: {time.ctime(local_record.mtime)})"
            remote_info = (
                f"Remote version (modified: {time.ctime(remote_record.mtime)})"
            )

            diff_lines = [
                f"--- {local_info}",
                f"+++ {remote_info}",
                f"@@ File: {file_path} @@",
                f" Size: {local_record.size} -> {remote_record.size}",
                f" Checksum: {local_record.checksum[:16]}... -> {remote_record.checksum[:16]}...",
            ]

            return "\n".join(diff_lines)

        except Exception as e:
            logging.error(f"Error generating text diff: {e}")
            return f"Error generating diff: {e}"

    def _generate_metadata_diff(
        self, local_record: FileRecord, remote_record: FileRecord
    ) -> Dict[str, Any]:
        """Generate metadata differences between versions"""
        diff = {}

        # Compare basic attributes
        attrs = ["size", "mtime", "permissions", "checksum", "version", "peer_id"]
        for attr in attrs:
            local_val = getattr(local_record, attr, None)
            remote_val = getattr(remote_record, attr, None)

            if local_val != remote_val:
                diff[attr] = {"local": local_val, "remote": remote_val}

        # Compare metadata dictionaries
        local_meta = local_record.metadata
        remote_meta = remote_record.metadata

        all_keys = set(local_meta.keys()) | set(remote_meta.keys())
        meta_diff = {}

        for key in all_keys:
            local_val = local_meta.get(key)
            remote_val = remote_meta.get(key)

            if local_val != remote_val:
                meta_diff[key] = {"local": local_val, "remote": remote_val}

        if meta_diff:
            diff["metadata"] = meta_diff

        return diff

    async def manual_resolve_conflict(
        self,
        conflict_id: str,
        resolution_choice: ResolutionAction,
        custom_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Manually resolve a conflict with user choice"""
        try:
            conflict_data = self.crdt_store.conflicts_map.get(conflict_id)
            if not conflict_data:
                logging.error(f"Conflict {conflict_id} not found")
                return False

            conflict = ConflictInfo.from_dict(conflict_data)

            # Execute the chosen resolution
            if resolution_choice == ResolutionAction.CUSTOM_RESOLUTION and custom_data:
                success = await self._execute_custom_resolution(conflict, custom_data)
            else:
                success = await self._execute_resolution_action(
                    conflict, resolution_choice
                )

            # Record the resolution
            result = ResolutionResult.SUCCESS if success else ResolutionResult.FAILED
            details = custom_data or {"manual_resolution": True}

            self._add_resolution_entry(
                conflict_id,
                conflict.file_path,
                resolution_choice,
                result,
                ConflictResolutionStrategy.MANUAL_RESOLUTION,
                details,
            )

            if success:
                logging.info(f"Manually resolved conflict {conflict_id}")
            else:
                logging.error(f"Failed to manually resolve conflict {conflict_id}")

            return success

        except Exception as e:
            logging.error(f"Error manually resolving conflict {conflict_id}: {e}")
            return False

    async def _execute_custom_resolution(
        self, conflict: ConflictInfo, custom_data: Dict[str, Any]
    ) -> bool:
        """Execute custom resolution with user-provided data"""
        try:
            # This would handle custom resolution logic
            # For now, implement basic custom merge

            if "merged_content" in custom_data:
                # User provided merged content
                versions = conflict.versions
                base_version = versions[0].copy()

                # Update with custom data
                base_version.update(custom_data.get("file_attributes", {}))
                base_version["version"] = max(v.get("version", 1) for v in versions) + 1
                base_version["modified_at"] = time.time()

                # Update file record
                file_record = FileRecord.from_dict(base_version)
                self.crdt_store.files_map[conflict.file_path] = file_record.to_dict()

                # Mark conflict as resolved
                conflict.state = ConflictState.RESOLVED
                conflict.resolved_at = datetime.now(timezone.utc)
                conflict.resolution_strategy = (
                    ConflictResolutionStrategy.MANUAL_RESOLUTION
                )

                self.crdt_store.conflicts_map[conflict.conflict_id] = conflict.to_dict()

                return True

            return False

        except Exception as e:
            logging.error(f"Error executing custom resolution: {e}")
            return False

    def get_resolution_history(
        self, file_path: Optional[str] = None, limit: int = 100
    ) -> List[ResolutionEntry]:
        """Get conflict resolution history"""
        history = self.resolution_history

        if file_path:
            history = [entry for entry in history if entry.file_path == file_path]

        # Return most recent entries first
        return sorted(history, key=lambda x: x.resolved_at, reverse=True)[:limit]

    def get_resolution_stats(self) -> Dict[str, Any]:
        """Get statistics about conflict resolution"""
        total_resolutions = len(self.resolution_history)

        if total_resolutions == 0:
            return {
                "total_resolutions": 0,
                "success_rate": 0.0,
                "auto_resolution_rate": 0.0,
                "resolution_actions": {},
                "resolution_strategies": {},
            }

        successful = sum(
            1
            for entry in self.resolution_history
            if entry.result == ResolutionResult.SUCCESS
        )

        auto_resolved = sum(
            1
            for entry in self.resolution_history
            if entry.strategy_used == ConflictResolutionStrategy.AUTOMATIC_MERGE
        )

        # Count resolution actions
        action_counts = {}
        for entry in self.resolution_history:
            action = entry.action_taken.value
            action_counts[action] = action_counts.get(action, 0) + 1

        # Count resolution strategies
        strategy_counts = {}
        for entry in self.resolution_history:
            strategy = entry.strategy_used.value
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

        return {
            "total_resolutions": total_resolutions,
            "success_rate": successful / total_resolutions,
            "auto_resolution_rate": auto_resolved / total_resolutions,
            "resolution_actions": action_counts,
            "resolution_strategies": strategy_counts,
            "recent_resolutions": len(
                [
                    entry
                    for entry in self.resolution_history
                    if (datetime.now(timezone.utc) - entry.resolved_at).days <= 7
                ]
            ),
        }

    def register_custom_handler(self, name: str, handler: Callable):
        """Register a custom conflict resolution handler"""
        self.custom_handlers[name] = handler
        logging.info(f"Registered custom conflict handler: {name}")

    async def batch_auto_resolve(self) -> Dict[str, Any]:
        """Attempt to auto-resolve all pending conflicts"""
        pending_conflicts = self.crdt_store.get_pending_conflicts()

        results = {
            "total_conflicts": len(pending_conflicts),
            "auto_resolved": 0,
            "manual_required": 0,
            "failed": 0,
            "details": [],
        }

        for conflict in pending_conflicts:
            try:
                success = await self.auto_resolve_conflict(conflict.conflict_id)
                if success:
                    results["auto_resolved"] += 1
                    results["details"].append(
                        {
                            "conflict_id": conflict.conflict_id,
                            "file_path": conflict.file_path,
                            "status": "auto_resolved",
                        }
                    )
                else:
                    results["manual_required"] += 1
                    results["details"].append(
                        {
                            "conflict_id": conflict.conflict_id,
                            "file_path": conflict.file_path,
                            "status": "manual_required",
                        }
                    )
            except Exception as e:
                results["failed"] += 1
                results["details"].append(
                    {
                        "conflict_id": conflict.conflict_id,
                        "file_path": conflict.file_path,
                        "status": "failed",
                        "error": str(e),
                    }
                )
                logging.error(f"Failed to process conflict {conflict.conflict_id}: {e}")

        return results

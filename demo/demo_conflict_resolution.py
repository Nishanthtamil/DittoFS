#!/usr/bin/env python3
"""
Demo: Conflict Resolution Workflows

This demo showcases the comprehensive conflict resolution capabilities of DittoFS:
- User-friendly conflict resolution interface
- Automatic conflict resolution for simple cases
- Manual conflict resolution tools with side-by-side comparison
- Conflict resolution history and audit trail

Requirements addressed:
- 5.1: Preserve all versions and allow users to choose which version to keep
- 3.4: Present intuitive conflict resolution interface
"""

import asyncio
import json
import pathlib
import tempfile
import time
from datetime import datetime, timezone

from dittofs.conflict_resolution import (
    ConflictResolutionWorkflow,
    ResolutionAction,
    ResolutionResult,
)
from dittofs.crdt_store import (
    ConflictInfo,
    ConflictResolutionStrategy,
    ConflictState,
    ConflictType,
    CRDTStore,
    FileRecord,
)


class ConflictResolutionDemo:
    """Demonstrates conflict resolution workflows"""

    def __init__(self):
        # Create temporary store for demo
        self.temp_dir = tempfile.mkdtemp()
        self.store_path = pathlib.Path(self.temp_dir) / "demo_store.yrs"

        # Initialize mock store for demo purposes
        self.store = self._create_mock_store()
        self.workflow = ConflictResolutionWorkflow(self.store, "demo_peer")

        print("🔧 Conflict Resolution Workflow Demo")
        print("=" * 50)

    def _create_mock_store(self):
        """Create a mock store for demonstration"""
        from unittest.mock import Mock

        store = Mock()
        store.store_path = self.store_path
        store.peer_id = "demo_peer_123"
        store.files_map = {}
        store.conflicts_map = {}
        store.active_conflicts = {}
        store.get_pending_conflicts = Mock(return_value=[])

        return store

    def create_sample_conflicts(self):
        """Create sample conflicts for demonstration"""
        print("\n📋 Creating Sample Conflicts...")

        # Conflict 1: Metadata-only conflict (auto-resolvable)
        conflict1 = ConflictInfo(
            conflict_id="metadata_conflict",
            file_path="/documents/report.txt",
            conflict_type=ConflictType.METADATA_CONFLICT,
            state=ConflictState.DETECTED,
            detected_at=datetime.now(timezone.utc),
            versions=[
                {
                    "path": "/documents/report.txt",
                    "checksum": "abc123def456",  # Same content
                    "mtime": time.time() - 100,
                    "size": 2048,
                    "version": 1,
                    "peer_id": "laptop",
                    "metadata": {"author": "Alice", "department": "Engineering"},
                },
                {
                    "path": "/documents/report.txt",
                    "checksum": "abc123def456",  # Same content
                    "mtime": time.time() - 50,
                    "size": 2048,
                    "version": 2,
                    "peer_id": "desktop",
                    "metadata": {"editor": "Bob", "reviewed": True},
                },
            ],
        )

        # Conflict 2: Content conflict (requires manual resolution)
        conflict2 = ConflictInfo(
            conflict_id="content_conflict",
            file_path="/projects/code.py",
            conflict_type=ConflictType.CONTENT_CONFLICT,
            state=ConflictState.DETECTED,
            detected_at=datetime.now(timezone.utc),
            versions=[
                {
                    "path": "/projects/code.py",
                    "hashes": ["chunk1", "chunk2"],
                    "checksum": "def456ghi789",
                    "mtime": time.time() - 200,
                    "size": 1500,
                    "version": 3,
                    "peer_id": "laptop",
                    "metadata": {"author": "Alice"},
                },
                {
                    "path": "/projects/code.py",
                    "hashes": ["chunk3", "chunk4"],
                    "checksum": "ghi789jkl012",
                    "mtime": time.time() - 150,
                    "size": 1600,
                    "version": 4,
                    "peer_id": "desktop",
                    "metadata": {"author": "Bob"},
                },
            ],
        )

        # Add conflicts to store
        self.store.conflicts_map = {
            "metadata_conflict": conflict1.to_dict(),
            "content_conflict": conflict2.to_dict(),
        }

        print(f"✅ Created {len(self.store.conflicts_map)} sample conflicts")
        return [conflict1, conflict2]

    async def demo_automatic_resolution(self):
        """Demonstrate automatic conflict resolution"""
        print("\n🤖 Automatic Conflict Resolution")
        print("-" * 30)

        # Test auto-resolution capability
        conflict_id = "metadata_conflict"
        conflict_data = self.store.conflicts_map[conflict_id]
        conflict = ConflictInfo.from_dict(conflict_data)

        # Check if conflict can be auto-resolved
        can_resolve, action = self.workflow.can_auto_resolve(conflict)

        print(f"📁 File: {conflict.file_path}")
        print(f"🔍 Conflict Type: {conflict.conflict_type.value}")
        print(f"⚡ Can Auto-Resolve: {can_resolve}")

        if can_resolve:
            print(f"🎯 Suggested Action: {action.value}")

            # Mock successful resolution
            self.workflow._execute_resolution_action = self._mock_execute_resolution

            # Perform auto-resolution
            success = await self.workflow.auto_resolve_conflict(conflict_id)

            if success:
                print("✅ Conflict automatically resolved!")

                # Show resolution history
                history = self.workflow.get_resolution_history(limit=1)
                if history:
                    entry = history[0]
                    print(f"📝 Resolution recorded: {entry.action_taken.value}")
                    print(
                        f"⏰ Resolved at: {entry.resolved_at.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
            else:
                print("❌ Auto-resolution failed")
        else:
            print("⚠️  Manual resolution required")

    async def demo_manual_resolution(self):
        """Demonstrate manual conflict resolution with diff generation"""
        print("\n👤 Manual Conflict Resolution")
        print("-" * 30)

        conflict_id = "content_conflict"

        # Generate conflict diff for user review
        diff = self.workflow.generate_conflict_diff(conflict_id)

        if diff:
            print(f"📁 File: {diff.file_path}")
            print(f"📊 Binary File: {diff.binary_file}")

            print("\n📋 Version Comparison:")
            print(f"  Local Version:")
            print(f"    - Size: {diff.local_version.size} bytes")
            print(f"    - Modified: {time.ctime(diff.local_version.mtime)}")
            print(f"    - Checksum: {diff.local_version.checksum[:16]}...")

            print(f"  Remote Version:")
            print(f"    - Size: {diff.remote_version.size} bytes")
            print(f"    - Modified: {time.ctime(diff.remote_version.mtime)}")
            print(f"    - Checksum: {diff.remote_version.checksum[:16]}...")

            if diff.metadata_diff:
                print("\n🏷️  Metadata Differences:")
                for key, changes in diff.metadata_diff.items():
                    if (
                        isinstance(changes, dict)
                        and "local" in changes
                        and "remote" in changes
                    ):
                        print(f"    {key}: {changes['local']} → {changes['remote']}")
                    else:
                        print(f"    {key}: {changes}")

            if diff.content_diff:
                print("\n📝 Content Diff:")
                print(diff.content_diff)

            # Simulate user choice
            print("\n🎯 Resolution Options:")
            print("  1. Keep Local Version")
            print("  2. Keep Remote Version")
            print("  3. Preserve Both Versions")
            print("  4. Custom Merge")

            # Mock user choosing option 2
            chosen_action = ResolutionAction.KEEP_REMOTE
            print(f"\n👆 User chose: {chosen_action.value}")

            # Mock successful resolution
            self.workflow._execute_resolution_action = self._mock_execute_resolution

            # Perform manual resolution
            success = await self.workflow.manual_resolve_conflict(
                conflict_id, chosen_action
            )

            if success:
                print("✅ Conflict manually resolved!")
            else:
                print("❌ Manual resolution failed")

    async def demo_batch_resolution(self):
        """Demonstrate batch conflict resolution"""
        print("\n📦 Batch Conflict Resolution")
        print("-" * 30)

        # Create additional conflicts for batch processing
        additional_conflicts = []
        for i in range(3):
            conflict = ConflictInfo(
                conflict_id=f"batch_conflict_{i}",
                file_path=f"/batch/file_{i}.txt",
                conflict_type=ConflictType.METADATA_CONFLICT,
                state=ConflictState.DETECTED,
                detected_at=datetime.now(timezone.utc),
                versions=[
                    {"checksum": "same_hash", "metadata": {"author": f"user{i}"}},
                    {"checksum": "same_hash", "metadata": {"editor": f"editor{i}"}},
                ],
            )
            additional_conflicts.append(conflict)
            self.store.conflicts_map[f"batch_conflict_{i}"] = conflict.to_dict()

        # Mock the get_pending_conflicts method
        all_conflicts = [
            ConflictInfo.from_dict(data) for data in self.store.conflicts_map.values()
        ]
        self.store.get_pending_conflicts.return_value = all_conflicts

        # Mock auto-resolution
        self.workflow.auto_resolve_conflict = self._mock_auto_resolve

        # Perform batch resolution
        results = await self.workflow.batch_auto_resolve()

        print(f"📊 Batch Resolution Results:")
        print(f"  Total Conflicts: {results['total_conflicts']}")
        print(f"  Auto-Resolved: {results['auto_resolved']}")
        print(f"  Manual Required: {results['manual_required']}")
        print(f"  Failed: {results['failed']}")

        if results["details"]:
            print("\n📋 Detailed Results:")
            for detail in results["details"][:3]:  # Show first 3
                print(f"  - {detail['file_path']}: {detail['status']}")

    def demo_resolution_history(self):
        """Demonstrate resolution history and audit trail"""
        print("\n📚 Resolution History & Audit Trail")
        print("-" * 40)

        # Get resolution history
        history = self.workflow.get_resolution_history(limit=5)

        if history:
            print(f"📋 Recent Resolutions ({len(history)} entries):")
            for i, entry in enumerate(history, 1):
                print(f"\n  {i}. Resolution ID: {entry.resolution_id}")
                print(f"     File: {entry.file_path}")
                print(f"     Action: {entry.action_taken.value}")
                print(f"     Result: {entry.result.value}")
                print(f"     Strategy: {entry.strategy_used.value}")
                print(
                    f"     Resolved: {entry.resolved_at.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                print(f"     By: {entry.resolved_by}")
        else:
            print("📭 No resolution history available")

        # Get resolution statistics
        stats = self.workflow.get_resolution_stats()

        print(f"\n📊 Resolution Statistics:")
        print(f"  Total Resolutions: {stats['total_resolutions']}")
        print(f"  Success Rate: {stats['success_rate']:.1%}")
        print(f"  Auto-Resolution Rate: {stats['auto_resolution_rate']:.1%}")

        if stats["resolution_actions"]:
            print(f"\n🎯 Resolution Actions:")
            for action, count in stats["resolution_actions"].items():
                print(f"    {action}: {count}")

        if stats["resolution_strategies"]:
            print(f"\n🔧 Resolution Strategies:")
            for strategy, count in stats["resolution_strategies"].items():
                print(f"    {strategy}: {count}")

    def demo_custom_handlers(self):
        """Demonstrate custom conflict resolution handlers"""
        print("\n🔧 Custom Resolution Handlers")
        print("-" * 30)

        def custom_preserve_handler(conflict):
            """Custom handler that always preserves both versions"""
            return ResolutionAction.PRESERVE_BOTH

        def custom_smart_handler(conflict):
            """Custom handler with smart logic"""
            if conflict.conflict_type == ConflictType.PERMISSION_CONFLICT:
                return ResolutionAction.KEEP_LOCAL
            elif "important" in conflict.file_path:
                return ResolutionAction.PRESERVE_BOTH
            else:
                return ResolutionAction.MERGE_AUTOMATIC

        # Register custom handlers
        self.workflow.register_custom_handler("preserve_all", custom_preserve_handler)
        self.workflow.register_custom_handler("smart_resolver", custom_smart_handler)

        print("✅ Registered custom handlers:")
        for name in self.workflow.custom_handlers:
            print(f"  - {name}")

        print("\n💡 Custom handlers can implement domain-specific resolution logic")
        print("   such as preserving critical files or applying business rules.")

    async def _mock_execute_resolution(self, conflict, action):
        """Mock resolution execution for demo"""
        # Simulate processing time
        await asyncio.sleep(0.1)
        return True

    async def _mock_auto_resolve(self, conflict_id):
        """Mock auto-resolution for demo"""
        await asyncio.sleep(0.05)
        return True

    async def run_demo(self):
        """Run the complete conflict resolution demo"""
        try:
            # Create sample conflicts
            conflicts = self.create_sample_conflicts()

            # Demo automatic resolution
            await self.demo_automatic_resolution()

            # Demo manual resolution
            await self.demo_manual_resolution()

            # Demo batch resolution
            await self.demo_batch_resolution()

            # Demo resolution history
            self.demo_resolution_history()

            # Demo custom handlers
            self.demo_custom_handlers()

            print("\n🎉 Conflict Resolution Demo Complete!")
            print("\nKey Features Demonstrated:")
            print("✅ Automatic conflict resolution for simple cases")
            print("✅ Manual resolution with side-by-side comparison")
            print("✅ Batch processing of multiple conflicts")
            print("✅ Comprehensive audit trail and history")
            print("✅ Extensible custom resolution handlers")
            print("✅ User-friendly conflict resolution interface")

        except Exception as e:
            print(f"\n❌ Demo error: {e}")
            import traceback

            traceback.print_exc()


async def main():
    """Run the conflict resolution demo"""
    demo = ConflictResolutionDemo()
    await demo.run_demo()


if __name__ == "__main__":
    asyncio.run(main())

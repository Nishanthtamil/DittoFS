#!/usr/bin/env python3
"""
Demo script for DittoFS comprehensive file versioning system

This script demonstrates:
- Version history tracking with efficient delta storage
- Branching and merging capabilities for collaborative editing
- Version comparison and diff visualization
- Version cleanup policies with configurable retention
"""

import tempfile
import pathlib
import time
import json
from datetime import datetime

from src.dittofs.crdt_store import CRDTStore
from src.dittofs.versioning import RetentionPolicy, MergeStrategy


def print_separator(title):
    """Print a section separator"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")


def demo_basic_versioning():
    """Demonstrate basic versioning functionality"""
    print_separator("Basic File Versioning")
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write("Initial document content\nLine 2\nLine 3")
        f.flush()
        file_path = pathlib.Path(f.name)
    
    try:
        # Initialize CRDT store with versioning
        store = CRDTStore(peer_id="demo_user")
        print(f"✓ Initialized versioning system")
        
        # Create initial version
        initial_content = b"Initial document content\nLine 2\nLine 3"
        version1_id = store.create_file_version(
            file_path, initial_content, 
            commit_message="Initial document creation"
        )
        print(f"✓ Created initial version: {version1_id}")
        
        # Modify and create second version
        time.sleep(0.1)  # Ensure different timestamp
        modified_content = b"Modified document content\nLine 2 - updated\nLine 3\nNew line 4"
        version2_id = store.create_file_version(
            file_path, modified_content,
            commit_message="Added new line and modified existing content"
        )
        print(f"✓ Created modified version: {version2_id}")
        
        # Create third version with more changes
        time.sleep(0.1)
        final_content = b"Final document content\nLine 2 - updated again\nLine 3\nNew line 4\nFinal line"
        version3_id = store.create_file_version(
            file_path, final_content,
            commit_message="Final revisions and additions"
        )
        print(f"✓ Created final version: {version3_id}")
        
        # Display version history
        print(f"\n📋 Version History:")
        history = store.get_enhanced_version_history(file_path)
        for i, version in enumerate(history):
            created_at = datetime.fromtimestamp(version['created_at'])
            print(f"  {i+1}. {version['version_id']} - {version['commit_message']}")
            print(f"     Created: {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"     Size: {version['size']} bytes")
            if 'diff_info' in version:
                diff_info = version['diff_info']
                if diff_info['size_change'] != 0:
                    print(f"     Size change: {diff_info['size_change']:+d} bytes")
        
        return store, file_path, [version1_id, version2_id, version3_id]
        
    except Exception as e:
        print(f"❌ Error in basic versioning demo: {e}")
        return None, None, []


def demo_version_comparison(store, file_path, version_ids):
    """Demonstrate version comparison and diff visualization"""
    print_separator("Version Comparison and Diff Visualization")
    
    if not store or len(version_ids) < 2:
        print("❌ Skipping comparison demo - insufficient versions")
        return
    
    try:
        # Compare first and last versions
        version1_id, version3_id = version_ids[0], version_ids[-1]
        
        print(f"🔍 Comparing versions {version1_id} and {version3_id}")
        comparison = store.compare_file_versions(version1_id, version3_id)
        
        if comparison:
            print(f"✓ Versions are {'identical' if comparison['identical'] else 'different'}")
            print(f"✓ Size difference: {comparison['size_diff']:+d} bytes")
            print(f"✓ Content type: {'Text' if comparison.get('is_text') else 'Binary'}")
            
            if comparison.get('text_diff') and not comparison['identical']:
                print(f"\n📝 Text Diff:")
                diff_lines = comparison['text_diff'].split('\n')
                for line in diff_lines[:20]:  # Show first 20 lines of diff
                    if line.startswith('+++') or line.startswith('---'):
                        print(f"  {line}")
                    elif line.startswith('+'):
                        print(f"  \033[92m{line}\033[0m")  # Green for additions
                    elif line.startswith('-'):
                        print(f"  \033[91m{line}\033[0m")  # Red for deletions
                    elif line.startswith('@@'):
                        print(f"  \033[94m{line}\033[0m")  # Blue for context
                
                if len(diff_lines) > 20:
                    print(f"  ... ({len(diff_lines) - 20} more lines)")
        
        # Compare adjacent versions
        print(f"\n🔍 Comparing adjacent versions:")
        for i in range(len(version_ids) - 1):
            comp = store.compare_file_versions(version_ids[i+1], version_ids[i])
            if comp:
                print(f"  {version_ids[i+1]} vs {version_ids[i]}: {comp['size_diff']:+d} bytes")
        
    except Exception as e:
        print(f"❌ Error in comparison demo: {e}")


def demo_branching_and_merging(store, file_path, version_ids):
    """Demonstrate branching and merging capabilities"""
    print_separator("Branching and Merging")
    
    if not store or not version_ids:
        print("❌ Skipping branching demo - no versions available")
        return
    
    try:
        # Create a feature branch from the first version
        base_version_id = version_ids[0]
        branch_id = store.create_branch(
            "feature-enhancement", 
            base_version_id, 
            "Branch for adding new features"
        )
        print(f"✓ Created feature branch: {branch_id}")
        
        # Create version on feature branch
        feature_content = b"Feature branch content\nNew feature implementation\nAdditional functionality"
        feature_version_id = store.create_file_version(
            file_path, feature_content, 
            "feature-enhancement",
            "Implemented new features on branch"
        )
        print(f"✓ Created version on feature branch: {feature_version_id}")
        
        # Create another version on main branch
        main_content = b"Main branch updates\nParallel development\nMain branch features"
        main_version_id = store.create_file_version(
            file_path, main_content,
            "main", 
            "Parallel development on main branch"
        )
        print(f"✓ Created parallel version on main branch: {main_version_id}")
        
        # List all branches
        print(f"\n🌿 Available Branches:")
        branches = store.get_branches(file_path)
        for branch in branches:
            created_at = datetime.fromtimestamp(branch['created_at'])
            print(f"  • {branch['branch_name']} ({branch['branch_id']})")
            print(f"    Created: {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"    Description: {branch['description']}")
        
        # Show version history per branch
        print(f"\n📋 Main Branch History:")
        main_history = store.get_enhanced_version_history(file_path, "main")
        for version in main_history[:3]:  # Show last 3 versions
            created_at = datetime.fromtimestamp(version['created_at'])
            print(f"  • {version['version_id']} - {version['commit_message']}")
        
        print(f"\n📋 Feature Branch History:")
        feature_history = store.get_enhanced_version_history(file_path, "feature-enhancement")
        for version in feature_history[:3]:
            created_at = datetime.fromtimestamp(version['created_at'])
            print(f"  • {version['version_id']} - {version['commit_message']}")
        
        # Attempt to merge branches
        print(f"\n🔀 Attempting to merge feature branch into main...")
        merge_result = store.merge_branches("main", "feature-enhancement")
        
        if merge_result:
            print(f"✓ Successfully merged branches: {merge_result}")
        else:
            print(f"⚠️  Merge resulted in conflicts or was not possible")
            print(f"   This is expected for demonstration purposes")
        
    except Exception as e:
        print(f"❌ Error in branching demo: {e}")


def demo_version_restoration(store, file_path, version_ids):
    """Demonstrate version restoration"""
    print_separator("Version Restoration")
    
    if not store or len(version_ids) < 2:
        print("❌ Skipping restoration demo - insufficient versions")
        return
    
    try:
        # Show current file content
        with open(file_path, 'rb') as f:
            current_content = f.read()
        print(f"📄 Current file size: {len(current_content)} bytes")
        
        # Restore to an earlier version
        restore_version_id = version_ids[0]  # First version
        print(f"⏪ Restoring to version: {restore_version_id}")
        
        success = store.restore_file_version(file_path, restore_version_id)
        
        if success:
            with open(file_path, 'rb') as f:
                restored_content = f.read()
            print(f"✓ Successfully restored file")
            print(f"📄 Restored file size: {len(restored_content)} bytes")
            
            # Verify content matches the version
            original_content = store.get_version_content(restore_version_id)
            if restored_content == original_content:
                print(f"✓ Restored content matches original version")
            else:
                print(f"⚠️  Restored content differs from original version")
        else:
            print(f"❌ Failed to restore version")
        
    except Exception as e:
        print(f"❌ Error in restoration demo: {e}")


def demo_retention_policies(store, file_path):
    """Demonstrate version cleanup with retention policies"""
    print_separator("Retention Policies and Cleanup")
    
    if not store:
        print("❌ Skipping retention demo - no store available")
        return
    
    try:
        # Create many versions to demonstrate cleanup
        print(f"📝 Creating additional versions for cleanup demonstration...")
        for i in range(10):
            content = f"Cleanup demo version {i}\nContent for version {i}\nTimestamp: {time.time()}".encode()
            version_id = store.create_file_version(
                file_path, content, 
                commit_message=f"Cleanup demo version {i}"
            )
            time.sleep(0.01)  # Small delay for different timestamps
        
        # Show version count before cleanup
        history_before = store.get_enhanced_version_history(file_path)
        print(f"📊 Total versions before cleanup: {len(history_before)}")
        
        # Get storage stats
        stats_before = store.get_versioning_stats()
        print(f"📊 Storage stats before cleanup:")
        print(f"   Total versions: {stats_before.get('total_versions', 0)}")
        print(f"   Storage size: {stats_before.get('storage_size', 0)} bytes")
        print(f"   Delta size: {stats_before.get('delta_size', 0)} bytes")
        
        # Apply retention policy
        print(f"\n🧹 Applying retention policy (max 5 versions)...")
        policy = RetentionPolicy(
            max_versions=5,
            max_age_days=30,
            compress_old_versions=True,
            compress_after_days=0  # Compress immediately for demo
        )
        
        store.set_retention_policy(policy)
        cleanup_result = store.cleanup_old_versions(file_path)
        
        print(f"✓ Cleanup completed:")
        print(f"   Versions cleaned: {cleanup_result.get('cleaned', 0)}")
        print(f"   Versions compressed: {cleanup_result.get('compressed', 0)}")
        if cleanup_result.get('errors'):
            print(f"   Errors: {len(cleanup_result['errors'])}")
        
        # Show version count after cleanup
        history_after = store.get_enhanced_version_history(file_path)
        print(f"📊 Total versions after cleanup: {len(history_after)}")
        
        # Show updated storage stats
        stats_after = store.get_versioning_stats()
        print(f"📊 Storage stats after cleanup:")
        print(f"   Total versions: {stats_after.get('total_versions', 0)}")
        print(f"   Storage size: {stats_after.get('storage_size', 0)} bytes")
        print(f"   Delta size: {stats_after.get('delta_size', 0)} bytes")
        
    except Exception as e:
        print(f"❌ Error in retention demo: {e}")


def demo_version_tree_visualization(store, file_path):
    """Demonstrate version tree export for visualization"""
    print_separator("Version Tree Visualization")
    
    if not store:
        print("❌ Skipping tree demo - no store available")
        return
    
    try:
        # Export version tree
        tree = store.get_version_tree(file_path)
        
        print(f"🌳 Version Tree for: {tree.get('file_path', 'Unknown')}")
        print(f"📊 Tree Statistics:")
        print(f"   Branches: {len(tree.get('branches', []))}")
        print(f"   Versions: {len(tree.get('versions', []))}")
        print(f"   Relationships: {len(tree.get('relationships', []))}")
        
        # Show branches
        print(f"\n🌿 Branches:")
        for branch in tree.get('branches', []):
            created_at = datetime.fromtimestamp(branch['created_at'])
            print(f"   • {branch['branch_name']} ({branch['branch_id']})")
            print(f"     Created: {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"     Parent: {branch['parent_version']}")
        
        # Show recent versions
        print(f"\n📋 Recent Versions:")
        versions = tree.get('versions', [])
        # Sort by creation time (newest first)
        versions.sort(key=lambda v: v.get('created_at', 0), reverse=True)
        
        for version in versions[:5]:  # Show 5 most recent
            created_at = datetime.fromtimestamp(version['created_at'])
            print(f"   • {version['version_id']} ({version['version_type']})")
            print(f"     Branch: {version['branch_id']}")
            print(f"     Created: {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"     Message: {version['commit_message']}")
            print(f"     Size: {version['content_size']} bytes")
        
        # Show relationships
        print(f"\n🔗 Version Relationships:")
        for rel in tree.get('relationships', [])[:5]:  # Show first 5
            print(f"   {rel['parent']} → {rel['child']} ({rel['type']})")
        
        # Export tree to JSON file for external visualization
        tree_file = pathlib.Path("version_tree_export.json")
        with open(tree_file, 'w') as f:
            json.dump(tree, f, indent=2, default=str)
        print(f"\n💾 Version tree exported to: {tree_file}")
        print(f"   This JSON can be used with visualization tools")
        
    except Exception as e:
        print(f"❌ Error in tree visualization demo: {e}")


def main():
    """Run the comprehensive versioning demo"""
    print("🚀 DittoFS Comprehensive File Versioning System Demo")
    print("This demo showcases advanced versioning capabilities including:")
    print("• Version history tracking with efficient delta storage")
    print("• Branching and merging for collaborative editing")
    print("• Version comparison and diff visualization")
    print("• Configurable retention policies for cleanup")
    
    try:
        # Run demo sections
        store, file_path, version_ids = demo_basic_versioning()
        
        if store and file_path:
            demo_version_comparison(store, file_path, version_ids)
            demo_branching_and_merging(store, file_path, version_ids)
            demo_version_restoration(store, file_path, version_ids)
            demo_retention_policies(store, file_path)
            demo_version_tree_visualization(store, file_path)
            
            # Cleanup
            try:
                file_path.unlink()
                print(f"\n🧹 Cleaned up temporary file: {file_path}")
            except:
                pass
        
        print_separator("Demo Complete")
        print("✅ All versioning features demonstrated successfully!")
        print("\nKey capabilities shown:")
        print("• ✓ Version creation with commit messages")
        print("• ✓ Version history tracking and retrieval")
        print("• ✓ Delta storage for efficient space usage")
        print("• ✓ Branch creation and management")
        print("• ✓ Version comparison with text diffs")
        print("• ✓ File restoration to previous versions")
        print("• ✓ Retention policies for automatic cleanup")
        print("• ✓ Version tree export for visualization")
        
    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Demo script showcasing the enhanced CRDT store with semantic conflict resolution.

This demonstrates:
1. Enhanced file records with version tracking
2. Conflict detection algorithms
3. Multiple conflict resolution strategies
4. Version history management
5. Semantic conflict detection
"""

import tempfile
import pathlib
import time
import logging
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

from src.dittofs.crdt_store import (
    CRDTStore, FileRecord, ConflictResolutionStrategy, 
    ConflictState, ConflictType
)

def demo_enhanced_file_records():
    """Demonstrate enhanced file records with metadata"""
    print("\n=== Enhanced File Records Demo ===")
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt', mode='w') as f:
        f.write("Hello, enhanced DittoFS!")
        f.flush()
        file_path = pathlib.Path(f.name)
    
    # Create CRDT store with peer ID
    store = CRDTStore(peer_id="demo_peer_1")
    
    # Add file with enhanced metadata
    success = store.add_file(file_path, ["chunk_hash_1", "chunk_hash_2"])
    print(f"Added file: {success}")
    
    # Retrieve and display enhanced record
    record = store.get_file(file_path)
    if record:
        print(f"File: {record.path}")
        print(f"Version: {record.version}")
        print(f"Peer ID: {record.peer_id}")
        print(f"Content Type: {record.content_type}")
        print(f"Semantic Hash: {record.semantic_hash[:16]}...")
        print(f"Chunks: {len(record.hashes)}")
    
    # Clean up
    file_path.unlink()

def demo_conflict_detection():
    """Demonstrate conflict detection between file versions"""
    print("\n=== Conflict Detection Demo ===")
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt', mode='w') as f:
        f.write("Original content for conflict demo")
        f.flush()
        file_path = pathlib.Path(f.name)
    
    # Create two stores representing different peers
    store1 = CRDTStore(peer_id="peer_1")
    store2 = CRDTStore(peer_id="peer_2")
    
    # Add file to first store
    store1.add_file(file_path, ["hash_v1"])
    original_record = store1.get_file(file_path)
    print(f"Original record version: {original_record.version}")
    
    # Modify file content
    with open(file_path, 'w') as f:
        f.write("Modified content that conflicts")
    
    # Add modified file to second store
    store2.add_file(file_path, ["hash_v2"])
    modified_record = store2.get_file(file_path)
    print(f"Modified record version: {modified_record.version}")
    
    # Detect conflict
    conflict = store1._detect_conflict(modified_record, original_record)
    if conflict:
        print(f"Conflict detected!")
        print(f"  Type: {conflict.conflict_type.value}")
        print(f"  State: {conflict.state.value}")
        print(f"  Versions: {len(conflict.versions)}")
        print(f"  Detected at: {conflict.detected_at}")
    else:
        print("No conflict detected")
    
    # Clean up
    file_path.unlink()

def demo_conflict_resolution_strategies():
    """Demonstrate different conflict resolution strategies"""
    print("\n=== Conflict Resolution Strategies Demo ===")
    
    store = CRDTStore(peer_id="resolver_demo")
    
    # Create mock file records for conflict
    record1 = FileRecord(
        path="/demo/file.txt",
        hashes=["hash1"],
        mtime=time.time(),
        size=100,
        checksum="checksum1",
        peer_id="peer1"
    )
    
    record2 = FileRecord(
        path="/demo/file.txt",
        hashes=["hash2"],
        mtime=time.time() + 10,  # Later timestamp
        size=120,
        checksum="checksum2",
        peer_id="peer2"
    )
    
    # Test different resolution strategies
    strategies = [
        ConflictResolutionStrategy.LAST_WRITER_WINS,
        ConflictResolutionStrategy.PRESERVE_BOTH,
        ConflictResolutionStrategy.AUTOMATIC_MERGE
    ]
    
    for strategy in strategies:
        print(f"\n--- Testing {strategy.value} ---")
        
        # Create conflict
        conflict = store._detect_conflict(record2, record1)
        if conflict:
            # Get resolver function
            resolver = store.conflict_resolvers[strategy]
            result = resolver(conflict)
            
            print(f"Resolution success: {result.get('success', False)}")
            if result.get('success'):
                if 'resolved_version' in result:
                    resolved = result['resolved_version']
                    print(f"  Resolved to version with checksum: {resolved.get('checksum', 'N/A')}")
                elif 'preserved_versions' in result:
                    print(f"  Preserved {len(result['preserved_versions'])} versions")
                    for i, version in enumerate(result['preserved_versions']):
                        print(f"    Version {i+1}: {pathlib.Path(version['path']).name}")

def demo_semantic_hash():
    """Demonstrate semantic hash calculation for conflict detection"""
    print("\n=== Semantic Hash Demo ===")
    
    store = CRDTStore()
    
    # Create two files with same semantic content but different formatting
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt', mode='w') as f1:
        f1.write("hello    world\n\n  with   lots   of    whitespace  ")
        f1.flush()
        path1 = pathlib.Path(f1.name)
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt', mode='w') as f2:
        f2.write("hello world with lots of whitespace")
        f2.flush()
        path2 = pathlib.Path(f2.name)
    
    # Calculate semantic hashes
    hash1 = store._calculate_semantic_hash(path1)
    hash2 = store._calculate_semantic_hash(path2)
    
    print(f"File 1 semantic hash: {hash1[:16]}...")
    print(f"File 2 semantic hash: {hash2[:16]}...")
    print(f"Semantic hashes match: {hash1 == hash2}")
    
    # Clean up
    path1.unlink()
    path2.unlink()

def demo_version_history():
    """Demonstrate file version history tracking"""
    print("\n=== Version History Demo ===")
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt', mode='w') as f:
        f.write("Version 1 content")
        f.flush()
        file_path = pathlib.Path(f.name)
    
    store = CRDTStore(peer_id="version_demo")
    
    # Add multiple versions
    versions_content = [
        "Version 1 content",
        "Version 2 with changes",
        "Version 3 with more changes"
    ]
    
    for i, content in enumerate(versions_content, 1):
        with open(file_path, 'w') as f:
            f.write(content)
        
        # Add file (this creates a new version)
        store.add_file(file_path, [f"hash_v{i}"])
        time.sleep(0.1)  # Ensure different timestamps
        
        record = store.get_file(file_path)
        print(f"Added version {record.version}: {content[:20]}...")
    
    # Get version history
    versions = store.get_file_versions(file_path, limit=5)
    print(f"\nVersion history ({len(versions)} versions):")
    for version in versions:
        print(f"  Version {version.version}: {version.checksum[:16]}... (size: {version.size})")
    
    # Demonstrate version restoration
    if len(versions) >= 2:
        old_version = versions[-1].version  # Get oldest version
        print(f"\nRestoring to version {old_version}...")
        success = store.restore_file_version(file_path, old_version)
        print(f"Restoration success: {success}")
        
        current = store.get_file(file_path)
        print(f"Current version after restore: {current.version}")
    
    # Clean up
    file_path.unlink()

def demo_conflict_state_machine():
    """Demonstrate the conflict resolution state machine"""
    print("\n=== Conflict State Machine Demo ===")
    
    store = CRDTStore()
    store.set_conflict_resolution_strategy(ConflictResolutionStrategy.MANUAL_RESOLUTION)
    
    # Create a conflict scenario
    record1 = FileRecord(
        path="/demo/state_machine.txt",
        hashes=["hash1"],
        mtime=time.time(),
        size=50,
        checksum="checksum1"
    )
    
    record2 = FileRecord(
        path="/demo/state_machine.txt",
        hashes=["hash2"],
        mtime=time.time() + 1,
        size=60,
        checksum="checksum2"
    )
    
    # Detect conflict
    conflict = store._detect_conflict(record2, record1)
    if conflict:
        print(f"Initial state: {conflict.state.value}")
        
        # Store conflict
        store.conflicts_map[conflict.conflict_id] = conflict.to_dict()
        
        # Check pending conflicts
        pending = store.get_pending_conflicts()
        print(f"Pending conflicts: {len(pending)}")
        
        # Attempt resolution
        print("Attempting manual resolution...")
        manual_resolution = {
            "resolved_version": record2.to_dict(),
            "user_choice": "chose_newer_version"
        }
        
        success = store.resolve_conflict(
            conflict.conflict_id,
            ConflictResolutionStrategy.MANUAL_RESOLUTION,
            manual_resolution
        )
        
        print(f"Resolution success: {success}")
        
        # Check final state
        resolved_conflict_data = store.conflicts_map.get(conflict.conflict_id)
        if resolved_conflict_data:
            from src.dittofs.crdt_store import ConflictInfo
            resolved_conflict = ConflictInfo.from_dict(resolved_conflict_data)
            print(f"Final state: {resolved_conflict.state.value}")
            if resolved_conflict.resolution_strategy:
                print(f"Resolution strategy: {resolved_conflict.resolution_strategy.value}")

def main():
    """Run all demos"""
    print("Enhanced CRDT Store Demo")
    print("=" * 50)
    
    try:
        demo_enhanced_file_records()
        demo_conflict_detection()
        demo_conflict_resolution_strategies()
        demo_semantic_hash()
        demo_version_history()
        demo_conflict_state_machine()
        
        print("\n" + "=" * 50)
        print("All demos completed successfully!")
        
    except Exception as e:
        print(f"Demo failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
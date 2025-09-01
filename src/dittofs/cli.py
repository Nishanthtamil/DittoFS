import asyncio
import argparse
import pathlib
import sys
import logging
import signal
import time
from typing import Optional
from .crdt_store import CRDTStore
from .sync_manager import SyncManager, run_dittofs_daemon  # Fixed import
from .transport import TransportManager
from .chunker import split, join
from .integrity import IntegrityVerifier, RecoveryProcedures
from .garbage_collector import (
    GarbageCollector, RetentionPolicy, get_garbage_collector, 
    run_garbage_collection, get_gc_stats
)
from .snapshot_system import (
    SnapshotSystem, SnapshotType, SnapshotStatus, RestoreScope,
    RestoreOptions, SnapshotPolicy, get_snapshot_system
)
from .conflict_resolution import (
    ConflictResolutionWorkflow, ResolutionAction, ResolutionResult
)

# For GUI (optional)
try:
    from .gui import TrayApp
    GUI_AVAILABLE = True
except ImportError:
    GUI_AVAILABLE = False
    logging.warning("GUI not available - install PyQt6 for tray functionality")

# For FUSE (optional)  
try:
    import pyfuse3
    FUSE_AVAILABLE = True
except ImportError:
    FUSE_AVAILABLE = False
    logging.warning("FUSE not available - install pyfuse3 for mounting")

class DittoFSCLI:
    """Fixed CLI application"""
    
    def __init__(self):
        self.store = None
        self.transport = None
        self.sync_manager = None
        
    async def init_components(self):
        """Initialize DittoFS components"""
        if not self.store:
            try:
                self.store = CRDTStore()
                self.transport = TransportManager()
                self.sync_manager = SyncManager(self.store, self.transport)
            except Exception as e:
                print(f"Failed to initialize components: {e}")
                import traceback
                traceback.print_exc()
                return False
        return True
    
    async def cmd_add(self, args):
        """Add a file to the distributed file system"""
        if not await self.init_components():
            return 1
        
        file_path = pathlib.Path(args.file)
        if not file_path.exists():
            print(f"Error: File '{file_path}' does not exist")
            return 1
        
        try:
            from .chunker import split
            
            # Split file into chunks
            print(f"Chunking file: {file_path}")
            hashes = split(file_path)
            
            if not hashes:
                print(f"Failed to chunk file: {file_path}")
                return 1
            
            # Add to CRDT store
            if self.store.add_file(file_path, hashes):
                self.store.save()
                print(f"✓ Added file: {file_path}")
                print(f"  Chunks: {len(hashes)}")
                if args.verbose:
                    for i, h in enumerate(hashes):
                        print(f"    {i+1}: {h[:16]}...")
                return 0
            else:
                print(f"Failed to add file: {file_path}")
                return 1
                
        except Exception as e:
            print(f"Error adding file: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    async def cmd_list(self, args):
        """List files in the distributed file system"""
        if not await self.init_components():
            return 1
        
        try:
            files = self.store.list_files()
            
            if not files:
                print("No files in the distributed file system")
                return 0
            
            print(f"Files in DittoFS ({len(files)} total):")
            print("-" * 80)
            
            for file_record in files:
                path = pathlib.Path(file_record.path)
                status = "✓" if path.exists() else "✗"
                
                print(f"{status} {file_record.path}")
                if args.verbose:
                    print(f"    Size: {file_record.size} bytes")
                    print(f"    Chunks: {len(file_record.hashes)}")
                    print(f"    Modified: {time.ctime(file_record.mtime)}")
                    print(f"    Checksum: {file_record.checksum[:16]}...")
            
            # Show missing chunks
            if args.missing:
                missing = self.store.get_missing_chunks()
                if missing:
                    print(f"\nMissing chunks ({len(missing)}):")
                    for chunk_hash in list(missing)[:10]:  # Show first 10
                        print(f"  {chunk_hash[:16]}...")
                    if len(missing) > 10:
                        print(f"  ... and {len(missing) - 10} more")
                        
        except Exception as e:
            print(f"Error listing files: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
        
        return 0
    
    async def cmd_get(self, args):
        """Reconstruct a file from chunks"""
        if not await self.init_components():
            return 1
        
        try:
            from .chunker import join, CHUNK_DIR
            
            if args.hashes:
                # Get by explicit hash list
                hashes = args.hashes.split(',')
                out_path = pathlib.Path(args.output)
                
            else:
                # Get by file path from store
                file_path = pathlib.Path(args.file)
                file_record = self.store.get_file(file_path)
                
                if not file_record:
                    print(f"File not found in store: {file_path}")
                    return 1
                
                hashes = file_record.hashes
                out_path = pathlib.Path(args.output) if args.output else file_path
            
            # Check if all chunks exist
            missing = []
            for h in hashes:
                chunk_path = CHUNK_DIR / h
                if not chunk_path.exists():
                    missing.append(h)
            
            if missing:
                print(f"Missing {len(missing)} chunks:")
                for h in missing[:5]:  # Show first 5
                    print(f"  {h[:16]}...")
                if len(missing) > 5:
                    print(f"  ... and {len(missing) - 5} more")
                print("\nTry running 'dittofs sync' to fetch missing chunks")
                return 1
            
            # Reconstruct file
            print(f"Reconstructing file from {len(hashes)} chunks...")
            if join(hashes, out_path):
                print(f"✓ File reconstructed: {out_path}")
                return 0
            else:
                print("Failed to reconstruct file")
                return 1
            
        except Exception as e:
            print(f"Error reconstructing file: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    async def cmd_sync(self, args):
        """Force synchronization with peers"""
        if not await self.init_components():
            return 1
        
        try:
            print("Starting synchronization...")
            
            # Start transports
            if not await self.transport.start_all():
                print("Failed to start network transports")
                return 1
            
            # Discover peers
            print("Discovering peers...")
            peers = await self.transport.discover_all_peers(timeout=args.timeout)
            
            if not peers:
                print("No peers found")
                await self.transport.stop_all()
                return 0
            
            print(f"Found {len(peers)} peers:")
            for peer in peers:
                print(f"  {peer.peer_id} via {peer.transport_type}")
            
            # Perform sync
            sync_count = 0
            for peer in peers:
                try:
                    print(f"Syncing with {peer.peer_id}...")
                    if await self.sync_manager.force_sync_with_peer(peer.peer_id):
                        sync_count += 1
                        print(f"  ✓ Synced with: {peer.peer_id}")
                    else:
                        print(f"  ✗ Failed to sync with: {peer.peer_id}")
                except Exception as e:
                    print(f"  ✗ Sync failed with {peer.peer_id}: {e}")
            
            print(f"\nSynchronization complete: {sync_count}/{len(peers)} peers")
            
            # Save changes
            self.store.save()
            
            await self.transport.stop_all()
            return 0
            
        except Exception as e:
            print(f"Sync error: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    async def cmd_status(self, args):
        """Show DittoFS status"""
        if not await self.init_components():
            return 1
        
        try:
            files = self.store.list_files()
            missing_chunks = self.store.get_missing_chunks()
            
            print("DittoFS Status")
            print("=" * 40)
            print(f"Files tracked: {len(files)}")
            print(f"Missing chunks: {len(missing_chunks)}")
            
            # Check local file status
            local_files = sum(1 for f in files if pathlib.Path(f.path).exists())
            print(f"Local files: {local_files}")
            print(f"Remote files: {len(files) - local_files}")
            
            # Storage usage
            from .chunker import CHUNK_DIR
            if CHUNK_DIR.exists():
                chunks = list(CHUNK_DIR.glob("*"))
                chunk_count = len(chunks)
                total_size = sum(f.stat().st_size for f in chunks if f.is_file())
                print(f"Chunk storage: {chunk_count} chunks, {total_size // 1024} KB")
            else:
                print("Chunk storage: 0 chunks, 0 KB")
            
            if args.verbose and files:
                print("\nRecent files:")
                recent_files = sorted(files, key=lambda f: f.mtime, reverse=True)[:5]
                for f in recent_files:
                    status = "✓" if pathlib.Path(f.path).exists() else "✗"
                    print(f"  {status} {pathlib.Path(f.path).name}")
                    
        except Exception as e:
            print(f"Status error: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
        
        return 0
    
    async def cmd_daemon(self, args):
        """Run the DittoFS daemon"""
        return await run_dittofs_daemon()
    
    async def cmd_verify(self, args):
        """Verify integrity of chunks and files"""
        if not await self.init_components():
            return 1
        
        try:
            verifier = IntegrityVerifier(self.store)
            
            if args.chunk:
                # Verify specific chunk
                print(f"Verifying chunk: {args.chunk}")
                is_valid, report = await verifier.verify_chunk(args.chunk)
                
                if is_valid:
                    print("✓ Chunk is valid")
                    return 0
                else:
                    print(f"✗ Chunk is corrupted: {report.error_message}")
                    return 1
            
            elif args.file:
                # Verify specific file
                file_path = pathlib.Path(args.file)
                file_record = self.store.get_file(file_path)
                
                if not file_record:
                    print(f"File not found in store: {file_path}")
                    return 1
                
                print(f"Verifying file: {file_path}")
                reports = await verifier.verify_file_integrity(file_record)
                
                if not reports:
                    print("✓ File integrity verified")
                    return 0
                else:
                    print(f"✗ Found {len(reports)} corruptions:")
                    for report in reports:
                        print(f"  {report.corruption_type.value}: {report.error_message}")
                    return 1
            
            else:
                # Verify all chunks
                print("Starting full integrity verification...")
                reports = await verifier.verify_all_chunks()
                
                if not reports:
                    print("✓ All chunks verified successfully")
                    return 0
                else:
                    print(f"✗ Found {len(reports)} corruptions:")
                    
                    # Group by type
                    by_type = {}
                    for report in reports:
                        corruption_type = report.corruption_type.value
                        by_type[corruption_type] = by_type.get(corruption_type, 0) + 1
                    
                    for corruption_type, count in by_type.items():
                        print(f"  {corruption_type}: {count}")
                    
                    if args.verbose:
                        print("\nDetailed reports:")
                        for report in reports[:10]:  # Show first 10
                            print(f"  {report.chunk_hash[:16]}... - {report.error_message}")
                        if len(reports) > 10:
                            print(f"  ... and {len(reports) - 10} more")
                    
                    return 1
                    
        except Exception as e:
            print(f"Verification error: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    async def cmd_repair(self, args):
        """Repair corrupted chunks using peer data"""
        if not await self.init_components():
            return 1
        
        try:
            verifier = IntegrityVerifier(self.store)
            
            # Register sync manager as peer provider
            if self.sync_manager:
                verifier.register_peer_provider("sync_manager", self.sync_manager._get_chunk_from_peers)
            
            if args.chunk:
                # Repair specific chunk
                print(f"Repairing chunk: {args.chunk}")
                result = await verifier.repair_chunk(args.chunk)
                
                if result.status.value == "success":
                    print(f"✓ Chunk repaired successfully from peer: {result.peer_source}")
                    return 0
                else:
                    print(f"✗ Repair failed: {result.error_message}")
                    return 1
            
            elif args.file:
                # Repair specific file
                file_path = args.file
                print(f"Repairing file: {file_path}")
                
                if await verifier.recover_file(file_path):
                    print("✓ File repaired successfully")
                    return 0
                else:
                    print("✗ File repair failed")
                    return 1
            
            else:
                # Repair all corruptions
                print("Starting automatic repair of all corruptions...")
                
                # First verify to find corruptions
                reports = await verifier.verify_all_chunks()
                
                if not reports:
                    print("No corruptions found")
                    return 0
                
                print(f"Found {len(reports)} corruptions, attempting repair...")
                
                # Start network transports for peer communication
                if not await self.transport.start_all():
                    print("Warning: Failed to start network transports")
                
                try:
                    results = await verifier.repair_all_corruptions()
                    
                    successful = sum(1 for r in results if r.status.value == "success")
                    failed = sum(1 for r in results if r.status.value == "failed")
                    no_peers = sum(1 for r in results if r.status.value == "no_peers")
                    
                    print(f"\nRepair results:")
                    print(f"  Successful: {successful}")
                    print(f"  Failed: {failed}")
                    print(f"  No peers: {no_peers}")
                    
                    if args.verbose and results:
                        print("\nDetailed results:")
                        for result in results[:10]:
                            status_icon = "✓" if result.status.value == "success" else "✗"
                            print(f"  {status_icon} {result.chunk_hash[:16]}... - {result.status.value}")
                    
                    return 0 if successful > 0 else 1
                    
                finally:
                    await self.transport.stop_all()
                    
        except Exception as e:
            print(f"Repair error: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    async def cmd_recover(self, args):
        """Emergency recovery procedures"""
        if not await self.init_components():
            return 1
        
        try:
            verifier = IntegrityVerifier(self.store)
            recovery = RecoveryProcedures(verifier, self.store)
            
            # Register sync manager as peer provider
            if self.sync_manager:
                verifier.register_peer_provider("sync_manager", self.sync_manager._get_chunk_from_peers)
            
            # Start network transports
            if not await self.transport.start_all():
                print("Warning: Failed to start network transports")
            
            try:
                if args.emergency:
                    print("Starting emergency recovery procedure...")
                    stats = await recovery.emergency_recovery()
                    
                    print(f"\nRecovery completed:")
                    print(f"  Chunks recovered: {stats['chunks_recovered']}")
                    print(f"  Files recovered: {stats['files_recovered']}")
                    print(f"  Metadata rebuilt: {stats['metadata_rebuilt']}")
                    
                    if stats['errors']:
                        print(f"  Errors: {len(stats['errors'])}")
                        if args.verbose:
                            for error in stats['errors'][:5]:
                                print(f"    {error}")
                    
                    return 0 if not stats['errors'] else 1
                
                elif args.files:
                    file_paths = args.files.split(',')
                    print(f"Recovering {len(file_paths)} files...")
                    
                    stats = await recovery.selective_recovery(file_paths)
                    
                    print(f"\nRecovery completed:")
                    print(f"  Files attempted: {stats['files_attempted']}")
                    print(f"  Files recovered: {stats['files_recovered']}")
                    
                    if stats['errors']:
                        print(f"  Errors: {len(stats['errors'])}")
                        if args.verbose:
                            for error in stats['errors']:
                                print(f"    {error}")
                    
                    return 0 if stats['files_recovered'] > 0 else 1
                
                else:
                    print("No recovery option specified. Use --emergency or --files")
                    return 1
                    
            finally:
                await self.transport.stop_all()
                
        except Exception as e:
            print(f"Recovery error: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    async def cmd_gc(self, args):
        """Garbage collection commands"""
        if not await self.init_components():
            return 1
        
        try:
            if args.action == "run":
                # Run garbage collection
                print("Starting garbage collection...")
                
                # Create custom policy if specified
                policy = None
                if any([args.min_age, args.max_deletions, args.min_free_space]):
                    policy = RetentionPolicy()
                    if args.min_age:
                        policy.min_age_seconds = args.min_age
                    if args.max_deletions:
                        policy.max_deletions_per_run = args.max_deletions
                    if args.min_free_space:
                        policy.min_free_space_bytes = args.min_free_space * 1024 * 1024  # Convert MB to bytes
                
                gc = get_garbage_collector(self.store, policy)
                stats = await gc.collect_garbage(force=args.force)
                
                print(f"\nGarbage collection completed:")
                print(f"  Chunks scanned: {stats.chunks_scanned}")
                print(f"  Chunks deleted: {stats.chunks_deleted}")
                print(f"  Bytes freed: {stats.bytes_freed:,} bytes ({stats.bytes_freed / 1024 / 1024:.2f} MB)")
                print(f"  Orphaned chunks: {stats.orphaned_chunks}")
                print(f"  Referenced chunks: {stats.referenced_chunks}")
                print(f"  Errors: {stats.errors}")
                print(f"  Duration: {stats.duration_seconds:.2f} seconds")
                
                return 0 if stats.errors == 0 else 1
            
            elif args.action == "status":
                # Show garbage collection status
                gc = get_garbage_collector(self.store)
                
                # Get storage info
                storage_info = gc.get_storage_info()
                print("Garbage Collection Status")
                print("=" * 40)
                print(f"Free space: {storage_info['free_bytes']:,} bytes ({storage_info['free_bytes'] / 1024 / 1024:.2f} MB)")
                print(f"Total space: {storage_info['total_bytes']:,} bytes ({storage_info['total_bytes'] / 1024 / 1024:.2f} MB)")
                print(f"Chunk storage: {storage_info['chunk_bytes']:,} bytes ({storage_info['chunk_bytes'] / 1024 / 1024:.2f} MB)")
                print(f"Chunk count: {storage_info['chunk_count']:,}")
                
                # Check if GC should run
                should_run_regular = gc.should_run_regular_gc()
                should_run_emergency = gc.should_run_emergency_gc()
                
                if should_run_emergency:
                    print("\n⚠️  Emergency GC recommended (critically low space)")
                elif should_run_regular:
                    print("\n💡 Regular GC recommended (low space)")
                else:
                    print("\n✅ No GC needed (sufficient space)")
                
                # Show recent stats
                latest_stats = gc.get_latest_stats()
                if latest_stats:
                    print(f"\nLast GC run: {latest_stats.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"  Deleted: {latest_stats.chunks_deleted} chunks")
                    print(f"  Freed: {latest_stats.bytes_freed:,} bytes")
                else:
                    print("\nNo previous GC runs")
                
                return 0
            
            elif args.action == "stats":
                # Show garbage collection statistics
                stats_history = get_gc_stats()
                
                if not stats_history:
                    print("No garbage collection statistics available")
                    return 0
                
                print(f"Garbage Collection Statistics ({len(stats_history)} runs)")
                print("=" * 60)
                
                # Summary statistics
                total_deleted = sum(s.chunks_deleted for s in stats_history)
                total_freed = sum(s.bytes_freed for s in stats_history)
                total_errors = sum(s.errors for s in stats_history)
                avg_duration = sum(s.duration_seconds for s in stats_history) / len(stats_history)
                
                print(f"Total chunks deleted: {total_deleted:,}")
                print(f"Total bytes freed: {total_freed:,} bytes ({total_freed / 1024 / 1024:.2f} MB)")
                print(f"Total errors: {total_errors}")
                print(f"Average duration: {avg_duration:.2f} seconds")
                
                if args.verbose:
                    print("\nRecent runs:")
                    print("-" * 60)
                    for stats in stats_history[-10:]:  # Show last 10 runs
                        timestamp = stats.timestamp.strftime('%Y-%m-%d %H:%M:%S')
                        print(f"{timestamp}: {stats.chunks_deleted} deleted, "
                              f"{stats.bytes_freed:,} bytes freed, "
                              f"{stats.errors} errors, "
                              f"{stats.duration_seconds:.1f}s")
                
                return 0
            
            elif args.action == "schedule":
                # Start/stop garbage collection scheduler
                gc = get_garbage_collector(self.store)
                
                if args.start:
                    interval = args.interval or 3600  # Default 1 hour
                    await gc.start_scheduler(interval)
                    print(f"Started GC scheduler with {interval} second interval")
                    
                    # Keep running until interrupted
                    try:
                        print("GC scheduler running... Press Ctrl+C to stop")
                        while True:
                            await asyncio.sleep(1)
                    except KeyboardInterrupt:
                        print("\nStopping GC scheduler...")
                        await gc.stop_scheduler()
                        print("GC scheduler stopped")
                    
                    return 0
                
                elif args.stop:
                    await gc.stop_scheduler()
                    print("GC scheduler stopped")
                    return 0
                
                else:
                    print("Use --start or --stop with schedule action")
                    return 1
            
            else:
                print(f"Unknown GC action: {args.action}")
                return 1
                
        except Exception as e:
            print(f"Garbage collection error: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    async def cmd_snapshot(self, args):
        """Snapshot management commands"""
        if not await self.init_components():
            return 1
        
        try:
            # Initialize snapshot system
            snapshot_system = get_snapshot_system(crdt_store=self.store)
            
            if args.action == "create":
                # Create a new snapshot
                snapshot_type = SnapshotType(args.type) if args.type else SnapshotType.INCREMENTAL
                
                print(f"Creating {snapshot_type.value} snapshot...")
                snapshot_id = snapshot_system.create_snapshot(
                    snapshot_type=snapshot_type,
                    description=args.description or f"{snapshot_type.value} snapshot",
                    tags=args.tags.split(',') if args.tags else [],
                    parent_snapshot=args.parent
                )
                
                if snapshot_id:
                    print(f"✓ Created snapshot: {snapshot_id}")
                    
                    # Show snapshot info
                    info = snapshot_system.get_snapshot_info(snapshot_id)
                    if info:
                        metadata = info['metadata']
                        print(f"  Type: {metadata['snapshot_type']}")
                        print(f"  Files: {metadata['file_count']}")
                        print(f"  Size: {metadata['total_size']:,} bytes")
                        if metadata['compressed_size'] > 0:
                            ratio = (1 - metadata['compressed_size'] / metadata['total_size']) * 100
                            print(f"  Compressed: {metadata['compressed_size']:,} bytes ({ratio:.1f}% saved)")
                    return 0
                else:
                    print("Failed to create snapshot")
                    return 1
            
            elif args.action == "list":
                # List snapshots
                snapshots = snapshot_system.list_snapshots(
                    tags=args.tags.split(',') if args.tags else None
                )
                
                if not snapshots:
                    print("No snapshots found")
                    return 0
                
                print(f"Found {len(snapshots)} snapshots:")
                print()
                
                for snapshot in snapshots:
                    created = time.strftime('%Y-%m-%d %H:%M:%S', 
                                         time.localtime(snapshot.created_at))
                    print(f"Snapshot: {snapshot.snapshot_id}")
                    print(f"  Type: {snapshot.snapshot_type.value}")
                    print(f"  Created: {created}")
                    print(f"  Files: {snapshot.file_count}")
                    print(f"  Size: {snapshot.total_size:,} bytes")
                    print(f"  Status: {snapshot.status.value}")
                    if snapshot.description:
                        print(f"  Description: {snapshot.description}")
                    if snapshot.tags:
                        print(f"  Tags: {', '.join(snapshot.tags)}")
                    if snapshot.parent_snapshot:
                        print(f"  Parent: {snapshot.parent_snapshot}")
                    print()
                
                return 0
            
            elif args.action == "restore":
                # Restore from snapshot
                if not args.snapshot_id:
                    print("Error: --snapshot-id required for restore")
                    return 1
                
                # Parse restore options
                scope = RestoreScope.FULL
                if args.selective:
                    scope = RestoreScope.SELECTIVE
                
                restore_options = RestoreOptions(
                    scope=scope,
                    target_path=pathlib.Path(args.target) if args.target else None,
                    file_patterns=args.patterns.split(',') if args.patterns else [],
                    exclude_patterns=args.exclude.split(',') if args.exclude else [],
                    overwrite_existing=args.overwrite,
                    preserve_permissions=not args.no_permissions,
                    verify_integrity=not args.no_verify
                )
                
                print(f"Restoring snapshot {args.snapshot_id}...")
                if args.target:
                    print(f"Target: {args.target}")
                if args.patterns:
                    print(f"Patterns: {args.patterns}")
                if args.exclude:
                    print(f"Exclude: {args.exclude}")
                
                success = snapshot_system.restore_snapshot(args.snapshot_id, restore_options)
                
                if success:
                    print("✓ Snapshot restored successfully")
                    return 0
                else:
                    print("Failed to restore snapshot")
                    return 1
            
            elif args.action == "delete":
                # Delete snapshot
                if not args.snapshot_id:
                    print("Error: --snapshot-id required for delete")
                    return 1
                
                if not args.force:
                    response = input(f"Delete snapshot {args.snapshot_id}? (y/N): ")
                    if response.lower() != 'y':
                        print("Cancelled")
                        return 0
                
                success = snapshot_system.delete_snapshot(args.snapshot_id)
                
                if success:
                    print(f"✓ Deleted snapshot: {args.snapshot_id}")
                    return 0
                else:
                    print("Failed to delete snapshot")
                    return 1
            
            elif args.action == "info":
                # Show detailed snapshot info
                if not args.snapshot_id:
                    print("Error: --snapshot-id required for info")
                    return 1
                
                info = snapshot_system.get_snapshot_info(args.snapshot_id)
                if not info:
                    print(f"Snapshot {args.snapshot_id} not found")
                    return 1
                
                metadata = info['metadata']
                files = info['files']
                
                created = time.strftime('%Y-%m-%d %H:%M:%S', 
                                     time.localtime(metadata['created_at']))
                
                print(f"Snapshot: {metadata['snapshot_id']}")
                print(f"Type: {metadata['snapshot_type']}")
                print(f"Created: {created}")
                print(f"Created by: {metadata['created_by']}")
                print(f"Status: {metadata['status']}")
                print(f"Files: {metadata['file_count']}")
                print(f"Total size: {metadata['total_size']:,} bytes")
                
                if metadata['compressed_size'] > 0:
                    ratio = (1 - metadata['compressed_size'] / metadata['total_size']) * 100
                    print(f"Compressed size: {metadata['compressed_size']:,} bytes ({ratio:.1f}% saved)")
                
                if metadata.get('description'):
                    print(f"Description: {metadata['description']}")
                
                if metadata.get('tags'):
                    print(f"Tags: {', '.join(metadata['tags'])}")
                
                if metadata.get('parent_snapshot'):
                    print(f"Parent snapshot: {metadata['parent_snapshot']}")
                
                if args.verbose and files:
                    print(f"\nFiles ({len(files)}):")
                    for file_info in files[:20]:  # Show first 20 files
                        mtime = time.strftime('%Y-%m-%d %H:%M:%S', 
                                            time.localtime(file_info['mtime']))
                        print(f"  {file_info['path']} ({file_info['size']:,} bytes, {mtime})")
                    
                    if len(files) > 20:
                        print(f"  ... and {len(files) - 20} more files")
                
                return 0
            
            elif args.action == "policy":
                # Manage snapshot policies
                if args.policy_action == "create":
                    if not args.policy_name:
                        print("Error: --policy-name required")
                        return 1
                    
                    policy = SnapshotPolicy(
                        name=args.policy_name,
                        enabled=not args.disabled,
                        schedule_cron=args.schedule or "0 2 * * *",
                        snapshot_type=SnapshotType(args.type) if args.type else SnapshotType.INCREMENTAL,
                        max_snapshots=args.max_snapshots,
                        max_age_days=args.max_age_days,
                        full_snapshot_interval=args.full_interval or 7,
                        compress_snapshots=not args.no_compress,
                        tags=args.tags.split(',') if args.tags else []
                    )
                    
                    success = snapshot_system.create_policy(policy)
                    if success:
                        print(f"✓ Created policy: {args.policy_name}")
                        return 0
                    else:
                        print("Failed to create policy")
                        return 1
                
                elif args.policy_action == "list":
                    policies = list(snapshot_system.policy_cache.values())
                    if not policies:
                        print("No policies found")
                        return 0
                    
                    print(f"Found {len(policies)} policies:")
                    print()
                    
                    for policy in policies:
                        print(f"Policy: {policy.name}")
                        print(f"  Enabled: {policy.enabled}")
                        print(f"  Schedule: {policy.schedule_cron}")
                        print(f"  Type: {policy.snapshot_type.value}")
                        print(f"  Max snapshots: {policy.max_snapshots}")
                        print(f"  Max age: {policy.max_age_days} days")
                        print(f"  Full interval: {policy.full_snapshot_interval}")
                        if policy.tags:
                            print(f"  Tags: {', '.join(policy.tags)}")
                        print()
                    
                    return 0
                
                elif args.policy_action == "delete":
                    if not args.policy_name:
                        print("Error: --policy-name required")
                        return 1
                    
                    success = snapshot_system.delete_policy(args.policy_name)
                    if success:
                        print(f"✓ Deleted policy: {args.policy_name}")
                        return 0
                    else:
                        print("Policy not found")
                        return 1
                
                else:
                    print(f"Unknown policy action: {args.policy_action}")
                    return 1
            
            elif args.action == "cleanup":
                # Run cleanup
                policy_name = args.policy_name if args.policy_name else None
                deleted_count = snapshot_system.cleanup_snapshots(policy_name)
                
                print(f"✓ Cleanup completed: deleted {deleted_count} snapshots")
                return 0
            
            else:
                print(f"Unknown snapshot action: {args.action}")
                return 1
                
        except Exception as e:
            print(f"Snapshot error: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    async def cmd_conflicts(self, args):
        """Handle conflict resolution commands"""
        if not await self.init_components():
            return 1
        
        try:
            # Initialize conflict resolution workflow
            workflow = ConflictResolutionWorkflow(self.store)
            
            if args.action == "list":
                # List conflicts
                conflicts = self.store.get_pending_conflicts() if args.pending_only else []
                if not args.pending_only:
                    # Get all conflicts from conflicts_map
                    all_conflicts = []
                    for conflict_data in self.store.conflicts_map.values():
                        from .crdt_store import ConflictInfo
                        conflict = ConflictInfo.from_dict(conflict_data)
                        if not args.file or args.file in conflict.file_path:
                            all_conflicts.append(conflict)
                    conflicts = all_conflicts
                
                if not conflicts:
                    print("No conflicts found")
                    return 0
                
                print(f"Conflicts ({len(conflicts)} total):")
                print("-" * 80)
                
                for conflict in conflicts:
                    status_icon = "⚠️" if conflict.state.value == "detected" else "✅"
                    print(f"{status_icon} {conflict.conflict_id[:16]}... - {conflict.file_path}")
                    print(f"    Type: {conflict.conflict_type.value}")
                    print(f"    State: {conflict.state.value}")
                    print(f"    Detected: {conflict.detected_at.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"    Versions: {len(conflict.versions)}")
                    
                    # Check if auto-resolvable
                    can_resolve, action = workflow.can_auto_resolve(conflict)
                    if can_resolve:
                        print(f"    🤖 Auto-resolvable: {action.value}")
                    else:
                        print(f"    👤 Manual resolution required")
                    print()
                
                return 0
            
            elif args.action == "show":
                # Show detailed conflict information
                if not args.conflict_id:
                    print("Error: --conflict-id required for show action")
                    return 1
                
                conflict_data = self.store.conflicts_map.get(args.conflict_id)
                if not conflict_data:
                    print(f"Conflict not found: {args.conflict_id}")
                    return 1
                
                from .crdt_store import ConflictInfo
                conflict = ConflictInfo.from_dict(conflict_data)
                
                print(f"Conflict Details: {conflict.conflict_id}")
                print("=" * 50)
                print(f"File: {conflict.file_path}")
                print(f"Type: {conflict.conflict_type.value}")
                print(f"State: {conflict.state.value}")
                print(f"Detected: {conflict.detected_at.strftime('%Y-%m-%d %H:%M:%S')}")
                
                if conflict.resolved_at:
                    print(f"Resolved: {conflict.resolved_at.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"Strategy: {conflict.resolution_strategy.value}")
                
                print(f"\nVersions ({len(conflict.versions)}):")
                for i, version in enumerate(conflict.versions, 1):
                    print(f"  Version {i}:")
                    print(f"    Peer: {version.get('peer_id', 'unknown')}")
                    print(f"    Size: {version.get('size', 0)} bytes")
                    print(f"    Modified: {time.ctime(version.get('mtime', 0))}")
                    print(f"    Checksum: {version.get('checksum', 'unknown')[:16]}...")
                
                # Show diff if requested
                if args.diff:
                    print("\nConflict Diff:")
                    print("-" * 30)
                    diff = workflow.generate_conflict_diff(args.conflict_id)
                    if diff:
                        if diff.binary_file:
                            print("Binary file - no text diff available")
                        else:
                            if diff.content_diff:
                                print(diff.content_diff)
                            
                            if diff.metadata_diff:
                                print("\nMetadata Differences:")
                                for key, changes in diff.metadata_diff.items():
                                    print(f"  {key}: {changes['local']} → {changes['remote']}")
                    else:
                        print("Unable to generate diff")
                
                return 0
            
            elif args.action == "resolve":
                # Resolve specific conflict
                if not args.conflict_id:
                    print("Error: --conflict-id required for resolve action")
                    return 1
                
                if not args.resolution:
                    print("Error: --resolution required for resolve action")
                    return 1
                
                # Map CLI resolution choices to ResolutionAction
                resolution_map = {
                    "keep-local": ResolutionAction.KEEP_LOCAL,
                    "keep-remote": ResolutionAction.KEEP_REMOTE,
                    "merge-auto": ResolutionAction.MERGE_AUTOMATIC,
                    "preserve-both": ResolutionAction.PRESERVE_BOTH
                }
                
                action = resolution_map.get(args.resolution)
                if not action:
                    print(f"Invalid resolution: {args.resolution}")
                    return 1
                
                print(f"Resolving conflict {args.conflict_id[:16]}... with {action.value}")
                
                success = await workflow.manual_resolve_conflict(args.conflict_id, action)
                
                if success:
                    print("✅ Conflict resolved successfully")
                    return 0
                else:
                    print("❌ Failed to resolve conflict")
                    return 1
            
            elif args.action == "auto-resolve":
                # Auto-resolve conflicts
                if args.batch:
                    print("Running batch auto-resolution...")
                    results = await workflow.batch_auto_resolve()
                    
                    print(f"Batch Resolution Results:")
                    print(f"  Total conflicts: {results['total_conflicts']}")
                    print(f"  Auto-resolved: {results['auto_resolved']}")
                    print(f"  Manual required: {results['manual_required']}")
                    print(f"  Failed: {results['failed']}")
                    
                    if results['details'] and args.verbose:
                        print("\nDetailed Results:")
                        for detail in results['details']:
                            status_icon = "✅" if detail['status'] == 'auto_resolved' else "⚠️"
                            print(f"  {status_icon} {detail['file_path']}: {detail['status']}")
                    
                    return 0
                
                elif args.conflict_id:
                    print(f"Auto-resolving conflict {args.conflict_id[:16]}...")
                    
                    success = await workflow.auto_resolve_conflict(args.conflict_id)
                    
                    if success:
                        print("✅ Conflict auto-resolved successfully")
                        return 0
                    else:
                        print("⚠️  Conflict requires manual resolution")
                        return 1
                else:
                    print("Error: --conflict-id or --batch required for auto-resolve")
                    return 1
            
            elif args.action == "history":
                # Show resolution history
                history = workflow.get_resolution_history(
                    file_path=args.file,
                    limit=args.limit
                )
                
                if not history:
                    print("No resolution history found")
                    return 0
                
                print(f"Resolution History ({len(history)} entries):")
                print("-" * 80)
                
                for entry in history:
                    result_icon = "✅" if entry.result == ResolutionResult.SUCCESS else "❌"
                    print(f"{result_icon} {entry.resolution_id[:16]}... - {entry.file_path}")
                    print(f"    Action: {entry.action_taken.value}")
                    print(f"    Strategy: {entry.strategy_used.value}")
                    print(f"    Result: {entry.result.value}")
                    print(f"    Resolved: {entry.resolved_at.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"    By: {entry.resolved_by}")
                    if entry.details and args.verbose:
                        print(f"    Details: {entry.details}")
                    print()
                
                return 0
            
            elif args.action == "stats":
                # Show resolution statistics
                stats = workflow.get_resolution_stats()
                
                print("Conflict Resolution Statistics:")
                print("=" * 40)
                print(f"Total resolutions: {stats['total_resolutions']}")
                print(f"Success rate: {stats['success_rate']:.1%}")
                print(f"Auto-resolution rate: {stats['auto_resolution_rate']:.1%}")
                print(f"Recent resolutions (7 days): {stats['recent_resolutions']}")
                
                if stats['resolution_actions']:
                    print(f"\nResolution Actions:")
                    for action, count in stats['resolution_actions'].items():
                        print(f"  {action}: {count}")
                
                if stats['resolution_strategies']:
                    print(f"\nResolution Strategies:")
                    for strategy, count in stats['resolution_strategies'].items():
                        print(f"  {strategy}: {count}")
                
                return 0
            
            else:
                print(f"Unknown conflicts action: {args.action}")
                return 1
                
        except Exception as e:
            print(f"Conflicts error: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1

def build_parser():
    """Build the argument parser"""
    parser = argparse.ArgumentParser(
        prog="dittofs",
        description="Distributed offline file system"
    )
    
    parser.add_argument("-v", "--verbose", action="store_true",
                       help="Enable verbose output")
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Daemon command
    daemon_parser = subparsers.add_parser("daemon", help="Run DittoFS daemon")
    
    # Add command  
    add_parser = subparsers.add_parser("add", help="Add file to DittoFS")
    add_parser.add_argument("file", help="File to add")
    
    # List command
    list_parser = subparsers.add_parser("list", help="List files")
    list_parser.add_argument("--missing", action="store_true",
                            help="Show missing chunks")
    
    # Get command
    get_parser = subparsers.add_parser("get", help="Reconstruct file from chunks")
    get_group = get_parser.add_mutually_exclusive_group(required=True)
    get_group.add_argument("--file", help="File path from store")  
    get_group.add_argument("--hashes", help="Comma-separated chunk hashes")
    get_parser.add_argument("-o", "--output", help="Output path")
    
    # Sync command
    sync_parser = subparsers.add_parser("sync", help="Sync with peers")
    sync_parser.add_argument("--timeout", type=float, default=10.0,
                            help="Discovery timeout")
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Show status")
    
    # Verify command
    verify_parser = subparsers.add_parser("verify", help="Verify integrity")
    verify_group = verify_parser.add_mutually_exclusive_group()
    verify_group.add_argument("--chunk", help="Verify specific chunk hash")
    verify_group.add_argument("--file", help="Verify specific file")
    
    # Repair command
    repair_parser = subparsers.add_parser("repair", help="Repair corrupted data")
    repair_group = repair_parser.add_mutually_exclusive_group()
    repair_group.add_argument("--chunk", help="Repair specific chunk hash")
    repair_group.add_argument("--file", help="Repair specific file")
    
    # Recover command
    recover_parser = subparsers.add_parser("recover", help="Emergency recovery")
    recover_group = recover_parser.add_mutually_exclusive_group(required=True)
    recover_group.add_argument("--emergency", action="store_true", 
                              help="Full emergency recovery")
    recover_group.add_argument("--files", help="Comma-separated file paths to recover")
    
    # Garbage collection command
    gc_parser = subparsers.add_parser("gc", help="Garbage collection")
    gc_parser.add_argument("action", choices=["run", "status", "stats", "schedule"],
                          help="GC action to perform")
    
    # GC run options
    gc_parser.add_argument("--force", action="store_true",
                          help="Force GC even if already running")
    gc_parser.add_argument("--min-age", type=int, metavar="SECONDS",
                          help="Minimum age for chunk deletion (seconds)")
    gc_parser.add_argument("--max-deletions", type=int, metavar="COUNT",
                          help="Maximum chunks to delete per run")
    gc_parser.add_argument("--min-free-space", type=int, metavar="MB",
                          help="Minimum free space threshold (MB)")
    
    # GC schedule options
    gc_parser.add_argument("--start", action="store_true",
                          help="Start GC scheduler")
    gc_parser.add_argument("--stop", action="store_true",
                          help="Stop GC scheduler")
    gc_parser.add_argument("--interval", type=int, metavar="SECONDS",
                          help="Scheduler interval (default: 3600)")
    
    # Snapshot command
    snapshot_parser = subparsers.add_parser("snapshot", help="Snapshot management")
    snapshot_parser.add_argument("action", 
                                choices=["create", "list", "restore", "delete", "info", "policy", "cleanup"],
                                help="Snapshot action to perform")
    
    # Create snapshot options
    snapshot_parser.add_argument("--type", choices=["full", "incremental", "differential"],
                                help="Snapshot type (default: incremental)")
    snapshot_parser.add_argument("--description", help="Snapshot description")
    snapshot_parser.add_argument("--tags", help="Comma-separated tags")
    snapshot_parser.add_argument("--parent", help="Parent snapshot ID for incremental")
    
    # Restore options
    snapshot_parser.add_argument("--snapshot-id", help="Snapshot ID for restore/delete/info")
    snapshot_parser.add_argument("--target", help="Target directory for restore")
    snapshot_parser.add_argument("--selective", action="store_true",
                                help="Selective restore mode")
    snapshot_parser.add_argument("--patterns", help="Comma-separated file patterns")
    snapshot_parser.add_argument("--exclude", help="Comma-separated exclude patterns")
    snapshot_parser.add_argument("--overwrite", action="store_true",
                                help="Overwrite existing files")
    snapshot_parser.add_argument("--no-permissions", action="store_true",
                                help="Don't restore file permissions")
    snapshot_parser.add_argument("--no-verify", action="store_true",
                                help="Skip integrity verification")
    
    # Delete options
    snapshot_parser.add_argument("--force", action="store_true",
                                help="Force deletion without confirmation")
    
    # Policy management
    snapshot_parser.add_argument("--policy-action", choices=["create", "list", "delete"],
                                help="Policy management action")
    snapshot_parser.add_argument("--policy-name", help="Policy name")
    snapshot_parser.add_argument("--schedule", help="Cron schedule (default: '0 2 * * *')")
    snapshot_parser.add_argument("--disabled", action="store_true",
                                help="Create policy as disabled")
    snapshot_parser.add_argument("--max-snapshots", type=int,
                                help="Maximum snapshots to keep")
    snapshot_parser.add_argument("--max-age-days", type=int,
                                help="Maximum age in days")
    snapshot_parser.add_argument("--full-interval", type=int,
                                help="Full snapshot interval (default: 7)")
    snapshot_parser.add_argument("--no-compress", action="store_true",
                                help="Disable snapshot compression")
    
    # Conflict resolution command
    conflict_parser = subparsers.add_parser("conflicts", help="Conflict resolution management")
    conflict_parser.add_argument("action", 
                                choices=["list", "show", "resolve", "auto-resolve", "history", "stats"],
                                help="Conflict resolution action")
    
    # List conflicts options
    conflict_parser.add_argument("--pending-only", action="store_true",
                                help="Show only pending conflicts")
    conflict_parser.add_argument("--file", help="Filter by file path")
    
    # Show conflict options
    conflict_parser.add_argument("--conflict-id", help="Specific conflict ID to show")
    conflict_parser.add_argument("--diff", action="store_true",
                                help="Show detailed diff for conflict")
    
    # Resolve conflict options
    conflict_parser.add_argument("--resolution", 
                                choices=["keep-local", "keep-remote", "merge-auto", "preserve-both"],
                                help="Resolution action to take")
    conflict_parser.add_argument("--batch", action="store_true",
                                help="Resolve all auto-resolvable conflicts")
    
    # History options
    conflict_parser.add_argument("--limit", type=int, default=20,
                                help="Limit number of history entries")
    
    return parser

async def async_main():
    """Async main function"""
    parser = build_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Set up logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    cli = DittoFSCLI()
    
    # Map commands to methods
    command_map = {
        "daemon": cli.cmd_daemon,
        "add": cli.cmd_add,
        "list": cli.cmd_list,
        "get": cli.cmd_get,
        "sync": cli.cmd_sync,
        "status": cli.cmd_status,
        "verify": cli.cmd_verify,
        "repair": cli.cmd_repair,
        "recover": cli.cmd_recover,
        "gc": cli.cmd_gc,
        "snapshot": cli.cmd_snapshot,
        "conflicts": cli.cmd_conflicts,
    }
    
    if args.command in command_map:
        try:
            return await command_map[args.command](args)
        except KeyboardInterrupt:
            print("\nInterrupted by user")
            return 1
        except Exception as e:
            print(f"Error: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    else:
        print(f"Unknown command: {args.command}")
        return 1

def main():
    """Main entry point"""
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        return 1

if __name__ == "__main__":
    sys.exit(main())
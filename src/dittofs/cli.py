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
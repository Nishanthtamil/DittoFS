# src/dittofs/sync_manager.py
import asyncio
import logging
import pathlib
import json
import time
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
import hashlib

try:
    import pycrdt as Y
except ImportError:
    logging.error("pycrdt not found. Install with: pip install pycrdt")
    raise

from .transport_manager import TransportManager, Connection
from .chunker import split, join, CHUNK_DIR

@dataclass
class FileRecord:
    path: str
    hashes: List[str]
    mtime: float
    size: int
    permissions: int = 0o644
    checksum: str = ""
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FileRecord':
        return cls(**data)

@dataclass
class ChunkRequest:
    chunk_hash: str
    requester_id: str
    timestamp: float

class CRDTStore:
    """Improved CRDT-based file metadata store"""
    
    def __init__(self, store_path: pathlib.Path = None):
        self.store_path = store_path or (pathlib.Path.home() / ".dittofs" / "crdt_store.yrs")
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.doc = Y.Doc()
        self.files_map = None
        self.chunks_map = None
        self._setup_maps()
        
        # Load existing data
        self._load()
    
    def _setup_maps(self):
        """Initialize CRDT maps"""
        # Get or create the maps - this is the correct pycrdt pattern
        self.files_map = self.doc.get("files", type=Y.Map)
        self.chunks_map = self.doc.get("chunks", type=Y.Map)  # Track chunk ownership
        
        # If maps don't exist, create them
        if not self.files_map:
            self.files_map = Y.Map()
            self.doc["files"] = self.files_map
            
        if not self.chunks_map:
            self.chunks_map = Y.Map()
            self.doc["chunks"] = self.chunks_map
    
    def add_file(self, file_path: pathlib.Path, hashes: List[str]) -> bool:
        """Add or update a file record"""
        try:
            if not file_path.exists():
                logging.error(f"File does not exist: {file_path}")
                return False
            
            stat = file_path.stat()
            
            # Calculate file checksum for integrity
            file_checksum = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    file_checksum.update(chunk)
            
            record = FileRecord(
                path=str(file_path.resolve()),
                hashes=hashes,
                mtime=stat.st_mtime,
                size=stat.st_size,
                permissions=stat.st_mode & 0o777,
                checksum=file_checksum.hexdigest()
            )
            
            # Store in CRDT map
            self.files_map[record.path] = record.to_dict()
            
            # Track chunk ownership
            for chunk_hash in hashes:
                chunk_owners = self.chunks_map.get(chunk_hash, [])
                if record.path not in chunk_owners:
                    chunk_owners.append(record.path)
                    self.chunks_map[chunk_hash] = chunk_owners
            
            logging.info(f"Added file: {record.path} with {len(hashes)} chunks")
            return True
            
        except Exception as e:
            logging.error(f"Failed to add file {file_path}: {e}")
            return False
    
    def get_file(self, file_path: pathlib.Path) -> Optional[FileRecord]:
        """Get file record by path"""
        try:
            path_str = str(file_path.resolve())
            data = self.files_map.get(path_str)
            if data:
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
                    files.append(FileRecord.from_dict(data))
                except Exception as e:
                    logging.warning(f"Skipping corrupted file record: {path_str}, {e}")
            return files
        except Exception as e:
            logging.error(f"Failed to list files: {e}")
            return []
    
    def get_missing_chunks(self) -> Set[str]:
        """Find chunks that we have metadata for but missing files"""
        missing = set()
        
        for chunk_hash in self.chunks_map.keys():
            chunk_path = CHUNK_DIR / chunk_hash
            if not chunk_path.exists():
                missing.add(chunk_hash)
        
        return missing
    
    def find_chunk_owners(self, chunk_hash: str) -> List[str]:
        """Find which files contain this chunk"""
        return self.chunks_map.get(chunk_hash, [])
    
    def merge_updates(self, update_data: bytes) -> bool:
        """Merge updates from remote peer"""
        try:
            # Apply the update to our document
            Y.apply_update(self.doc, update_data)
            
            # Re-get the maps as they might have changed
            self._setup_maps()
            
            logging.info("Successfully merged CRDT update")
            return True
            
        except Exception as e:
            logging.error(f"Failed to merge CRDT update: {e}")
            return False
    
    def get_state_vector(self) -> bytes:
        """Get state vector for efficient sync"""
        return Y.encode_state_vector(self.doc)
    
    def get_update_since(self, state_vector: bytes) -> bytes:
        """Get update since given state vector"""
        return Y.encode_state_as_update(self.doc, state_vector)
    
    def save(self) -> bool:
        """Save the CRDT document to disk"""
        try:
            update_data = Y.encode_state_as_update(self.doc)
            with open(self.store_path, 'wb') as f:
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
                with open(self.store_path, 'rb') as f:
                    update_data = f.read()
                    if update_data:
                        Y.apply_update(self.doc, update_data)
                        self._setup_maps()
                        logging.debug(f"Loaded CRDT store from {self.store_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to load CRDT store: {e}")
            return False

class SyncManager:
    """Manages synchronization with peers"""
    
    def __init__(self, store: CRDTStore, transport_manager: TransportManager):
        self.store = store
        self.transport = transport_manager
        self.sync_tasks = {}
        self.chunk_requests = {}  # Track pending chunk requests
        self.peer_id = f"dittofs-{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"
        
    async def start_sync_daemon(self):
        """Start the background sync process"""
        await asyncio.create_task(self._sync_loop())
    
    async def _sync_loop(self):
        """Main sync loop"""
        while True:
            try:
                # Discover peers periodically
                peers = await self.transport.discover_all_peers(timeout=5.0)
                
                # Try to sync with each peer
                for peer in peers:
                    if peer.peer_id not in self.sync_tasks:
                        # Start sync task for this peer
                        task = asyncio.create_task(self._sync_with_peer(peer))
                        self.sync_tasks[peer.peer_id] = task
                
                # Clean up completed tasks
                completed = [
                    peer_id for peer_id, task in self.sync_tasks.items()
                    if task.done()
                ]
                for peer_id in completed:
                    del self.sync_tasks[peer_id]
                
                # Check for missing chunks
                await self._request_missing_chunks()
                
            except Exception as e:
                logging.error(f"Sync loop error: {e}")
            
            await asyncio.sleep(30)  # Sync every 30 seconds
    
    async def _sync_with_peer(self, peer):
        """Sync CRDT state with a specific peer"""
        connection = None
        try:
            connection = await self.transport.connect_best_peer(peer.peer_id)
            if not connection:
                logging.warning(f"Failed to connect to peer {peer.peer_id}")
                return
            
            # Exchange state vectors for efficient sync
            our_state = self.store.get_state_vector()
            
            # Send sync request
            await connection.send_message({
                "type": "sync_request",
                "peer_id": self.peer_id,
                "state_vector": our_state.hex()
            })
            
            # Wait for response
            response = await connection.receive_message(timeout=10.0)
            if not response or response.get("type") != "sync_response":
                logging.warning(f"Invalid sync response from {peer.peer_id}")
                return
            
            # Apply their update
            if "update_data" in response:
                update_bytes = bytes.fromhex(response["update_data"])
                self.store.merge_updates(update_bytes)
                self.store.save()
                logging.info(f"Synced with peer {peer.peer_id}")
            
            # Send our update to them
            their_state = bytes.fromhex(response.get("state_vector", ""))
            our_update = self.store.get_update_since(their_state)
            
            if our_update:
                await connection.send_message({
                    "type": "sync_update",
                    "update_data": our_update.hex()
                })
                
        except Exception as e:
            logging.error(f"Sync with {peer.peer_id} failed: {e}")
        finally:
            if connection:
                await connection.close()
    
    async def handle_sync_request(self, connection: Connection, message: dict):
        """Handle incoming sync request"""
        try:
            peer_id = message.get("peer_id", "unknown")
            their_state = bytes.fromhex(message.get("state_vector", ""))
            
            # Calculate what to send them
            update_for_them = self.store.get_update_since(their_state)
            our_state = self.store.get_state_vector()
            
            # Send response
            await connection.send_message({
                "type": "sync_response",
                "peer_id": self.peer_id,
                "state_vector": our_state.hex(),
                "update_data": update_for_them.hex() if update_for_them else ""
            })
            
            logging.info(f"Handled sync request from {peer_id}")
            
        except Exception as e:
            logging.error(f"Failed to handle sync request: {e}")
    
    async def _request_missing_chunks(self):
        """Request missing chunks from peers"""
        try:
            missing_chunks = self.store.get_missing_chunks()
            if not missing_chunks:
                return
            
            logging.info(f"Found {len(missing_chunks)} missing chunks")
            
            # Request chunks from peers
            peers = await self.transport.discover_all_peers(timeout=2.0)
            for chunk_hash in list(missing_chunks)[:5]:  # Limit concurrent requests
                if chunk_hash in self.chunk_requests:
                    continue  # Already requesting this chunk
                
                for peer in peers:
                    try:
                        connection = await self.transport.connect_best_peer(peer.peer_id)
                        if connection:
                            await connection.send_message({
                                "type": "chunk_request",
                                "chunk_hash": chunk_hash,
                                "requester_id": self.peer_id
                            })
                            
                            self.chunk_requests[chunk_hash] = time.time()
                            await connection.close()
                            break  # Found a peer to request from
                    except Exception as e:
                        logging.warning(f"Failed to request chunk from {peer.peer_id}: {e}")
            
            # Clean up old requests
            current_time = time.time()
            expired = [
                chunk_hash for chunk_hash, request_time in self.chunk_requests.items()
                if current_time - request_time > 60  # 1 minute timeout
            ]
            for chunk_hash in expired:
                del self.chunk_requests[chunk_hash]
                
        except Exception as e:
            logging.error(f"Failed to request missing chunks: {e}")
    
    async def handle_chunk_request(self, connection: Connection, message: dict):
        """Handle incoming chunk request"""
        try:
            chunk_hash = message.get("chunk_hash")
            requester_id = message.get("requester_id")
            
            if not chunk_hash:
                return
            
            chunk_path = CHUNK_DIR / chunk_hash
            if chunk_path.exists():
                # Read and send chunk data
                chunk_data = chunk_path.read_bytes()
                await connection.send_message({
                    "type": "chunk_response",
                    "chunk_hash": chunk_hash,
                    "chunk_data": chunk_data.hex(),
                    "size": len(chunk_data)
                })
                logging.info(f"Sent chunk {chunk_hash} to {requester_id}")
            else:
                # Send not found response
                await connection.send_message({
                    "type": "chunk_not_found",
                    "chunk_hash": chunk_hash
                })
                
        except Exception as e:
            logging.error(f"Failed to handle chunk request: {e}")
    
    async def handle_chunk_response(self, connection: Connection, message: dict):
        """Handle incoming chunk response"""
        try:
            chunk_hash = message.get("chunk_hash")
            chunk_data_hex = message.get("chunk_data")
            
            if not chunk_hash or not chunk_data_hex:
                return
            
            # Verify and save chunk
            chunk_data = bytes.fromhex(chunk_data_hex)
            
            # Verify hash
            import blake3
            actual_hash = blake3.blake3(chunk_data).hexdigest()
            if actual_hash != chunk_hash:
                logging.error(f"Chunk hash mismatch: expected {chunk_hash}, got {actual_hash}")
                return
            
            # Save chunk
            chunk_path = CHUNK_DIR / chunk_hash
            chunk_path.write_bytes(chunk_data)
            
            # Remove from pending requests
            if chunk_hash in self.chunk_requests:
                del self.chunk_requests[chunk_hash]
            
            logging.info(f"Received and saved chunk {chunk_hash}")
            
        except Exception as e:
            logging.error(f"Failed to handle chunk response: {e}")
    
    async def handle_message(self, connection: Connection, message: dict):
        """Handle incoming messages"""
        message_type = message.get("type")
        
        if message_type == "sync_request":
            await self.handle_sync_request(connection, message)
        elif message_type == "chunk_request":
            await self.handle_chunk_request(connection, message)
        elif message_type == "chunk_response":
            await self.handle_chunk_response(connection, message)
        elif message_type == "sync_update":
            # Handle sync update
            if "update_data" in message:
                update_bytes = bytes.fromhex(message["update_data"])
                self.store.merge_updates(update_bytes)
                self.store.save()
        else:
            logging.warning(f"Unknown message type: {message_type}")
    
    async def force_sync_with_peer(self, peer_id: str) -> bool:
        """Force immediate sync with a specific peer"""
        try:
            connection = await self.transport.connect_best_peer(peer_id)
            if connection:
                # Create a fake peer info for sync
                class FakePeer:
                    def __init__(self, peer_id):
                        self.peer_id = peer_id
                
                await self._sync_with_peer(FakePeer(peer_id))
                await connection.close()
                return True
        except Exception as e:
            logging.error(f"Force sync with {peer_id} failed: {e}")
        return False
    
    async def get_sync_status(self) -> dict:
        """Get current sync status"""
        return {
            "peer_id": self.peer_id,
            "active_syncs": len(self.sync_tasks),
            "pending_chunk_requests": len(self.chunk_requests),
            "missing_chunks": len(self.store.get_missing_chunks()),
            "total_files": len(self.store.list_files())
        }

# Network message handler for incoming connections
class NetworkHandler:
    """Handles incoming network connections and messages"""
    
    def __init__(self, sync_manager: SyncManager):
        self.sync_manager = sync_manager
        self.active_connections = set()
    
    async def handle_connection(self, connection: Connection):
        """Handle a new incoming connection"""
        self.active_connections.add(connection)
        try:
            while True:
                message = await connection.receive_message(timeout=60.0)
                if not message:
                    break
                
                await self.sync_manager.handle_message(connection, message)
                
        except Exception as e:
            logging.error(f"Connection handler error: {e}")
        finally:
            self.active_connections.discard(connection)
            await connection.close()

# Example integration
async def run_dittofs_daemon():
    """Run the complete DittoFS daemon"""
    logging.basicConfig(level=logging.INFO)
    
    # Initialize components
    store = CRDTStore()
    transport = TransportManager()
    sync_manager = SyncManager(store, transport)
    handler = NetworkHandler(sync_manager)
    
    # Start transports
    if not await transport.start_all():
        logging.error("Failed to start any transport")
        return
    
    # Override connection handler in transports to use our handler
    # This is a simplified approach - in reality you'd integrate this more cleanly
    original_tcp_accept = transport.transports['tcp']._accept_connections
    
    async def enhanced_accept_connections():
        loop = asyncio.get_event_loop()
        server_socket = transport.transports['tcp'].server_socket
        while True:
            try:
                client_socket, addr = await loop.sock_accept(server_socket)
                from .transport_manager import TCPConnection
                conn = TCPConnection(client_socket)
                # Handle connection in background
                asyncio.create_task(handler.handle_connection(conn))
                logging.info(f"New connection from {addr}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error accepting connection: {e}")
                await asyncio.sleep(1)
    
    # Replace the accept connections method
    transport.transports['tcp']._accept_connections = enhanced_accept_connections
    
    # Start sync daemon
    sync_task = asyncio.create_task(sync_manager.start_sync_daemon())
    
    try:
        logging.info("DittoFS daemon started")
        
        # Keep running
        while True:
            status = await sync_manager.get_sync_status()
            logging.info(f"Status: {status}")
            await asyncio.sleep(60)  # Status update every minute
            
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        sync_task.cancel()
        await transport.stop_all()

if __name__ == "__main__":
    asyncio.run(run_dittofs_daemon())
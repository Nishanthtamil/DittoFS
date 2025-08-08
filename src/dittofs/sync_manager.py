import asyncio
import logging
import pathlib
import json
import time
import hashlib
from typing import Dict, List, Optional, Set
from dataclasses import dataclass

from .transport import TransportManager, Connection
from .crdt_store import CRDTStore, FileRecord

@dataclass
class ChunkRequest:
    chunk_hash: str
    requester_id: str
    timestamp: float

class SyncManager:
    """Fixed synchronization manager"""
    
    def __init__(self, store: CRDTStore, transport_manager: TransportManager):
        self.store = store
        self.transport = transport_manager
        self.sync_tasks = {}
        self.chunk_requests = {}
        self.peer_id = f"dittofs-{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"
        self.is_running = False
        
    async def start_sync_daemon(self):
        """Start the background sync process"""
        self.is_running = True
        await self._sync_loop()
    
    async def _sync_loop(self):
        """Main sync loop"""
        while self.is_running:
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
                    try:
                        await self.sync_tasks[peer_id]  # Get any exception
                    except Exception as e:
                        logging.error(f"Sync task error for {peer_id}: {e}")
                    del self.sync_tasks[peer_id]
                
                # Request missing chunks periodically
                if len(peers) > 0:
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
                logging.debug(f"Failed to connect to peer {peer.peer_id}")
                return
            
            # Exchange state vectors for efficient sync
            our_state = self.store.get_state_vector()
            
            # Send sync request
            await connection.send_message({
                "type": "sync_request",
                "peer_id": self.peer_id,
                "state_vector": our_state.hex() if our_state else ""
            })
            
            # Wait for response
            response = await connection.receive_message(timeout=15.0)
            if not response or response.get("type") != "sync_response":
                logging.debug(f"No valid sync response from {peer.peer_id}")
                return
            
            # Apply their update if they have one
            if "update_data" in response and response["update_data"]:
                try:
                    update_bytes = bytes.fromhex(response["update_data"])
                    if self.store.merge_updates(update_bytes):
                        self.store.save()
                        logging.info(f"Synced with peer {peer.peer_id}")
                except Exception as e:
                    logging.error(f"Failed to apply update from {peer.peer_id}: {e}")
            
            # Send our update to them if we have changes
            if "state_vector" in response and response["state_vector"]:
                try:
                    their_state = bytes.fromhex(response["state_vector"])
                    our_update = self.store.get_update_since(their_state)
                    
                    if our_update:
                        await connection.send_message({
                            "type": "sync_update",
                            "update_data": our_update.hex()
                        })
                except Exception as e:
                    logging.error(f"Failed to send update to {peer.peer_id}: {e}")
                    
        except Exception as e:
            logging.error(f"Sync with {peer.peer_id} failed: {e}")
        finally:
            if connection:
                await connection.close()
    
    async def handle_sync_request(self, connection: Connection, message: dict):
        """Handle incoming sync request"""
        try:
            peer_id = message.get("peer_id", "unknown")
            their_state_hex = message.get("state_vector", "")
            their_state = bytes.fromhex(their_state_hex) if their_state_hex else b""
            
            # Calculate what to send them
            update_for_them = self.store.get_update_since(their_state)
            our_state = self.store.get_state_vector()
            
            # Send response
            await connection.send_message({
                "type": "sync_response",
                "peer_id": self.peer_id,
                "state_vector": our_state.hex() if our_state else "",
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
            
            # Request chunks from peers (limit concurrent requests)
            peers = await self.transport.discover_all_peers(timeout=2.0)
            requested_count = 0
            
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
                            requested_count += 1
                            break  # Found a peer to request from
                    except Exception as e:
                        logging.debug(f"Failed to request chunk from {peer.peer_id}: {e}")
            
            if requested_count > 0:
                logging.info(f"Requested {requested_count} chunks from peers")
            
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
            from .chunker import CHUNK_DIR
            
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
                logging.info(f"Sent chunk {chunk_hash[:8]}... to {requester_id}")
            else:
                # Send not found response
                await connection.send_message({
                    "type": "chunk_not_found",
                    "chunk_hash": chunk_hash
                })
                logging.debug(f"Chunk {chunk_hash[:8]}... not found")
                
        except Exception as e:
            logging.error(f"Failed to handle chunk request: {e}")
    
    async def handle_chunk_response(self, connection: Connection, message: dict):
        """Handle incoming chunk response"""
        try:
            from .chunker import CHUNK_DIR
            import blake3
            
            chunk_hash = message.get("chunk_hash")
            chunk_data_hex = message.get("chunk_data")
            
            if not chunk_hash or not chunk_data_hex:
                return
            
            # Decode and verify chunk
            chunk_data = bytes.fromhex(chunk_data_hex)
            
            # Verify hash
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
            
            logging.info(f"Received and saved chunk {chunk_hash[:8]}...")
            
        except Exception as e:
            logging.error(f"Failed to handle chunk response: {e}")
    
    async def handle_message(self, connection: Connection, message: dict):
        """Handle incoming messages"""
        message_type = message.get("type")
        
        try:
            if message_type == "sync_request":
                await self.handle_sync_request(connection, message)
            elif message_type == "chunk_request":
                await self.handle_chunk_request(connection, message)
            elif message_type == "chunk_response":
                await self.handle_chunk_response(connection, message)
            elif message_type == "sync_update":
                # Handle sync update
                if "update_data" in message and message["update_data"]:
                    update_bytes = bytes.fromhex(message["update_data"])
                    if self.store.merge_updates(update_bytes):
                        self.store.save()
            else:
                logging.debug(f"Unknown message type: {message_type}")
        except Exception as e:
            logging.error(f"Error handling message type {message_type}: {e}")
    
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
    
    async def stop(self):
        """Stop the sync manager"""
        self.is_running = False
        
        # Cancel all sync tasks
        for task in self.sync_tasks.values():
            task.cancel()
        
        # Wait for tasks to complete
        if self.sync_tasks:
            await asyncio.gather(*self.sync_tasks.values(), return_exceptions=True)
        
        self.sync_tasks.clear()
        self.chunk_requests.clear()


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
            logging.info("New connection established")
            
            while True:
                message = await connection.receive_message(timeout=60.0)
                if not message:
                    logging.debug("Connection closed or timed out")
                    break
                
                # Handle the message
                await self.sync_manager.handle_message(connection, message)
                
        except Exception as e:
            logging.error(f"Connection handler error: {e}")
        finally:
            self.active_connections.discard(connection)
            await connection.close()
            logging.debug("Connection closed")


# Daemon runner
async def run_dittofs_daemon():
    """Run the complete DittoFS daemon"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize components
        store = CRDTStore()
        transport = TransportManager()
        sync_manager = SyncManager(store, transport)
        handler = NetworkHandler(sync_manager)
        
        # Start transports
        if not await transport.start_all():
            logging.error("Failed to start any transport")
            return
        
        logging.info("DittoFS daemon started successfully")
        
        # Create enhanced TCP accept handler
        async def handle_new_connection(connection):
            """Handle new incoming connection"""
            asyncio.create_task(handler.handle_connection(connection))
        
        # Monkey-patch the TCP transport to use our handler
        original_accept = transport.transports['tcp']._accept_connections
        
        async def enhanced_accept():
            loop = asyncio.get_event_loop()
            tcp_transport = transport.transports['tcp']
            
            while tcp_transport.is_running:
                try:
                    client_socket, addr = await loop.sock_accept(tcp_transport.server_socket)
                    from .transport import TCPConnection
                    conn = TCPConnection(client_socket)
                    tcp_transport.connections.append(conn)
                    
                    # Handle connection in background
                    asyncio.create_task(handler.handle_connection(conn))
                    logging.info(f"New connection from {addr}")
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    if tcp_transport.is_running:
                        logging.error(f"Error accepting connection: {e}")
                    await asyncio.sleep(1)
        
        # Replace the accept method
        transport.transports['tcp']._accept_connections = enhanced_accept
        
        # Start sync daemon
        sync_task = asyncio.create_task(sync_manager.start_sync_daemon())
        
        # Status reporting task
        async def status_reporter():
            while True:
                await asyncio.sleep(120)  # Every 2 minutes
                status = await sync_manager.get_sync_status()
                logging.info(f"Status - Peer: {status['peer_id'][:8]}..., "
                           f"Files: {status['total_files']}, "
                           f"Missing chunks: {status['missing_chunks']}, "
                           f"Active syncs: {status['active_syncs']}")
        
        status_task = asyncio.create_task(status_reporter())
        
        try:
            logging.info("DittoFS daemon running. Press Ctrl+C to stop.")
            
            # Keep running until interrupted
            await asyncio.gather(sync_task, status_task)
            
        except KeyboardInterrupt:
            logging.info("Shutting down gracefully...")
        finally:
            # Cleanup
            sync_task.cancel()
            status_task.cancel()
            await sync_manager.stop()
            await transport.stop_all()
            
            # Wait for tasks to complete
            await asyncio.gather(sync_task, status_task, return_exceptions=True)
            
            logging.info("DittoFS daemon stopped")
            
    except Exception as e:
        logging.error(f"Daemon error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0
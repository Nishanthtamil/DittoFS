# src/dittofs/transport_manager.py
import asyncio
import logging
import json
import time
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import socket
import threading

@dataclass
class PeerInfo:
    peer_id: str
    transport_type: str
    address: str
    port: int
    last_seen: float
    metadata: Dict[str, Any]

class TransportProtocol(ABC):
    """Abstract base for all transport protocols"""
    
    @abstractmethod
    async def start_server(self, port: int = None) -> bool:
        """Start serving on this transport"""
        pass
    
    @abstractmethod
    async def discover_peers(self, timeout: float = 10.0) -> List[PeerInfo]:
        """Discover available peers"""
        pass
    
    @abstractmethod
    async def connect_peer(self, peer: PeerInfo) -> 'Connection':
        """Connect to a specific peer"""
        pass
    
    @abstractmethod
    async def stop(self):
        """Stop the transport"""
        pass

class Connection(ABC):
    """Abstract connection to a peer"""
    
    @abstractmethod
    async def send_message(self, message: Dict[str, Any]) -> bool:
        """Send a structured message"""
        pass
    
    @abstractmethod
    async def receive_message(self, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """Receive a structured message"""
        pass
    
    @abstractmethod
    async def close(self):
        """Close the connection"""
        pass

class TCPTransport(TransportProtocol):
    """Reliable TCP-based transport with mDNS discovery"""
    
    def __init__(self):
        self.server_socket = None
        self.server_task = None
        self.zeroconf = None
        self.service_info = None
        self.connections = []
        self.port = None
        
    async def start_server(self, port: int = None) -> bool:
        try:
            from zeroconf import Zeroconf, ServiceInfo
            import socket
            
            if port is None:
                port = 0  # Let OS choose
                
            # Create server socket
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', port))
            self.port = self.server_socket.getsockname()[1]
            self.server_socket.listen(5)
            self.server_socket.setblocking(False)
            
            # Start accepting connections
            self.server_task = asyncio.create_task(self._accept_connections())
            
            # Register mDNS service
            self.zeroconf = Zeroconf()
            hostname = socket.gethostname()
            self.service_info = ServiceInfo(
                "_dittofs._tcp.local.",
                f"DittoFS-{hostname}._dittofs._tcp.local.",
                addresses=[socket.inet_aton("127.0.0.1")],
                port=self.port,
                properties={'version': '1.0'},
                server=f"{hostname}.local."
            )
            self.zeroconf.register_service(self.service_info)
            
            logging.info(f"TCP server started on port {self.port}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to start TCP server: {e}")
            return False
    
    async def _accept_connections(self):
        """Accept incoming connections"""
        loop = asyncio.get_event_loop()
        while True:
            try:
                client_socket, addr = await loop.sock_accept(self.server_socket)
                conn = TCPConnection(client_socket)
                self.connections.append(conn)
                logging.info(f"New connection from {addr}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Error accepting connection: {e}")
                await asyncio.sleep(1)
    
    async def discover_peers(self, timeout: float = 10.0) -> List[PeerInfo]:
        """Discover peers via mDNS"""
        try:
            from zeroconf import Zeroconf, ServiceBrowser, ServiceListener
            
            peers = []
            discovered = threading.Event()
            
            class PeerListener(ServiceListener):
                def add_service(self, zc, type_, name):
                    info = zc.get_service_info(type_, name)
                    if info and info.port != self.port:  # Don't discover ourselves
                        try:
                            address = socket.inet_ntoa(info.addresses[0])
                            peer = PeerInfo(
                                peer_id=name,
                                transport_type="tcp",
                                address=address,
                                port=info.port,
                                last_seen=time.time(),
                                metadata=dict(info.properties) if info.properties else {}
                            )
                            peers.append(peer)
                        except Exception as e:
                            logging.warning(f"Error processing service {name}: {e}")
            
            zc = Zeroconf()
            browser = ServiceBrowser(zc, "_dittofs._tcp.local.", PeerListener())
            
            # Wait for discovery
            await asyncio.sleep(min(timeout, 5.0))
            zc.close()
            
            logging.info(f"Discovered {len(peers)} TCP peers")
            return peers
            
        except Exception as e:
            logging.error(f"TCP discovery failed: {e}")
            return []
    
    async def connect_peer(self, peer: PeerInfo) -> Optional[Connection]:
        """Connect to a TCP peer"""
        try:
            reader, writer = await asyncio.open_connection(peer.address, peer.port)
            return TCPConnection(writer.get_extra_info('socket'), reader, writer)
        except Exception as e:
            logging.error(f"Failed to connect to {peer.peer_id}: {e}")
            return None
    
    async def stop(self):
        """Stop TCP transport"""
        if self.server_task:
            self.server_task.cancel()
            try:
                await self.server_task
            except asyncio.CancelledError:
                pass
        
        if self.server_socket:
            self.server_socket.close()
        
        if self.zeroconf and self.service_info:
            self.zeroconf.unregister_service(self.service_info)
            self.zeroconf.close()
        
        # Close all connections
        for conn in self.connections:
            await conn.close()

class TCPConnection(Connection):
    """TCP connection implementation"""
    
    def __init__(self, socket, reader=None, writer=None):
        self.socket = socket
        self.reader = reader
        self.writer = writer
        self._closed = False
    
    async def send_message(self, message: Dict[str, Any]) -> bool:
        """Send JSON message with length prefix"""
        if self._closed:
            return False
        
        try:
            data = json.dumps(message).encode('utf-8')
            length = len(data).to_bytes(4, 'big')
            
            if self.writer:
                self.writer.write(length + data)
                await self.writer.drain()
            else:
                # Fallback to socket
                loop = asyncio.get_event_loop()
                await loop.sock_sendall(self.socket, length + data)
            return True
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            await self.close()
            return False
    
    async def receive_message(self, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """Receive JSON message with length prefix"""
        if self._closed:
            return None
        
        try:
            # Read length prefix
            if self.reader:
                length_data = await asyncio.wait_for(self.reader.readexactly(4), timeout)
            else:
                loop = asyncio.get_event_loop()
                length_data = await asyncio.wait_for(
                    loop.sock_recv(self.socket, 4), timeout
                )
            
            if not length_data or len(length_data) != 4:
                return None
            
            length = int.from_bytes(length_data, 'big')
            
            # Read message data
            if self.reader:
                data = await asyncio.wait_for(self.reader.readexactly(length), timeout)
            else:
                data = b''
                while len(data) < length:
                    chunk = await asyncio.wait_for(
                        loop.sock_recv(self.socket, length - len(data)), timeout
                    )
                    if not chunk:
                        return None
                    data += chunk
            
            return json.loads(data.decode('utf-8'))
            
        except asyncio.TimeoutError:
            logging.warning("Message receive timeout")
            return None
        except Exception as e:
            logging.error(f"Failed to receive message: {e}")
            await self.close()
            return None
    
    async def close(self):
        """Close connection"""
        if self._closed:
            return
        
        self._closed = True
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except:
                pass
        elif self.socket:
            self.socket.close()

class UDPTransport(TransportProtocol):
    """UDP-based transport for local network discovery and fast messaging"""
    
    def __init__(self):
        self.socket = None
        self.port = None
        self.server_task = None
        self.peers_cache = {}
        
    async def start_server(self, port: int = 9876) -> bool:
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.socket.bind(('0.0.0.0', port))
            self.socket.setblocking(False)
            self.port = port
            
            self.server_task = asyncio.create_task(self._handle_broadcasts())
            
            # Send periodic announcements
            asyncio.create_task(self._announce_presence())
            
            logging.info(f"UDP server started on port {port}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to start UDP server: {e}")
            return False
    
    async def _handle_broadcasts(self):
        """Handle incoming UDP messages"""
        loop = asyncio.get_event_loop()
        while True:
            try:
                data, addr = await loop.sock_recvfrom(self.socket, 1024)
                message = json.loads(data.decode('utf-8'))
                
                if message.get('type') == 'announce':
                    peer_id = f"{addr[0]}:{message.get('port', addr[1])}"
                    self.peers_cache[peer_id] = PeerInfo(
                        peer_id=peer_id,
                        transport_type="udp",
                        address=addr[0],
                        port=message.get('port', addr[1]),
                        last_seen=time.time(),
                        metadata=message.get('metadata', {})
                    )
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"UDP handler error: {e}")
                await asyncio.sleep(0.1)
    
    async def _announce_presence(self):
        """Periodically announce our presence"""
        while True:
            try:
                message = {
                    'type': 'announce',
                    'port': self.port,
                    'metadata': {'version': '1.0'}
                }
                data = json.dumps(message).encode('utf-8')
                
                # Broadcast to local network
                loop = asyncio.get_event_loop()
                await loop.sock_sendto(self.socket, data, ('<broadcast>', self.port))
                
            except Exception as e:
                logging.error(f"Broadcast error: {e}")
            
            await asyncio.sleep(30)  # Announce every 30 seconds
    
    async def discover_peers(self, timeout: float = 10.0) -> List[PeerInfo]:
        """Return cached discovered peers"""
        # Clean old peers
        current_time = time.time()
        self.peers_cache = {
            k: v for k, v in self.peers_cache.items()
            if current_time - v.last_seen < 120  # 2 minute timeout
        }
        
        return list(self.peers_cache.values())
    
    async def connect_peer(self, peer: PeerInfo) -> Optional[Connection]:
        """Create UDP connection to peer"""
        return UDPConnection(self.socket, (peer.address, peer.port))
    
    async def stop(self):
        """Stop UDP transport"""
        if self.server_task:
            self.server_task.cancel()
        if self.socket:
            self.socket.close()

class UDPConnection(Connection):
    """UDP connection (connectionless)"""
    
    def __init__(self, socket, peer_addr):
        self.socket = socket
        self.peer_addr = peer_addr
        self._closed = False
    
    async def send_message(self, message: Dict[str, Any]) -> bool:
        if self._closed:
            return False
        
        try:
            data = json.dumps(message).encode('utf-8')
            loop = asyncio.get_event_loop()
            await loop.sock_sendto(self.socket, data, self.peer_addr)
            return True
        except Exception as e:
            logging.error(f"UDP send failed: {e}")
            return False
    
    async def receive_message(self, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """Note: UDP is connectionless, so this is not peer-specific"""
        # This would need to be implemented differently for UDP
        # For now, return None as UDP connections are handled at transport level
        return None
    
    async def close(self):
        self._closed = True

class TransportManager:
    """Manages multiple transport protocols"""
    
    def __init__(self):
        self.transports = {
            'tcp': TCPTransport(),
            'udp': UDPTransport(),
        }
        self.active_transports = set()
        self.discovered_peers = {}
        
    async def start_all(self) -> bool:
        """Start all available transports"""
        success = False
        
        for name, transport in self.transports.items():
            try:
                if await transport.start_server():
                    self.active_transports.add(name)
                    success = True
                    logging.info(f"Started {name} transport")
            except Exception as e:
                logging.error(f"Failed to start {name} transport: {e}")
        
        return success
    
    async def discover_all_peers(self, timeout: float = 10.0) -> List[PeerInfo]:
        """Discover peers across all transports"""
        all_peers = []
        
        for name in self.active_transports:
            try:
                peers = await self.transports[name].discover_peers(timeout / len(self.active_transports))
                all_peers.extend(peers)
                logging.info(f"Found {len(peers)} peers via {name}")
            except Exception as e:
                logging.error(f"Discovery failed for {name}: {e}")
        
        # Update cache
        for peer in all_peers:
            self.discovered_peers[peer.peer_id] = peer
        
        return all_peers
    
    async def connect_best_peer(self, peer_id: str) -> Optional[Connection]:
        """Connect to a peer using the best available transport"""
        if peer_id not in self.discovered_peers:
            return None
        
        peer = self.discovered_peers[peer_id]
        transport_name = peer.transport_type
        
        if transport_name in self.active_transports:
            return await self.transports[transport_name].connect_peer(peer)
        
        return None
    
    async def stop_all(self):
        """Stop all transports"""
        for name in self.active_transports:
            try:
                await self.transports[name].stop()
                logging.info(f"Stopped {name} transport")
            except Exception as e:
                logging.error(f"Error stopping {name}: {e}")
        
        self.active_transports.clear()
        self.discovered_peers.clear()

# Example usage
async def main():
    logging.basicConfig(level=logging.INFO)
    
    manager = TransportManager()
    
    # Start transports
    if await manager.start_all():
        print("Transports started successfully")
        
        # Discover peers
        peers = await manager.discover_all_peers()
        print(f"Found {len(peers)} peers:")
        for peer in peers:
            print(f"  {peer.peer_id} via {peer.transport_type}")
        
        # Try connecting to first peer
        if peers:
            conn = await manager.connect_best_peer(peers[0].peer_id)
            if conn:
                # Send a test message
                await conn.send_message({"type": "hello", "message": "Hello from DittoFS!"})
                response = await conn.receive_message(timeout=5.0)
                print(f"Response: {response}")
                await conn.close()
    
    await manager.stop_all()

if __name__ == "__main__":
    asyncio.run(main())
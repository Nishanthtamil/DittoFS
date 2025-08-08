import asyncio
import logging
import json
import time
import socket
import threading
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

@dataclass
class PeerInfo:
    peer_id: str
    transport_type: str
    address: str
    port: int
    last_seen: float
    metadata: Dict[str, Any]

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
    async def connect_peer(self, peer: PeerInfo) -> Optional[Connection]:
        """Connect to a specific peer"""
        pass
    
    @abstractmethod
    async def stop(self):
        """Stop the transport"""
        pass

class TCPConnection(Connection):
    """TCP connection implementation"""
    
    def __init__(self, socket_obj, reader=None, writer=None):
        self.socket = socket_obj
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
            logging.debug("Message receive timeout")
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
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        elif self.socket:
            try:
                self.socket.close()
            except Exception:
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
        self.is_running = False
        
    async def start_server(self, port: int = None) -> bool:
        try:
            # Try to import zeroconf
            try:
                from zeroconf import Zeroconf, ServiceInfo
            except ImportError:
                logging.error("zeroconf library required: pip install zeroconf")
                return False
            
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
            self.is_running = True
            
            # Register mDNS service
            self.zeroconf = Zeroconf()
            hostname = socket.gethostname()
            
            # Get local IP address
            try:
                # Connect to a remote address to determine local IP
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                    s.connect(("8.8.8.8", 80))
                    local_ip = s.getsockname()[0]
            except Exception:
                local_ip = "127.0.0.1"
            
            self.service_info = ServiceInfo(
                "_dittofs._tcp.local.",
                f"DittoFS-{hostname}._dittofs._tcp.local.",
                addresses=[socket.inet_aton(local_ip)],
                port=self.port,
                properties={'version': '1.0'},
                server=f"{hostname}.local."
            )
            await self.zeroconf.async_register_service(self.service_info)
            
            logging.info(f"TCP server started on {local_ip}:{self.port}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to start TCP server: {e}")
            return False
    
    async def _accept_connections(self):
        """Accept incoming connections"""
        loop = asyncio.get_event_loop()
        while self.is_running:
            try:
                client_socket, addr = await loop.sock_accept(self.server_socket)
                conn = TCPConnection(client_socket)
                self.connections.append(conn)
                logging.info(f"New TCP connection from {addr}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self.is_running:
                    logging.error(f"Error accepting connection: {e}")
                await asyncio.sleep(1)
    
    async def discover_peers(self, timeout: float = 10.0) -> List[PeerInfo]:
        """Discover peers via mDNS"""
        try:
            from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser, AsyncServiceListener
        except ImportError:
            logging.error("zeroconf library required for discovery")
            return []
        
        peers = []
        discovery_event = asyncio.Event()
        
        class PeerListener(AsyncServiceListener):
            def add_service(self, zc, type_, name):
                try:
                    info = zc.get_service_info(type_, name)
                    if info and info.port != self.port:  # Don't discover ourselves
                        address = socket.inet_ntoa(info.addresses[0])
                        peer = PeerInfo(
                            peer_id=f"tcp-{name}",
                            transport_type="tcp",
                            address=address,
                            port=info.port,
                            last_seen=time.time(),
                            metadata=dict(info.properties) if info.properties else {}
                        )
                        peers.append(peer)
                except Exception as e:
                    logging.warning(f"Error processing service {name}: {e}")
            
            def remove_service(self, zc, type_, name):
                pass
            
            def update_service(self, zc, type_, name):
                pass
        
        zc = AsyncZeroconf()
        listener = PeerListener()
        browser = AsyncServiceBrowser(zc.zeroconf, "_dittofs._tcp.local.", listener)
        
        # Wait for discovery
        await asyncio.sleep(min(timeout, 5.0))
        
        # Cleanup
        await browser.async_cancel()
        await zc.async_close()
        
        logging.info(f"Discovered {len(peers)} TCP peers")
        return peers
    
    async def connect_peer(self, peer: PeerInfo) -> Optional[Connection]:
        """Connect to a TCP peer"""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer.address, peer.port),
                timeout=10.0
            )
            return TCPConnection(writer.get_extra_info('socket'), reader, writer)
        except Exception as e:
            logging.error(f"Failed to connect to {peer.peer_id}: {e}")
            return None
    
    async def stop(self):
        """Stop TCP transport"""
        self.is_running = False
        
        if self.server_task:
            self.server_task.cancel()
            try:
                await self.server_task
            except asyncio.CancelledError:
                pass
        
        if self.server_socket:
            self.server_socket.close()
        
        if self.zeroconf and self.service_info:
            try:
                await self.zeroconf.async_unregister_service(self.service_info)
                self.zeroconf.close()
            except Exception as e:
                logging.warning(f"Error stopping zeroconf: {e}")
        
        # Close all connections
        for conn in self.connections:
            await conn.close()
        self.connections.clear()

class TransportManager:
    """Manages multiple transport protocols"""
    
    def __init__(self):
        self.transports = {
            'tcp': TCPTransport(),
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
                peers = await self.transports[name].discover_peers(
                    timeout / max(len(self.active_transports), 1)
                )
                all_peers.extend(peers)
                logging.info(f"Found {len(peers)} peers via {name}")
            except Exception as e:
                logging.error(f"Discovery failed for {name}: {e}")
        
        # Update cache and remove duplicates
        unique_peers = {}
        for peer in all_peers:
            unique_peers[peer.peer_id] = peer
        
        self.discovered_peers.update(unique_peers)
        return list(unique_peers.values())
    
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
        for name in list(self.active_transports):
            try:
                await self.transports[name].stop()
                logging.info(f"Stopped {name} transport")
            except Exception as e:
                logging.error(f"Error stopping {name}: {e}")
        
        self.active_transports.clear()
        self.discovered_peers.clear()
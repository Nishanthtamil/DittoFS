# src/dittofs/webrtc_transport.py
import asyncio
import logging
import json
import time
import socket
import hashlib
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass

try:
    from aiortc import RTCPeerConnection, RTCSessionDescription, RTCDataChannel
    from aiortc import RTCIceCandidate
    WEBRTC_AVAILABLE = True
except ImportError:
    WEBRTC_AVAILABLE = False
    logging.warning("WebRTC not available - install aiortc: pip install aiortc")

from .transport_manager import TransportProtocol, Connection, PeerInfo

class WebRTCSignalingServer:
    """Simple signaling server for WebRTC peer discovery"""
    
    def __init__(self, port: int = 9999):
        self.port = port
        self.peers = {}  # peer_id -> websocket
        self.server = None
        
    async def start(self):
        """Start the signaling server"""
        try:
            import websockets
            
            async def handle_client(websocket, path):
                peer_id = None
                try:
                    async for message in websocket:
                        data = json.loads(message)
                        msg_type = data.get('type')
                        
                        if msg_type == 'register':
                            peer_id = data.get('peer_id')
                            self.peers[peer_id] = websocket
                            await websocket.send(json.dumps({
                                'type': 'registered',
                                'peer_id': peer_id
                            }))
                            
                        elif msg_type in ['offer', 'answer', 'ice-candidate']:
                            target_peer = data.get('target')
                            if target_peer in self.peers:
                                await self.peers[target_peer].send(message)
                                
                except Exception as e:
                    logging.error(f"Signaling client error: {e}")
                finally:
                    if peer_id and peer_id in self.peers:
                        del self.peers[peer_id]
            
            self.server = await websockets.serve(
                handle_client, 
                "localhost", 
                self.port
            )
            logging.info(f"WebRTC signaling server started on port {self.port}")
            return True
            
        except ImportError:
            logging.error("websockets library required for WebRTC signaling")
            return False
        except Exception as e:
            logging.error(f"Failed to start signaling server: {e}")
            return False
    
    async def stop(self):
        """Stop the signaling server"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()

class WebRTCTransport(TransportProtocol):
    """WebRTC-based transport for direct P2P connections"""
    
    def __init__(self, signaling_server_url: str = None):
        self.signaling_server_url = signaling_server_url or "ws://localhost:9999"
        self.peer_id = f"dittofs-{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"
        self.signaling_ws = None
        self.peer_connections = {}  # peer_id -> RTCPeerConnection
        self.data_channels = {}  # peer_id -> RTCDataChannel
        self.discovered_peers = {}
        self.is_running = False
        self.message_handlers = {}  # peer_id -> callback
        
    async def start_server(self, port: int = None) -> bool:
        """Start WebRTC transport"""
        if not WEBRTC_AVAILABLE:
            logging.error("WebRTC not available - install aiortc")
            return False
        
        try:
            # Connect to signaling server
            import websockets
            
            self.signaling_ws = await websockets.connect(self.signaling_server_url)
            
            # Register with signaling server
            await self.signaling_ws.send(json.dumps({
                'type': 'register',
                'peer_id': self.peer_id
            }))
            
            # Start signaling message handler
            asyncio.create_task(self._handle_signaling_messages())
            
            self.is_running = True
            logging.info(f"WebRTC transport started with peer ID: {self.peer_id}")
            return True
            
        except ImportError:
            logging.error("websockets library required for WebRTC signaling")
            return False
        except Exception as e:
            logging.error(f"Failed to start WebRTC transport: {e}")
            return False
    
    async def _handle_signaling_messages(self):
        """Handle signaling messages from server"""
        try:
            async for message in self.signaling_ws:
                data = json.loads(message)
                msg_type = data.get('type')
                
                if msg_type == 'offer':
                    await self._handle_offer(data)
                elif msg_type == 'answer':
                    await self._handle_answer(data)
                elif msg_type == 'ice-candidate':
                    await self._handle_ice_candidate(data)
                elif msg_type == 'peer-list':
                    self._update_peer_list(data.get('peers', []))
                    
        except Exception as e:
            logging.error(f"Signaling handler error: {e}")
    
    async def _handle_offer(self, data):
        """Handle incoming WebRTC offer"""
        try:
            source_peer = data.get('source')
            offer_sdp = data.get('sdp')
            
            if not source_peer or not offer_sdp:
                return
            
            # Create peer connection
            pc = RTCPeerConnection()
            self.peer_connections[source_peer] = pc
            
            # Set up data channel handler
            @pc.on("datachannel")
            def on_datachannel(channel):
                self.data_channels[source_peer] = channel
                self._setup_data_channel(channel, source_peer)
            
            # Set remote description
            await pc.setRemoteDescription(RTCSessionDescription(offer_sdp, "offer"))
            
            # Create answer
            answer = await pc.createAnswer()
            await pc.setLocalDescription(answer)
            
            # Send answer via signaling
            await self.signaling_ws.send(json.dumps({
                'type': 'answer',
                'target': source_peer,
                'source': self.peer_id,
                'sdp': pc.localDescription.sdp
            }))
            
            logging.info(f"Created WebRTC answer for {source_peer}")
            
        except Exception as e:
            logging.error(f"Error handling offer: {e}")
    
    async def _handle_answer(self, data):
        """Handle incoming WebRTC answer"""
        try:
            source_peer = data.get('source')
            answer_sdp = data.get('sdp')
            
            if source_peer in self.peer_connections and answer_sdp:
                pc = self.peer_connections[source_peer]
                await pc.setRemoteDescription(RTCSessionDescription(answer_sdp, "answer"))
                logging.info(f"Received WebRTC answer from {source_peer}")
                
        except Exception as e:
            logging.error(f"Error handling answer: {e}")
    
    async def _handle_ice_candidate(self, data):
        """Handle incoming ICE candidate"""
        try:
            source_peer = data.get('source')
            candidate_data = data.get('candidate')
            
            if source_peer in self.peer_connections and candidate_data:
                pc = self.peer_connections[source_peer]
                candidate = RTCIceCandidate(
                    candidate_data.get('candidate'),
                    candidate_data.get('sdpMLineIndex')
                )
                await pc.addIceCandidate(candidate)
                
        except Exception as e:
            logging.error(f"Error handling ICE candidate: {e}")
    
    def _setup_data_channel(self, channel: RTCDataChannel, peer_id: str):
        """Set up data channel event handlers"""
        
        @channel.on("open")
        def on_open():
            logging.info(f"WebRTC data channel opened with {peer_id}")
        
        @channel.on("message")
        def on_message(message):
            try:
                if isinstance(message, str):
                    data = json.loads(message)
                    # Handle incoming message
                    if peer_id in self.message_handlers:
                        asyncio.create_task(self.message_handlers[peer_id](data))
            except Exception as e:
                logging.error(f"Error handling data channel message: {e}")
        
        @channel.on("close")
        def on_close():
            logging.info(f"WebRTC data channel closed with {peer_id}")
            if peer_id in self.data_channels:
                del self.data_channels[peer_id]
    
    async def discover_peers(self, timeout: float = 10.0) -> List[PeerInfo]:
        """Discover WebRTC peers via signaling server"""
        if not self.is_running:
            return []
        
        try:
            # Request peer list from signaling server
            await self.signaling_ws.send(json.dumps({
                'type': 'list-peers',
                'peer_id': self.peer_id
            }))
            
            # Wait a bit for response
            await asyncio.sleep(2.0)
            
            # Return discovered peers
            peers = []
            for peer_id in self.discovered_peers:
                if peer_id != self.peer_id:  # Don't include ourselves
                    peers.append(PeerInfo(
                        peer_id=peer_id,
                        transport_type="webrtc",
                        address="",  # WebRTC doesn't use addresses
                        port=0,
                        last_seen=time.time(),
                        metadata={}
                    ))
            
            return peers
            
        except Exception as e:
            logging.error(f"WebRTC peer discovery failed: {e}")
            return []
    
    def _update_peer_list(self, peers: List[str]):
        """Update discovered peers list"""
        self.discovered_peers = {peer: time.time() for peer in peers}
    
    async def connect_peer(self, peer: PeerInfo) -> Optional[Connection]:
        """Initiate WebRTC connection to peer"""
        try:
            peer_id = peer.peer_id
            
            if peer_id in self.data_channels:
                # Already connected
                return WebRTCConnection(self.data_channels[peer_id], peer_id)
            
            # Create peer connection
            pc = RTCPeerConnection()
            self.peer_connections[peer_id] = pc
            
            # Create data channel
            channel = pc.createDataChannel("dittofs", ordered=True)
            self.data_channels[peer_id] = channel
            self._setup_data_channel(channel, peer_id)
            
            # Set up ICE candidate handler
            @pc.on("icecandidate")
            def on_icecandidate(candidate):
                if candidate:
                    asyncio.create_task(self.signaling_ws.send(json.dumps({
                        'type': 'ice-candidate',
                        'target': peer_id,
                        'source': self.peer_id,
                        'candidate': {
                            'candidate': candidate.candidate,
                            'sdpMLineIndex': candidate.sdpMLineIndex
                        }
                    })))
            
            # Create offer
            offer = await pc.createOffer()
            await pc.setLocalDescription(offer)
            
            # Send offer via signaling
            await self.signaling_ws.send(json.dumps({
                'type': 'offer',
                'target': peer_id,
                'source': self.peer_id,
                'sdp': pc.localDescription.sdp
            }))
            
            # Wait for connection to establish
            for _ in range(50):  # 5 seconds timeout
                if channel.readyState == "open":
                    break
                await asyncio.sleep(0.1)
            
            if channel.readyState == "open":
                logging.info(f"WebRTC connection established with {peer_id}")
                return WebRTCConnection(channel, peer_id)
            else:
                logging.error(f"WebRTC connection timeout with {peer_id}")
                return None
                
        except Exception as e:
            logging.error(f"Failed to connect to WebRTC peer {peer.peer_id}: {e}")
            return None
    
    async def stop(self):
        """Stop WebRTC transport"""
        self.is_running = False
        
        # Close all peer connections
        for peer_id, pc in self.peer_connections.items():
            try:
                await pc.close()
            except:
                pass
        
        # Close signaling connection
        if self.signaling_ws:
            try:
                await self.signaling_ws.close()
            except:
                pass
        
        self.peer_connections.clear()
        self.data_channels.clear()
        self.discovered_peers.clear()

class WebRTCConnection(Connection):
    """WebRTC data channel connection"""
    
    def __init__(self, channel: RTCDataChannel, peer_id: str):
        self.channel = channel
        self.peer_id = peer_id
        self.message_queue = asyncio.Queue()
        self._closed = False
        
        # Set up message handler
        @self.channel.on("message")
        def on_message(message):
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                    self.message_queue.put_nowait(data)
                except (json.JSONDecodeError, asyncio.QueueFull) as e:
                    logging.error(f"WebRTC message error: {e}")
    
    async def send_message(self, message: Dict[str, Any]) -> bool:
        """Send message via WebRTC data channel"""
        if self._closed or self.channel.readyState != "open":
            return False
        
        try:
            data = json.dumps(message)
            self.channel.send(data)
            return True
        except Exception as e:
            logging.error(f"WebRTC send failed: {e}")
            await self.close()
            return False
    
    async def receive_message(self, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """Receive message via WebRTC data channel"""
        if self._closed:
            return None
        
        try:
            message = await asyncio.wait_for(
                self.message_queue.get(),
                timeout=timeout
            )
            return message
        except asyncio.TimeoutError:
            logging.debug("WebRTC receive timeout")
            return None
        except Exception as e:
            logging.error(f"WebRTC receive error: {e}")
            await self.close()
            return None
    
    async def close(self):
        """Close WebRTC connection"""
        if self._closed:
            return
        
        self._closed = True
        # WebRTC data channel will be closed when peer connection closes

# Integration function
def add_webrtc_to_transport_manager(transport_manager, signaling_url: str = None):
    """Add WebRTC transport to transport manager"""
    if WEBRTC_AVAILABLE:
        webrtc_transport = WebRTCTransport(signaling_url)
        transport_manager.transports['webrtc'] = webrtc_transport
        logging.info("WebRTC transport added to manager")
    else:
        logging.warning("WebRTC not available - install aiortc and websockets")

# Standalone WebRTC signaling server
class StandaloneSignalingServer:
    """Standalone signaling server that can be run separately"""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 9999):
        self.host = host
        self.port = port
        self.peers = {}
        self.rooms = {}  # For organizing peers into rooms/groups
        
    async def start(self):
        """Start the signaling server"""
        try:
            import websockets
            
            async def handle_peer(websocket, path):
                peer_id = None
                room = "default"
                
                try:
                    await websocket.send(json.dumps({
                        'type': 'welcome',
                        'message': 'Connected to DittoFS signaling server'
                    }))
                    
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            await self._handle_message(websocket, data, peer_id, room)
                            
                            # Update peer_id and room from message handling
                            if data.get('type') == 'join':
                                peer_id = data.get('peer_id')
                                room = data.get('room', 'default')
                                
                        except json.JSONDecodeError:
                            await websocket.send(json.dumps({
                                'type': 'error',
                                'message': 'Invalid JSON message'
                            }))
                            
                except websockets.exceptions.ConnectionClosed:
                    pass
                except Exception as e:
                    logging.error(f"Peer handler error: {e}")
                finally:
                    # Clean up peer
                    if peer_id:
                        self._remove_peer(peer_id, room)
            
            server = await websockets.serve(handle_peer, self.host, self.port)
            logging.info(f"WebRTC signaling server running on {self.host}:{self.port}")
            
            # Keep server running
            await server.wait_closed()
            
        except ImportError:
            logging.error("websockets library required for signaling server")
        except Exception as e:
            logging.error(f"Signaling server error: {e}")
    
    async def _handle_message(self, websocket, data, current_peer_id, current_room):
        """Handle incoming signaling message"""
        msg_type = data.get('type')
        
        if msg_type == 'join':
            # Peer wants to join a room
            peer_id = data.get('peer_id')
            room = data.get('room', 'default')
            
            if not peer_id:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': 'peer_id required'
                }))
                return
            
            # Add peer to room
            if room not in self.rooms:
                self.rooms[room] = {}
            
            self.rooms[room][peer_id] = websocket
            self.peers[peer_id] = {'websocket': websocket, 'room': room}
            
            # Notify peer of successful join
            await websocket.send(json.dumps({
                'type': 'joined',
                'peer_id': peer_id,
                'room': room,
                'peers': list(self.rooms[room].keys())
            }))
            
            # Notify other peers in room
            await self._broadcast_to_room(room, {
                'type': 'peer_joined',
                'peer_id': peer_id
            }, exclude=peer_id)
            
            logging.info(f"Peer {peer_id} joined room {room}")
            
        elif msg_type == 'list_peers':
            # Return list of peers in current room
            room = current_room
            if room in self.rooms:
                peers_list = [pid for pid in self.rooms[room].keys() if pid != current_peer_id]
                await websocket.send(json.dumps({
                    'type': 'peer_list',
                    'peers': peers_list
                }))
            
        elif msg_type in ['offer', 'answer', 'ice_candidate']:
            # Forward signaling message to target peer
            target_peer = data.get('target')
            
            if target_peer and target_peer in self.peers:
                target_ws = self.peers[target_peer]['websocket']
                # Add source peer ID
                data['source'] = current_peer_id
                await target_ws.send(json.dumps(data))
            else:
                await websocket.send(json.dumps({
                    'type': 'error',
                    'message': f'Peer {target_peer} not found'
                }))
        
        elif msg_type == 'ping':
            # Health check
            await websocket.send(json.dumps({
                'type': 'pong',
                'timestamp': time.time()
            }))
    
    async def _broadcast_to_room(self, room: str, message: dict, exclude: str = None):
        """Broadcast message to all peers in a room"""
        if room not in self.rooms:
            return
        
        message_json = json.dumps(message)
        
        for peer_id, websocket in self.rooms[room].items():
            if peer_id != exclude:
                try:
                    await websocket.send(message_json)
                except Exception as e:
                    logging.error(f"Failed to send to {peer_id}: {e}")
                    # Mark for cleanup
                    self._remove_peer(peer_id, room)
    
    def _remove_peer(self, peer_id: str, room: str):
        """Remove peer from tracking"""
        if peer_id in self.peers:
            del self.peers[peer_id]
        
        if room in self.rooms and peer_id in self.rooms[room]:
            del self.rooms[room][peer_id]
            
            # Clean up empty rooms
            if not self.rooms[room]:
                del self.rooms[room]
            else:
                # Notify other peers
                asyncio.create_task(self._broadcast_to_room(room, {
                    'type': 'peer_left',
                    'peer_id': peer_id
                }))

# Enhanced WebRTC transport with NAT traversal
class EnhancedWebRTCTransport(WebRTCTransport):
    """Enhanced WebRTC transport with STUN/TURN support"""
    
    def __init__(self, signaling_server_url: str = None, stun_servers: List[str] = None, turn_servers: List[Dict] = None):
        super().__init__(signaling_server_url)
        
        # Default STUN servers (public Google STUN servers)
        self.stun_servers = stun_servers or [
            "stun:stun.l.google.com:19302",
            "stun:stun1.l.google.com:19302"
        ]
        
        # TURN servers for NAT traversal (would need credentials in production)
        self.turn_servers = turn_servers or []
        
    def _create_peer_connection(self) -> 'RTCPeerConnection':
        """Create RTCPeerConnection with STUN/TURN servers"""
        from aiortc import RTCConfiguration, RTCIceServer
        
        ice_servers = []
        
        # Add STUN servers
        for stun_url in self.stun_servers:
            ice_servers.append(RTCIceServer(urls=[stun_url]))
        
        # Add TURN servers
        for turn_config in self.turn_servers:
            ice_servers.append(RTCIceServer(
                urls=[turn_config['url']],
                username=turn_config.get('username'),
                credential=turn_config.get('password')
            ))
        
        config = RTCConfiguration(iceServers=ice_servers)
        return RTCPeerConnection(configuration=config)
    
    async def _handle_offer(self, data):
        """Enhanced offer handling with proper peer connection setup"""
        try:
            source_peer = data.get('source')
            offer_sdp = data.get('sdp')
            
            if not source_peer or not offer_sdp:
                return
            
            # Create peer connection with STUN/TURN
            pc = self._create_peer_connection()
            self.peer_connections[source_peer] = pc
            
            # Set up ICE candidate handler
            @pc.on("icecandidate")
            def on_icecandidate(candidate):
                if candidate and self.signaling_ws:
                    asyncio.create_task(self.signaling_ws.send(json.dumps({
                        'type': 'ice_candidate',
                        'target': source_peer,
                        'source': self.peer_id,
                        'candidate': {
                            'candidate': candidate.candidate,
                            'sdpMLineIndex': candidate.sdpMLineIndex,
                            'sdpMid': candidate.sdpMid
                        }
                    })))
            
            # Set up data channel handler
            @pc.on("datachannel")
            def on_datachannel(channel):
                self.data_channels[source_peer] = channel
                self._setup_data_channel(channel, source_peer)
            
            # Set remote description
            await pc.setRemoteDescription(RTCSessionDescription(offer_sdp, "offer"))
            
            # Create and set local description
            answer = await pc.createAnswer()
            await pc.setLocalDescription(answer)
            
            # Send answer
            if self.signaling_ws:
                await self.signaling_ws.send(json.dumps({
                    'type': 'answer',
                    'target': source_peer,
                    'source': self.peer_id,
                    'sdp': pc.localDescription.sdp
                }))
            
            logging.info(f"Created enhanced WebRTC answer for {source_peer}")
            
        except Exception as e:
            logging.error(f"Enhanced offer handling error: {e}")

# Command-line interface for signaling server
async def run_signaling_server():
    """Run standalone signaling server"""
    import argparse
    
    parser = argparse.ArgumentParser(description="DittoFS WebRTC Signaling Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=9999, help="Port to bind to")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    server = StandaloneSignalingServer(args.host, args.port)
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logging.info("Signaling server stopped")

# Test WebRTC functionality
async def test_webrtc_with_signaling():
    """Test WebRTC transport with local signaling server"""
    logging.basicConfig(level=logging.INFO)
    
    # Start local signaling server in background
    signaling_server = StandaloneSignalingServer("localhost", 9999)
    server_task = asyncio.create_task(signaling_server.start())
    
    # Give server time to start
    await asyncio.sleep(1)
    
    try:
        # Create two WebRTC transports
        transport1 = EnhancedWebRTCTransport("ws://localhost:9999")
        transport2 = EnhancedWebRTCTransport("ws://localhost:9999")
        
        # Start both transports
        await transport1.start_server()
        await transport2.start_server()
        
        # Give time for registration
        await asyncio.sleep(2)
        
        # Discover peers from transport1
        peers = await transport1.discover_peers()
        print(f"Transport1 discovered {len(peers)} peers")
        
        if peers:
            # Connect to first peer
            peer = peers[0]
            print(f"Connecting to {peer.peer_id}...")
            
            conn = await transport1.connect_peer(peer)
            if conn:
                print("Connected! Testing message exchange...")
                
                # Send test message
                await conn.send_message({
                    "type": "test",
                    "message": "Hello from WebRTC!",
                    "timestamp": time.time()
                })
                
                print("Message sent successfully")
                await conn.close()
            else:
                print("Failed to connect")
        
        # Cleanup
        await transport1.stop()
        await transport2.stop()
        
    except Exception as e:
        logging.error(f"WebRTC test failed: {e}")
    finally:
        server_task.cancel()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "server":
        # Run signaling server
        asyncio.run(run_signaling_server())
    else:
        # Run test
        asyncio.run(test_webrtc_with_signaling())
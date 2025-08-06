# src/dittofs/ble_transport.py
import asyncio
import logging
import json
import time
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

try:
    from bleak import BleakClient, BleakScanner, BleakGATTCharacteristic
    from bleak.backends.service import BleakGATTService
    BLEAK_AVAILABLE = True
except ImportError:
    BLEAK_AVAILABLE = False
    logging.warning("Bleak not available - BLE transport disabled")

# Try alternative BLE libraries
try:
    import bluetooth
    PYBLUEZ_AVAILABLE = True
except ImportError:
    PYBLUEZ_AVAILABLE = False

from .transport_manager import TransportProtocol, Connection, PeerInfo

# DittoFS BLE Service and Characteristic UUIDs
DITTOFS_SERVICE_UUID = "6E400001-B5A3-F393-E0A9-E50E24DCCA9E"  # Nordic UART Service UUID
DITTOFS_TX_CHAR_UUID = "6E400002-B5A3-F393-E0A9-E50E24DCCA9E"  # TX Characteristic
DITTOFS_RX_CHAR_UUID = "6E400003-B5A3-F393-E0A9-E50E24DCCA9E"  # RX Characteristic

class BLETransport(TransportProtocol):
    """BLE transport using Bleak library"""
    
    def __init__(self):
        self.is_advertising = False
        self.discovered_devices = {}
        self.server_task = None
        self.advertisement_data = None
        
    async def start_server(self, port: int = None) -> bool:
        """Start BLE advertising (server mode)"""
        if not BLEAK_AVAILABLE:
            logging.error("BLE not available - Bleak library not installed")
            return False
        
        try:
            # For BLE, we can't easily create a server with Bleak on most platforms
            # Instead, we'll use a scanning-based approach where devices discover each other
            logging.info("BLE transport started in discovery mode")
            self.is_advertising = True
            
            # Start periodic scanning for other devices
            self.server_task = asyncio.create_task(self._periodic_scan())
            return True
            
        except Exception as e:
            logging.error(f"Failed to start BLE server: {e}")
            return False
    
    async def _periodic_scan(self):
        """Periodically scan for BLE devices"""
        while self.is_advertising:
            try:
                await self._scan_once()
                await asyncio.sleep(30)  # Scan every 30 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"BLE scan error: {e}")
                await asyncio.sleep(60)  # Wait longer on error
    
    async def _scan_once(self):
        """Perform one BLE scan"""
        try:
            devices = await BleakScanner.discover(timeout=10.0, return_adv=True)
            
            found_count = 0
            for device, advertisement_data in devices.values():
                # Look for DittoFS devices
                if self._is_dittofs_device(device, advertisement_data):
                    peer_id = f"ble-{device.address}"
                    self.discovered_devices[peer_id] = PeerInfo(
                        peer_id=peer_id,
                        transport_type="ble",
                        address=device.address,
                        port=0,  # BLE doesn't use ports
                        last_seen=time.time(),
                        metadata={
                            "name": device.name or "Unknown",
                            "rssi": advertisement_data.rssi if advertisement_data else -999
                        }
                    )
                    found_count += 1
            
            if found_count > 0:
                logging.info(f"BLE scan found {found_count} DittoFS devices")
                
        except Exception as e:
            logging.error(f"BLE scan failed: {e}")
    
    def _is_dittofs_device(self, device, advertisement_data) -> bool:
        """Check if a BLE device is a DittoFS device"""
        # Check device name
        if device.name and "DittoFS" in device.name:
            return True
        
        # Check service UUIDs
        if advertisement_data and advertisement_data.service_uuids:
            return DITTOFS_SERVICE_UUID in advertisement_data.service_uuids
        
        # Check manufacturer data or other indicators
        if advertisement_data and advertisement_data.manufacturer_data:
            # You could add custom manufacturer data here
            pass
        
        return False
    
    async def discover_peers(self, timeout: float = 10.0) -> List[PeerInfo]:
        """Discover BLE peers"""
        if not BLEAK_AVAILABLE:
            return []
        
        # Perform fresh scan
        await self._scan_once()
        
        # Clean old discoveries
        current_time = time.time()
        self.discovered_devices = {
            k: v for k, v in self.discovered_devices.items()
            if current_time - v.last_seen < 300  # 5 minute timeout
        }
        
        return list(self.discovered_devices.values())
    
    async def connect_peer(self, peer: PeerInfo) -> Optional[Connection]:
        """Connect to a BLE peer"""
        if not BLEAK_AVAILABLE:
            return None
        
        try:
            client = BleakClient(peer.address)
            await client.connect(timeout=15.0)
            
            # Verify the device has our service
            services = await client.get_services()
            dittofs_service = None
            
            for service in services:
                if service.uuid.lower() == DITTOFS_SERVICE_UUID.lower():
                    dittofs_service = service
                    break
            
            if not dittofs_service:
                logging.warning(f"Device {peer.address} doesn't have DittoFS service")
                await client.disconnect()
                return None
            
            return BLEConnection(client, dittofs_service)
            
        except Exception as e:
            logging.error(f"Failed to connect to BLE peer {peer.address}: {e}")
            return None
    
    async def stop(self):
        """Stop BLE transport"""
        self.is_advertising = False
        
        if self.server_task:
            self.server_task.cancel()
            try:
                await self.server_task
            except asyncio.CancelledError:
                pass
        
        self.discovered_devices.clear()

class BLEConnection(Connection):
    """BLE connection using GATT characteristics"""
    
    def __init__(self, client: 'BleakClient', service: 'BleakGATTService'):
        self.client = client
        self.service = service
        self.tx_char = None
        self.rx_char = None
        self.receive_buffer = bytearray()
        self.message_queue = asyncio.Queue()
        self._closed = False
        
        # Find TX/RX characteristics
        for char in service.characteristics:
            if char.uuid.lower() == DITTOFS_TX_CHAR_UUID.lower():
                self.tx_char = char
            elif char.uuid.lower() == DITTOFS_RX_CHAR_UUID.lower():
                self.rx_char = char
        
        # Start notification handler
        if self.rx_char:
            asyncio.create_task(self._setup_notifications())
    
    async def _setup_notifications(self):
        """Setup notifications for receiving data"""
        try:
            await self.client.start_notify(self.rx_char.uuid, self._notification_handler)
        except Exception as e:
            logging.error(f"Failed to setup BLE notifications: {e}")
    
    def _notification_handler(self, sender: BleakGATTCharacteristic, data: bytearray):
        """Handle incoming BLE notifications"""
        try:
            self.receive_buffer.extend(data)
            
            # Try to parse complete messages
            while len(self.receive_buffer) >= 4:
                # Read message length (first 4 bytes)
                msg_length = int.from_bytes(self.receive_buffer[:4], 'big')
                
                if len(self.receive_buffer) >= 4 + msg_length:
                    # We have a complete message
                    msg_data = self.receive_buffer[4:4 + msg_length]
                    self.receive_buffer = self.receive_buffer[4 + msg_length:]
                    
                    try:
                        message = json.loads(msg_data.decode('utf-8'))
                        # Put message in queue (non-blocking)
                        try:
                            self.message_queue.put_nowait(message)
                        except asyncio.QueueFull:
                            logging.warning("BLE message queue full, dropping message")
                    except json.JSONDecodeError as e:
                        logging.error(f"Invalid JSON in BLE message: {e}")
                else:
                    # Wait for more data
                    break
                    
        except Exception as e:
            logging.error(f"BLE notification handler error: {e}")
    
    async def send_message(self, message: Dict[str, Any]) -> bool:
        """Send message via BLE"""
        if self._closed or not self.tx_char:
            return False
        
        try:
            # Serialize message
            data = json.dumps(message).encode('utf-8')
            
            # Add length prefix
            length_prefix = len(data).to_bytes(4, 'big')
            full_data = length_prefix + data
            
            # BLE has MTU limitations (usually 20-244 bytes)
            # We need to chunk the data
            mtu = 20  # Conservative MTU
            
            for i in range(0, len(full_data), mtu):
                chunk = full_data[i:i + mtu]
                await self.client.write_gatt_char(self.tx_char.uuid, chunk, response=False)
                # Small delay to avoid overwhelming the receiver
                await asyncio.sleep(0.01)
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to send BLE message: {e}")
            await self.close()
            return False
    
    async def receive_message(self, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """Receive message via BLE"""
        if self._closed:
            return None
        
        try:
            message = await asyncio.wait_for(
                self.message_queue.get(), 
                timeout=timeout
            )
            return message
            
        except asyncio.TimeoutError:
            logging.debug("BLE receive timeout")
            return None
        except Exception as e:
            logging.error(f"BLE receive error: {e}")
            await self.close()
            return None
    
    async def close(self):
        """Close BLE connection"""
        if self._closed:
            return
        
        self._closed = True
        
        try:
            if self.rx_char and self.client.is_connected:
                await self.client.stop_notify(self.rx_char.uuid)
        except Exception as e:
            logging.debug(f"Error stopping BLE notifications: {e}")
        
        try:
            if self.client.is_connected:
                await self.client.disconnect()
        except Exception as e:
            logging.debug(f"Error disconnecting BLE client: {e}")

class SimpleBLETransport(TransportProtocol):
    """Simplified BLE transport using PyBluez (Linux only)"""
    
    def __init__(self):
        self.server_socket = None
        self.is_advertising = False
        self.discovered_devices = {}
        
    async def start_server(self, port: int = 1) -> bool:
        """Start BLE server using PyBluez"""
        if not PYBLUEZ_AVAILABLE:
            logging.error("PyBluez not available for BLE transport")
            return False
        
        try:
            import bluetooth
            
            # Create RFCOMM socket
            self.server_socket = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
            self.server_socket.bind(("", port))
            self.server_socket.listen(1)
            
            # Make device discoverable
            bluetooth.advertise_service(
                self.server_socket,
                "DittoFS",
                service_id="1e0ca4ea-299d-4335-93eb-27fcfe7fa848",
                service_classes=[bluetooth.SERIAL_PORT_CLASS],
                profiles=[bluetooth.SERIAL_PORT_PROFILE],
            )
            
            self.is_advertising = True
            logging.info(f"BLE server started on RFCOMM port {port}")
            
            # Start accepting connections
            asyncio.create_task(self._accept_connections())
            return True
            
        except Exception as e:
            logging.error(f"Failed to start PyBluez BLE server: {e}")
            return False
    
    async def _accept_connections(self):
        """Accept incoming BLE connections"""
        loop = asyncio.get_event_loop()
        
        while self.is_advertising:
            try:
                # Accept connection (blocking, so run in thread)
                client_sock, client_info = await loop.run_in_executor(
                    None, self.server_socket.accept
                )
                
                logging.info(f"BLE connection from {client_info}")
                # Handle connection (you'd implement this)
                
            except Exception as e:
                if self.is_advertising:  # Only log if we're still supposed to be running
                    logging.error(f"BLE accept error: {e}")
                break
    
    async def discover_peers(self, timeout: float = 10.0) -> List[PeerInfo]:
        """Discover BLE peers using PyBluez"""
        if not PYBLUEZ_AVAILABLE:
            return []
        
        try:
            import bluetooth
            
            # Run discovery in thread to avoid blocking
            loop = asyncio.get_event_loop()
            nearby_devices = await loop.run_in_executor(
                None, 
                lambda: bluetooth.discover_devices(duration=int(timeout), lookup_names=True)
            )
            
            peers = []
            for addr, name in nearby_devices:
                if name and "DittoFS" in name:
                    peer_id = f"bt-{addr}"
                    peers.append(PeerInfo(
                        peer_id=peer_id,
                        transport_type="bluetooth",
                        address=addr,
                        port=1,  # RFCOMM port
                        last_seen=time.time(),
                        metadata={"name": name}
                    ))
            
            logging.info(f"Bluetooth discovered {len(peers)} DittoFS peers")
            return peers
            
        except Exception as e:
            logging.error(f"Bluetooth discovery failed: {e}")
            return []
    
    async def connect_peer(self, peer: PeerInfo) -> Optional[Connection]:
        """Connect to Bluetooth peer"""
        if not PYBLUEZ_AVAILABLE:
            return None
        
        try:
            import bluetooth
            
            sock = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
            loop = asyncio.get_event_loop()
            
            # Connect in thread
            await loop.run_in_executor(
                None, 
                lambda: sock.connect((peer.address, peer.port))
            )
            
            return SimpleBLEConnection(sock)
            
        except Exception as e:
            logging.error(f"Failed to connect to Bluetooth peer {peer.address}: {e}")
            return None
    
    async def stop(self):
        """Stop BLE transport"""
        self.is_advertising = False
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass

class SimpleBLEConnection(Connection):
    """Simple Bluetooth connection using PyBluez"""
    
    def __init__(self, socket):
        self.socket = socket
        self._closed = False
    
    async def send_message(self, message: Dict[str, Any]) -> bool:
        """Send message over Bluetooth"""
        if self._closed:
            return False
        
        try:
            data = json.dumps(message).encode('utf-8')
            length_prefix = len(data).to_bytes(4, 'big')
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.socket.send(length_prefix + data)
            )
            return True
            
        except Exception as e:
            logging.error(f"Bluetooth send failed: {e}")
            await self.close()
            return False
    
    async def receive_message(self, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """Receive message over Bluetooth"""
        if self._closed:
            return None
        
        try:
            # Set socket timeout
            self.socket.settimeout(timeout)
            
            loop = asyncio.get_event_loop()
            
            # Read length prefix
            length_data = await loop.run_in_executor(
                None,
                lambda: self.socket.recv(4)
            )
            
            if len(length_data) != 4:
                return None
            
            length = int.from_bytes(length_data, 'big')
            
            # Read message data
            data = await loop.run_in_executor(
                None,
                lambda: self.socket.recv(length)
            )
            
            return json.loads(data.decode('utf-8'))
            
        except Exception as e:
            logging.error(f"Bluetooth receive failed: {e}")
            await self.close()
            return None
    
    async def close(self):
        """Close Bluetooth connection"""
        if self._closed:
            return
        
        self._closed = True
        try:
            self.socket.close()
        except:
            pass

# Factory function to choose best BLE transport
def create_ble_transport() -> Optional[TransportProtocol]:
    """Create the best available BLE transport"""
    if BLEAK_AVAILABLE:
        logging.info("Using Bleak BLE transport")
        return BLETransport()
    elif PYBLUEZ_AVAILABLE:
        logging.info("Using PyBluez BLE transport")
        return SimpleBLETransport()
    else:
        logging.warning("No BLE transport available")
        return None

# Integration with transport manager
def add_ble_to_transport_manager(transport_manager):
    """Add BLE transport to transport manager"""
    ble_transport = create_ble_transport()
    if ble_transport:
        transport_manager.transports['ble'] = ble_transport
        logging.info("BLE transport added to manager")
    else:
        logging.warning("No BLE transport available to add")

# Example usage and testing
async def test_ble_transport():
    """Test BLE transport functionality"""
    logging.basicConfig(level=logging.INFO)
    
    ble = create_ble_transport()
    if not ble:
        print("No BLE transport available")
        return
    
    print("Starting BLE transport...")
    if await ble.start_server():
        print("BLE server started")
        
        # Discover peers
        print("Discovering peers...")
        peers = await ble.discover_peers(timeout=10.0)
        print(f"Found {len(peers)} peers:")
        for peer in peers:
            print(f"  {peer.peer_id} - {peer.address}")
            
            # Try connecting to first peer
            if peers:
                peer = peers[0]
                print(f"Connecting to {peer.peer_id}...")
                conn = await ble.connect_peer(peer)
                if conn:
                    print("Connected! Sending test message...")
                    
                    success = await conn.send_message({
                        "type": "hello",
                        "message": "Hello from DittoFS BLE!"
                    })
                    
                    if success:
                        print("Message sent, waiting for response...")
                        response = await conn.receive_message(timeout=10.0)
                        print(f"Response: {response}")
                    
                    await conn.close()
                else:
                    print("Failed to connect")
    
    await ble.stop()
    print("BLE transport stopped")

if __name__ == "__main__":
    asyncio.run(test_ble_transport())
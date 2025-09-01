"""
Mock objects for external dependencies in DittoFS testing.

This module provides comprehensive mocks for network, filesystem, and crypto operations.
"""

import asyncio
import hashlib
import pathlib
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Union
from unittest.mock import AsyncMock, MagicMock, Mock

# Import DittoFS types for proper mocking
try:
    from dittofs.crdt_store import FileRecord
    from dittofs.transport import Connection, PeerInfo, TransportManager
except ImportError:
    # Fallback for when modules don't exist yet
    Connection = object
    PeerInfo = object
    TransportManager = object
    FileRecord = object


# ============================================================================
# Network Mocks
# ============================================================================


@dataclass
class NetworkCondition:
    """Represents network conditions for testing."""

    latency_ms: int = 0
    bandwidth_kbps: Optional[int] = None
    packet_loss_percent: float = 0.0
    jitter_ms: int = 0
    is_connected: bool = True
    connection_failures: List[str] = field(default_factory=list)


class MockConnection:
    """Mock connection for testing network operations."""

    def __init__(
        self, peer_id: str, network_conditions: Optional[NetworkCondition] = None
    ):
        self.peer_id = peer_id
        self.network = network_conditions or NetworkCondition()
        self.is_closed = False
        self.sent_messages = []
        self.received_messages = []
        self.message_queue = asyncio.Queue()
        self.bytes_sent = 0
        self.bytes_received = 0
        self.connection_time = time.time()

    async def send_message(self, message: Dict[str, Any]) -> bool:
        """Mock send message with network simulation."""
        if self.is_closed or not self.network.is_connected:
            return False

        # Simulate packet loss
        if random.random() < (self.network.packet_loss_percent / 100.0):
            return False

        # Simulate network latency
        if self.network.latency_ms > 0:
            jitter = random.randint(0, self.network.jitter_ms) / 1000.0
            delay = (self.network.latency_ms / 1000.0) + jitter
            await asyncio.sleep(delay)

        # Simulate bandwidth limitations
        message_size = len(str(message).encode())
        if self.network.bandwidth_kbps:
            transfer_time = message_size / (self.network.bandwidth_kbps * 1024 / 8)
            await asyncio.sleep(transfer_time)

        self.sent_messages.append(message)
        self.bytes_sent += message_size
        return True

    async def receive_message(self, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """Mock receive message with timeout."""
        if self.is_closed:
            return None

        try:
            message = await asyncio.wait_for(self.message_queue.get(), timeout)
            self.received_messages.append(message)
            self.bytes_received += len(str(message).encode())
            return message
        except asyncio.TimeoutError:
            return None

    async def send_bytes(self, data: bytes) -> bool:
        """Mock send raw bytes."""
        if self.is_closed or not self.network.is_connected:
            return False

        # Simulate packet loss
        if random.random() < (self.network.packet_loss_percent / 100.0):
            return False

        # Simulate bandwidth limitations
        if self.network.bandwidth_kbps:
            transfer_time = len(data) / (self.network.bandwidth_kbps * 1024 / 8)
            await asyncio.sleep(transfer_time)

        self.bytes_sent += len(data)
        return True

    async def receive_bytes(self, size: int, timeout: float = 30.0) -> Optional[bytes]:
        """Mock receive raw bytes."""
        if self.is_closed:
            return None

        # Simulate receiving data
        await asyncio.sleep(0.01)  # Small delay to simulate I/O
        data = b"mock_data" * (size // 9 + 1)
        return data[:size]

    async def close(self):
        """Mock close connection."""
        self.is_closed = True

    def inject_message(self, message: Dict[str, Any]):
        """Inject a message into the receive queue."""
        self.message_queue.put_nowait(message)

    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        return {
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "messages_sent": len(self.sent_messages),
            "messages_received": len(self.received_messages),
            "connection_time": time.time() - self.connection_time,
            "is_closed": self.is_closed,
        }


class MockTransportManager:
    """Mock transport manager for testing."""

    def __init__(self):
        self.peers: Dict[str, PeerInfo] = {}
        self.connections: Dict[str, MockConnection] = {}
        self.is_started = False
        self.discovery_results = []
        self.network_conditions = NetworkCondition()
        self.transport_failures = set()

    async def start_all(self) -> bool:
        """Mock start all transports."""
        if "start" in self.transport_failures:
            return False
        self.is_started = True
        return True

    async def stop_all(self):
        """Mock stop all transports."""
        self.is_started = False
        for conn in self.connections.values():
            await conn.close()
        self.connections.clear()

    async def discover_peers(self, timeout: float = 10.0) -> List[PeerInfo]:
        """Mock peer discovery."""
        if "discovery" in self.transport_failures:
            return []

        await asyncio.sleep(0.1)  # Simulate discovery time
        return self.discovery_results.copy()

    async def connect_to_peer(self, peer_id: str) -> Optional[MockConnection]:
        """Mock peer connection."""
        if peer_id in self.transport_failures:
            return None

        if peer_id in self.connections:
            return self.connections[peer_id]

        if peer_id in self.peers:
            conn = MockConnection(peer_id, self.network_conditions)
            self.connections[peer_id] = conn
            return conn

        return None

    def add_peer(self, peer_info: PeerInfo):
        """Add a peer to the mock transport."""
        self.peers[peer_info.peer_id] = peer_info
        if peer_info not in self.discovery_results:
            self.discovery_results.append(peer_info)

    def remove_peer(self, peer_id: str):
        """Remove a peer from the mock transport."""
        if peer_id in self.peers:
            del self.peers[peer_id]
        self.discovery_results = [
            p for p in self.discovery_results if p.peer_id != peer_id
        ]
        if peer_id in self.connections:
            asyncio.create_task(self.connections[peer_id].close())
            del self.connections[peer_id]

    def set_network_conditions(self, conditions: NetworkCondition):
        """Set network conditions for all connections."""
        self.network_conditions = conditions
        for conn in self.connections.values():
            conn.network = conditions

    def simulate_transport_failure(self, operation: str):
        """Simulate transport failure for specific operations."""
        self.transport_failures.add(operation)

    def clear_transport_failures(self):
        """Clear all transport failures."""
        self.transport_failures.clear()


# ============================================================================
# Filesystem Mocks
# ============================================================================


class MockFilesystem:
    """Mock filesystem for testing file operations."""

    def __init__(self):
        self.files: Dict[str, bytes] = {}
        self.directories: Set[str] = set()
        self.read_failures: Set[str] = set()
        self.write_failures: Set[str] = set()
        self.disk_full = False
        self.disk_space_used = 0
        self.disk_space_total = 1024 * 1024 * 1024  # 1GB default
        self.file_permissions: Dict[str, int] = {}
        self.access_times: Dict[str, float] = {}

    def exists(self, path: Union[str, pathlib.Path]) -> bool:
        """Check if file or directory exists."""
        path_str = str(path)
        return path_str in self.files or path_str in self.directories

    def is_file(self, path: Union[str, pathlib.Path]) -> bool:
        """Check if path is a file."""
        return str(path) in self.files

    def is_dir(self, path: Union[str, pathlib.Path]) -> bool:
        """Check if path is a directory."""
        return str(path) in self.directories

    def read_bytes(self, path: Union[str, pathlib.Path]) -> bytes:
        """Read file content as bytes."""
        path_str = str(path)

        if path_str in self.read_failures:
            raise IOError(f"Simulated read failure for {path_str}")

        if path_str not in self.files:
            raise FileNotFoundError(f"File not found: {path_str}")

        self.access_times[path_str] = time.time()
        return self.files[path_str]

    def write_bytes(self, path: Union[str, pathlib.Path], data: bytes):
        """Write bytes to file."""
        path_str = str(path)

        if self.disk_full or (self.disk_space_used + len(data) > self.disk_space_total):
            raise IOError("No space left on device")

        if path_str in self.write_failures:
            raise IOError(f"Simulated write failure for {path_str}")

        # Update disk usage
        old_size = len(self.files.get(path_str, b""))
        self.disk_space_used = self.disk_space_used - old_size + len(data)

        self.files[path_str] = data
        self.access_times[path_str] = time.time()

        # Create parent directories
        parent = str(pathlib.Path(path_str).parent)
        if parent != path_str:  # Avoid infinite recursion for root
            self.mkdir(parent)

    def mkdir(self, path: Union[str, pathlib.Path], parents: bool = True):
        """Create directory."""
        path_str = str(path)

        if parents:
            # Create all parent directories
            parts = pathlib.Path(path_str).parts
            for i in range(1, len(parts) + 1):
                dir_path = str(pathlib.Path(*parts[:i]))
                self.directories.add(dir_path)
        else:
            self.directories.add(path_str)

    def unlink(self, path: Union[str, pathlib.Path]):
        """Delete file."""
        path_str = str(path)

        if path_str not in self.files:
            raise FileNotFoundError(f"File not found: {path_str}")

        # Update disk usage
        self.disk_space_used -= len(self.files[path_str])
        del self.files[path_str]

        if path_str in self.access_times:
            del self.access_times[path_str]

    def rmdir(self, path: Union[str, pathlib.Path]):
        """Remove directory."""
        path_str = str(path)

        if path_str not in self.directories:
            raise FileNotFoundError(f"Directory not found: {path_str}")

        # Check if directory is empty
        for file_path in self.files:
            if file_path.startswith(path_str + "/"):
                raise OSError(f"Directory not empty: {path_str}")

        self.directories.remove(path_str)

    def listdir(self, path: Union[str, pathlib.Path]) -> List[str]:
        """List directory contents."""
        path_str = str(path)

        if path_str not in self.directories:
            raise FileNotFoundError(f"Directory not found: {path_str}")

        contents = []
        prefix = path_str + "/" if not path_str.endswith("/") else path_str

        # Find files in directory
        for file_path in self.files:
            if file_path.startswith(prefix):
                relative_path = file_path[len(prefix) :]
                if "/" not in relative_path:  # Direct child
                    contents.append(relative_path)

        # Find subdirectories
        for dir_path in self.directories:
            if dir_path.startswith(prefix) and dir_path != path_str:
                relative_path = dir_path[len(prefix) :]
                if "/" not in relative_path:  # Direct child
                    contents.append(relative_path)

        return contents

    def stat(self, path: Union[str, pathlib.Path]) -> Mock:
        """Get file statistics."""
        path_str = str(path)

        if path_str not in self.files and path_str not in self.directories:
            raise FileNotFoundError(f"Path not found: {path_str}")

        mock_stat = Mock()
        mock_stat.st_size = len(self.files.get(path_str, b""))
        mock_stat.st_mtime = self.access_times.get(path_str, time.time())
        mock_stat.st_atime = self.access_times.get(path_str, time.time())
        mock_stat.st_ctime = self.access_times.get(path_str, time.time())
        mock_stat.st_mode = self.file_permissions.get(path_str, 0o644)

        return mock_stat

    def simulate_disk_full(self, enabled: bool = True):
        """Simulate disk full condition."""
        self.disk_full = enabled

    def simulate_read_failure(self, path: Union[str, pathlib.Path]):
        """Simulate read failure for specific path."""
        self.read_failures.add(str(path))

    def simulate_write_failure(self, path: Union[str, pathlib.Path]):
        """Simulate write failure for specific path."""
        self.write_failures.add(str(path))

    def clear_failures(self):
        """Clear all simulated failures."""
        self.read_failures.clear()
        self.write_failures.clear()
        self.disk_full = False

    def get_disk_usage(self) -> Dict[str, int]:
        """Get disk usage statistics."""
        return {
            "used": self.disk_space_used,
            "total": self.disk_space_total,
            "free": self.disk_space_total - self.disk_space_used,
        }


# ============================================================================
# Cryptography Mocks
# ============================================================================


class MockCrypto:
    """Mock cryptographic operations for testing."""

    def __init__(self):
        self.hash_failures: Set[bytes] = set()
        self.encryption_failures: Set[bytes] = set()
        self.deterministic_hashes: Dict[bytes, str] = {}
        self.hash_collision_pairs: List[tuple] = []
        self.weak_keys: Set[bytes] = set()

    def blake3_hash(self, data: bytes) -> str:
        """Mock BLAKE3 hash function."""
        if data in self.hash_failures:
            raise RuntimeError("Simulated hash failure")

        # Check for intentional collisions
        for pair in self.hash_collision_pairs:
            if data == pair[0]:
                return self.blake3_hash(pair[1])

        # Return deterministic hash if set
        if data in self.deterministic_hashes:
            return self.deterministic_hashes[data]

        # Use SHA256 as a substitute for testing
        return hashlib.sha256(data).hexdigest()

    def sha256_hash(self, data: bytes) -> str:
        """Mock SHA256 hash function."""
        if data in self.hash_failures:
            raise RuntimeError("Simulated hash failure")

        return hashlib.sha256(data).hexdigest()

    def encrypt_data(self, data: bytes, key: bytes) -> bytes:
        """Mock data encryption."""
        if data in self.encryption_failures:
            raise RuntimeError("Simulated encryption failure")

        if key in self.weak_keys:
            raise ValueError("Weak encryption key")

        # Simple XOR encryption for testing
        key_repeated = (key * (len(data) // len(key) + 1))[: len(data)]
        return bytes(a ^ b for a, b in zip(data, key_repeated))

    def decrypt_data(self, encrypted_data: bytes, key: bytes) -> bytes:
        """Mock data decryption."""
        if key in self.weak_keys:
            raise ValueError("Weak encryption key")

        # XOR decryption (same as encryption for XOR)
        return self.encrypt_data(encrypted_data, key)

    def generate_key(self, size: int = 32) -> bytes:
        """Mock key generation."""
        return b"mock_key_" + bytes(range(size - 9))

    def generate_keypair(self) -> tuple:
        """Mock keypair generation."""
        private_key = b"mock_private_key_" + bytes(range(16))
        public_key = b"mock_public_key_" + bytes(range(17))
        return private_key, public_key

    def sign_data(self, data: bytes, private_key: bytes) -> bytes:
        """Mock data signing."""
        signature = hashlib.sha256(data + private_key).digest()
        return signature

    def verify_signature(
        self, data: bytes, signature: bytes, public_key: bytes
    ) -> bool:
        """Mock signature verification."""
        # Simple verification based on hash
        expected = hashlib.sha256(data + public_key).digest()
        return signature == expected

    def set_deterministic_hash(self, data: bytes, hash_value: str):
        """Set a deterministic hash for specific data."""
        self.deterministic_hashes[data] = hash_value

    def simulate_hash_failure(self, data: bytes):
        """Simulate hash failure for specific data."""
        self.hash_failures.add(data)

    def simulate_encryption_failure(self, data: bytes):
        """Simulate encryption failure for specific data."""
        self.encryption_failures.add(data)

    def add_hash_collision(self, data1: bytes, data2: bytes):
        """Add intentional hash collision for testing."""
        self.hash_collision_pairs.append((data1, data2))

    def mark_key_as_weak(self, key: bytes):
        """Mark a key as weak for testing."""
        self.weak_keys.add(key)

    def clear_failures(self):
        """Clear all simulated failures."""
        self.hash_failures.clear()
        self.encryption_failures.clear()
        self.weak_keys.clear()
        self.hash_collision_pairs.clear()


# ============================================================================
# CRDT Store Mocks
# ============================================================================


class MockCRDTStore:
    """Mock CRDT store for testing."""

    def __init__(self):
        self.files: Dict[str, Dict[str, Any]] = {}
        self.save_failures = False
        self.load_failures = False
        self.corruption_simulation = False
        self.version = 1

    def add_file(self, path: pathlib.Path, hashes: List[str]) -> bool:
        """Mock add file to store."""
        if self.corruption_simulation:
            # Simulate data corruption
            hashes = hashes[:-1] if hashes else []

        file_key = str(path)
        self.files[file_key] = {
            "path": file_key,
            "hashes": hashes,
            "mtime": time.time(),
            "size": sum(len(h) for h in hashes),  # Mock size calculation
            "version": self.version,
        }
        self.version += 1
        return True

    def get_file(self, path: pathlib.Path) -> Optional[Dict[str, Any]]:
        """Mock get file from store."""
        return self.files.get(str(path))

    def remove_file(self, path: pathlib.Path) -> bool:
        """Mock remove file from store."""
        file_key = str(path)
        if file_key in self.files:
            del self.files[file_key]
            return True
        return False

    def list_files(self) -> List[Dict[str, Any]]:
        """Mock list all files in store."""
        return list(self.files.values())

    def save(self) -> bool:
        """Mock save store to disk."""
        if self.save_failures:
            raise IOError("Simulated save failure")
        return True

    def load(self) -> bool:
        """Mock load store from disk."""
        if self.load_failures:
            raise IOError("Simulated load failure")
        return True

    def merge_with(self, other_store: "MockCRDTStore"):
        """Mock merge with another store."""
        for file_key, file_data in other_store.files.items():
            if (
                file_key not in self.files
                or file_data["version"] > self.files[file_key]["version"]
            ):
                self.files[file_key] = file_data.copy()

    def get_conflicts(self) -> List[Dict[str, Any]]:
        """Mock get conflicted files."""
        # For testing, return files with "conflict" in the name
        return [f for f in self.files.values() if "conflict" in f["path"]]

    def simulate_save_failure(self, enabled: bool = True):
        """Simulate save failures."""
        self.save_failures = enabled

    def simulate_load_failure(self, enabled: bool = True):
        """Simulate load failures."""
        self.load_failures = enabled

    def simulate_corruption(self, enabled: bool = True):
        """Simulate data corruption."""
        self.corruption_simulation = enabled


# ============================================================================
# Sync Manager Mocks
# ============================================================================


class MockSyncManager:
    """Mock sync manager for testing."""

    def __init__(self, store: MockCRDTStore, transport: MockTransportManager):
        self.store = store
        self.transport = transport
        self.is_running = False
        self.sync_failures: Set[str] = set()
        self.sync_history: List[Dict[str, Any]] = []
        self.conflict_resolution_strategy = "last_writer_wins"

    async def start(self):
        """Mock start sync manager."""
        if "start" not in self.sync_failures:
            self.is_running = True

    async def stop(self):
        """Mock stop sync manager."""
        self.is_running = False

    async def sync_with_peer(self, peer_id: str) -> bool:
        """Mock sync with specific peer."""
        if peer_id in self.sync_failures:
            return False

        sync_record = {
            "peer_id": peer_id,
            "timestamp": time.time(),
            "files_synced": len(self.store.files),
            "success": True,
        }
        self.sync_history.append(sync_record)
        return True

    async def sync_all_peers(self) -> Dict[str, bool]:
        """Mock sync with all known peers."""
        results = {}
        for peer_id in self.transport.peers:
            results[peer_id] = await self.sync_with_peer(peer_id)
        return results

    def get_sync_status(self) -> Dict[str, Any]:
        """Mock get sync status."""
        return {
            "is_running": self.is_running,
            "connected_peers": len(self.transport.connections),
            "total_files": len(self.store.files),
            "last_sync": (
                self.sync_history[-1]["timestamp"] if self.sync_history else None
            ),
            "conflicts": len(self.store.get_conflicts()),
        }

    def simulate_sync_failure(self, peer_id: str):
        """Simulate sync failure with specific peer."""
        self.sync_failures.add(peer_id)

    def clear_sync_failures(self):
        """Clear all sync failures."""
        self.sync_failures.clear()

    def set_conflict_resolution_strategy(self, strategy: str):
        """Set conflict resolution strategy."""
        self.conflict_resolution_strategy = strategy


# ============================================================================
# Factory Functions
# ============================================================================


def create_mock_peer_info(peer_id: str, **kwargs) -> Mock:
    """Create a mock PeerInfo object."""
    mock_peer = Mock()
    mock_peer.peer_id = peer_id
    mock_peer.transport_type = kwargs.get("transport_type", "tcp")
    mock_peer.address = kwargs.get("address", "127.0.0.1")
    mock_peer.port = kwargs.get("port", 8000)
    mock_peer.last_seen = kwargs.get("last_seen", time.time())
    mock_peer.metadata = kwargs.get("metadata", {})
    return mock_peer


def create_mock_file_record(path: str, **kwargs) -> Mock:
    """Create a mock FileRecord object."""
    mock_record = Mock()
    mock_record.path = path
    mock_record.hashes = kwargs.get("hashes", ["mock_hash"])
    mock_record.size = kwargs.get("size", 1024)
    mock_record.mtime = kwargs.get("mtime", time.time())
    mock_record.checksum = kwargs.get("checksum", "mock_checksum")
    return mock_record


def create_integrated_mock_system() -> Dict[str, Any]:
    """Create an integrated mock system with all components."""
    filesystem = MockFilesystem()
    crypto = MockCrypto()
    store = MockCRDTStore()
    transport = MockTransportManager()
    sync_manager = MockSyncManager(store, transport)

    return {
        "filesystem": filesystem,
        "crypto": crypto,
        "store": store,
        "transport": transport,
        "sync_manager": sync_manager,
    }

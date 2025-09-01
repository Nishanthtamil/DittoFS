"""
Comprehensive test configuration and fixtures for DittoFS testing.

This module provides fixtures for:
- Multi-peer scenarios and network simulation
- Mock objects for external dependencies
- Base test classes for common testing patterns
- Property-based testing utilities
"""

import asyncio
import tempfile
import pathlib
import shutil
import socket
import time
import hashlib
from typing import Dict, List, Optional, Any, AsyncGenerator, Generator
from unittest.mock import Mock, AsyncMock, MagicMock, patch
from dataclasses import dataclass, field

import pytest
import pytest_asyncio
from hypothesis import strategies as st

# Import DittoFS components
from dittofs.crdt_store import CRDTStore, FileRecord
from dittofs.chunker import DEFAULT_AVG_CHUNK_SIZE, CHUNK_DIR
from dittofs.transport import TransportManager, PeerInfo, Connection
from dittofs.sync_manager import SyncManager

# Use the default average chunk size for backward compatibility
CHUNK_SIZE = DEFAULT_AVG_CHUNK_SIZE


# ============================================================================
# Test Configuration and Markers
# ============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers and settings."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "network: marks tests that require network access"
    )
    config.addinivalue_line(
        "markers", "crypto: marks tests that involve cryptographic operations"
    )
    config.addinivalue_line(
        "markers", "filesystem: marks tests that interact with the filesystem"
    )


# ============================================================================
# Data Classes for Test Scenarios
# ============================================================================

@dataclass
class TestPeer:
    """Represents a test peer in multi-peer scenarios."""
    peer_id: str
    store: CRDTStore
    transport: TransportManager
    sync_manager: SyncManager
    temp_dir: pathlib.Path
    chunk_dir: pathlib.Path
    files: Dict[str, FileRecord] = field(default_factory=dict)
    is_online: bool = True
    network_delay: float = 0.0
    packet_loss_rate: float = 0.0


@dataclass
class NetworkCondition:
    """Represents network conditions for testing."""
    latency_ms: int = 0
    bandwidth_kbps: Optional[int] = None
    packet_loss_percent: float = 0.0
    jitter_ms: int = 0
    is_connected: bool = True


@dataclass
class TestFileData:
    """Test file data with various characteristics."""
    name: str
    content: bytes
    size: int
    mime_type: str = "application/octet-stream"
    should_chunk: bool = True


# ============================================================================
# Temporary Directory and File System Fixtures
# ============================================================================

@pytest.fixture
def temp_dir() -> Generator[pathlib.Path, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield pathlib.Path(tmp_dir)


@pytest.fixture
def isolated_chunk_dir(temp_dir: pathlib.Path, monkeypatch) -> pathlib.Path:
    """Create an isolated chunk directory for testing."""
    chunk_dir = temp_dir / "chunks"
    chunk_dir.mkdir(exist_ok=True)
    
    # Monkey patch the global CHUNK_DIR
    monkeypatch.setattr("dittofs.chunker.CHUNK_DIR", chunk_dir)
    
    return chunk_dir


@pytest.fixture
def test_files() -> Dict[str, TestFileData]:
    """Generate various test files with different characteristics."""
    return {
        "small_text": TestFileData(
            name="small.txt",
            content=b"Hello, DittoFS!",
            size=15,
            mime_type="text/plain"
        ),
        "large_binary": TestFileData(
            name="large.bin",
            content=b"A" * (CHUNK_SIZE * 3 + 1000),  # Multiple chunks
            size=CHUNK_SIZE * 3 + 1000,
            mime_type="application/octet-stream"
        ),
        "empty_file": TestFileData(
            name="empty.txt",
            content=b"",
            size=0,
            mime_type="text/plain"
        ),
        "unicode_text": TestFileData(
            name="unicode.txt",
            content="Hello 世界! 🌍".encode('utf-8'),
            size=len("Hello 世界! 🌍".encode('utf-8')),
            mime_type="text/plain"
        ),
        "exact_chunk": TestFileData(
            name="exact_chunk.bin",
            content=b"X" * CHUNK_SIZE,  # Exactly one chunk
            size=CHUNK_SIZE,
            mime_type="application/octet-stream"
        ),
    }


@pytest.fixture
def create_test_file():
    """Factory fixture to create test files."""
    def _create_file(temp_dir: pathlib.Path, file_data: TestFileData) -> pathlib.Path:
        file_path = temp_dir / file_data.name
        file_path.write_bytes(file_data.content)
        return file_path
    return _create_file


# ============================================================================
# Mock Objects for External Dependencies
# ============================================================================

@pytest.fixture
def mock_network():
    """Mock network operations with configurable behavior."""
    class MockNetwork:
        def __init__(self):
            self.is_connected = True
            self.latency = 0.0
            self.packet_loss = 0.0
            self.bandwidth_limit = None
            self.connection_failures = []
            
        async def simulate_delay(self):
            """Simulate network latency."""
            if self.latency > 0:
                await asyncio.sleep(self.latency / 1000.0)
        
        def should_drop_packet(self) -> bool:
            """Simulate packet loss."""
            import random
            return random.random() < (self.packet_loss / 100.0)
        
        def set_conditions(self, condition: NetworkCondition):
            """Set network conditions."""
            self.is_connected = condition.is_connected
            self.latency = condition.latency_ms
            self.packet_loss = condition.packet_loss_percent
            self.bandwidth_limit = condition.bandwidth_kbps
    
    return MockNetwork()


@pytest.fixture
def mock_filesystem():
    """Mock filesystem operations."""
    class MockFilesystem:
        def __init__(self):
            self.files = {}
            self.read_failures = set()
            self.write_failures = set()
            self.disk_full = False
            
        def add_file(self, path: str, content: bytes):
            """Add a file to the mock filesystem."""
            self.files[path] = content
            
        def get_file(self, path: str) -> Optional[bytes]:
            """Get file content from mock filesystem."""
            if path in self.read_failures:
                raise IOError(f"Simulated read failure for {path}")
            return self.files.get(path)
        
        def write_file(self, path: str, content: bytes):
            """Write file to mock filesystem."""
            if self.disk_full:
                raise IOError("No space left on device")
            if path in self.write_failures:
                raise IOError(f"Simulated write failure for {path}")
            self.files[path] = content
            
        def simulate_disk_full(self, enabled: bool = True):
            """Simulate disk full condition."""
            self.disk_full = enabled
            
        def simulate_read_failure(self, path: str):
            """Simulate read failure for specific path."""
            self.read_failures.add(path)
            
        def simulate_write_failure(self, path: str):
            """Simulate write failure for specific path."""
            self.write_failures.add(path)
    
    return MockFilesystem()


@pytest.fixture
def mock_crypto():
    """Mock cryptographic operations for testing."""
    class MockCrypto:
        def __init__(self):
            self.hash_failures = set()
            self.encryption_failures = set()
            self.deterministic_hashes = {}
            
        def blake3_hash(self, data: bytes) -> str:
            """Mock BLAKE3 hash function."""
            if data in self.hash_failures:
                raise RuntimeError("Simulated hash failure")
            
            # Return deterministic hash for testing
            if data in self.deterministic_hashes:
                return self.deterministic_hashes[data]
            
            # Use actual blake3 for real hashing
            import blake3
            return blake3.blake3(data).hexdigest()
        
        def set_deterministic_hash(self, data: bytes, hash_value: str):
            """Set a deterministic hash for specific data."""
            self.deterministic_hashes[data] = hash_value
            
        def simulate_hash_failure(self, data: bytes):
            """Simulate hash failure for specific data."""
            self.hash_failures.add(data)
    
    return MockCrypto()


# ============================================================================
# CRDT Store Fixtures
# ============================================================================

@pytest.fixture
def clean_crdt_store(temp_dir: pathlib.Path, monkeypatch) -> CRDTStore:
    """Create a clean CRDT store for testing."""
    store_path = temp_dir / "test_store.yrs"
    
    # Monkey patch the store path
    monkeypatch.setattr("dittofs.crdt_store.STORE_PATH", store_path)
    
    return CRDTStore()


@pytest.fixture
def populated_crdt_store(clean_crdt_store: CRDTStore, test_files: Dict[str, TestFileData]) -> CRDTStore:
    """Create a CRDT store populated with test files."""
    store = clean_crdt_store
    
    for file_data in test_files.values():
        # Create fake hashes for test files
        fake_hashes = [f"hash_{file_data.name}_{i}" for i in range(max(1, file_data.size // CHUNK_SIZE + 1))]
        
        # Add file to store
        store.add_file(pathlib.Path(file_data.name), fake_hashes)
    
    store.save()
    return store


# ============================================================================
# Transport and Connection Mocks
# ============================================================================

class MockConnection(Connection):
    """Mock connection for testing."""
    
    def __init__(self, peer_id: str, network_mock=None):
        self.peer_id = peer_id
        self.network = network_mock
        self.is_closed = False
        self.sent_messages = []
        self.received_messages = []
        self.message_queue = asyncio.Queue()
        
    async def send_message(self, message: Dict[str, Any]) -> bool:
        """Mock send message."""
        if self.is_closed:
            return False
            
        if self.network and self.network.should_drop_packet():
            return False
            
        if self.network:
            await self.network.simulate_delay()
            
        self.sent_messages.append(message)
        return True
    
    async def receive_message(self, timeout: float = 30.0) -> Optional[Dict[str, Any]]:
        """Mock receive message."""
        if self.is_closed:
            return None
            
        try:
            message = await asyncio.wait_for(self.message_queue.get(), timeout)
            self.received_messages.append(message)
            return message
        except asyncio.TimeoutError:
            return None
    
    async def close(self):
        """Mock close connection."""
        self.is_closed = True
    
    def inject_message(self, message: Dict[str, Any]):
        """Inject a message into the receive queue."""
        self.message_queue.put_nowait(message)


@pytest.fixture
def mock_transport_manager():
    """Create a mock transport manager."""
    class MockTransportManager:
        def __init__(self):
            self.peers = {}
            self.connections = {}
            self.is_started = False
            self.discovery_results = []
            
        async def start_all(self) -> bool:
            """Mock start all transports."""
            self.is_started = True
            return True
            
        async def stop_all(self):
            """Mock stop all transports."""
            self.is_started = False
            for conn in self.connections.values():
                await conn.close()
            self.connections.clear()
            
        async def discover_all_peers(self, timeout: float = 10.0) -> List[PeerInfo]:
            """Mock peer discovery."""
            await asyncio.sleep(0.1)  # Simulate discovery time
            return self.discovery_results.copy()
            
        async def connect_best_peer(self, peer_id: str) -> Optional[Connection]:
            """Mock peer connection."""
            if peer_id in self.connections:
                return self.connections[peer_id]
            
            if peer_id in self.peers:
                conn = MockConnection(peer_id)
                self.connections[peer_id] = conn
                return conn
                
            return None
        
        def add_peer(self, peer: PeerInfo):
            """Add a peer to the mock transport."""
            self.peers[peer.peer_id] = peer
            self.discovery_results.append(peer)
            
        def remove_peer(self, peer_id: str):
            """Remove a peer from the mock transport."""
            if peer_id in self.peers:
                del self.peers[peer_id]
            self.discovery_results = [p for p in self.discovery_results if p.peer_id != peer_id]
    
    return MockTransportManager()


# ============================================================================
# Multi-Peer Test Fixtures
# ============================================================================

@pytest_asyncio.fixture
async def test_peer_factory(temp_dir: pathlib.Path):
    """Factory to create test peers."""
    created_peers = []
    
    async def _create_peer(peer_id: str, files: Optional[List[TestFileData]] = None) -> TestPeer:
        """Create a test peer with isolated storage."""
        peer_dir = temp_dir / f"peer_{peer_id}"
        peer_dir.mkdir(exist_ok=True)
        
        chunk_dir = peer_dir / "chunks"
        chunk_dir.mkdir(exist_ok=True)
        
        # Create isolated CRDT store
        store_path = peer_dir / "store.yrs"
        
        # Mock the global paths for this peer
        with patch("dittofs.chunker.CHUNK_DIR", chunk_dir), \
             patch("dittofs.crdt_store.STORE_PATH", store_path):
            
            store = CRDTStore()
            transport = MockTransportManager()
            sync_manager = SyncManager(store, transport)
            
            peer = TestPeer(
                peer_id=peer_id,
                store=store,
                transport=transport,
                sync_manager=sync_manager,
                temp_dir=peer_dir,
                chunk_dir=chunk_dir
            )
            
            # Add test files if provided
            if files:
                for file_data in files:
                    file_path = peer_dir / file_data.name
                    file_path.write_bytes(file_data.content)
                    
                    # Chunk the file and add to store
                    from dittofs.chunker import split
                    hashes = list(split(file_path))
                    store.add_file(file_path, hashes)
                    peer.files[file_data.name] = FileRecord(
                        path=str(file_path),
                        hashes=hashes,
                        size=file_data.size,
                        mtime=time.time(),
                        checksum=hashlib.sha256(file_data.content).hexdigest()
                    )
            
            store.save()
            created_peers.append(peer)
            return peer
    
    yield _create_peer
    
    # Cleanup
    for peer in created_peers:
        await peer.sync_manager.stop()
        await peer.transport.stop_all()


@pytest_asyncio.fixture
async def two_peer_scenario(test_peer_factory, test_files):
    """Create a two-peer test scenario."""
    peer1 = await test_peer_factory("peer1", [test_files["small_text"]])
    peer2 = await test_peer_factory("peer2", [test_files["large_binary"]])
    
    # Connect peers to each other
    peer1_info = PeerInfo(
        peer_id="peer1",
        transport_type="mock",
        address="127.0.0.1",
        port=8001,
        last_seen=time.time(),
        metadata={}
    )
    
    peer2_info = PeerInfo(
        peer_id="peer2",
        transport_type="mock",
        address="127.0.0.1",
        port=8002,
        last_seen=time.time(),
        metadata={}
    )
    
    peer1.transport.add_peer(peer2_info)
    peer2.transport.add_peer(peer1_info)
    
    return peer1, peer2


@pytest_asyncio.fixture
async def multi_peer_network(test_peer_factory, test_files):
    """Create a multi-peer network scenario."""
    peers = []
    
    # Create 5 peers with different files
    file_list = list(test_files.values())
    for i in range(5):
        peer_files = [file_list[i % len(file_list)]]
        peer = await test_peer_factory(f"peer{i}", peer_files)
        peers.append(peer)
    
    # Connect all peers to each other
    for i, peer in enumerate(peers):
        for j, other_peer in enumerate(peers):
            if i != j:
                peer_info = PeerInfo(
                    peer_id=other_peer.peer_id,
                    transport_type="mock",
                    address="127.0.0.1",
                    port=8000 + j,
                    last_seen=time.time(),
                    metadata={}
                )
                peer.transport.add_peer(peer_info)
    
    return peers


# ============================================================================
# Network Simulation Fixtures
# ============================================================================

@pytest.fixture
def network_simulator():
    """Create a network simulator for testing various conditions."""
    class NetworkSimulator:
        def __init__(self):
            self.conditions = NetworkCondition()
            self.active_connections = {}
            
        def set_conditions(self, **kwargs):
            """Set network conditions."""
            for key, value in kwargs.items():
                if hasattr(self.conditions, key):
                    setattr(self.conditions, key, value)
        
        async def simulate_partition(self, peer1_id: str, peer2_id: str, duration: float):
            """Simulate network partition between two peers."""
            # Store original connection state
            original_state = self.conditions.is_connected
            
            # Disconnect peers
            self.conditions.is_connected = False
            await asyncio.sleep(duration)
            
            # Restore connection
            self.conditions.is_connected = original_state
        
        async def simulate_slow_network(self, latency_ms: int, duration: float):
            """Simulate slow network conditions."""
            original_latency = self.conditions.latency_ms
            self.conditions.latency_ms = latency_ms
            await asyncio.sleep(duration)
            self.conditions.latency_ms = original_latency
        
        def get_conditions(self) -> NetworkCondition:
            """Get current network conditions."""
            return self.conditions
    
    return NetworkSimulator()


# ============================================================================
# Property-Based Testing Strategies
# ============================================================================

# Hypothesis strategies for property-based testing
file_content_strategy = st.binary(min_size=0, max_size=CHUNK_SIZE * 5)

file_name_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters=".-_"),
    min_size=1,
    max_size=100
).filter(lambda x: not x.startswith('.') and '/' not in x and '\\' not in x)

chunk_hash_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("Nd",), whitelist_characters="abcdef"),
    min_size=64,
    max_size=64
)

peer_id_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"),
    min_size=1,
    max_size=50
)

network_condition_strategy = st.builds(
    NetworkCondition,
    latency_ms=st.integers(min_value=0, max_value=1000),
    bandwidth_kbps=st.one_of(st.none(), st.integers(min_value=1, max_value=100000)),
    packet_loss_percent=st.floats(min_value=0.0, max_value=50.0),
    jitter_ms=st.integers(min_value=0, max_value=100),
    is_connected=st.booleans()
)


# ============================================================================
# Base Test Classes
# ============================================================================

class BaseTestCase:
    """Base test class with common utilities."""
    
    def assert_file_chunks_exist(self, chunk_dir: pathlib.Path, hashes: List[str]):
        """Assert that all chunk files exist."""
        for chunk_hash in hashes:
            chunk_path = chunk_dir / chunk_hash
            assert chunk_path.exists(), f"Chunk {chunk_hash} does not exist"
    
    def assert_file_content_matches(self, file_path: pathlib.Path, expected_content: bytes):
        """Assert that file content matches expected content."""
        actual_content = file_path.read_bytes()
        assert actual_content == expected_content, "File content does not match"
    
    def create_random_file(self, temp_dir: pathlib.Path, size: int) -> pathlib.Path:
        """Create a random file of specified size."""
        import random
        
        file_path = temp_dir / f"random_{size}_{random.randint(1000, 9999)}.bin"
        content = bytes(random.getrandbits(8) for _ in range(size))
        file_path.write_bytes(content)
        return file_path


class AsyncTestCase(BaseTestCase):
    """Base test class for async tests."""
    
    async def wait_for_condition(self, condition_func, timeout: float = 5.0, interval: float = 0.1):
        """Wait for a condition to become true."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if await condition_func() if asyncio.iscoroutinefunction(condition_func) else condition_func():
                return True
            await asyncio.sleep(interval)
        return False
    
    async def assert_eventually(self, condition_func, timeout: float = 5.0, message: str = "Condition not met"):
        """Assert that a condition eventually becomes true."""
        result = await self.wait_for_condition(condition_func, timeout)
        assert result, f"{message} (timeout after {timeout}s)"


class IntegrationTestCase(AsyncTestCase):
    """Base test class for integration tests."""
    
    @pytest.fixture(autouse=True)
    def setup_integration_test(self, temp_dir, isolated_chunk_dir):
        """Set up integration test environment."""
        self.temp_dir = temp_dir
        self.chunk_dir = isolated_chunk_dir
    
    async def setup_peer_network(self, peer_count: int) -> List[TestPeer]:
        """Set up a network of test peers."""
        # This would be implemented by subclasses
        raise NotImplementedError("Subclasses must implement setup_peer_network")


# ============================================================================
# Utility Functions
# ============================================================================

def get_free_port() -> int:
    """Get a free port for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


def create_test_chunk(content: bytes) -> str:
    """Create a test chunk and return its hash."""
    import blake3
    return blake3.blake3(content).hexdigest()


async def wait_for_sync(peers: List[TestPeer], timeout: float = 10.0) -> bool:
    """Wait for peers to synchronize."""
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        # Check if all peers have the same file set
        all_files = set()
        for peer in peers:
            peer_files = set(f.path for f in peer.store.list_files())
            all_files.update(peer_files)
        
        # Check if all peers have all files
        synced = True
        for peer in peers:
            peer_files = set(f.path for f in peer.store.list_files())
            if peer_files != all_files:
                synced = False
                break
        
        if synced:
            return True
        
        await asyncio.sleep(0.1)
    
    return False
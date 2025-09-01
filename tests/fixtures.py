"""
Comprehensive test fixtures for DittoFS testing.

This module provides fixtures for multi-peer scenarios, network simulation,
and complex test setups.
"""

import asyncio
import pathlib
import tempfile
import time
from typing import Any, Callable, Dict, Generator, List, Optional
from unittest.mock import patch

import pytest
import pytest_asyncio
from hypothesis import strategies as st

from .mocks import (
    MockConnection,
    MockCRDTStore,
    MockCrypto,
    MockFilesystem,
    MockSyncManager,
    MockTransportManager,
    NetworkCondition,
    create_integrated_mock_system,
    create_mock_peer_info,
)
from .test_utils import create_file_with_content, generate_test_file_content

# Import DittoFS components with fallbacks
try:
    from dittofs.chunker import CHUNK_SIZE
    from dittofs.crdt_store import CRDTStore, FileRecord
    from dittofs.transport import PeerInfo
except ImportError:
    CHUNK_SIZE = 64 * 1024
    CRDTStore = object
    FileRecord = object
    PeerInfo = object


# ============================================================================
# Basic Infrastructure Fixtures
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

    # Monkey patch the global CHUNK_DIR if the module exists
    try:
        monkeypatch.setattr("dittofs.chunker.CHUNK_DIR", chunk_dir)
    except AttributeError:
        pass  # Module doesn't exist yet

    return chunk_dir


@pytest.fixture
def mock_filesystem() -> MockFilesystem:
    """Create a mock filesystem for testing."""
    return MockFilesystem()


@pytest.fixture
def mock_crypto() -> MockCrypto:
    """Create a mock crypto system for testing."""
    return MockCrypto()


@pytest.fixture
def mock_network_conditions() -> NetworkCondition:
    """Create default network conditions for testing."""
    return NetworkCondition()


# ============================================================================
# Test Data Fixtures
# ============================================================================


@pytest.fixture
def test_file_contents() -> Dict[str, bytes]:
    """Generate various test file contents."""
    return {
        "small_text": b"Hello, DittoFS! This is a small test file.",
        "empty": b"",
        "single_byte": b"X",
        "unicode_text": "Hello 世界! 🌍 Testing Unicode".encode("utf-8"),
        "binary_data": bytes(range(256)),
        "large_text": b"Line %d\n" * 1000 % tuple(range(1000)),
        "exact_chunk": b"A" * CHUNK_SIZE,
        "multi_chunk": b"B" * (CHUNK_SIZE * 2 + 500),
        "random_small": generate_test_file_content(1024, "random"),
        "random_large": generate_test_file_content(CHUNK_SIZE * 3, "random"),
        "zeros": b"\x00" * 10000,
        "ones": b"\xff" * 10000,
        "alternating": bytes([i % 256 for i in range(10000)]),
    }


@pytest.fixture
def create_test_file():
    """Factory fixture to create test files with specific content."""

    def _create_file(temp_dir: pathlib.Path, name: str, content: bytes) -> pathlib.Path:
        return create_file_with_content(temp_dir / name, content)

    return _create_file


@pytest.fixture
def test_file_factory(temp_dir: pathlib.Path, test_file_contents: Dict[str, bytes]):
    """Factory to create test files from predefined contents."""

    def _create_test_file(
        content_key: str, custom_name: Optional[str] = None
    ) -> pathlib.Path:
        if content_key not in test_file_contents:
            raise ValueError(f"Unknown content key: {content_key}")

        name = custom_name or f"{content_key}.bin"
        content = test_file_contents[content_key]
        return create_file_with_content(temp_dir / name, content)

    return _create_test_file


# ============================================================================
# CRDT Store Fixtures
# ============================================================================


@pytest.fixture
def clean_crdt_store(temp_dir: pathlib.Path, monkeypatch) -> MockCRDTStore:
    """Create a clean CRDT store for testing."""
    store_path = temp_dir / "test_store.yrs"

    # Monkey patch the store path if the module exists
    try:
        monkeypatch.setattr("dittofs.crdt_store.STORE_PATH", store_path)
    except AttributeError:
        pass  # Module doesn't exist yet

    return MockCRDTStore()


@pytest.fixture
def populated_crdt_store(
    clean_crdt_store: MockCRDTStore, test_file_contents: Dict[str, bytes]
) -> MockCRDTStore:
    """Create a CRDT store populated with test files."""
    store = clean_crdt_store

    for name, content in test_file_contents.items():
        # Create fake hashes for test files
        chunk_count = max(
            1, len(content) // CHUNK_SIZE + (1 if len(content) % CHUNK_SIZE else 0)
        )
        fake_hashes = [f"hash_{name}_{i}" for i in range(chunk_count)]

        # Add file to store
        file_path = pathlib.Path(f"test_{name}.bin")
        store.add_file(file_path, fake_hashes)

    return store


# ============================================================================
# Transport and Network Fixtures
# ============================================================================


@pytest.fixture
def mock_transport() -> MockTransportManager:
    """Create a mock transport manager."""
    return MockTransportManager()


@pytest.fixture
def network_simulator():
    """Create a network simulator for testing various conditions."""

    class NetworkSimulator:
        def __init__(self):
            self.conditions = NetworkCondition()
            self.active_connections = {}
            self.event_log = []

        def set_conditions(self, **kwargs):
            """Set network conditions."""
            for key, value in kwargs.items():
                if hasattr(self.conditions, key):
                    setattr(self.conditions, key, value)

            self.event_log.append(
                {
                    "type": "conditions_changed",
                    "conditions": kwargs,
                    "timestamp": time.time(),
                }
            )

        async def simulate_partition(
            self, duration: float, affected_peers: List[str] = None
        ):
            """Simulate network partition."""
            self.event_log.append(
                {
                    "type": "partition_start",
                    "duration": duration,
                    "affected_peers": affected_peers,
                    "timestamp": time.time(),
                }
            )

            original_connected = self.conditions.is_connected
            self.conditions.is_connected = False

            await asyncio.sleep(duration)

            self.conditions.is_connected = original_connected

            self.event_log.append({"type": "partition_end", "timestamp": time.time()})

        async def simulate_slow_network(self, latency_ms: int, duration: float):
            """Simulate slow network conditions."""
            original_latency = self.conditions.latency_ms
            self.conditions.latency_ms = latency_ms

            self.event_log.append(
                {
                    "type": "slow_network_start",
                    "latency_ms": latency_ms,
                    "duration": duration,
                    "timestamp": time.time(),
                }
            )

            await asyncio.sleep(duration)

            self.conditions.latency_ms = original_latency

            self.event_log.append(
                {"type": "slow_network_end", "timestamp": time.time()}
            )

        def get_event_log(self) -> List[Dict[str, Any]]:
            """Get the network event log."""
            return self.event_log.copy()

        def clear_event_log(self):
            """Clear the network event log."""
            self.event_log.clear()

    return NetworkSimulator()


# ============================================================================
# Multi-Peer Test Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def peer_factory(temp_dir: pathlib.Path):
    """Factory to create test peers with isolated environments."""
    created_peers = []

    async def _create_peer(
        peer_id: str,
        files: Optional[Dict[str, bytes]] = None,
        network_conditions: Optional[NetworkCondition] = None,
    ) -> Dict[str, Any]:
        """Create a test peer with isolated storage and mock components."""
        peer_dir = temp_dir / f"peer_{peer_id}"
        peer_dir.mkdir(exist_ok=True)

        chunk_dir = peer_dir / "chunks"
        chunk_dir.mkdir(exist_ok=True)

        # Create integrated mock system for the peer
        mock_system = create_integrated_mock_system()

        # Configure peer-specific settings
        if network_conditions:
            mock_system["transport"].set_network_conditions(network_conditions)

        # Add test files if provided
        peer_files = {}
        if files:
            for filename, content in files.items():
                file_path = peer_dir / filename
                create_file_with_content(file_path, content)

                # Add to mock store
                chunk_count = max(
                    1,
                    len(content) // CHUNK_SIZE
                    + (1 if len(content) % CHUNK_SIZE else 0),
                )
                fake_hashes = [
                    f"hash_{peer_id}_{filename}_{i}" for i in range(chunk_count)
                ]
                mock_system["store"].add_file(file_path, fake_hashes)

                peer_files[filename] = {
                    "path": file_path,
                    "content": content,
                    "hashes": fake_hashes,
                }

        peer_data = {
            "peer_id": peer_id,
            "directory": peer_dir,
            "chunk_directory": chunk_dir,
            "files": peer_files,
            "mock_system": mock_system,
            "created_at": time.time(),
        }

        created_peers.append(peer_data)
        return peer_data

    yield _create_peer

    # Cleanup
    for peer in created_peers:
        mock_system = peer["mock_system"]
        await mock_system["sync_manager"].stop()
        await mock_system["transport"].stop_all()


@pytest_asyncio.fixture
async def two_peer_scenario(peer_factory, test_file_contents):
    """Create a two-peer test scenario with different files."""
    peer1 = await peer_factory(
        "peer1",
        {
            "file1.txt": test_file_contents["small_text"],
            "shared.bin": test_file_contents["binary_data"],
        },
    )

    peer2 = await peer_factory(
        "peer2",
        {
            "file2.txt": test_file_contents["unicode_text"],
            "shared.bin": test_file_contents["binary_data"],  # Same file
        },
    )

    # Connect peers to each other
    peer1_info = create_mock_peer_info("peer1", address="127.0.0.1", port=8001)
    peer2_info = create_mock_peer_info("peer2", address="127.0.0.1", port=8002)

    peer1["mock_system"]["transport"].add_peer(peer2_info)
    peer2["mock_system"]["transport"].add_peer(peer1_info)

    return peer1, peer2


@pytest_asyncio.fixture
async def multi_peer_network(peer_factory, test_file_contents):
    """Create a multi-peer network scenario with 5 peers."""
    peers = []
    file_keys = list(test_file_contents.keys())

    # Create 5 peers with different files
    for i in range(5):
        peer_id = f"peer_{i}"

        # Give each peer 2-3 files, with some overlap
        peer_files = {}
        for j in range(2 + i % 2):  # 2 or 3 files per peer
            file_key = file_keys[(i + j) % len(file_keys)]
            peer_files[f"{file_key}_{i}.bin"] = test_file_contents[file_key]

        peer = await peer_factory(peer_id, peer_files)
        peers.append(peer)

    # Connect all peers to each other (full mesh)
    for i, peer in enumerate(peers):
        for j, other_peer in enumerate(peers):
            if i != j:
                peer_info = create_mock_peer_info(
                    other_peer["peer_id"], address="127.0.0.1", port=8000 + j
                )
                peer["mock_system"]["transport"].add_peer(peer_info)

    return peers


@pytest_asyncio.fixture
async def hierarchical_peer_network(peer_factory, test_file_contents):
    """Create a hierarchical peer network (star topology)."""
    # Create central hub peer
    hub_peer = await peer_factory(
        "hub", {"hub_file.txt": test_file_contents["large_text"]}
    )

    # Create spoke peers
    spoke_peers = []
    for i in range(4):
        spoke_id = f"spoke_{i}"
        spoke_files = {
            f"spoke_{i}_file.bin": test_file_contents[
                list(test_file_contents.keys())[i % len(test_file_contents)]
            ]
        }
        spoke_peer = await peer_factory(spoke_id, spoke_files)
        spoke_peers.append(spoke_peer)

        # Connect spoke to hub (but not to other spokes)
        hub_info = create_mock_peer_info("hub", address="127.0.0.1", port=8000)
        spoke_info = create_mock_peer_info(spoke_id, address="127.0.0.1", port=8001 + i)

        hub_peer["mock_system"]["transport"].add_peer(spoke_info)
        spoke_peer["mock_system"]["transport"].add_peer(hub_info)

    return hub_peer, spoke_peers


# ============================================================================
# Conflict Scenario Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def conflict_scenario(peer_factory, test_file_contents):
    """Create a scenario with file conflicts."""
    # Create two peers with conflicting versions of the same file
    peer1 = await peer_factory(
        "peer1",
        {
            "conflict_file.txt": b"Version from peer1",
            "normal_file.txt": test_file_contents["small_text"],
        },
    )

    peer2 = await peer_factory(
        "peer2",
        {
            "conflict_file.txt": b"Version from peer2",
            "normal_file.txt": test_file_contents["small_text"],  # Same content
        },
    )

    # Simulate that both files were modified at the same time
    # by marking them as conflicts in the store
    peer1["mock_system"]["store"].files["conflict_file.txt"]["version"] = 1
    peer2["mock_system"]["store"].files["conflict_file.txt"]["version"] = 1

    # Connect peers
    peer1_info = create_mock_peer_info("peer1", address="127.0.0.1", port=8001)
    peer2_info = create_mock_peer_info("peer2", address="127.0.0.1", port=8002)

    peer1["mock_system"]["transport"].add_peer(peer2_info)
    peer2["mock_system"]["transport"].add_peer(peer1_info)

    return peer1, peer2


# ============================================================================
# Performance Test Fixtures
# ============================================================================


@pytest.fixture
def performance_test_files(temp_dir: pathlib.Path) -> Dict[str, pathlib.Path]:
    """Create files of various sizes for performance testing."""
    files = {}

    # Small files (< 1KB)
    for i in range(10):
        content = generate_test_file_content(100 + i * 50, "random")
        file_path = create_file_with_content(temp_dir / f"small_{i}.bin", content)
        files[f"small_{i}"] = file_path

    # Medium files (1KB - 64KB)
    for i in range(5):
        size = 1024 * (2**i)  # 1KB, 2KB, 4KB, 8KB, 16KB
        content = generate_test_file_content(size, "random")
        file_path = create_file_with_content(temp_dir / f"medium_{i}.bin", content)
        files[f"medium_{i}"] = file_path

    # Large files (multiple chunks)
    for i in range(3):
        chunk_count = 2 + i  # 2, 3, 4 chunks
        size = CHUNK_SIZE * chunk_count + 1000
        content = generate_test_file_content(size, "random")
        file_path = create_file_with_content(temp_dir / f"large_{i}.bin", content)
        files[f"large_{i}"] = file_path

    return files


@pytest.fixture
def benchmark_environment():
    """Set up environment for benchmark tests."""

    class BenchmarkEnvironment:
        def __init__(self):
            self.metrics = {}
            self.baselines = {}

        def record_metric(self, name: str, value: float, unit: str = "seconds"):
            """Record a performance metric."""
            self.metrics[name] = {
                "value": value,
                "unit": unit,
                "timestamp": time.time(),
            }

        def set_baseline(self, name: str, value: float):
            """Set a baseline value for comparison."""
            self.baselines[name] = value

        def get_metric(self, name: str) -> Optional[Dict[str, Any]]:
            """Get a recorded metric."""
            return self.metrics.get(name)

        def compare_to_baseline(self, name: str) -> Optional[float]:
            """Compare metric to baseline (returns ratio)."""
            if name not in self.metrics or name not in self.baselines:
                return None
            return self.metrics[name]["value"] / self.baselines[name]

    return BenchmarkEnvironment()


# ============================================================================
# Security Test Fixtures
# ============================================================================


@pytest.fixture
def security_test_environment():
    """Set up environment for security testing."""

    class SecurityTestEnvironment:
        def __init__(self):
            self.mock_crypto = MockCrypto()
            self.security_events = []
            self.attack_simulations = {}

        def record_security_event(self, event_type: str, details: Dict[str, Any]):
            """Record a security event."""
            event = {"type": event_type, "details": details, "timestamp": time.time()}
            self.security_events.append(event)

        def simulate_attack(self, attack_type: str, **params):
            """Simulate various attack scenarios."""
            simulation_id = f"{attack_type}_{len(self.attack_simulations)}"

            simulation = {
                "type": attack_type,
                "params": params,
                "start_time": time.time(),
                "events": [],
            }

            self.attack_simulations[simulation_id] = simulation
            return simulation_id

        def get_security_events(
            self, event_type: Optional[str] = None
        ) -> List[Dict[str, Any]]:
            """Get security events, optionally filtered by type."""
            if event_type is None:
                return self.security_events.copy()
            return [e for e in self.security_events if e["type"] == event_type]

        def clear_events(self):
            """Clear all recorded events."""
            self.security_events.clear()
            self.attack_simulations.clear()

    return SecurityTestEnvironment()


# ============================================================================
# Integration Test Fixtures
# ============================================================================


@pytest.fixture
def integration_test_environment(
    temp_dir: pathlib.Path, isolated_chunk_dir: pathlib.Path
):
    """Set up comprehensive integration test environment."""

    class IntegrationTestEnvironment:
        def __init__(self):
            self.temp_dir = temp_dir
            self.chunk_dir = isolated_chunk_dir
            self.mock_systems = {}
            self.test_data = {}

        def create_mock_system(self, system_id: str) -> Dict[str, Any]:
            """Create a new mock system."""
            mock_system = create_integrated_mock_system()
            self.mock_systems[system_id] = mock_system
            return mock_system

        def get_mock_system(self, system_id: str) -> Optional[Dict[str, Any]]:
            """Get an existing mock system."""
            return self.mock_systems.get(system_id)

        def add_test_data(self, key: str, data: Any):
            """Add test data for use across tests."""
            self.test_data[key] = data

        def get_test_data(self, key: str) -> Any:
            """Get test data."""
            return self.test_data.get(key)

        async def cleanup(self):
            """Clean up all mock systems."""
            for mock_system in self.mock_systems.values():
                await mock_system["sync_manager"].stop()
                await mock_system["transport"].stop_all()

    return IntegrationTestEnvironment()


# ============================================================================
# Property-Based Testing Fixtures
# ============================================================================


@pytest.fixture
def hypothesis_strategies():
    """Provide Hypothesis strategies for property-based testing."""
    return {
        "file_content": st.binary(min_size=0, max_size=CHUNK_SIZE * 3),
        "file_name": st.text(
            alphabet=st.characters(
                whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters=".-_"
            ),
            min_size=1,
            max_size=100,
        ).filter(lambda x: not x.startswith(".") and "/" not in x and "\\" not in x),
        "peer_id": st.text(
            alphabet=st.characters(
                whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"
            ),
            min_size=1,
            max_size=50,
        ),
        "chunk_hash": st.text(alphabet="0123456789abcdef", min_size=64, max_size=64),
        "network_conditions": st.builds(
            NetworkCondition,
            latency_ms=st.integers(min_value=0, max_value=1000),
            bandwidth_kbps=st.one_of(
                st.none(), st.integers(min_value=1, max_value=100000)
            ),
            packet_loss_percent=st.floats(min_value=0.0, max_value=50.0),
            jitter_ms=st.integers(min_value=0, max_value=100),
            is_connected=st.booleans(),
        ),
        "timestamp": st.floats(
            min_value=time.time() - 365 * 24 * 3600,  # One year ago
            max_value=time.time() + 365 * 24 * 3600,  # One year from now
        ),
    }


# ============================================================================
# Utility Fixtures
# ============================================================================


@pytest.fixture
def wait_for_condition():
    """Provide wait_for_condition utility as a fixture."""
    from .test_utils import wait_for_condition as _wait_for_condition

    return _wait_for_condition


@pytest.fixture
def assert_eventually():
    """Provide assert_eventually utility as a fixture."""
    from .test_utils import assert_eventually as _assert_eventually

    return _assert_eventually

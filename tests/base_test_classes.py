"""
Base test classes for common testing patterns in DittoFS.

This module provides base classes that encapsulate common testing patterns
and utilities used across different test modules.
"""

import asyncio
import pathlib
import tempfile
import time
from typing import Any, Callable, Dict, List, Optional, Union
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from hypothesis import given
from hypothesis import strategies as st

from .mocks import (
    MockCRDTStore,
    MockCrypto,
    MockFilesystem,
    MockSyncManager,
    MockTransportManager,
    NetworkCondition,
    create_integrated_mock_system,
)
from .test_utils import (
    PerformanceTimer,
    assert_eventually,
    create_file_with_content,
    generate_test_file_content,
    wait_for_condition,
)

# Import DittoFS components
try:
    from dittofs.chunker import CHUNK_SIZE
    from dittofs.crdt_store import CRDTStore
except ImportError:
    # Fallback values for when modules don't exist yet
    CHUNK_SIZE = 64 * 1024


# ============================================================================
# Base Test Classes
# ============================================================================


class BaseTestCase:
    """Base test class with common utilities and assertions."""

    def assert_file_chunks_exist(self, chunk_dir: pathlib.Path, hashes: List[str]):
        """Assert that all chunk files exist in the chunk directory."""
        for chunk_hash in hashes:
            chunk_path = chunk_dir / chunk_hash
            assert (
                chunk_path.exists()
            ), f"Chunk {chunk_hash} does not exist at {chunk_path}"
            assert chunk_path.is_file(), f"Chunk {chunk_hash} is not a file"

    def assert_file_content_matches(
        self, file_path: pathlib.Path, expected_content: bytes
    ):
        """Assert that file content matches expected content exactly."""
        assert file_path.exists(), f"File {file_path} does not exist"
        actual_content = file_path.read_bytes()
        assert actual_content == expected_content, (
            f"File content mismatch for {file_path}. "
            f"Expected {len(expected_content)} bytes, got {len(actual_content)} bytes"
        )

    def assert_file_size(self, file_path: pathlib.Path, expected_size: int):
        """Assert that file has expected size."""
        assert file_path.exists(), f"File {file_path} does not exist"
        actual_size = file_path.stat().st_size
        assert actual_size == expected_size, (
            f"File size mismatch for {file_path}. "
            f"Expected {expected_size} bytes, got {actual_size} bytes"
        )

    def create_test_file(
        self, temp_dir: pathlib.Path, name: str, content: bytes
    ) -> pathlib.Path:
        """Create a test file with specific content."""
        file_path = temp_dir / name
        return create_file_with_content(file_path, content)

    def create_random_file(
        self, temp_dir: pathlib.Path, size: int, name: Optional[str] = None
    ) -> pathlib.Path:
        """Create a random file of specified size."""
        if name is None:
            name = f"random_{size}_{int(time.time())}.bin"

        content = generate_test_file_content(size, "random")
        return self.create_test_file(temp_dir, name, content)

    def create_large_file(
        self, temp_dir: pathlib.Path, chunk_count: int = 3
    ) -> pathlib.Path:
        """Create a large file that spans multiple chunks."""
        size = CHUNK_SIZE * chunk_count + 1000  # Slightly more than exact chunks
        return self.create_random_file(
            temp_dir, size, f"large_{chunk_count}_chunks.bin"
        )

    def assert_chunks_are_valid(self, chunk_dir: pathlib.Path, hashes: List[str]):
        """Assert that chunks exist and have valid content."""
        self.assert_file_chunks_exist(chunk_dir, hashes)

        for chunk_hash in hashes:
            chunk_path = chunk_dir / chunk_hash
            chunk_content = chunk_path.read_bytes()

            # Verify chunk is not empty (unless it's supposed to be)
            if chunk_hash != "empty_chunk_hash":
                assert len(chunk_content) > 0, f"Chunk {chunk_hash} is empty"

            # Verify chunk size is reasonable
            assert (
                len(chunk_content) <= CHUNK_SIZE
            ), f"Chunk {chunk_hash} is too large: {len(chunk_content)} bytes"

    def measure_performance(
        self, operation_name: str = "operation"
    ) -> PerformanceTimer:
        """Create a performance timer for measuring operation duration."""
        return PerformanceTimer()


class AsyncTestCase(BaseTestCase):
    """Base test class for async tests with additional async utilities."""

    async def wait_for_condition(
        self,
        condition: Callable[[], Union[bool, Any]],
        timeout: float = 5.0,
        message: str = "Condition not met",
    ) -> bool:
        """Wait for a condition to become true with timeout."""
        return await wait_for_condition(condition, timeout, message=message)

    async def assert_eventually(
        self,
        condition: Callable[[], Union[bool, Any]],
        timeout: float = 5.0,
        message: str = "Condition not met",
    ):
        """Assert that a condition eventually becomes true."""
        await assert_eventually(condition, timeout, message)

    async def wait_for_file_sync(
        self, file_path: pathlib.Path, expected_content: bytes, timeout: float = 10.0
    ):
        """Wait for a file to be synchronized with expected content."""

        async def check_file():
            if not file_path.exists():
                return False
            actual_content = file_path.read_bytes()
            return actual_content == expected_content

        await self.assert_eventually(
            check_file,
            timeout,
            f"File {file_path} was not synced with expected content",
        )

    async def wait_for_peer_connection(
        self, transport: MockTransportManager, peer_id: str, timeout: float = 5.0
    ):
        """Wait for a peer to be connected."""

        async def check_connection():
            return (
                peer_id in transport.connections
                and not transport.connections[peer_id].is_closed
            )

        await self.assert_eventually(
            check_connection, timeout, f"Peer {peer_id} was not connected"
        )

    async def simulate_async_operation(self, duration: float = 0.1):
        """Simulate an async operation with specified duration."""
        await asyncio.sleep(duration)


class IntegrationTestCase(AsyncTestCase):
    """Base test class for integration tests with multi-component setup."""

    @pytest.fixture(autouse=True)
    def setup_integration_test(self, temp_dir, isolated_chunk_dir):
        """Set up integration test environment automatically."""
        self.temp_dir = temp_dir
        self.chunk_dir = isolated_chunk_dir
        self.mock_system = create_integrated_mock_system()

    async def setup_peer_network(self, peer_count: int = 3) -> List[Dict[str, Any]]:
        """Set up a network of test peers with mock components."""
        peers = []

        for i in range(peer_count):
            peer_id = f"peer_{i}"
            peer_dir = self.temp_dir / peer_id
            peer_dir.mkdir(exist_ok=True)

            # Create isolated mock system for each peer
            peer_system = create_integrated_mock_system()

            # Configure peer-specific settings
            peer_system["transport"].peers = {}
            peer_system["sync_manager"].peer_id = peer_id

            peer_data = {
                "peer_id": peer_id,
                "directory": peer_dir,
                "system": peer_system,
            }
            peers.append(peer_data)

        # Connect all peers to each other
        for i, peer in enumerate(peers):
            for j, other_peer in enumerate(peers):
                if i != j:
                    from .mocks import create_mock_peer_info

                    peer_info = create_mock_peer_info(
                        other_peer["peer_id"], address="127.0.0.1", port=8000 + j
                    )
                    peer["system"]["transport"].add_peer(peer_info)

        return peers

    async def simulate_network_partition(
        self,
        peers: List[Dict[str, Any]],
        partition_groups: List[List[int]],
        duration: float = 1.0,
    ):
        """Simulate network partition between groups of peers."""
        # Store original network conditions
        original_conditions = {}
        for peer in peers:
            transport = peer["system"]["transport"]
            original_conditions[peer["peer_id"]] = transport.network_conditions

        # Create partitions
        for group in partition_groups:
            for i in group:
                for j in range(len(peers)):
                    if j not in group:
                        # Disconnect peers not in the same group
                        peer_transport = peers[i]["system"]["transport"]
                        other_peer_id = peers[j]["peer_id"]
                        if other_peer_id in peer_transport.connections:
                            await peer_transport.connections[other_peer_id].close()

        # Wait for partition duration
        await asyncio.sleep(duration)

        # Restore connections
        for peer in peers:
            transport = peer["system"]["transport"]
            transport.network_conditions = original_conditions[peer["peer_id"]]
            # Reconnect to all peers
            for other_peer in peers:
                if other_peer["peer_id"] != peer["peer_id"]:
                    await transport.connect_to_peer(other_peer["peer_id"])

    def assert_peers_eventually_consistent(
        self, peers: List[Dict[str, Any]], timeout: float = 10.0
    ):
        """Assert that all peers eventually reach consistency."""

        async def check_consistency():
            # Get file lists from all peers
            file_sets = []
            for peer in peers:
                store = peer["system"]["store"]
                files = {f["path"]: f["hashes"] for f in store.list_files()}
                file_sets.append(files)

            # Check if all peers have the same files
            if not file_sets:
                return True

            first_set = file_sets[0]
            return all(file_set == first_set for file_set in file_sets[1:])

        return self.assert_eventually(
            check_consistency, timeout, "Peers did not reach consistency"
        )


class PropertyBasedTestCase(BaseTestCase):
    """Base test class for property-based testing with Hypothesis."""

    def setup_hypothesis_settings(self):
        """Set up Hypothesis settings for property-based tests."""
        from hypothesis import settings

        return settings(
            max_examples=100,
            deadline=30000,  # 30 seconds
            suppress_health_check=[],
        )

    @given(st.binary(min_size=0, max_size=CHUNK_SIZE * 3))
    def property_test_file_roundtrip(self, file_content: bytes):
        """Property test: file splitting and joining should be lossless."""
        # This is a template - actual implementation would be in specific test files
        pass

    @given(st.lists(st.text(min_size=1, max_size=100), min_size=1, max_size=10))
    def property_test_file_paths(self, file_paths: List[str]):
        """Property test: file path handling should be consistent."""
        # This is a template - actual implementation would be in specific test files
        pass


class PerformanceTestCase(AsyncTestCase):
    """Base test class for performance and benchmark tests."""

    def setup_performance_test(self):
        """Set up performance test environment."""
        self.performance_metrics = {}
        self.baseline_metrics = {}

    def record_metric(self, name: str, value: float, unit: str = "seconds"):
        """Record a performance metric."""
        self.performance_metrics[name] = {
            "value": value,
            "unit": unit,
            "timestamp": time.time(),
        }

    def assert_performance_within_bounds(
        self, metric_name: str, max_value: float, message: Optional[str] = None
    ):
        """Assert that a performance metric is within acceptable bounds."""
        assert (
            metric_name in self.performance_metrics
        ), f"Metric {metric_name} not recorded"

        actual_value = self.performance_metrics[metric_name]["value"]
        unit = self.performance_metrics[metric_name]["unit"]

        if message is None:
            message = f"Performance metric {metric_name} exceeded bounds"

        assert (
            actual_value <= max_value
        ), f"{message}: {actual_value} {unit} > {max_value} {unit}"

    async def benchmark_async_operation(
        self, operation: Callable, *args, iterations: int = 10, **kwargs
    ) -> Dict[str, float]:
        """Benchmark an async operation and return statistics."""
        times = []

        for _ in range(iterations):
            start_time = time.perf_counter()

            if asyncio.iscoroutinefunction(operation):
                await operation(*args, **kwargs)
            else:
                operation(*args, **kwargs)

            end_time = time.perf_counter()
            times.append(end_time - start_time)

        return {
            "min": min(times),
            "max": max(times),
            "avg": sum(times) / len(times),
            "total": sum(times),
            "iterations": iterations,
        }


class SecurityTestCase(BaseTestCase):
    """Base test class for security-related tests."""

    def setup_security_test(self):
        """Set up security test environment."""
        self.security_events = []
        self.mock_crypto = MockCrypto()

    def record_security_event(self, event_type: str, details: Dict[str, Any]):
        """Record a security event for analysis."""
        event = {"type": event_type, "details": details, "timestamp": time.time()}
        self.security_events.append(event)

    def assert_no_security_violations(self):
        """Assert that no security violations were recorded."""
        violations = [e for e in self.security_events if e["type"] == "violation"]
        assert not violations, f"Security violations detected: {violations}"

    def simulate_attack_scenario(self, attack_type: str, **params):
        """Simulate various attack scenarios for testing."""
        if attack_type == "weak_password":
            return self.simulate_weak_password_attack(**params)
        elif attack_type == "hash_collision":
            return self.simulate_hash_collision_attack(**params)
        elif attack_type == "timing_attack":
            return self.simulate_timing_attack(**params)
        else:
            raise ValueError(f"Unknown attack type: {attack_type}")

    def simulate_weak_password_attack(self, password: str = "123456"):
        """Simulate weak password attack."""
        self.record_security_event(
            "attack_attempt", {"type": "weak_password", "password": password}
        )
        return len(password) < 8  # Simple weak password check

    def simulate_hash_collision_attack(self, data1: bytes, data2: bytes):
        """Simulate hash collision attack."""
        self.mock_crypto.add_hash_collision(data1, data2)
        self.record_security_event(
            "attack_attempt",
            {"type": "hash_collision", "data_sizes": [len(data1), len(data2)]},
        )

    def simulate_timing_attack(self, operation: Callable, *args, **kwargs):
        """Simulate timing attack by measuring operation duration."""
        start_time = time.perf_counter()
        result = operation(*args, **kwargs)
        duration = time.perf_counter() - start_time

        self.record_security_event(
            "timing_measurement",
            {"operation": operation.__name__, "duration": duration},
        )

        return result, duration


class NetworkTestCase(AsyncTestCase):
    """Base test class for network-related tests."""

    def setup_network_test(self):
        """Set up network test environment."""
        self.network_conditions = NetworkCondition()
        self.mock_transport = MockTransportManager()

    def set_network_conditions(self, **conditions):
        """Set network conditions for testing."""
        for key, value in conditions.items():
            if hasattr(self.network_conditions, key):
                setattr(self.network_conditions, key, value)

        self.mock_transport.set_network_conditions(self.network_conditions)

    async def simulate_network_failure(self, duration: float = 1.0):
        """Simulate network failure for specified duration."""
        original_connected = self.network_conditions.is_connected
        self.network_conditions.is_connected = False

        await asyncio.sleep(duration)

        self.network_conditions.is_connected = original_connected

    async def simulate_slow_network(
        self, latency_ms: int = 1000, duration: float = 1.0
    ):
        """Simulate slow network conditions."""
        original_latency = self.network_conditions.latency_ms
        self.network_conditions.latency_ms = latency_ms

        await asyncio.sleep(duration)

        self.network_conditions.latency_ms = original_latency

    def assert_network_resilience(self, operation: Callable, *args, **kwargs):
        """Assert that an operation is resilient to network issues."""
        # Test with various network conditions
        test_conditions = [
            {"latency_ms": 500},
            {"packet_loss_percent": 10.0},
            {"bandwidth_kbps": 56},  # Dial-up speed
        ]

        for conditions in test_conditions:
            self.set_network_conditions(**conditions)

            # Operation should still work, possibly with degraded performance
            try:
                if asyncio.iscoroutinefunction(operation):
                    asyncio.create_task(operation(*args, **kwargs))
                else:
                    operation(*args, **kwargs)
            except Exception as e:
                pytest.fail(
                    f"Operation failed under network conditions {conditions}: {e}"
                )


# ============================================================================
# Test Mixins
# ============================================================================


class FilesystemTestMixin:
    """Mixin for tests that need filesystem operations."""

    def setup_filesystem_test(self, temp_dir: pathlib.Path):
        """Set up filesystem test environment."""
        self.temp_dir = temp_dir
        self.mock_filesystem = MockFilesystem()

    def create_test_directory_structure(self) -> pathlib.Path:
        """Create a standard test directory structure."""
        structure = {
            "file1.txt": b"Hello, World!",
            "file2.bin": b"\x00\x01\x02\x03" * 100,
            "subdir": {
                "nested_file.txt": b"Nested content",
                "empty_file.txt": b"",
            },
            "large_file.bin": b"X" * (CHUNK_SIZE + 1000),
        }

        from .test_utils import create_directory_tree

        create_directory_tree(self.temp_dir, structure)
        return self.temp_dir


class ConcurrencyTestMixin:
    """Mixin for tests that need concurrency testing utilities."""

    async def run_concurrent_operations(
        self, operations: List[Callable], max_concurrent: int = 10
    ) -> List[Any]:
        """Run multiple operations concurrently with limited concurrency."""
        semaphore = asyncio.Semaphore(max_concurrent)

        async def run_with_semaphore(operation):
            async with semaphore:
                if asyncio.iscoroutinefunction(operation):
                    return await operation()
                else:
                    return operation()

        tasks = [run_with_semaphore(op) for op in operations]
        return await asyncio.gather(*tasks, return_exceptions=True)

    def assert_no_race_conditions(self, results: List[Any]):
        """Assert that concurrent operations didn't cause race conditions."""
        # Check for exceptions that might indicate race conditions
        exceptions = [r for r in results if isinstance(r, Exception)]
        race_condition_indicators = [
            "already exists",
            "file not found",
            "resource busy",
            "permission denied",
        ]

        for exc in exceptions:
            exc_str = str(exc).lower()
            for indicator in race_condition_indicators:
                if indicator in exc_str:
                    pytest.fail(f"Possible race condition detected: {exc}")


# ============================================================================
# Utility Functions for Test Classes
# ============================================================================


def skip_if_no_network():
    """Skip test if network is not available."""
    import socket

    try:
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        return False
    except OSError:
        return pytest.mark.skip("Network not available")


def requires_large_disk_space(required_gb: int = 1):
    """Skip test if insufficient disk space is available."""
    import shutil

    def decorator(test_func):
        def wrapper(*args, **kwargs):
            free_space = shutil.disk_usage(".").free
            required_bytes = required_gb * 1024 * 1024 * 1024

            if free_space < required_bytes:
                pytest.skip(f"Insufficient disk space: {free_space} < {required_bytes}")

            return test_func(*args, **kwargs)

        return wrapper

    return decorator

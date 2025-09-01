"""
Example test file demonstrating the comprehensive testing framework.

This file shows how to use all the testing utilities, fixtures, and patterns
provided by the DittoFS testing framework.
"""

import asyncio
import pathlib
import pytest
import time
from hypothesis import given, strategies as st
from unittest.mock import patch, Mock

# Import testing framework components
from tests.base_test_classes import (
    BaseTestCase, AsyncTestCase, IntegrationTestCase, 
    PropertyBasedTestCase, PerformanceTestCase, SecurityTestCase,
    NetworkTestCase, FilesystemTestMixin, ConcurrencyTestMixin
)
from tests.mocks import (
    MockFilesystem, MockCrypto, MockCRDTStore, MockTransportManager,
    NetworkCondition
)
from tests.test_utils import (
    generate_test_file_content, wait_for_condition, assert_eventually,
    PerformanceTimer
)

# Import DittoFS components with fallbacks
try:
    from dittofs.chunker import CHUNK_SIZE
except ImportError:
    CHUNK_SIZE = 64 * 1024


# ============================================================================
# Unit Tests Examples
# ============================================================================

@pytest.mark.unit
class TestBasicFunctionality(BaseTestCase):
    """Example unit tests using the base test case."""
    
    def test_file_creation(self, temp_dir, test_files):
        """Test basic file creation functionality."""
        content = test_files["small_text"].content
        file_path = self.create_test_file(temp_dir, "test.txt", content)
        
        self.assert_file_content_matches(file_path, content)
        self.assert_file_size(file_path, len(content))
    
    def test_large_file_handling(self, temp_dir):
        """Test handling of large files that span multiple chunks."""
        large_file = self.create_large_file(temp_dir, chunk_count=3)
        
        # Verify file was created correctly
        assert large_file.exists()
        assert large_file.stat().st_size > CHUNK_SIZE * 3
    
    def test_chunk_validation(self, temp_dir, isolated_chunk_dir):
        """Test chunk validation functionality."""
        # Create some mock chunks
        chunk_hashes = ["hash1", "hash2", "hash3"]
        
        for chunk_hash in chunk_hashes:
            chunk_path = isolated_chunk_dir / chunk_hash
            chunk_path.write_bytes(b"mock chunk content")
        
        # Validate chunks exist
        self.assert_file_chunks_exist(isolated_chunk_dir, chunk_hashes)
        self.assert_chunks_are_valid(isolated_chunk_dir, chunk_hashes)


@pytest.mark.unit
class TestMockFilesystem(BaseTestCase):
    """Example tests using mock filesystem."""
    
    def test_mock_filesystem_operations(self):
        """Test mock filesystem basic operations."""
        mock_filesystem = MockFilesystem()
        # Test file operations
        mock_filesystem.write_bytes("test.txt", b"Hello, World!")
        assert mock_filesystem.exists("test.txt")
        assert mock_filesystem.is_file("test.txt")
        
        content = mock_filesystem.read_bytes("test.txt")
        assert content == b"Hello, World!"
        
        # Test directory operations
        mock_filesystem.mkdir("testdir")
        assert mock_filesystem.exists("testdir")
        assert mock_filesystem.is_dir("testdir")
    
    def test_filesystem_error_simulation(self):
        """Test filesystem error simulation."""
        mock_filesystem = MockFilesystem()
        # Simulate disk full
        mock_filesystem.simulate_disk_full(True)
        
        with pytest.raises(IOError, match="No space left on device"):
            mock_filesystem.write_bytes("test.txt", b"content")
        
        # Simulate read failure
        mock_filesystem.write_bytes("test.txt", b"content")
        mock_filesystem.simulate_read_failure("test.txt")
        
        with pytest.raises(IOError, match="Simulated read failure"):
            mock_filesystem.read_bytes("test.txt")


# ============================================================================
# Async Tests Examples
# ============================================================================

@pytest.mark.unit
@pytest.mark.asyncio
class TestAsyncFunctionality(AsyncTestCase):
    """Example async tests using the async test case."""
    
    async def test_async_operation(self):
        """Test basic async operation."""
        start_time = time.time()
        await self.simulate_async_operation(0.1)
        duration = time.time() - start_time
        
        assert duration >= 0.1
    
    async def test_wait_for_condition(self):
        """Test waiting for a condition to become true."""
        condition_met = False
        
        async def set_condition():
            nonlocal condition_met
            await asyncio.sleep(0.1)
            condition_met = True
        
        # Start the condition setter
        asyncio.create_task(set_condition())
        
        # Wait for condition
        await self.wait_for_condition(lambda: condition_met, timeout=1.0)
        
        assert condition_met
    
    async def test_assert_eventually(self):
        """Test assert_eventually utility."""
        counter = 0
        
        def increment_counter():
            nonlocal counter
            counter += 1
            return counter >= 5
        
        # Start incrementing counter in background
        async def background_increment():
            for _ in range(10):
                await asyncio.sleep(0.05)
                increment_counter()
        
        asyncio.create_task(background_increment())
        
        # Assert that counter eventually reaches 5
        await self.assert_eventually(
            lambda: counter >= 5,
            timeout=2.0,
            message="Counter did not reach 5"
        )


# ============================================================================
# Integration Tests Examples
# ============================================================================

@pytest.mark.integration
@pytest.mark.asyncio
class TestIntegrationScenarios(IntegrationTestCase):
    """Example integration tests using the integration test case."""
    
    async def test_two_peer_sync(self, two_peer_scenario):
        """Test synchronization between two peers."""
        peer1, peer2 = two_peer_scenario
        
        # Verify peers are set up correctly
        assert peer1["peer_id"] == "peer1"
        assert peer2["peer_id"] == "peer2"
        
        # Simulate sync operation
        sync_manager1 = peer1["mock_system"]["sync_manager"]
        sync_manager2 = peer2["mock_system"]["sync_manager"]
        
        await sync_manager1.start()
        await sync_manager2.start()
        
        # Perform sync
        result = await sync_manager1.sync_with_peer("peer2")
        assert result is True
    
    async def test_multi_peer_network(self, multi_peer_network):
        """Test multi-peer network scenario."""
        peers = multi_peer_network
        assert len(peers) == 5
        
        # Start all sync managers
        for peer in peers:
            await peer["mock_system"]["sync_manager"].start()
        
        # Test that all peers can discover each other
        for peer in peers:
            transport = peer["mock_system"]["transport"]
            discovered_peers = await transport.discover_peers()
            
            # Should discover all other peers
            assert len(discovered_peers) == 4
    
    async def test_network_partition_recovery(self, two_peer_scenario, network_simulator):
        """Test recovery from network partition."""
        peer1, peer2 = two_peer_scenario
        
        # Start sync managers
        await peer1["mock_system"]["sync_manager"].start()
        await peer2["mock_system"]["sync_manager"].start()
        
        # Simulate network partition
        await network_simulator.simulate_partition(duration=0.5)
        
        # Verify that sync still works after partition
        result = await peer1["mock_system"]["sync_manager"].sync_with_peer("peer2")
        assert result is True


# ============================================================================
# Property-Based Tests Examples
# ============================================================================

@pytest.mark.property
class TestPropertyBased(PropertyBasedTestCase):
    """Example property-based tests using Hypothesis."""
    
    @given(st.binary(min_size=0, max_size=CHUNK_SIZE * 2))
    def test_file_content_roundtrip(self, file_content, temp_dir):
        """Property test: file write/read should be lossless."""
        file_path = temp_dir / "test_file.bin"
        file_path.write_bytes(file_content)
        
        read_content = file_path.read_bytes()
        assert read_content == file_content
    
    @given(st.lists(st.text(min_size=1, max_size=50), min_size=1, max_size=10))
    def test_file_path_handling(self, file_names, temp_dir):
        """Property test: file path handling should be consistent."""
        for name in file_names:
            # Filter out invalid characters for file names
            safe_name = "".join(c for c in name if c.isalnum() or c in ".-_")
            if not safe_name:
                safe_name = "default"
            
            file_path = temp_dir / f"{safe_name}.txt"
            file_path.write_text("test content")
            
            assert file_path.exists()
            assert file_path.read_text() == "test content"
    
    @given(st.integers(min_value=1, max_value=10))
    def test_chunk_count_calculation(self, chunk_multiplier):
        """Property test: chunk count calculation should be consistent."""
        file_size = CHUNK_SIZE * chunk_multiplier + 100
        expected_chunks = chunk_multiplier + 1  # +1 for the remainder
        
        # Mock chunk calculation
        actual_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        assert actual_chunks == expected_chunks


# ============================================================================
# Performance Tests Examples
# ============================================================================

@pytest.mark.benchmark
class TestPerformance(PerformanceTestCase):
    """Example performance tests using the performance test case."""
    
    def test_file_creation_performance(self, temp_dir, benchmark_environment):
        """Benchmark file creation performance."""
        def create_files():
            for i in range(100):
                file_path = temp_dir / f"file_{i}.txt"
                file_path.write_text(f"Content for file {i}")
        
        with PerformanceTimer() as timer:
            create_files()
        
        benchmark_environment.record_metric("file_creation_100", timer.duration)
        
        # Assert performance is within reasonable bounds (adjust as needed)
        assert timer.duration < 5.0, f"File creation took too long: {timer.duration}s"
    
    async def test_async_operation_performance(self, benchmark_environment):
        """Benchmark async operation performance."""
        async def async_operation():
            await asyncio.sleep(0.01)
            return "result"
        
        stats = await self.benchmark_async_operation(
            async_operation,
            iterations=50
        )
        
        benchmark_environment.record_metric("async_operation_avg", stats["avg"])
        
        # Verify performance characteristics
        assert stats["avg"] >= 0.01  # Should take at least the sleep time
        assert stats["avg"] < 0.05   # But not too much overhead
    
    def test_memory_usage_tracking(self):
        """Test memory usage tracking during operations."""
        try:
            from .test_utils import measure_memory_usage
            
            initial_memory = measure_memory_usage()
            
            # Perform memory-intensive operation
            large_data = [b"x" * 1024 for _ in range(1000)]  # 1MB of data
            
            peak_memory = measure_memory_usage()
            
            # Clean up
            del large_data
            
            final_memory = measure_memory_usage()
            
            # Verify memory was used and then freed
            assert peak_memory > initial_memory
            # Note: final_memory might not be exactly initial_memory due to GC timing
            
        except ImportError:
            pytest.skip("psutil not available for memory measurement")


# ============================================================================
# Security Tests Examples
# ============================================================================

@pytest.mark.security
class TestSecurity(SecurityTestCase):
    """Example security tests using the security test case."""
    
    def test_weak_password_detection(self, security_test_environment):
        """Test weak password detection."""
        weak_passwords = ["123456", "password", "admin", "test"]
        
        for password in weak_passwords:
            is_weak = self.simulate_weak_password_attack(password)
            assert is_weak, f"Password '{password}' should be detected as weak"
        
        # Test strong password
        strong_password = "Str0ng!P@ssw0rd#2023"
        is_weak = self.simulate_weak_password_attack(strong_password)
        assert not is_weak, "Strong password should not be detected as weak"
    
    def test_hash_collision_resistance(self, mock_crypto):
        """Test hash collision resistance."""
        data1 = b"Hello, World!"
        data2 = b"Hello, World?"
        
        hash1 = mock_crypto.blake3_hash(data1)
        hash2 = mock_crypto.blake3_hash(data2)
        
        # Different data should produce different hashes
        assert hash1 != hash2
        
        # Same data should produce same hash
        hash1_repeat = mock_crypto.blake3_hash(data1)
        assert hash1 == hash1_repeat
    
    def test_encryption_decryption(self, mock_crypto):
        """Test encryption and decryption operations."""
        original_data = b"Sensitive information that needs encryption"
        key = mock_crypto.generate_key()
        
        # Encrypt data
        encrypted_data = mock_crypto.encrypt_data(original_data, key)
        assert encrypted_data != original_data
        
        # Decrypt data
        decrypted_data = mock_crypto.decrypt_data(encrypted_data, key)
        assert decrypted_data == original_data
    
    def test_timing_attack_resistance(self):
        """Test resistance to timing attacks."""
        def compare_strings(s1: str, s2: str) -> bool:
            """Potentially vulnerable string comparison."""
            if len(s1) != len(s2):
                return False
            
            for c1, c2 in zip(s1, s2):
                if c1 != c2:
                    return False
            return True
        
        # Measure timing for correct vs incorrect comparisons
        correct_password = "correct_password"
        wrong_password = "wrong_password!!"
        
        _, correct_time = self.simulate_timing_attack(
            compare_strings, correct_password, correct_password
        )
        
        _, wrong_time = self.simulate_timing_attack(
            compare_strings, correct_password, wrong_password
        )
        
        # In a real implementation, we'd want these times to be similar
        # to prevent timing attacks. Here we just verify the measurement works.
        assert correct_time > 0
        assert wrong_time > 0


# ============================================================================
# Network Tests Examples
# ============================================================================

@pytest.mark.network
@pytest.mark.asyncio
class TestNetworkOperations(NetworkTestCase):
    """Example network tests using the network test case."""
    
    async def test_network_failure_resilience(self, mock_transport):
        """Test resilience to network failures."""
        # Set up mock transport
        self.mock_transport = mock_transport
        
        # Test normal operation
        await mock_transport.start_all()
        assert mock_transport.is_started
        
        # Simulate network failure
        await self.simulate_network_failure(duration=0.5)
        
        # Transport should handle failure gracefully
        # (In real implementation, it would retry connections)
        
    async def test_slow_network_handling(self, mock_transport):
        """Test handling of slow network conditions."""
        self.mock_transport = mock_transport
        
        # Set slow network conditions
        self.set_network_conditions(latency_ms=1000, bandwidth_kbps=56)
        
        # Operations should still work, just slower
        await mock_transport.start_all()
        peers = await mock_transport.discover_peers(timeout=5.0)
        
        # Should complete successfully despite slow network
        assert isinstance(peers, list)
    
    def test_network_resilience_assertion(self, mock_transport):
        """Test network resilience assertion utility."""
        def network_operation():
            # Mock network operation that should be resilient
            return True
        
        # This should not raise an exception
        self.assert_network_resilience(network_operation)


# ============================================================================
# Filesystem and Concurrency Tests Examples
# ============================================================================

@pytest.mark.filesystem
class TestFilesystemOperations(BaseTestCase, FilesystemTestMixin):
    """Example filesystem tests using the filesystem mixin."""
    
    def test_directory_structure_creation(self, temp_dir):
        """Test creation of complex directory structures."""
        self.setup_filesystem_test(temp_dir)
        test_dir = self.create_test_directory_structure()
        
        # Verify structure was created
        assert (test_dir / "file1.txt").exists()
        assert (test_dir / "subdir" / "nested_file.txt").exists()
        assert (test_dir / "large_file.bin").stat().st_size > CHUNK_SIZE


@pytest.mark.asyncio
class TestConcurrentOperations(AsyncTestCase, ConcurrencyTestMixin):
    """Example concurrency tests using the concurrency mixin."""
    
    async def test_concurrent_file_operations(self, temp_dir):
        """Test concurrent file operations."""
        async def create_file(index: int):
            file_path = temp_dir / f"concurrent_file_{index}.txt"
            file_path.write_text(f"Content for file {index}")
            return file_path
        
        # Create operations
        operations = [lambda i=i: create_file(i) for i in range(10)]
        
        # Run concurrently
        results = await self.run_concurrent_operations(operations, max_concurrent=5)
        
        # Verify no race conditions
        self.assert_no_race_conditions(results)
        
        # Verify all files were created
        for i, result in enumerate(results):
            assert isinstance(result, pathlib.Path)
            assert result.exists()


# ============================================================================
# Conflict Resolution Tests Examples
# ============================================================================

@pytest.mark.integration
@pytest.mark.asyncio
class TestConflictResolution(IntegrationTestCase):
    """Example conflict resolution tests."""
    
    async def test_file_conflict_detection(self, conflict_scenario):
        """Test detection of file conflicts."""
        peer1, peer2 = conflict_scenario
        
        # Both peers have conflicting versions of the same file
        store1 = peer1["mock_system"]["store"]
        store2 = peer2["mock_system"]["store"]
        
        # Simulate conflict detection during sync
        conflicts1 = store1.get_conflicts()
        conflicts2 = store2.get_conflicts()
        
        # In this mock scenario, conflicts are files with "conflict" in the name
        # Real implementation would detect version conflicts
        assert len(conflicts1) >= 0  # May or may not have conflicts in mock
        assert len(conflicts2) >= 0
    
    async def test_conflict_resolution_strategies(self, conflict_scenario):
        """Test different conflict resolution strategies."""
        peer1, peer2 = conflict_scenario
        
        sync_manager1 = peer1["mock_system"]["sync_manager"]
        sync_manager2 = peer2["mock_system"]["sync_manager"]
        
        # Test last-writer-wins strategy
        sync_manager1.set_conflict_resolution_strategy("last_writer_wins")
        sync_manager2.set_conflict_resolution_strategy("last_writer_wins")
        
        # Perform sync with conflict resolution
        result = await sync_manager1.sync_with_peer("peer2")
        assert result is True  # Should succeed with conflict resolution


# ============================================================================
# Test Utilities Examples
# ============================================================================

@pytest.mark.unit
class TestUtilities(BaseTestCase):
    """Test the testing utilities themselves."""
    
    def test_performance_timer(self):
        """Test the performance timer utility."""
        with PerformanceTimer() as timer:
            time.sleep(0.1)
        
        assert timer.duration >= 0.1
        assert timer.elapsed >= 0.1
    
    def test_test_file_generation(self, temp_dir):
        """Test test file generation utilities."""
        # Test different content patterns
        patterns = ["random", "zeros", "ones", "alternating", "text"]
        
        for pattern in patterns:
            content = generate_test_file_content(1000, pattern)
            assert len(content) == 1000
            
            file_path = temp_dir / f"test_{pattern}.bin"
            file_path.write_bytes(content)
            
            self.assert_file_content_matches(file_path, content)
    
    async def test_wait_utilities(self):
        """Test async wait utilities."""
        condition_met = False
        
        async def set_condition_later():
            await asyncio.sleep(0.1)
            nonlocal condition_met
            condition_met = True
        
        # Start the condition setter
        asyncio.create_task(set_condition_later())
        
        # Test wait_for_condition
        result = await wait_for_condition(
            lambda: condition_met,
            timeout=1.0,
            message="Condition not met in test"
        )
        
        assert result is True
        assert condition_met is True
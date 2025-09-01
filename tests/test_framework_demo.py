"""
Simple demonstration of the comprehensive testing framework.

This file shows basic usage of the testing framework components.
"""

import pytest
import asyncio
import time
from hypothesis import given, strategies as st

from tests.base_test_classes import BaseTestCase, AsyncTestCase
from tests.mocks import MockFilesystem, MockCrypto
from tests.test_utils import generate_test_file_content, PerformanceTimer


@pytest.mark.unit
class TestFrameworkDemo(BaseTestCase):
    """Demonstration of basic testing framework features."""
    
    def test_mock_filesystem(self):
        """Test that mock filesystem works correctly."""
        fs = MockFilesystem()
        
        # Test file operations
        fs.write_bytes("test.txt", b"Hello, DittoFS!")
        assert fs.exists("test.txt")
        assert fs.is_file("test.txt")
        
        content = fs.read_bytes("test.txt")
        assert content == b"Hello, DittoFS!"
    
    def test_mock_crypto(self):
        """Test that mock crypto operations work."""
        crypto = MockCrypto()
        
        # Test hashing
        data = b"test data"
        hash1 = crypto.blake3_hash(data)
        hash2 = crypto.blake3_hash(data)
        
        # Same data should produce same hash
        assert hash1 == hash2
        
        # Different data should produce different hash
        different_data = b"different data"
        hash3 = crypto.blake3_hash(different_data)
        assert hash1 != hash3
    
    def test_file_creation_utility(self, temp_dir):
        """Test file creation utilities."""
        content = b"Test file content"
        file_path = self.create_test_file(temp_dir, "demo.txt", content)
        
        self.assert_file_content_matches(file_path, content)
        self.assert_file_size(file_path, len(content))
    
    def test_performance_measurement(self):
        """Test performance measurement utilities."""
        with PerformanceTimer() as timer:
            # Simulate some work
            time.sleep(0.1)
        
        assert timer.duration >= 0.1
        assert timer.elapsed >= 0.1


@pytest.mark.unit
@pytest.mark.asyncio
class TestAsyncFrameworkDemo(AsyncTestCase):
    """Demonstration of async testing framework features."""
    
    async def test_async_wait_utilities(self):
        """Test async wait utilities."""
        condition_met = False
        
        async def set_condition_later():
            await asyncio.sleep(0.1)
            nonlocal condition_met
            condition_met = True
        
        # Start the condition setter
        asyncio.create_task(set_condition_later())
        
        # Wait for condition
        await self.wait_for_condition(
            lambda: condition_met,
            timeout=1.0,
            message="Condition should be met"
        )
        
        assert condition_met
    
    async def test_assert_eventually(self):
        """Test assert_eventually utility."""
        counter = 0
        
        async def increment_counter():
            nonlocal counter
            for _ in range(5):
                await asyncio.sleep(0.02)
                counter += 1
        
        # Start incrementing
        asyncio.create_task(increment_counter())
        
        # Assert counter eventually reaches 3
        await self.assert_eventually(
            lambda: counter >= 3,
            timeout=1.0,
            message="Counter should reach 3"
        )


@pytest.mark.property
class TestPropertyBasedDemo:
    """Demonstration of property-based testing."""
    
    @given(st.binary(min_size=0, max_size=1000))
    def test_file_content_generation(self, content):
        """Property test: generated content should match expected patterns."""
        # Test that we can generate content of various sizes
        generated = generate_test_file_content(len(content), "random")
        assert len(generated) == len(content)
    
    @given(st.integers(min_value=1, max_value=100))
    def test_mock_filesystem_consistency(self, file_count):
        """Property test: mock filesystem should handle multiple files."""
        fs = MockFilesystem()
        
        # Create multiple files
        for i in range(file_count):
            filename = f"file_{i}.txt"
            content = f"Content for file {i}".encode()
            fs.write_bytes(filename, content)
        
        # Verify all files exist and have correct content
        for i in range(file_count):
            filename = f"file_{i}.txt"
            assert fs.exists(filename)
            
            expected_content = f"Content for file {i}".encode()
            actual_content = fs.read_bytes(filename)
            assert actual_content == expected_content


@pytest.mark.unit
class TestErrorHandling(BaseTestCase):
    """Test error handling in the framework."""
    
    def test_filesystem_error_simulation(self):
        """Test filesystem error simulation."""
        fs = MockFilesystem()
        
        # Test disk full simulation
        fs.simulate_disk_full(True)
        
        with pytest.raises(IOError, match="No space left on device"):
            fs.write_bytes("test.txt", b"content")
        
        # Clear the error condition
        fs.simulate_disk_full(False)
        
        # Should work now
        fs.write_bytes("test.txt", b"content")
        assert fs.read_bytes("test.txt") == b"content"
    
    def test_crypto_error_simulation(self):
        """Test crypto error simulation."""
        crypto = MockCrypto()
        
        test_data = b"test data"
        
        # Simulate hash failure
        crypto.simulate_hash_failure(test_data)
        
        with pytest.raises(RuntimeError, match="Simulated hash failure"):
            crypto.blake3_hash(test_data)
        
        # Clear failures
        crypto.clear_failures()
        
        # Should work now
        hash_result = crypto.blake3_hash(test_data)
        assert isinstance(hash_result, str)
        assert len(hash_result) > 0
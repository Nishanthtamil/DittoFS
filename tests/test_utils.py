"""
Test utilities and helper functions for DittoFS testing.

This module provides common utilities used across different test modules.
"""

import asyncio
import hashlib
import pathlib
import random
import socket
import time
from typing import Any, Callable, Dict, List, Optional, Union
from unittest.mock import AsyncMock, Mock

import pytest
from hypothesis import strategies as st

# ============================================================================
# Test Data Generation
# ============================================================================


def generate_random_bytes(size: int, seed: Optional[int] = None) -> bytes:
    """Generate random bytes with optional seed for reproducibility."""
    if seed is not None:
        random.seed(seed)
    return bytes(random.getrandbits(8) for _ in range(size))


def generate_test_file_content(size: int, pattern: str = "random") -> bytes:
    """Generate test file content with different patterns."""
    if pattern == "random":
        return generate_random_bytes(size)
    elif pattern == "zeros":
        return b"\x00" * size
    elif pattern == "ones":
        return b"\xff" * size
    elif pattern == "alternating":
        return bytes([i % 256 for i in range(size)])
    elif pattern == "text":
        text = "Hello, DittoFS! This is a test file.\n" * (size // 40 + 1)
        return text.encode("utf-8")[:size]
    else:
        raise ValueError(f"Unknown pattern: {pattern}")


def create_file_with_content(path: pathlib.Path, content: bytes) -> pathlib.Path:
    """Create a file with specific content."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


# ============================================================================
# Network Utilities
# ============================================================================


def get_free_port() -> int:
    """Get a free port for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


def get_multiple_free_ports(count: int) -> List[int]:
    """Get multiple free ports for testing."""
    ports = []
    for _ in range(count):
        ports.append(get_free_port())
    return ports


# ============================================================================
# Async Test Utilities
# ============================================================================


async def wait_for_condition(
    condition: Callable[[], Union[bool, Any]],
    timeout: float = 5.0,
    interval: float = 0.1,
    message: str = "Condition not met",
) -> bool:
    """Wait for a condition to become true."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            if asyncio.iscoroutinefunction(condition):
                result = await condition()
            else:
                result = condition()

            if result:
                return True
        except Exception:
            # Ignore exceptions during condition checking
            pass

        await asyncio.sleep(interval)

    raise TimeoutError(f"{message} (timeout after {timeout}s)")


async def assert_eventually(
    condition: Callable[[], Union[bool, Any]],
    timeout: float = 5.0,
    message: str = "Condition not met",
):
    """Assert that a condition eventually becomes true."""
    await wait_for_condition(condition, timeout, message=message)


class AsyncContextManager:
    """Helper for creating async context managers in tests."""

    def __init__(self, enter_result=None, exit_result=None):
        self.enter_result = enter_result
        self.exit_result = exit_result
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self.enter_result

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.exited = True
        return self.exit_result


# ============================================================================
# File System Utilities
# ============================================================================


def assert_files_equal(file1: pathlib.Path, file2: pathlib.Path):
    """Assert that two files have identical content."""
    content1 = file1.read_bytes()
    content2 = file2.read_bytes()
    assert content1 == content2, f"Files {file1} and {file2} have different content"


def assert_file_has_content(file_path: pathlib.Path, expected_content: bytes):
    """Assert that a file has specific content."""
    actual_content = file_path.read_bytes()
    assert (
        actual_content == expected_content
    ), f"File {file_path} has unexpected content"


def calculate_file_hash(file_path: pathlib.Path, algorithm: str = "sha256") -> str:
    """Calculate hash of a file."""
    hasher = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def create_directory_tree(base_path: pathlib.Path, structure: Dict[str, Any]):
    """Create a directory tree from a nested dictionary structure.

    Args:
        base_path: Base directory to create structure in
        structure: Dict where keys are names and values are either:
            - bytes: create file with this content
            - dict: create subdirectory with this structure
    """
    base_path.mkdir(parents=True, exist_ok=True)

    for name, content in structure.items():
        path = base_path / name

        if isinstance(content, bytes):
            # Create file
            path.write_bytes(content)
        elif isinstance(content, dict):
            # Create subdirectory
            create_directory_tree(path, content)
        else:
            raise ValueError(f"Invalid structure content type: {type(content)}")


# ============================================================================
# Mock Utilities
# ============================================================================


class MockAsyncIterator:
    """Mock async iterator for testing."""

    def __init__(self, items: List[Any]):
        self.items = items
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.items):
            raise StopAsyncIteration

        item = self.items[self.index]
        self.index += 1
        return item


def create_mock_with_async_methods(spec=None, **kwargs) -> Mock:
    """Create a mock with async methods."""
    mock = Mock(spec=spec, **kwargs)

    # Make all methods async by default
    if spec:
        for attr_name in dir(spec):
            if not attr_name.startswith("_"):
                attr = getattr(spec, attr_name)
                if callable(attr):
                    setattr(mock, attr_name, AsyncMock())

    return mock


# ============================================================================
# Hypothesis Strategies
# ============================================================================

# File content strategies
small_file_content = st.binary(min_size=0, max_size=1024)
medium_file_content = st.binary(min_size=1024, max_size=64 * 1024)
large_file_content = st.binary(min_size=64 * 1024, max_size=5 * 64 * 1024)

# File name strategies
valid_filename = st.text(
    alphabet=st.characters(
        whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters=".-_"
    ),
    min_size=1,
    max_size=100,
).filter(lambda x: not x.startswith(".") and "/" not in x and "\\" not in x)

# Network strategies
port_number = st.integers(min_value=1024, max_value=65535)
ip_address = st.builds(
    lambda a, b, c, d: f"{a}.{b}.{c}.{d}",
    st.integers(min_value=0, max_value=255),
    st.integers(min_value=0, max_value=255),
    st.integers(min_value=0, max_value=255),
    st.integers(min_value=0, max_value=255),
)

# Peer ID strategies
peer_id = st.text(
    alphabet=st.characters(
        whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"
    ),
    min_size=1,
    max_size=50,
)

# Hash strategies (for chunk hashes)
chunk_hash = st.text(alphabet="0123456789abcdef", min_size=64, max_size=64)

# Time strategies
timestamp = st.floats(
    min_value=time.time() - 365 * 24 * 3600,  # One year ago
    max_value=time.time() + 365 * 24 * 3600,  # One year from now
)


# ============================================================================
# Performance Testing Utilities
# ============================================================================


class PerformanceTimer:
    """Context manager for measuring execution time."""

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.duration = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        self.duration = self.end_time - self.start_time

    @property
    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self.duration is not None:
            return self.duration
        elif self.start_time is not None:
            return time.perf_counter() - self.start_time
        else:
            return 0.0


def measure_memory_usage():
    """Measure current memory usage (requires psutil)."""
    try:
        import psutil

        process = psutil.Process()
        return process.memory_info().rss
    except ImportError:
        pytest.skip("psutil not available for memory measurement")


# ============================================================================
# Security Testing Utilities
# ============================================================================


def generate_weak_password() -> str:
    """Generate a weak password for security testing."""
    weak_passwords = [
        "password",
        "123456",
        "admin",
        "test",
        "qwerty",
        "password123",
        "admin123",
        "test123",
    ]
    return random.choice(weak_passwords)


def generate_strong_password(length: int = 16) -> str:
    """Generate a strong password for testing."""
    import string

    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(random.choice(chars) for _ in range(length))


def create_test_certificate_data() -> Dict[str, bytes]:
    """Create test certificate data for testing."""
    # This would normally generate actual certificates
    # For testing, we use fake data
    return {
        "private_key": b"fake_private_key_data",
        "public_key": b"fake_public_key_data",
        "certificate": b"fake_certificate_data",
    }


# ============================================================================
# Logging and Debug Utilities
# ============================================================================


class TestLogCapture:
    """Capture log messages during testing."""

    def __init__(self):
        self.messages = []

    def capture(self, record):
        """Capture a log record."""
        self.messages.append(
            {
                "level": record.levelname,
                "message": record.getMessage(),
                "module": record.module,
                "timestamp": record.created,
            }
        )

    def get_messages(self, level: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get captured messages, optionally filtered by level."""
        if level is None:
            return self.messages.copy()
        return [msg for msg in self.messages if msg["level"] == level]

    def clear(self):
        """Clear captured messages."""
        self.messages.clear()


def debug_print(*args, **kwargs):
    """Debug print that only prints if PYTEST_DEBUG is set."""
    import os

    if os.environ.get("PYTEST_DEBUG"):
        print(*args, **kwargs)

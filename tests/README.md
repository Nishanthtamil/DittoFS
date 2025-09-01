# DittoFS Comprehensive Testing Framework

This directory contains a comprehensive testing framework for DittoFS that supports unit tests, integration tests, property-based testing, performance testing, security testing, and more.

## Framework Components

### Core Files

- **`conftest.py`** - Main pytest configuration with fixtures for multi-peer scenarios and network simulation
- **`mocks.py`** - Comprehensive mock objects for external dependencies (network, filesystem, crypto)
- **`test_utils.py`** - Utility functions and Hypothesis strategies for testing
- **`base_test_classes.py`** - Base test classes with common testing patterns
- **`fixtures.py`** - Additional fixtures for complex test scenarios
- **`test_runner.py`** - Comprehensive test runner script with various test categories

### Test Categories

The framework supports the following test categories (marked with pytest markers):

- **`unit`** - Unit tests for individual components
- **`integration`** - Integration tests for multi-component scenarios
- **`property`** - Property-based tests using Hypothesis
- **`benchmark`** - Performance and benchmark tests
- **`security`** - Security-related tests
- **`network`** - Network operation tests
- **`filesystem`** - File system operation tests
- **`slow`** - Slow tests that are normally skipped
- **`crypto`** - Cryptographic operation tests

## Quick Start

### Installation

Install the development dependencies:

```bash
pip install -e .[dev]
# or
make install-dev
```

### Running Tests

Use the Makefile for convenient test execution:

```bash
# Run quick tests (unit + basic integration)
make test

# Run all tests except slow ones
make test-all

# Run specific test categories
make test-unit
make test-integration
make test-property
make test-performance
make test-security
make test-network

# Run tests in parallel
make test-parallel

# Run with coverage
make coverage
```

Or use the test runner directly:

```bash
# Run quick tests
python tests/test_runner.py quick --verbose

# Run all tests
python tests/test_runner.py all --verbose

# Run specific category
python tests/test_runner.py unit --verbose
```

Or use pytest directly:

```bash
# Run all tests
pytest tests/

# Run specific markers
pytest -m unit tests/
pytest -m "integration and not slow" tests/

# Run with coverage
pytest --cov=src/dittofs tests/
```

## Framework Features

### Mock Objects

The framework provides comprehensive mock objects for testing:

#### MockFilesystem
```python
from tests.mocks import MockFilesystem

fs = MockFilesystem()
fs.write_bytes("test.txt", b"Hello")
content = fs.read_bytes("test.txt")

# Simulate errors
fs.simulate_disk_full(True)
fs.simulate_read_failure("test.txt")
```

#### MockCrypto
```python
from tests.mocks import MockCrypto

crypto = MockCrypto()
hash_value = crypto.blake3_hash(b"data")
encrypted = crypto.encrypt_data(b"secret", b"key")

# Simulate failures
crypto.simulate_hash_failure(b"data")
```

#### MockTransportManager
```python
from tests.mocks import MockTransportManager, NetworkCondition

transport = MockTransportManager()
await transport.start_all()

# Simulate network conditions
conditions = NetworkCondition(latency_ms=100, packet_loss_percent=5.0)
transport.set_network_conditions(conditions)
```

### Base Test Classes

Use base test classes for common testing patterns:

```python
from tests.base_test_classes import BaseTestCase, AsyncTestCase

class TestMyFeature(BaseTestCase):
    def test_file_operations(self, temp_dir):
        file_path = self.create_test_file(temp_dir, "test.txt", b"content")
        self.assert_file_content_matches(file_path, b"content")

class TestAsyncFeature(AsyncTestCase):
    async def test_async_operation(self):
        await self.wait_for_condition(lambda: True, timeout=1.0)
```

### Multi-Peer Testing

Test multi-peer scenarios using fixtures:

```python
@pytest.mark.integration
@pytest.mark.asyncio
async def test_two_peer_sync(two_peer_scenario):
    peer1, peer2 = two_peer_scenario
    
    # Test synchronization between peers
    result = await peer1["mock_system"]["sync_manager"].sync_with_peer("peer2")
    assert result is True

@pytest.mark.integration
@pytest.mark.asyncio
async def test_multi_peer_network(multi_peer_network):
    peers = multi_peer_network
    assert len(peers) == 5
    
    # Test network-wide operations
    for peer in peers:
        await peer["mock_system"]["sync_manager"].start()
```

### Property-Based Testing

Use Hypothesis for property-based testing:

```python
from hypothesis import given, strategies as st

@pytest.mark.property
class TestProperties:
    @given(st.binary(min_size=0, max_size=1000))
    def test_file_roundtrip(self, content):
        # Test that file operations are lossless
        pass
```

### Performance Testing

Measure and benchmark performance:

```python
from tests.test_utils import PerformanceTimer

@pytest.mark.benchmark
class TestPerformance:
    def test_operation_speed(self):
        with PerformanceTimer() as timer:
            # Perform operation
            pass
        
        assert timer.duration < 1.0  # Should complete in under 1 second
```

### Network Simulation

Test network conditions and failures:

```python
@pytest.mark.network
@pytest.mark.asyncio
async def test_network_resilience(network_simulator):
    # Simulate network partition
    await network_simulator.simulate_partition(duration=1.0)
    
    # Simulate slow network
    await network_simulator.simulate_slow_network(latency_ms=1000, duration=1.0)
```

## Configuration

### pytest Configuration

The framework is configured in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
minversion = "7.0"
addopts = [
    "--strict-markers",
    "--strict-config",
    "--cov=src/dittofs",
    "--cov-report=term-missing",
    "--cov-report=html:htmlcov",
    "--cov-report=xml",
    "--cov-fail-under=80",
    "--timeout=300",
    "-ra",
    "-v",
]
testpaths = ["tests"]
asyncio_mode = "auto"
timeout = 300
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "integration: marks tests as integration tests",
    "network: marks tests that require network access",
    "crypto: marks tests that involve cryptographic operations",
    "filesystem: marks tests that interact with the filesystem",
    "unit: marks tests as unit tests",
    "property: marks property-based tests using hypothesis",
    "benchmark: marks performance benchmark tests",
    "security: marks security-related tests",
]
```

### Coverage Configuration

Coverage is configured to exclude test files and focus on source code:

```toml
[tool.coverage.run]
source = ["src"]
omit = [
    "*/tests/*",
    "*/test_*",
    "*/__pycache__/*",
    "*/site-packages/*",
    "*/hello_dittofs.py",  # Example/demo code
]
```

## Writing Tests

### Test Organization

Organize tests by functionality:

```
tests/
├── test_chunker.py          # Chunking functionality tests
├── test_crdt_store.py       # CRDT store tests
├── test_sync_manager.py     # Synchronization tests
├── test_transport.py        # Transport layer tests
├── test_security.py         # Security tests
├── test_integration.py      # Integration tests
└── test_performance.py      # Performance tests
```

### Test Naming

Follow consistent naming conventions:

- Test files: `test_<module>.py`
- Test classes: `Test<Feature>`
- Test methods: `test_<specific_behavior>`

### Test Markers

Use appropriate markers for test categorization:

```python
@pytest.mark.unit
def test_basic_functionality():
    pass

@pytest.mark.integration
@pytest.mark.asyncio
async def test_multi_component():
    pass

@pytest.mark.slow
def test_large_dataset():
    pass

@pytest.mark.property
@given(st.binary())
def test_property(data):
    pass
```

### Fixtures

Use fixtures for test setup:

```python
@pytest.fixture
def sample_data():
    return {"key": "value"}

def test_with_fixture(sample_data, temp_dir):
    # Use fixtures in tests
    pass
```

## Best Practices

### 1. Test Isolation

- Each test should be independent
- Use fixtures for setup and teardown
- Clean up resources after tests

### 2. Mock External Dependencies

- Mock network operations
- Mock file system operations
- Mock cryptographic operations
- Use dependency injection for testability

### 3. Test Different Scenarios

- Test happy path
- Test error conditions
- Test edge cases
- Test concurrent operations

### 4. Performance Considerations

- Mark slow tests appropriately
- Use timeouts for async tests
- Measure performance where relevant

### 5. Documentation

- Write clear test descriptions
- Document complex test scenarios
- Include examples in docstrings

## Continuous Integration

The framework is designed to work with CI/CD pipelines:

```bash
# CI test command
make ci-test

# Or using the test runner
python tests/test_runner.py all --verbose --no-coverage
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure the package is installed in development mode (`pip install -e .`)
2. **Fixture Not Found**: Check fixture names and imports
3. **Async Test Failures**: Ensure proper async/await usage and event loop handling
4. **Coverage Issues**: Check coverage configuration and source paths

### Debug Mode

Run tests in debug mode for more information:

```bash
PYTEST_DEBUG=1 python -m pytest -v -s tests/
```

### Test Collection

See what tests are available:

```bash
pytest --collect-only tests/
```

## Contributing

When adding new tests:

1. Follow the existing patterns and conventions
2. Add appropriate markers
3. Include docstrings
4. Test both success and failure cases
5. Update this documentation if needed

## Examples

See `test_framework_demo.py` for comprehensive examples of using all framework features.
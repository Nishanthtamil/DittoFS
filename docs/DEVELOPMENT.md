# DittoFS Development Guide

This guide provides detailed information for developers working on DittoFS.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Development Environment](#development-environment)
- [Code Organization](#code-organization)
- [Development Workflow](#development-workflow)
- [Testing Strategy](#testing-strategy)
- [Debugging and Profiling](#debugging-and-profiling)
- [Performance Considerations](#performance-considerations)
- [Security Guidelines](#security-guidelines)

## Architecture Overview

DittoFS is built with a modular, layered architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interfaces                         │
│  CLI │ GUI │ Web Interface │ Mobile Apps │ REST API        │
├─────────────────────────────────────────────────────────────┤
│                    Core Services                           │
│  Sync Manager │ CRDT Store │ Security │ Storage Manager    │
├─────────────────────────────────────────────────────────────┤
│                   Transport Layer                          │
│  TCP/mDNS │ WebRTC │ Bluetooth │ HTTP/HTTPS               │
├─────────────────────────────────────────────────────────────┤
│                    Data Layer                              │
│  Chunk Storage │ File Index │ Metadata │ Audit Logs       │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

1. **Chunker** (`chunker.py`): Splits files into content-addressable chunks
2. **CRDT Store** (`crdt_store.py`): Manages file metadata with conflict-free replication
3. **Sync Manager** (`sync_manager.py`): Coordinates peer discovery and synchronization
4. **Transport Layer**: Multiple transport protocols for peer communication
5. **CLI** (`cli.py`): Command-line interface and main entry point

## Development Environment

### Quick Setup

Choose your preferred setup method:

```bash
# Automated setup (recommended)
./scripts/dev-setup.sh

# Docker environment
./scripts/docker-dev.sh build && ./scripts/docker-dev.sh start

# Manual setup
python -m venv .ditto
source .ditto/bin/activate
pip install -e .[dev,all]
pre-commit install
```

### Environment Variables

```bash
# Development configuration
export DITTOFS_ENV=development
export DITTOFS_LOG_LEVEL=DEBUG
export DITTOFS_CONFIG_PATH=~/.dittofs/dev/config.toml

# Testing configuration
export PYTEST_CURRENT_TEST=1
export HYPOTHESIS_MAX_EXAMPLES=100
```

### IDE Configuration

#### VS Code

Create `.vscode/settings.json`:

```json
{
    "python.defaultInterpreterPath": "./.ditto/bin/python",
    "python.linting.enabled": true,
    "python.linting.flake8Enabled": true,
    "python.linting.mypyEnabled": true,
    "python.formatting.provider": "black",
    "python.sortImports.args": ["--profile", "black"],
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": ["tests/"],
    "files.exclude": {
        "**/__pycache__": true,
        "**/*.pyc": true,
        ".pytest_cache": true,
        ".mypy_cache": true,
        "htmlcov": true
    }
}
```

#### PyCharm

1. Set interpreter to `.ditto/bin/python`
2. Enable pytest as test runner
3. Configure code style to use Black
4. Enable type checking with mypy

## Code Organization

### Package Structure

```
src/dittofs/
├── __init__.py           # Package initialization
├── cli.py               # Command-line interface
├── chunker.py           # File chunking and reconstruction
├── crdt_store.py        # CRDT-based metadata storage
├── sync_manager.py      # Synchronization coordination
├── transport.py         # Base transport abstractions
├── webrtc_transport.py  # WebRTC transport implementation
├── ble_peer.py          # Bluetooth Low Energy transport
├── lan_fallback.py      # Local network fallback
├── gui.py               # Optional GUI application
└── hello_dittofs.py     # Example/demo code
```

### Import Conventions

```python
# Standard library imports (alphabetical)
import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

# Third-party imports (alphabetical)
import blake3
import trio
from pycrdt import Doc

# Local imports (relative)
from .chunker import Chunker
from .crdt_store import CRDTStore
from .transport import Transport
```

### Naming Conventions

- **Files**: `snake_case.py`
- **Classes**: `PascalCase`
- **Functions/Methods**: `snake_case`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private members**: `_leading_underscore`

## Development Workflow

### 1. Feature Development

```bash
# Create feature branch
git checkout -b feature/delta-sync

# Develop with TDD approach
# 1. Write failing test
# 2. Implement minimal code to pass
# 3. Refactor and improve

# Run tests frequently
make test-unit

# Check code quality
make lint
make format
```

### 2. Testing Workflow

```bash
# Run specific test categories
make test-unit           # Fast unit tests
make test-integration    # Integration tests
make test-property       # Property-based tests
make test-security       # Security tests

# Run tests with coverage
make coverage

# Run specific test file
python tests/test_runner.py specific --test-path tests/test_chunker.py

# Debug specific test
pytest -v -s tests/test_chunker.py::test_chunk_large_file
```

### 3. Code Quality Checks

```bash
# Format code
black src/ tests/
isort src/ tests/

# Lint code
flake8 src/ tests/

# Type checking
mypy src/

# Security scanning
bandit -r src/

# Comprehensive quality check
make quality-check
```

## Testing Strategy

### Test Categories

1. **Unit Tests** (`tests/unit/`): Test individual components
2. **Integration Tests** (`tests/integration/`): Test component interactions
3. **Property Tests** (`tests/property/`): Property-based testing with Hypothesis
4. **Performance Tests** (`tests/performance/`): Benchmark and performance tests
5. **Security Tests** (`tests/security/`): Security-focused tests

### Writing Effective Tests

#### Unit Test Example

```python
import pytest
from unittest.mock import AsyncMock, patch
from dittofs.chunker import Chunker

class TestChunker:
    @pytest.fixture
    def chunker(self):
        return Chunker(chunk_size=1024)
    
    @pytest.mark.asyncio
    async def test_chunk_small_file(self, chunker):
        data = b"small file content"
        chunks = await chunker.chunk_data(data)
        
        assert len(chunks) == 1
        assert chunks[0].data == data
        assert chunks[0].hash == blake3.blake3(data).hexdigest()
    
    @pytest.mark.asyncio
    async def test_chunk_large_file(self, chunker):
        # Test with file larger than chunk size
        data = b"x" * 2048  # 2KB file with 1KB chunks
        chunks = await chunker.chunk_data(data)
        
        assert len(chunks) == 2
        assert len(chunks[0].data) == 1024
        assert len(chunks[1].data) == 1024
        
        # Verify reconstruction
        reconstructed = b"".join(chunk.data for chunk in chunks)
        assert reconstructed == data
```

#### Integration Test Example

```python
@pytest.mark.integration
@pytest.mark.asyncio
async def test_peer_synchronization():
    """Test file synchronization between two peers."""
    async with trio.open_nursery() as nursery:
        # Set up two peers
        peer1 = await create_test_peer("peer1", port=8001)
        peer2 = await create_test_peer("peer2", port=8002)
        
        # Add file to peer1
        test_file = Path("test_data.txt")
        test_file.write_text("Hello, DittoFS!")
        await peer1.add_file(test_file)
        
        # Connect peers
        await peer1.connect_to_peer("localhost", 8002)
        
        # Wait for synchronization
        await trio.sleep(1)
        
        # Verify file exists on peer2
        assert await peer2.has_file("test_data.txt")
        content = await peer2.read_file("test_data.txt")
        assert content == "Hello, DittoFS!"
```

#### Property-based Test Example

```python
from hypothesis import given, strategies as st

@given(st.binary(min_size=0, max_size=10000))
def test_chunker_roundtrip_property(data):
    """Property: chunking and reassembly should preserve data."""
    chunker = Chunker()
    
    # Chunk the data
    chunks = asyncio.run(chunker.chunk_data(data))
    
    # Reassemble the data
    reconstructed = b"".join(chunk.data for chunk in chunks)
    
    # Property: original data should be preserved
    assert reconstructed == data
```

### Test Fixtures and Utilities

```python
# tests/conftest.py
import pytest
import tempfile
from pathlib import Path

@pytest.fixture
def temp_dir():
    """Provide a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)

@pytest.fixture
async def test_peer():
    """Create a test peer instance."""
    peer = await create_peer(
        peer_id="test-peer",
        storage_dir=temp_dir(),
        port=0  # Use random available port
    )
    yield peer
    await peer.cleanup()
```

## Debugging and Profiling

### Logging Configuration

```python
import logging

# Configure logging for development
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('dittofs.log')
    ]
)

# Use structured logging
logger = logging.getLogger(__name__)
logger.info("Starting sync operation", extra={
    "peer_id": peer_id,
    "file_count": len(files),
    "operation": "sync"
})
```

### Debugging Techniques

#### 1. Interactive Debugging

```python
# Add breakpoint in code
import pdb; pdb.set_trace()

# Or use modern debugger
import ipdb; ipdb.set_trace()

# For async code
import aiomonitor
aiomonitor.start_monitor(loop=asyncio.get_event_loop())
```

#### 2. Async Debugging

```python
# Debug async operations
import trio

async def debug_sync_operation():
    with trio.CancelScope() as cancel_scope:
        cancel_scope.deadline = trio.current_time() + 30  # 30 second timeout
        await sync_operation()
```

#### 3. Network Debugging

```bash
# Monitor network traffic
sudo tcpdump -i lo port 8080

# Use netstat to check connections
netstat -an | grep 8080

# Debug WebRTC connections
export WEBRTC_DEBUG=1
```

### Performance Profiling

#### 1. CPU Profiling

```python
import cProfile
import pstats

# Profile a function
profiler = cProfile.Profile()
profiler.enable()
await your_function()
profiler.disable()

# Analyze results
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(20)
```

#### 2. Memory Profiling

```bash
# Install memory profiler
pip install memory-profiler

# Profile memory usage
python -m memory_profiler your_script.py

# Line-by-line memory profiling
@profile
def your_function():
    # Your code here
    pass
```

#### 3. Async Profiling

```python
import trio

# Profile async operations
async def profile_async_operation():
    start_time = trio.current_time()
    await your_async_operation()
    duration = trio.current_time() - start_time
    print(f"Operation took {duration:.2f} seconds")
```

## Performance Considerations

### Chunking Performance

```python
# Optimize chunk size based on file size and network conditions
def calculate_optimal_chunk_size(file_size: int, network_speed: int) -> int:
    """Calculate optimal chunk size for given conditions."""
    if file_size < 1024 * 1024:  # < 1MB
        return 64 * 1024  # 64KB chunks
    elif file_size < 100 * 1024 * 1024:  # < 100MB
        return 256 * 1024  # 256KB chunks
    else:
        return 1024 * 1024  # 1MB chunks
```

### Memory Management

```python
# Use generators for large data processing
async def process_large_file(file_path: Path):
    """Process large file without loading entirely into memory."""
    async with await trio.open_file(file_path, 'rb') as f:
        while True:
            chunk = await f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield await process_chunk(chunk)
```

### Concurrency Optimization

```python
# Use structured concurrency for parallel operations
async def sync_multiple_files(files: List[Path]):
    """Sync multiple files concurrently."""
    async with trio.open_nursery() as nursery:
        for file_path in files:
            nursery.start_soon(sync_file, file_path)
```

## Security Guidelines

### Input Validation

```python
from pathlib import Path
import re

def validate_file_path(path: str) -> Path:
    """Validate and sanitize file path."""
    # Prevent directory traversal
    if '..' in path or path.startswith('/'):
        raise ValueError("Invalid file path")
    
    # Validate characters
    if not re.match(r'^[a-zA-Z0-9._/-]+$', path):
        raise ValueError("Invalid characters in path")
    
    return Path(path).resolve()
```

### Cryptographic Operations

```python
import secrets
from cryptography.fernet import Fernet

def generate_encryption_key() -> bytes:
    """Generate cryptographically secure encryption key."""
    return Fernet.generate_key()

def secure_random_bytes(length: int) -> bytes:
    """Generate cryptographically secure random bytes."""
    return secrets.token_bytes(length)
```

### Secure Communication

```python
import ssl

def create_secure_context() -> ssl.SSLContext:
    """Create secure SSL context for peer communication."""
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.check_hostname = False  # For development only
    context.verify_mode = ssl.CERT_REQUIRED
    return context
```

## Common Patterns

### Error Handling

```python
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class DittoFSError(Exception):
    """Base exception for DittoFS errors."""
    pass

class SyncError(DittoFSError):
    """Error during synchronization."""
    pass

async def safe_operation() -> Optional[str]:
    """Example of proper error handling."""
    try:
        result = await risky_operation()
        return result
    except SpecificError as e:
        logger.warning(f"Expected error occurred: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise SyncError(f"Operation failed: {e}") from e
```

### Configuration Management

```python
from dataclasses import dataclass
from pathlib import Path
import os

@dataclass
class DittoFSConfig:
    """DittoFS configuration."""
    chunk_size: int = 64 * 1024
    storage_dir: Path = Path.home() / ".dittofs"
    log_level: str = "INFO"
    enable_encryption: bool = True
    
    @classmethod
    def from_env(cls) -> 'DittoFSConfig':
        """Create configuration from environment variables."""
        return cls(
            chunk_size=int(os.getenv('DITTOFS_CHUNK_SIZE', cls.chunk_size)),
            storage_dir=Path(os.getenv('DITTOFS_STORAGE_DIR', cls.storage_dir)),
            log_level=os.getenv('DITTOFS_LOG_LEVEL', cls.log_level),
            enable_encryption=os.getenv('DITTOFS_ENCRYPTION', 'true').lower() == 'true'
        )
```

### Resource Management

```python
import trio
from contextlib import asynccontextmanager

@asynccontextmanager
async def managed_peer_connection(host: str, port: int):
    """Manage peer connection lifecycle."""
    connection = None
    try:
        connection = await establish_connection(host, port)
        yield connection
    except Exception as e:
        logger.error(f"Connection error: {e}")
        raise
    finally:
        if connection:
            await connection.close()
```

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   # Ensure virtual environment is activated
   source .ditto/bin/activate
   
   # Reinstall in development mode
   pip install -e .[dev,all]
   ```

2. **Test Failures**
   ```bash
   # Run tests with verbose output
   pytest -v -s tests/failing_test.py
   
   # Check test dependencies
   pip install -e .[dev]
   ```

3. **Performance Issues**
   ```bash
   # Profile the application
   python -m cProfile -o profile.stats your_script.py
   
   # Analyze with snakeviz
   pip install snakeviz
   snakeviz profile.stats
   ```

### Getting Help

1. Check existing issues and documentation
2. Run diagnostic commands:
   ```bash
   python -c "import dittofs; print(dittofs.__version__)"
   python -m dittofs.cli --version
   ```
3. Create minimal reproduction case
4. Include environment information in bug reports

---

This development guide should help you get started with DittoFS development. For specific questions, please check the issues or start a discussion.
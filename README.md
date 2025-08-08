# DittoFS - Distributed Offline File System

DittoFS is a distributed file system that allows you to share files across devices without requiring a central server. It uses Conflict-free Replicated Data Types (CRDTs) to ensure consistency and supports various transport mechanisms including TCP/mDNS, Bluetooth, and WebRTC.

## Features

- **Offline-first**: Works without internet connectivity
- **Distributed**: No central server required
- **Conflict-free**: Uses CRDTs for automatic conflict resolution
- **Multiple transports**: TCP, Bluetooth, WebRTC support
- **Deduplication**: Automatic chunk-level deduplication
- **Cross-platform**: Works on Linux, macOS, and Windows

## Installation

### Basic Installation

```bash
pip install -e .
```

### With Optional Features

```bash
# With GUI support
pip install -e .[gui]

# With FUSE filesystem support
pip install -e .[fuse]

# With all optional features
pip install -e .[all]
```
### Development Installation

```bash
pip install -e .[dev]
```

## Quick Start

1. **Start the daemon** (in one terminal):
   ```bash
   dittofs daemon
   ```

2. **Add a file** (in another terminal):
   ```bash
   dittofs add /path/to/your/file.txt
   ```

3. **List files**:
   ```bash
   dittofs list
   ```

4. **Sync with peers**:
   ```bash
   dittofs sync
   ```

5. **Check status**:
   ```bash
   dittofs status
   ```

## Usage

### Adding Files
```bash
# Add a single file
dittofs add document.pdf

# Add with verbose output
dittofs -v add large-file.zip
```

### Listing Files
```bash
# List all tracked files
dittofs list

# List with detailed information
dittofs -v list

# Show missing chunks
dittofs list --missing
```

### Retrieving Files
```bash
# Reconstruct a file from chunks
dittofs get --file /path/to/original/file.txt -o restored-file.txt

# Reconstruct from specific hashes
dittofs get --hashes hash1,hash2,hash3 -o output.txt
```

### Synchronization
```bash
# Discover and sync with all peers
dittofs sync

# Sync with longer discovery timeout
dittofs sync --timeout 30.0
```

## Architecture

DittoFS consists of several key components:

### 1. **Chunker**
- Splits files into 64KB chunks
- Uses BLAKE3 for content-addressable hashing
- Provides automatic deduplication

### 2. **CRDT Store**
- Uses Yjs/pycrdt for conflict-free metadata storage
- Tracks file metadata and chunk ownership
- Enables seamless synchronization between peers

### 3. **Transport Manager**
- Supports multiple transport protocols:
  - **TCP + mDNS**: Local network discovery and reliable transfer
  - **Bluetooth**: Short-range device-to-device communication
  - **WebRTC**: NAT traversal for internet-wide connectivity
  - **UDP**: Fast local network broadcasting

### 4. **Sync Manager**
- Coordinates peer discovery and synchronization
- Handles chunk requests and responses
- Manages background sync processes

## Configuration

DittoFS stores its data in `~/.dittofs/`:
- `chunks/`: Content-addressable chunk storage
- `crdt_store.yrs`: CRDT metadata store

## Transport Protocols

### TCP + mDNS (Default)
- **Pros**: Reliable, works on local networks
- **Cons**: Limited to same network segment
- **Requirements**: `zeroconf` package

### Bluetooth
- **Pros**: Works without network infrastructure
- **Cons**: Limited range, slower transfer speeds
- **Requirements**: `bleak` package (or `pybluez` on Linux)

### WebRTC
- **Pros**: NAT traversal, works over internet
- **Cons**: Requires signaling server
- **Requirements**: `aiortc` and `websockets` packages

## Examples

### Basic File Sharing
```bash
# On Device A
dittofs daemon &
dittofs add shared-document.pdf

# On Device B (same network)
dittofs daemon &
dittofs sync
dittofs list  # Should show shared-document.pdf
dittofs get --file shared-document.pdf -o local-copy.pdf
```

### Setting up WebRTC
```bash
# Start signaling server
python -m dittofs.webrtc_transport server

# On devices, configure WebRTC transport
# (This requires code modification currently)
```

## API Reference

### CLI Commands

- `dittofs daemon`: Run the background synchronization daemon
- `dittofs add <file>`: Add a file to the distributed system
- `dittofs list [--missing]`: List tracked files and optionally missing chunks
- `dittofs get --file <path> [-o output]`: Reconstruct a file from chunks
- `dittofs sync [--timeout seconds]`: Force synchronization with peers
- `dittofs status`: Show system status and statistics

### Python API

```python
import asyncio
from dittofs.crdt_store import CRDTStore
from dittofs.transport_manager import TransportManager
from dittofs.sync_manager import SyncManager
from dittofs.chunker import split, join

async def example():
    # Initialize components
    store = CRDTStore()
    transport = TransportManager()
    sync_manager = SyncManager(store, transport)
    
    # Add a file
    file_path = Path("example.txt")
    hashes = split(file_path)
    store.add_file(file_path, hashes)
    
    # Start networking
    await transport.start_all()
    
    # Discover peers and sync
    peers = await transport.discover_all_peers()
    for peer in peers:
        await sync_manager.force_sync_with_peer(peer.peer_id)
    
    await transport.stop_all()

asyncio.run(example())
```

## Troubleshooting

### Common Issues

1. **"pycrdt not found"**
   ```bash
   pip install pycrdt
   ```

2. **"zeroconf not found" (for mDNS discovery)**
   ```bash
   pip install zeroconf
   ```

3. **No peers discovered**
   - Ensure devices are on the same network
   - Check firewall settings
   - Try increasing sync timeout: `dittofs sync --timeout 30`

4. **Missing chunks**
   - Run `dittofs sync` to fetch missing chunks from peers
   - Check `dittofs status` for chunk storage information

### Debug Mode
```bash
# Run with verbose logging
dittofs -v daemon

# Check detailed file status
dittofs -v list

# Debug sync issues
dittofs -v sync
```

## Development

### Running Tests
```bash
# Install development dependencies
pip install -e .[dev]

# Run tests
pytest tests/

# Run async tests
pytest -v tests/test_async.py
```

### Code Style
```bash
# Format code
black src/ tests/

# Check style
flake8 src/ tests/

# Type checking
mypy src/
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run the test suite
6. Submit a pull request

## Security Considerations

- DittoFS currently has minimal authentication/authorization
- Network communications are not encrypted by default
- Consider using VPN or encrypted transports for sensitive data
- File integrity is verified using BLAKE3 checksums

## Performance

### Benchmarks (approximate)
- **Chunking**: ~100 MB/s on modern hardware
- **TCP transfer**: Limited by network bandwidth
- **Bluetooth**: ~1-10 MB/s depending on version
- **Memory usage**: ~10-50 MB per daemon instance

### Optimization Tips
- Use SSD storage for chunk directory
- Ensure good network connectivity between peers
- Consider chunk size tuning for your use case

## Limitations

- No built-in encryption (transport-level encryption recommended)
- Limited conflict resolution (last-writer-wins for file content)
- No access control or permissions model
- Requires manual daemon management (no auto-start)

## Future Roadmap

- [ ] Built-in encryption support
- [ ] Web UI for management
- [ ] Mobile app support
- [ ] Improved conflict resolution
- [ ] Access control and permissions
- [ ] Automatic daemon startup
- [ ] Cloud storage integration
- [ ] Smart sync policies

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Search existing GitHub issues
3. Create a new issue with:
   - DittoFS version
   - Operating system
   - Error messages
   - Steps to reproduce

## Acknowledgments

- [Yjs](https://github.com/yjs/yjs) for CRDT implementation
- [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) for fast hashing
- [zeroconf](https://github.com/python-zeroconf/python-zeroconf) for service discovery
"""

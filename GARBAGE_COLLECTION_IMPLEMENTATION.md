# DittoFS Garbage Collection Implementation

## Overview

This document describes the implementation of the efficient garbage collection system for DittoFS, which provides automatic cleanup of unreferenced chunks with configurable retention policies, safe cleanup procedures, and comprehensive metrics and reporting.

## Architecture

### Core Components

#### 1. ChunkReferenceTracker
- **Purpose**: Tracks chunk references and usage patterns
- **Key Features**:
  - Reference counting system for chunk usage tracking
  - Access time tracking for recently used chunks
  - Orphaned chunk detection
  - Chunks from missing files detection
  - Thread-safe caching with invalidation

#### 2. RetentionPolicy
- **Purpose**: Configurable policies for garbage collection behavior
- **Key Features**:
  - Minimum age requirements before deletion
  - Maximum deletions per run limits
  - Grace periods for recently accessed chunks
  - Emergency mode thresholds for low disk space
  - Selective deletion options (orphaned vs missing files)

#### 3. GarbageCollector
- **Purpose**: Main garbage collection engine
- **Key Features**:
  - Async garbage collection with safety checks
  - Automatic scheduling with configurable intervals
  - Storage monitoring and emergency mode detection
  - Comprehensive statistics collection and persistence
  - Safe cleanup procedures with verification

#### 4. GCStats
- **Purpose**: Statistics tracking and reporting
- **Key Features**:
  - Detailed metrics for each GC run
  - Historical data persistence
  - JSON serialization for storage
  - Performance and effectiveness tracking

## Implementation Details

### Reference Counting System

The reference counting system tracks which files reference each chunk:

```python
# Chunk ownership tracking in CRDT store
chunk_owners = {
    'chunk_hash_1': ['/path/to/file1.txt', '/path/to/file2.txt'],
    'chunk_hash_2': ['/path/to/file1.txt'],
    'chunk_hash_3': ['/path/to/file3.txt']
}
```

Key features:
- **Automatic Updates**: References updated when files are added/removed
- **Deduplication Aware**: Handles shared chunks across multiple files
- **Thread-Safe**: Uses locks for concurrent access
- **Cache Invalidation**: Efficient cache management for performance

### Safe Cleanup Procedures

The garbage collector implements multiple safety checks before deleting chunks:

1. **Reference Verification**: Double-check that chunks are not referenced
2. **Age Requirements**: Ensure chunks meet minimum age requirements
3. **Access Time Checks**: Respect grace periods for recently accessed chunks
4. **File Existence**: Verify referencing files still exist
5. **Emergency Mode**: More aggressive cleanup when disk space is critically low

### Configurable Retention Policies

```python
policy = RetentionPolicy(
    min_age_seconds=3600,           # 1 hour minimum age
    max_deletions_per_run=1000,     # Limit deletions per run
    delete_orphaned=True,           # Clean up orphaned chunks
    delete_from_missing_files=True, # Clean up chunks from deleted files
    access_grace_period=86400,      # 24 hour grace period
    min_free_space_bytes=1GB,       # Regular GC threshold
    emergency_mode_threshold=100MB   # Emergency GC threshold
)
```

### Automatic Scheduling

The garbage collector includes a built-in scheduler:

- **Configurable Intervals**: Default 1 hour, customizable
- **Condition-Based Triggering**: Runs based on disk space thresholds
- **Background Operation**: Non-blocking async execution
- **Graceful Shutdown**: Clean stop with pending operation completion

### Integration Points

#### CLI Integration
New `gc` command with subcommands:
- `dittofs gc run` - Manual garbage collection
- `dittofs gc status` - Show GC status and recommendations
- `dittofs gc stats` - Display historical statistics
- `dittofs gc schedule --start` - Start automatic scheduling

#### Chunker Integration
- **Access Time Tracking**: Updates chunk access times during retrieval
- **Transparent Operation**: No impact on existing chunk operations
- **Error Resilience**: GC failures don't affect chunk retrieval

#### CRDT Store Integration
- **File Removal**: Clean chunk ownership when files are removed
- **Missing File Cleanup**: Automatic cleanup of metadata for missing files
- **Reference Maintenance**: Consistent chunk ownership tracking

## Performance Characteristics

### Memory Usage
- **Efficient Caching**: Reference cache with invalidation
- **Bounded Memory**: Configurable limits on operations
- **Streaming Processing**: Large file sets processed in batches

### Disk I/O
- **Batch Operations**: Efficient bulk chunk processing
- **Minimal Overhead**: Only scans when necessary
- **Background Processing**: Non-blocking operation

### Network Impact
- **No Network Operations**: Purely local garbage collection
- **Peer-Aware**: Considers peer availability for safety

## Safety Guarantees

### Data Integrity
- **Multiple Verification Steps**: Extensive safety checks before deletion
- **Atomic Operations**: Safe chunk deletion procedures
- **Error Recovery**: Graceful handling of failures
- **Audit Trail**: Comprehensive logging of all operations

### Concurrent Access
- **Thread-Safe Operations**: Safe concurrent access to shared data
- **Lock Management**: Efficient locking with minimal contention
- **Race Condition Prevention**: Careful ordering of operations

### Emergency Scenarios
- **Disk Space Monitoring**: Automatic detection of low space conditions
- **Emergency Mode**: More aggressive cleanup when critically low on space
- **Graceful Degradation**: Continues operation even with partial failures

## Metrics and Reporting

### Real-Time Metrics
- Chunks scanned, deleted, and bytes freed
- Orphaned and referenced chunk counts
- Error counts and types
- Execution duration and performance

### Historical Data
- Persistent statistics storage
- Trend analysis capabilities
- Performance regression detection
- Effectiveness measurement

### Reporting Features
- JSON export for external analysis
- CLI-based status and statistics display
- Configurable retention of historical data
- Integration-ready metrics format

## Configuration Options

### Policy Configuration
```python
# Conservative policy for production
conservative = RetentionPolicy(
    min_age_seconds=86400,      # 24 hours
    max_deletions_per_run=100,  # Limited deletions
    access_grace_period=604800  # 7 days grace period
)

# Aggressive policy for development
aggressive = RetentionPolicy(
    min_age_seconds=3600,       # 1 hour
    max_deletions_per_run=10000, # High deletion limit
    access_grace_period=86400   # 1 day grace period
)
```

### Scheduling Configuration
- **Interval Control**: Customizable scheduling intervals
- **Condition-Based**: Trigger based on disk space or time
- **Manual Override**: Force execution regardless of conditions

## Testing Strategy

### Unit Tests
- **Component Isolation**: Individual component testing
- **Mock Integration**: Comprehensive mocking of dependencies
- **Edge Case Coverage**: Boundary condition testing
- **Error Scenario Testing**: Failure mode validation

### Integration Tests
- **Real Component Integration**: Testing with actual CRDT store
- **File System Integration**: Real chunk directory operations
- **Concurrent Access Testing**: Multi-threaded safety validation

### Performance Tests
- **Large Dataset Testing**: Thousands of chunks and files
- **Memory Usage Validation**: Bounded memory consumption
- **Execution Time Measurement**: Performance regression detection

## Usage Examples

### Basic Usage
```python
from dittofs.garbage_collector import GarbageCollector, RetentionPolicy
from dittofs.crdt_store import CRDTStore

# Create garbage collector
store = CRDTStore()
policy = RetentionPolicy(min_age_seconds=3600)
gc = GarbageCollector(store, policy)

# Run garbage collection
stats = await gc.collect_garbage()
print(f"Deleted {stats.chunks_deleted} chunks, freed {stats.bytes_freed} bytes")
```

### Scheduled Garbage Collection
```python
# Start automatic scheduling
await gc.start_scheduler(interval_seconds=3600)  # Every hour

# Stop scheduling
await gc.stop_scheduler()
```

### CLI Usage
```bash
# Manual garbage collection
dittofs gc run

# Check status
dittofs gc status

# View statistics
dittofs gc stats --verbose

# Start scheduler
dittofs gc schedule --start --interval 1800  # Every 30 minutes
```

## Requirements Satisfaction

This implementation satisfies the following requirements from the specification:

- **Requirement 8.1**: Efficient batch operations and avoiding overwhelming the file system
  - Implements configurable batch sizes and limits
  - Uses background processing to avoid system impact
  - Provides efficient chunk scanning and processing

- **Requirement 8.4**: Selective synchronization and cloud offloading when storage is nearly full
  - Monitors disk space and triggers cleanup automatically
  - Provides emergency mode for critically low space situations
  - Enables selective cleanup policies based on storage conditions

## Future Enhancements

### Planned Improvements
1. **Distributed Garbage Collection**: Coordinate GC across peers
2. **Predictive Cleanup**: ML-based prediction of chunk usage patterns
3. **Compression Integration**: Automatic compression of old chunks
4. **Cloud Storage Integration**: Offload old chunks to cloud storage

### Performance Optimizations
1. **Parallel Processing**: Multi-threaded chunk processing
2. **Incremental Scanning**: Only scan changed areas
3. **Smart Scheduling**: Adaptive scheduling based on usage patterns
4. **Memory Optimization**: Further reduce memory footprint

## Conclusion

The DittoFS garbage collection system provides a robust, efficient, and safe solution for managing chunk storage. With comprehensive safety checks, configurable policies, automatic scheduling, and detailed metrics, it ensures optimal storage utilization while maintaining data integrity and system performance.

The implementation is production-ready and provides the foundation for advanced storage management features in DittoFS.
import blake3
import logging
from typing import List, Optional, Dict, Set, Tuple
import pathlib
import mimetypes
import struct

from .encryption import (
    ChunkEncryption, KeyManager, EncryptedChunk,
    create_encryption_system, load_encryption_system
)

# Chunking configuration
DEFAULT_MIN_CHUNK_SIZE = 8 * 1024    # 8KB minimum
DEFAULT_AVG_CHUNK_SIZE = 64 * 1024   # 64KB average (same as old fixed size)
DEFAULT_MAX_CHUNK_SIZE = 1024 * 1024 # 1MB maximum

# Rolling hash parameters
ROLLING_HASH_WINDOW = 64
ROLLING_HASH_MODULUS = 0x8000  # 32768, creates ~64KB average chunks

CHUNK_DIR = pathlib.Path.home() / ".dittofs" / "chunks"

# Global encryption system - will be initialized when needed
_encryption_system: Optional[Tuple[KeyManager, ChunkEncryption]] = None
_encryption_enabled = False

# File type specific chunking parameters
FILE_TYPE_CONFIGS = {
    'text': {
        'min_size': 4 * 1024,    # 4KB for text files
        'avg_size': 32 * 1024,   # 32KB average for better line-based dedup
        'max_size': 256 * 1024,  # 256KB max
        'modulus': 0x4000,       # Smaller modulus for more boundaries
    },
    'binary': {
        'min_size': 16 * 1024,   # 16KB for binary files
        'avg_size': 128 * 1024,  # 128KB average for better performance
        'max_size': 2048 * 1024, # 2MB max
        'modulus': 0x10000,      # Larger modulus for fewer boundaries
    },
    'image': {
        'min_size': 32 * 1024,   # 32KB for images
        'avg_size': 256 * 1024,  # 256KB average
        'max_size': 4096 * 1024, # 4MB max
        'modulus': 0x20000,      # Even larger modulus
    },
    'default': {
        'min_size': DEFAULT_MIN_CHUNK_SIZE,
        'avg_size': DEFAULT_AVG_CHUNK_SIZE,
        'max_size': DEFAULT_MAX_CHUNK_SIZE,
        'modulus': ROLLING_HASH_MODULUS,
    }
}

def ensure_chunk_dir():
    """Ensure chunk directory exists"""
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)


def enable_encryption(password: str) -> bool:
    """Enable encryption for chunk storage."""
    global _encryption_system, _encryption_enabled
    
    try:
        # Try to load existing encryption system first
        encryption_system = load_encryption_system(password)
        
        if encryption_system is None:
            # Create new encryption system
            encryption_system = create_encryption_system(password)
            logging.info("Created new encryption system")
        else:
            logging.info("Loaded existing encryption system")
        
        _encryption_system = encryption_system
        _encryption_enabled = True
        return True
        
    except Exception as e:
        logging.error(f"Failed to enable encryption: {e}")
        return False


def disable_encryption():
    """Disable encryption for chunk storage."""
    global _encryption_system, _encryption_enabled
    
    if _encryption_system:
        _encryption_system[0].clear_key_cache()
    
    _encryption_system = None
    _encryption_enabled = False
    logging.info("Encryption disabled")


def is_encryption_enabled() -> bool:
    """Check if encryption is currently enabled."""
    return _encryption_enabled and _encryption_system is not None


def get_encryption_system() -> Optional[Tuple[KeyManager, ChunkEncryption]]:
    """Get the current encryption system."""
    return _encryption_system if _encryption_enabled else None


class RollingHash:
    """Rabin-Karp rolling hash implementation for content-defined chunking"""
    
    def __init__(self, window_size: int = ROLLING_HASH_WINDOW):
        self.window_size = window_size
        self.base = 257  # Prime base for polynomial rolling hash
        self.mod = 2**32 - 1  # Large prime modulus
        self.base_power = pow(self.base, window_size - 1, self.mod)
        self.hash_value = 0
        self.window = bytearray(window_size)
        self.window_pos = 0
        self.window_full = False
        self.position = 0  # Track absolute position for better uniform data handling
    
    def update(self, byte_val: int) -> int:
        """Update rolling hash with new byte and return current hash"""
        if self.window_full:
            # Remove the oldest byte's contribution
            old_byte = self.window[self.window_pos]
            self.hash_value = (self.hash_value - (old_byte * self.base_power) % self.mod) % self.mod
        
        # Add new byte with position-dependent mixing for uniform data
        mixed_byte = byte_val ^ (self.position & 0xFF)  # XOR with position for better distribution
        self.hash_value = (self.hash_value * self.base + mixed_byte) % self.mod
        self.window[self.window_pos] = byte_val
        
        self.window_pos = (self.window_pos + 1) % self.window_size
        if self.window_pos == 0:
            self.window_full = True
        
        self.position += 1
        return self.hash_value


def get_file_type_category(file_path: pathlib.Path) -> str:
    """Determine file type category for chunking optimization"""
    mime_type, _ = mimetypes.guess_type(str(file_path))
    
    if not mime_type:
        return 'default'
    
    if mime_type.startswith('text/'):
        return 'text'
    elif mime_type.startswith('image/'):
        return 'image'
    elif mime_type in ['application/pdf', 'application/msword', 
                       'application/vnd.openxmlformats-officedocument']:
        return 'text'  # Document files benefit from text-like chunking
    else:
        return 'binary'


def find_chunk_boundaries(data: bytes, config: Dict) -> List[int]:
    """Find chunk boundaries using content-defined chunking with rolling hash"""
    if len(data) <= config['min_size']:
        return [len(data)]
    
    boundaries = []
    rolling_hash = RollingHash()
    modulus = config['modulus']
    min_size = config['min_size']
    max_size = config['max_size']
    
    current_chunk_start = 0
    
    for i, byte_val in enumerate(data):
        hash_val = rolling_hash.update(byte_val)
        chunk_size = i - current_chunk_start + 1
        
        # Check for boundary conditions
        is_boundary = False
        
        if chunk_size >= max_size:
            # Force boundary at max size
            is_boundary = True
        elif chunk_size >= min_size:
            # Content-defined boundary - use multiple conditions for better distribution
            # Primary condition: hash modulus
            if (hash_val % modulus) == 0:
                is_boundary = True
            # Secondary condition: for uniform data, use position-based boundaries
            elif chunk_size >= config['avg_size'] and (hash_val & 0xFF) < 4:  # ~1.5% chance
                is_boundary = True
            # Tertiary condition: ensure we don't create overly large chunks
            elif chunk_size >= config['avg_size'] * 2:
                is_boundary = True
        
        if is_boundary:
            boundaries.append(i + 1)
            current_chunk_start = i + 1
            rolling_hash = RollingHash()  # Reset for next chunk
    
    # Add final boundary if needed
    if not boundaries or boundaries[-1] < len(data):
        boundaries.append(len(data))
    
    return boundaries


def optimize_chunk_boundaries(data: bytes, boundaries: List[int], file_type: str) -> List[int]:
    """Optimize chunk boundaries based on content patterns"""
    if file_type != 'text' or len(boundaries) <= 1:
        return boundaries
    
    optimized = []
    data_view = memoryview(data)
    
    for i, boundary in enumerate(boundaries):
        start = optimized[-1] if optimized else 0
        end = boundary
        
        # For text files, try to align boundaries with line breaks
        if file_type == 'text' and end - start > DEFAULT_MIN_CHUNK_SIZE:
            # Look for line breaks near the boundary (within 1KB)
            search_start = max(start, end - 1024)
            search_end = min(len(data), end + 1024)
            
            best_boundary = end
            best_score = 0
            
            for pos in range(search_start, search_end):
                if pos < len(data) and data[pos] in (ord('\n'), ord('\r')):
                    # Score based on distance from original boundary
                    distance = abs(pos - end)
                    score = 1000 - distance  # Closer is better
                    
                    if score > best_score:
                        best_score = score
                        best_boundary = pos + 1
            
            optimized.append(best_boundary)
        else:
            optimized.append(boundary)
    
    return optimized

def split(file_path: pathlib.Path) -> List[str]:
    """Split file into variable-size chunks using content-defined chunking"""
    ensure_chunk_dir()
    
    hashes = []
    try:
        # Determine file type for optimization
        file_type = get_file_type_category(file_path)
        config = FILE_TYPE_CONFIGS.get(file_type, FILE_TYPE_CONFIGS['default'])
        
        # Read entire file for content-defined chunking
        # For very large files, we might want to process in segments
        file_size = file_path.stat().st_size
        
        if file_size > 100 * 1024 * 1024:  # 100MB threshold
            # Process large files in segments to manage memory
            hashes = _split_large_file(file_path, config, file_type)
        else:
            # Process smaller files entirely in memory
            data = file_path.read_bytes()
            hashes = _split_data(data, config, file_type)
        
        logging.info(f"Split {file_path} ({file_type}) into {len(hashes)} variable-size chunks")
        return hashes
        
    except Exception as e:
        logging.error(f"Failed to split file {file_path}: {e}")
        return []


def _split_data(data: bytes, config: Dict, file_type: str) -> List[str]:
    """Split data into chunks using content-defined boundaries"""
    if not data:
        return []
    
    # Find chunk boundaries
    boundaries = find_chunk_boundaries(data, config)
    
    # Optimize boundaries based on content type
    boundaries = optimize_chunk_boundaries(data, boundaries, file_type)
    
    hashes = []
    start = 0
    
    for boundary in boundaries:
        chunk_data = data[start:boundary]
        if chunk_data:  # Skip empty chunks
            # Use blake3 for hashing
            h = blake3.blake3(chunk_data).hexdigest()
            chunk_path = CHUNK_DIR / h
            
            # Only write if chunk doesn't exist (deduplication)
            if not chunk_path.exists():
                _store_chunk(chunk_data, h, chunk_path)
            
            hashes.append(h)
        
        start = boundary
    
    return hashes


def _store_chunk(chunk_data: bytes, chunk_hash: str, chunk_path: pathlib.Path) -> None:
    """Store a chunk, with optional encryption."""
    if is_encryption_enabled():
        encryption_system = get_encryption_system()
        if encryption_system:
            _, chunk_encryption = encryption_system
            try:
                encrypted_chunk = chunk_encryption.encrypt_chunk(chunk_data, chunk_hash)
                chunk_path.write_bytes(encrypted_chunk.to_bytes())
                logging.debug(f"Stored encrypted chunk {chunk_hash}")
            except Exception as e:
                logging.error(f"Failed to encrypt chunk {chunk_hash}: {e}")
                # Fall back to unencrypted storage
                chunk_path.write_bytes(chunk_data)
        else:
            chunk_path.write_bytes(chunk_data)
    else:
        chunk_path.write_bytes(chunk_data)


def _split_large_file(file_path: pathlib.Path, config: Dict, file_type: str) -> List[str]:
    """Split large files by processing in segments to manage memory usage"""
    hashes = []
    segment_size = 50 * 1024 * 1024  # 50MB segments
    
    with open(file_path, "rb") as f:
        rolling_hash = RollingHash()
        buffer = bytearray()
        current_chunk_start = 0
        file_position = 0
        
        while True:
            segment = f.read(segment_size)
            if not segment:
                break
            
            buffer.extend(segment)
            
            # Process the buffer to find chunk boundaries
            boundaries = find_chunk_boundaries(bytes(buffer), config)
            
            # Process complete chunks (keep incomplete chunk in buffer)
            processed_up_to = 0
            for boundary in boundaries[:-1]:  # Skip last boundary as it might be incomplete
                chunk_data = buffer[processed_up_to:boundary]
                if chunk_data:
                    h = blake3.blake3(chunk_data).hexdigest()
                    chunk_path = CHUNK_DIR / h
                    
                    if not chunk_path.exists():
                        _store_chunk(bytes(chunk_data), h, chunk_path)
                    
                    hashes.append(h)
                
                processed_up_to = boundary
            
            # Keep remaining data in buffer for next iteration
            buffer = buffer[processed_up_to:]
            file_position += len(segment)
        
        # Process final chunk if any data remains
        if buffer:
            h = blake3.blake3(bytes(buffer)).hexdigest()
            chunk_path = CHUNK_DIR / h
            
            if not chunk_path.exists():
                _store_chunk(bytes(buffer), h, chunk_path)
            
            hashes.append(h)
    
    return hashes

def join(hashes: List[str], out_path: pathlib.Path) -> bool:
    """Join chunks into output file"""
    try:
        with open(out_path, "wb") as out:
            for h in hashes:
                chunk_path = CHUNK_DIR / h
                if not chunk_path.exists():
                    raise FileNotFoundError(f"Missing chunk: {h}")
                
                chunk_data = _retrieve_chunk(h, chunk_path)
                out.write(chunk_data)
        
        logging.info(f"Joined {len(hashes)} chunks into {out_path}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to join chunks: {e}")
        return False


def _retrieve_chunk(chunk_hash: str, chunk_path: pathlib.Path) -> bytes:
    """Retrieve a chunk, with optional decryption."""
    chunk_bytes = chunk_path.read_bytes()
    
    # Update chunk access time for garbage collection
    try:
        from .garbage_collector import get_garbage_collector
        gc = get_garbage_collector()
        gc.tracker.update_chunk_access_time(chunk_hash)
    except Exception:
        # Ignore GC errors to avoid breaking chunk retrieval
        pass
    
    if is_encryption_enabled():
        encryption_system = get_encryption_system()
        if encryption_system:
            _, chunk_encryption = encryption_system
            try:
                # Try to parse as encrypted chunk
                encrypted_chunk = EncryptedChunk.from_bytes(chunk_bytes)
                decrypted_data = chunk_encryption.decrypt_chunk(encrypted_chunk, chunk_hash)
                logging.debug(f"Retrieved and decrypted chunk {chunk_hash}")
                return decrypted_data
            except Exception as e:
                logging.warning(f"Failed to decrypt chunk {chunk_hash}, trying as unencrypted: {e}")
                # Fall back to treating as unencrypted
                return chunk_bytes
        else:
            return chunk_bytes
    else:
        return chunk_bytes


def get_chunk_stats(hashes: List[str]) -> Dict:
    """Get statistics about chunk sizes and distribution"""
    if not hashes:
        return {
            'count': 0,
            'total_size': 0,
            'avg_size': 0,
            'min_size': 0,
            'max_size': 0,
            'size_distribution': {},
            'encrypted_count': 0
        }
    
    sizes = []
    total_size = 0
    encrypted_count = 0
    
    for h in hashes:
        chunk_path = CHUNK_DIR / h
        if chunk_path.exists():
            # Get the actual chunk data size (decrypted if encrypted)
            try:
                chunk_data = _retrieve_chunk(h, chunk_path)
                size = len(chunk_data)
                sizes.append(size)
                total_size += size
                
                # Check if chunk was encrypted
                if is_encryption_enabled():
                    try:
                        chunk_bytes = chunk_path.read_bytes()
                        EncryptedChunk.from_bytes(chunk_bytes)
                        encrypted_count += 1
                    except:
                        pass  # Not encrypted
                        
            except Exception as e:
                logging.warning(f"Failed to retrieve chunk {h} for stats: {e}")
                # Fall back to file size
                size = chunk_path.stat().st_size
                sizes.append(size)
                total_size += size
    
    if not sizes:
        return get_chunk_stats([])  # Return empty stats
    
    # Calculate size distribution buckets
    size_buckets = {
        '< 8KB': 0,
        '8KB - 32KB': 0,
        '32KB - 128KB': 0,
        '128KB - 512KB': 0,
        '> 512KB': 0
    }
    
    for size in sizes:
        if size < 8 * 1024:
            size_buckets['< 8KB'] += 1
        elif size < 32 * 1024:
            size_buckets['8KB - 32KB'] += 1
        elif size < 128 * 1024:
            size_buckets['32KB - 128KB'] += 1
        elif size < 512 * 1024:
            size_buckets['128KB - 512KB'] += 1
        else:
            size_buckets['> 512KB'] += 1
    
    return {
        'count': len(sizes),
        'total_size': total_size,
        'avg_size': total_size // len(sizes),
        'min_size': min(sizes),
        'max_size': max(sizes),
        'size_distribution': size_buckets,
        'encrypted_count': encrypted_count
    }


def verify_chunk_integrity(chunk_hash: str) -> bool:
    """Verify the integrity of a stored chunk."""
    chunk_path = CHUNK_DIR / chunk_hash
    if not chunk_path.exists():
        return False
    
    try:
        if is_encryption_enabled():
            encryption_system = get_encryption_system()
            if encryption_system:
                _, chunk_encryption = encryption_system
                try:
                    chunk_bytes = chunk_path.read_bytes()
                    encrypted_chunk = EncryptedChunk.from_bytes(chunk_bytes)
                    return chunk_encryption.verify_chunk_integrity(encrypted_chunk, chunk_hash)
                except Exception:
                    # Fall back to basic verification for unencrypted chunks
                    pass
        
        # For unencrypted chunks, verify by attempting to read
        chunk_data = _retrieve_chunk(chunk_hash, chunk_path)
        
        # Verify the hash matches the content
        computed_hash = blake3.blake3(chunk_data).hexdigest()
        return computed_hash == chunk_hash
        
    except Exception as e:
        logging.error(f"Failed to verify chunk {chunk_hash}: {e}")
        return False


def repair_chunk_from_peers(chunk_hash: str, peer_chunks: Dict[str, bytes]) -> bool:
    """Repair a corrupted chunk using data from peers."""
    if chunk_hash not in peer_chunks:
        logging.error(f"No peer data available for chunk {chunk_hash}")
        return False
    
    try:
        chunk_data = peer_chunks[chunk_hash]
        
        # Verify the peer data is correct
        computed_hash = blake3.blake3(chunk_data).hexdigest()
        if computed_hash != chunk_hash:
            logging.error(f"Peer data for chunk {chunk_hash} has incorrect hash")
            return False
        
        # Store the repaired chunk
        chunk_path = CHUNK_DIR / chunk_hash
        _store_chunk(chunk_data, chunk_hash, chunk_path)
        
        logging.info(f"Successfully repaired chunk {chunk_hash}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to repair chunk {chunk_hash}: {e}")
        return False


def analyze_deduplication_ratio(file_paths: List[pathlib.Path]) -> Dict:
    """Analyze deduplication effectiveness across multiple files"""
    all_hashes = []
    total_original_size = 0
    
    for file_path in file_paths:
        if file_path.exists():
            hashes = split(file_path)
            all_hashes.extend(hashes)
            total_original_size += file_path.stat().st_size
    
    unique_hashes = set(all_hashes)
    unique_chunks_size = 0
    
    for h in unique_hashes:
        chunk_path = CHUNK_DIR / h
        if chunk_path.exists():
            unique_chunks_size += chunk_path.stat().st_size
    
    dedup_ratio = 1.0 - (unique_chunks_size / total_original_size) if total_original_size > 0 else 0.0
    
    return {
        'total_chunks': len(all_hashes),
        'unique_chunks': len(unique_hashes),
        'duplicate_chunks': len(all_hashes) - len(unique_hashes),
        'original_size': total_original_size,
        'deduplicated_size': unique_chunks_size,
        'deduplication_ratio': dedup_ratio,
        'space_saved': total_original_size - unique_chunks_size
    }
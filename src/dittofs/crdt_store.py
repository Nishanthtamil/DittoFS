import pathlib
import hashlib
import os
import logging

try:
    import pycrdt as Y
    PYCRDT_AVAILABLE = True
except ImportError:
    PYCRDT_AVAILABLE = False
    logging.warning("pycrdt not available. Install with: pip install pycrdt")

from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set

@dataclass
class FileRecord:
    path: str
    hashes: List[str]
    mtime: float
    size: int
    permissions: int = 0o644
    checksum: str = ""
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FileRecord':
        return cls(**data)

class CRDTStore:
    """Fixed CRDT-based file metadata store"""
    
    def __init__(self, store_path: pathlib.Path = None):
        if not PYCRDT_AVAILABLE:
            raise ImportError("pycrdt is required. Install with: pip install pycrdt")
            
        self.store_path = store_path or (pathlib.Path.home() / ".dittofs" / "crdt_store.yrs")
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize CRDT document
        self.doc = Y.Doc()
        
        # Load existing data first
        self._load()
        
        # Initialize maps - create them if they don't exist
        self._ensure_maps_exist()
    
    def _ensure_maps_exist(self):
        """Ensure root, files, and chunks maps exist"""
        try:
            # Get or create root map using the correct pycrdt API
            self.root = self.doc.get("root", type=Y.Map)
            
            # Get or create files map - Map.get() works like dict.get()
            self.files_map = self.root.get("files")
            if self.files_map is None:
                self.files_map = Y.Map()
                self.root["files"] = self.files_map
            
            # Get or create chunks map
            self.chunks_map = self.root.get("chunks")
            if self.chunks_map is None:
                self.chunks_map = Y.Map()
                self.root["chunks"] = self.chunks_map
            
        except Exception as e:
            logging.error(f"Error ensuring maps exist: {e}")
            # Fallback: create new maps
            self.root = Y.Map()
            self.files_map = Y.Map()
            self.chunks_map = Y.Map()
            self.doc["root"] = self.root
            self.root["files"] = self.files_map
            self.root["chunks"] = self.chunks_map
    
    def add_file(self, file_path: pathlib.Path, hashes: List[str]) -> bool:
        """Add or update a file record"""
        try:
            if not file_path.exists():
                logging.error(f"File does not exist: {file_path}")
                return False
            
            stat = file_path.stat()
            
            # Calculate file checksum for integrity
            file_checksum = hashlib.sha256()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    file_checksum.update(chunk)
            
            record = FileRecord(
                path=str(file_path.resolve()),
                hashes=hashes,
                mtime=stat.st_mtime,
                size=stat.st_size,
                permissions=stat.st_mode & 0o777,
                checksum=file_checksum.hexdigest()
            )
            
            # Store in CRDT map (convert to dict for JSON serialization)
            self.files_map[record.path] = record.to_dict()
            
            # Track chunk ownership
            for chunk_hash in hashes:
                try:
                    chunk_owners = self.chunks_map.get(chunk_hash, [])
                    if isinstance(chunk_owners, str):
                        # Handle legacy single owner format
                        chunk_owners = [chunk_owners]
                    elif not isinstance(chunk_owners, list):
                        chunk_owners = []
                    
                    if record.path not in chunk_owners:
                        chunk_owners.append(record.path)
                        self.chunks_map[chunk_hash] = chunk_owners
                except Exception as e:
                    logging.warning(f"Failed to update chunk ownership for {chunk_hash}: {e}")
            
            logging.info(f"Added file: {record.path} with {len(hashes)} chunks")
            return True
            
        except Exception as e:
            logging.error(f"Failed to add file {file_path}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_file(self, file_path: pathlib.Path) -> Optional[FileRecord]:
        """Get file record by path"""
        try:
            path_str = str(file_path.resolve())
            data = self.files_map.get(path_str)
            if data and isinstance(data, dict):
                return FileRecord.from_dict(data)
            return None
        except Exception as e:
            logging.error(f"Failed to get file {file_path}: {e}")
            return None
    
    def list_files(self) -> List[FileRecord]:
        """List all files in the store"""
        files = []
        try:
            for path_str, data in self.files_map.items():
                try:
                    if isinstance(data, dict):
                        files.append(FileRecord.from_dict(data))
                except Exception as e:
                    logging.warning(f"Skipping corrupted file record: {path_str}, {e}")
            return files
        except Exception as e:
            logging.error(f"Failed to list files: {e}")
            return []
    
    def get_missing_chunks(self) -> Set[str]:
        """Find chunks that we have metadata for but missing files"""
        from .chunker import CHUNK_DIR
        missing = set()
        
        try:
            for chunk_hash in self.chunks_map.keys():
                chunk_path = CHUNK_DIR / chunk_hash
                if not chunk_path.exists():
                    missing.add(chunk_hash)
        except Exception as e:
            logging.error(f"Error finding missing chunks: {e}")
        
        return missing
    
    def find_chunk_owners(self, chunk_hash: str) -> List[str]:
        """Find which files contain this chunk"""
        try:
            owners = self.chunks_map.get(chunk_hash, [])
            if isinstance(owners, str):
                return [owners]
            elif isinstance(owners, list):
                return owners
            else:
                return []
        except Exception as e:
            logging.error(f"Error finding chunk owners for {chunk_hash}: {e}")
            return []
    
    def merge_updates(self, update_data: bytes) -> bool:
        """Merge updates from remote peer"""
        try:
            # Use the correct pycrdt API - apply_update on the document
            self.doc.apply_update(update_data)
            logging.info("Successfully merged CRDT update")
            return True
            
        except Exception as e:
            logging.error(f"Failed to merge CRDT update: {e}")
            return False
    
    def get_state_vector(self) -> bytes:
        """Get state vector for efficient sync"""
        try:
            return self.doc.get_state()
        except Exception as e:
            logging.error(f"Failed to get state vector: {e}")
            return b""
    
    def get_update_since(self, state_vector: bytes) -> bytes:
        """Get update since given state vector"""
        try:
            return self.doc.get_update(state_vector)
        except Exception as e:
            logging.error(f"Failed to get update: {e}")
            return b""
    
    def save(self) -> bool:
        """Save the CRDT document to disk"""
        try:
            # Get the current document state as bytes (full update from creation)
            update_data = self.doc.get_update()
            with open(self.store_path, 'wb') as f:
                f.write(update_data)
            logging.debug(f"Saved CRDT store to {self.store_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to save CRDT store: {e}")
            return False
    
    def _load(self) -> bool:
        """Load the CRDT document from disk"""
        try:
            if self.store_path.exists():
                with open(self.store_path, 'rb') as f:
                    update_data = f.read()
                    if update_data:
                        # Use the doc.apply_update() method instead of Y.merge_updates
                        self.doc.apply_update(update_data)
                        logging.debug(f"Loaded CRDT store from {self.store_path}")
                        # Re-ensure maps exist after loading
                        self._ensure_maps_exist()
            return True
        except Exception as e:
            logging.error(f"Failed to load CRDT store: {e}")
            # If loading fails, continue with empty document
            logging.info("Starting with empty CRDT document")
            return False
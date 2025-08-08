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
        self.root = Y.Map()
        self.doc["root"] = self.root
        
        # Initialize files and chunks maps if they don't exist
        if "files" not in self.root:
            self.root["files"] = Y.Map()
        if "chunks" not in self.root:
            self.root["chunks"] = Y.Map()
        
        self.files_map = self.root["files"]
        self.chunks_map = self.root["chunks"]
        
        # Load existing data
        self._load()
    
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
                chunk_owners = self.chunks_map.get(chunk_hash, [])
                if isinstance(chunk_owners, str):
                    # Handle legacy single owner format
                    chunk_owners = [chunk_owners]
                elif not isinstance(chunk_owners, list):
                    chunk_owners = []
                
                if record.path not in chunk_owners:
                    chunk_owners.append(record.path)
                    self.chunks_map[chunk_hash] = chunk_owners
            
            logging.info(f"Added file: {record.path} with {len(hashes)} chunks")
            return True
            
        except Exception as e:
            logging.error(f"Failed to add file {file_path}: {e}")
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
        owners = self.chunks_map.get(chunk_hash, [])
        if isinstance(owners, str):
            return [owners]
        elif isinstance(owners, list):
            return owners
        else:
            return []
    
    def merge_updates(self, update_data: bytes) -> bool:
        """Merge updates from remote peer"""
        try:
            # Apply the update to our document
            Y.apply_update(self.doc, update_data)
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
            update_data = self.doc.get_state()
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
                        Y.merge_updates([update_data], self.doc)
                        # Re-get the maps after loading
                        self.root = self.doc.get("root", Y.Map())
                        self.files_map = self.root.get("files", Y.Map())
                        self.chunks_map = self.root.get("chunks", Y.Map())
                        logging.debug(f"Loaded CRDT store from {self.store_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to load CRDT store: {e}")
            return False
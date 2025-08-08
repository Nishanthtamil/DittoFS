import blake3
import logging
from typing import List, Optional, Dict, Set
import pathlib
CHUNK_SIZE = 64 * 1024
CHUNK_DIR = pathlib.Path.home() / ".dittofs" / "chunks"

def ensure_chunk_dir():
    """Ensure chunk directory exists"""
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)

def split(file_path: pathlib.Path) -> List[str]:
    """Split file into chunks and return list of hashes"""
    ensure_chunk_dir()
    
    hashes = []
    try:
        with open(file_path, "rb") as f:
            chunk_index = 0
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                
                # Use blake3 for hashing
                h = blake3.blake3(data).hexdigest()
                chunk_path = CHUNK_DIR / h
                
                # Only write if chunk doesn't exist (deduplication)
                if not chunk_path.exists():
                    chunk_path.write_bytes(data)
                
                hashes.append(h)
                chunk_index += 1
                
        logging.info(f"Split {file_path} into {len(hashes)} chunks")
        return hashes
        
    except Exception as e:
        logging.error(f"Failed to split file {file_path}: {e}")
        return []

def join(hashes: List[str], out_path: pathlib.Path) -> bool:
    """Join chunks into output file"""
    try:
        with open(out_path, "wb") as out:
            for h in hashes:
                chunk_path = CHUNK_DIR / h
                if not chunk_path.exists():
                    raise FileNotFoundError(f"Missing chunk: {h}")
                out.write(chunk_path.read_bytes())
        
        logging.info(f"Joined {len(hashes)} chunks into {out_path}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to join chunks: {e}")
        return False
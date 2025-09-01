"""
DittoFS - Distributed offline file system.

A peer-to-peer file synchronization system that works without a central server.
"""

__version__ = "0.1.0"
__author__ = "DittoFS Team"
__email__ = "team@dittofs.org"

# Import main components for easy access
try:
    from .chunker import Chunker
    from .crdt_store import CRDTStore
    from .sync_manager import SyncManager
    
    __all__ = [
        "Chunker",
        "CRDTStore", 
        "SyncManager",
        "__version__",
    ]
except ImportError:
    # Handle case where modules don't exist yet during development
    __all__ = ["__version__"]
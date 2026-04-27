//! Context Store Module
//! Handles the persistence layers using Sled DB and integrates with Loro.

use loro::{LoroDoc, VersionVector};
#[cfg(not(target_arch = "wasm32"))]
use sled::Db;
use std::sync::Arc;
use parking_lot::RwLock;
use dashmap::DashMap;

pub struct ContextStore {
    #[cfg(not(target_arch = "wasm32"))]
    db: Db,
    docs: DashMap<String, Arc<RwLock<LoroDoc>>>,
}

impl ContextStore {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(path: &str) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self { 
            db,
            docs: DashMap::new(),
        })
    }

    #[cfg(target_arch = "wasm32")]
    pub fn new(_path: &str) -> Result<Self, String> {
        Ok(Self { 
            docs: DashMap::new(),
        })
    }

    pub fn get_or_create_doc(&self, file_id: &str) -> Arc<RwLock<LoroDoc>> {
        self.docs.entry(file_id.to_string()).or_insert_with(|| {
            let doc = LoroDoc::new();

            #[cfg(not(target_arch = "wasm32"))]
            {
                if let Ok(Some(snapshot)) = self.db.get(file_id) {
                    let _ = doc.import(&snapshot);
                }
            }

            // Initialize containers exactly once, here, not on every get_fs_root call
            if file_id == "root" {
                let _tree = doc.get_tree("fs_tree");
                let _metadata = doc.get_map("fs_metadata");
            }

            Arc::new(RwLock::new(doc))
        }).clone()
    }

    // get_fs_root is now a zero-cost passthrough — NO locking here
    pub fn get_fs_root(&self) -> Arc<RwLock<LoroDoc>> {
        self.get_or_create_doc("root")
    }
    
    pub fn get_all_keys(&self) -> Vec<String> {
        self.docs.iter().map(|kv| kv.key().clone()).collect()
    }

    pub fn get_doc(&self, file_id: &str) -> Option<Arc<RwLock<LoroDoc>>> {
        self.docs.get(file_id).map(|d| d.clone())
    }
    
    #[cfg(not(target_arch = "wasm32"))]
    pub fn save_doc(&self, file_id: &str, doc: &LoroDoc) -> Result<(), sled::Error> {
        let snapshot = doc.export(loro::ExportMode::snapshot()).unwrap();
        self.db.insert(file_id, snapshot)?;
        self.db.flush()?;
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    pub fn save_doc(&self, _file_id: &str, _doc: &LoroDoc) -> Result<(), String> {
        // No persistence in WASM yet
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_blob(&self, key: &str) -> Option<Vec<u8>> {
        self.db.get(key).ok().flatten().map(|v| v.to_vec())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_blob(&self, key: &str, data: &[u8]) -> Result<(), sled::Error> {
        self.db.insert(key, data)?;
        self.db.flush()?;
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    pub fn get_blob(&self, _key: &str) -> Option<Vec<u8>> {
        None
    }

    #[cfg(target_arch = "wasm32")]
    pub fn set_blob(&self, _key: &str, _data: &[u8]) -> Result<(), String> {
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn last_known_vv(&self, doc_id: &str) -> VersionVector {
        let key = format!("vv:{}", doc_id);
        if let Ok(Some(bytes)) = self.db.get(key) {
            bincode::deserialize(&bytes).unwrap_or_default()
        } else {
            VersionVector::default()
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn save_vv(&self, doc_id: &str, vv: &VersionVector) -> Result<(), sled::Error> {
        let key = format!("vv:{}", doc_id);
        let bytes = bincode::serialize(vv).unwrap_or_default();
        self.db.insert(key, bytes)?;
        self.db.flush()?;
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    pub fn last_known_vv(&self, _doc_id: &str) -> VersionVector {
        VersionVector::new()
    }

    #[cfg(target_arch = "wasm32")]
    pub fn save_vv(&self, _doc_id: &str, _vv: &VersionVector) -> Result<(), String> {
        Ok(())
    }
}

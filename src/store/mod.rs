//! Context Store Module
//! Handles the persistence layers using Sled DB and integrates with Loro.

use loro::LoroDoc;
#[cfg(not(target_arch = "wasm32"))]
use sled::Db;

pub struct ContextStore {
    #[cfg(not(target_arch = "wasm32"))]
    db: Db,
    docs: dashmap::DashMap<String, LoroDoc>,
}

impl ContextStore {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(path: &str) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self { 
            db,
            docs: dashmap::DashMap::new(),
        })
    }

    #[cfg(target_arch = "wasm32")]
    pub fn new(_path: &str) -> Result<Self, String> {
        Ok(Self { 
            docs: dashmap::DashMap::new(),
        })
    }

    pub fn get_or_create_doc(&self, file_id: &str) -> LoroDoc {
        if let Some(doc) = self.docs.get(file_id) {
            return doc.clone();
        }

        let doc = LoroDoc::new();
        
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Ok(Some(snapshot)) = self.db.get(file_id) {
                let _ = doc.import(&snapshot);
            }
        }

        self.docs.insert(file_id.to_string(), doc.clone());
        doc
    }
    
    pub fn get_fs_root(&self) -> LoroDoc {
        let doc = self.get_or_create_doc("root");
        // Ensure tree and metadata maps are initialized
        let _tree = doc.get_tree("fs_tree");
        let _metadata = doc.get_map("fs_metadata");
        doc
    }
    
    pub fn get_all_keys(&self) -> Vec<String> {
        self.docs.iter().map(|kv| kv.key().clone()).collect()
    }

    pub fn get_doc(&self, file_id: &str) -> Option<LoroDoc> {
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
}

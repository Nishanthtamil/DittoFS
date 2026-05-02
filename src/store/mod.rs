//! Context Store Module
//! Handles the persistence layers using Sled DB and integrates with Loro.

use loro::{LoroDoc, VersionVector, ValueOrContainer, Container, Frontiers, ID};
#[cfg(not(target_arch = "wasm32"))]
use sled::Db;
use std::sync::Arc;
use parking_lot::RwLock;
use dashmap::DashMap;

pub mod crypto;

pub struct ContextStore {
    #[cfg(not(target_arch = "wasm32"))]
    pub db: Db,
    docs: DashMap<String, Arc<RwLock<LoroDoc>>>,
    #[cfg(not(target_arch = "wasm32"))]
    pub space_key: Option<crypto::SpaceKey>,
    #[cfg(not(target_arch = "wasm32"))]
    pub local_peer_id: u64,
}

impl ContextStore {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(path: &str, passphrase: Option<&str>) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        let space_key = passphrase.map(|p| crypto::SpaceKey::from_passphrase(p, b"dittofs-space-v1"));
        
        let local_peer_id = if let Ok(Some(bytes)) = db.get("local_peer_id") {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&bytes);
            u64::from_le_bytes(arr)
        } else {
            let id = rand::random::<u64>();
            let _ = db.insert("local_peer_id", &id.to_le_bytes());
            id
        };

        Ok(Self { 
            db,
            docs: DashMap::new(),
            space_key,
            local_peer_id,
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
            doc.set_peer_id(self.local_peer_id).unwrap();

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
        crate::metrics::BLOB_READS.inc();
        self.db.get(key).ok().flatten().map(|v| v.to_vec())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_blob(&self, key: &str, data: &[u8]) -> Result<(), sled::Error> {
        crate::metrics::BLOB_WRITES.inc();
        self.db.insert(key, data)?;
        self.db.flush()?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_raw_blob(&self, key: &str) -> Option<Vec<u8>> {
        self.db.get(key).ok().flatten().map(|v| v.to_vec())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_raw_blob(&self, key: &str, data: &[u8]) -> Result<(), sled::Error> {
        self.db.insert(key, data)?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_blob_cas(&self, data: &[u8]) -> Result<(String, bool), sled::Error> {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash = hasher.finalize();
        let hash_hex = hex::encode(hash);
        let cas_key = format!("cas:{}", hash_hex);
        
        let mut is_new = false;
        if self.db.get(&cas_key)?.is_none() {
            let data_to_store = if let Some(key) = &self.space_key {
                key.encrypt_blob(data, &hash_hex)
            } else {
                data.to_vec()
            };

            crate::metrics::BLOB_WRITES.inc();
            self.db.insert(&cas_key, data_to_store)?;
            self.db.flush()?;
            is_new = true;
        }
        Ok((hash_hex, is_new))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_blob_cas(&self, hash_hex: &str) -> Option<Vec<u8>> {
        let cas_key = format!("cas:{}", hash_hex);
        crate::metrics::BLOB_READS.inc();
        let bytes = self.db.get(cas_key).ok().flatten().map(|v| v.to_vec())?;
        
        if let Some(key) = &self.space_key {
            key.decrypt_blob(&bytes, hash_hex).ok()
        } else {
            Some(bytes)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_blob_chunks(&self, data: &[u8]) -> Result<Vec<(String, bool)>, sled::Error> {
        use fastcdc::v2020::FastCDC;
        let avg_size = 256 * 1024; // 256KB
        let min_size = 64 * 1024;  // 64KB
        let max_size = 1024 * 1024; // 1MB
        
        let chunker = FastCDC::new(data, min_size, avg_size, max_size);
        let mut results = Vec::new();
        
        for chunk in chunker {
            let chunk_data = &data[chunk.offset..chunk.offset + chunk.length];
            let res = self.set_blob_cas(chunk_data)?;
            results.push(res);
        }
        
        Ok(results)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_blob_chunks(&self, hashes: &[String]) -> Option<Vec<u8>> {
        let mut data = Vec::new();
        for hash in hashes {
            if let Some(chunk) = self.get_blob_cas(hash) {
                data.extend_from_slice(&chunk);
            } else {
                return None;
            }
        }
        Some(data)
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

    #[cfg(not(target_arch = "wasm32"))]
    pub fn save_peer(&self, peer_id: &libp2p::PeerId, addr: &libp2p::Multiaddr) -> Result<(), sled::Error> {
        let key = format!("peer:{}", peer_id.to_base58());
        self.db.insert(key, addr.to_vec())?;
        self.db.flush()?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_known_peers(&self) -> Vec<(libp2p::PeerId, libp2p::Multiaddr)> {
        let mut peers = Vec::new();
        for res in self.db.scan_prefix("peer:") {
            if let Ok((key, value)) = res {
                let key_str = String::from_utf8_lossy(&key);
                if let Some(peer_id_str) = key_str.strip_prefix("peer:") {
                    if let Ok(peer_id) = peer_id_str.parse::<libp2p::PeerId>() {
                        if let Ok(addr) = libp2p::Multiaddr::try_from(value.to_vec()) {
                            peers.push((peer_id, addr));
                        }
                    }
                }
            }
        }
        peers
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn migrate_to_cas(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.db.get("cas_migrated")?.is_some() {
            return Ok(());
        }

        println!("Migrating legacy blobs to CAS...");
        let doc_lock = self.get_fs_root();
        let doc = doc_lock.write();
        let metadata = doc.get_map("fs_metadata");

        let mut legacy_keys = Vec::new();
        for res in self.db.scan_prefix("blob:") {
            if let Ok((key, value)) = res {
                let key_str = String::from_utf8_lossy(&key).to_string();
                legacy_keys.push((key_str, value.to_vec()));
            }
        }

        for (key, data) in legacy_keys {
            if let Some(tree_id_str) = key.strip_prefix("blob:") {
                let (hash_hex, _) = self.set_blob_cas(&data)?;
                if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(tree_id_str) {
                    m.insert("blob_hash", hash_hex).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                    self.db.remove(key)?;
                }
            }
        }

        doc.commit();
        self.save_doc("root", &doc)?;
        self.db.insert("cas_migrated", "true")?;
        self.db.flush()?;
        println!("Migration complete.");
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    pub fn migrate_to_cas(&self) -> Result<(), String> {
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn save_checkpoint(&self, name: &str, frontiers: &Frontiers) -> Result<(), sled::Error> {
        let key = format!("checkpoint:{}", name);
        let ids: Vec<ID> = frontiers.iter().collect();
        let bytes = bincode::serialize(&ids).unwrap_or_default();
        self.db.insert(key, bytes)?;
        self.db.flush()?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_checkpoint(&self, name: &str) -> Option<Frontiers> {
        let key = format!("checkpoint:{}", name);
        if let Ok(Some(bytes)) = self.db.get(key) {
            let ids: Vec<ID> = bincode::deserialize(&bytes).ok()?;
            Some(Frontiers::from(ids))
        } else {
            None
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn list_checkpoints(&self) -> Vec<String> {
        let mut names = Vec::new();
        for res in self.db.scan_prefix("checkpoint:") {
            if let Ok((key, _)) = res {
                let key_str = String::from_utf8_lossy(&key);
                if let Some(name) = key_str.strip_prefix("checkpoint:") {
                    names.push(name.to_string());
                }
            }
        }
        names.sort();
        names.reverse(); // Newest first
        names
    }
}

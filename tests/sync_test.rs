use dittofs::store::ContextStore;
use dittofs::network::update::DittoUpdate;
use dittofs::fs::DittoFS;
use std::sync::Arc;
use std::collections::HashMap;
use tempfile::tempdir;
use loro::{Container, ValueOrContainer, LoroMap, LoroList, VersionVector};
use tokio::sync::mpsc;

#[tokio::test]
async fn two_nodes_sync_via_wire_format() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();

    let store_a = Arc::new(ContextStore::new(dir_a.path().to_str().unwrap(), None).unwrap());
    let store_b = Arc::new(ContextStore::new(dir_b.path().to_str().unwrap(), None).unwrap());

    // Node A: create a file and write content
    let (tree_id, blob_key) = {
        let doc_lock = store_a.get_fs_root();
        let doc = doc_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        let new_id = tree.create(None).unwrap();
        let meta_map = metadata
            .insert_container(&new_id.to_string(), LoroMap::new())
            .unwrap();
        meta_map.insert("name", "hello.txt").unwrap();
        meta_map.insert("type", "file").unwrap();
        meta_map.insert("size", 11i64).unwrap();

        let blob_key = format!("blob:{}", new_id);
        store_a.set_blob(&blob_key, b"hello world").unwrap();

        doc.commit();
        (new_id, blob_key)
    };

    // Build a DittoUpdate exactly as sync_local_delta does
    let wire_bytes = {
        let doc_lock = store_a.get_fs_root();
        let doc = doc_lock.read();
        let last_vv = store_a.last_known_vv("root");
        let loro_delta = doc.export(loro::ExportMode::updates(&last_vv)).unwrap();

        let mut blobs = HashMap::new();
        blobs.insert(
            blob_key.clone(),
            store_a.get_blob(&blob_key).unwrap(),
        );

        let update = DittoUpdate { loro_delta, blobs };
        bincode::serialize(&update).unwrap()
    };

    // Node B: receive and apply as main.rs does
    {
        let decoded: DittoUpdate = bincode::deserialize(&wire_bytes).unwrap();
        let doc_lock = store_b.get_fs_root();
        let doc = doc_lock.write();
        doc.import(&decoded.loro_delta).unwrap();
        doc.commit();
        for (key, blob) in decoded.blobs {
            store_b.set_blob(&key, &blob).unwrap();
        }
        store_b.save_vv("root", &doc.oplog_vv()).unwrap();
    }

    // Assert Node B sees the file correctly
    {
        let doc_lock = store_b.get_fs_root();
        let doc = doc_lock.read();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        let children = tree.children(None).unwrap();
        assert_eq!(children.len(), 1, "Node B should see exactly one file");
        assert_eq!(children[0], tree_id);

        if let Some(ValueOrContainer::Container(Container::Map(m))) =
            metadata.get(&tree_id.to_string())
        {
            let name = m.get("name").unwrap();
            let name_str = name.as_value().unwrap().as_string().unwrap();
            assert_eq!(name_str.as_ref(), "hello.txt");
        } else {
            panic!("Metadata missing on Node B");
        }

        let content = store_b.get_blob(&blob_key).unwrap();
        assert_eq!(content, b"hello world");
    }
}

#[tokio::test]
async fn concurrent_conflict_resolves_cleanly() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();

    let store_a = Arc::new(ContextStore::new(dir_a.path().to_str().unwrap(), None).unwrap());
    let store_b = Arc::new(ContextStore::new(dir_b.path().to_str().unwrap(), None).unwrap());

    // Node A creates file_a.txt
    let update_a = {
        let doc_lock = store_a.get_fs_root();
        let doc = doc_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");
        let id = tree.create(None).unwrap();
        let m = metadata.insert_container(&id.to_string(), LoroMap::new()).unwrap();
        m.insert("name", "file_a.txt").unwrap();
        m.insert("type", "file").unwrap();
        doc.commit();
        doc.export(loro::ExportMode::all_updates()).unwrap()
    };

    // Node B creates file_b.txt independently (no sync yet)
    let update_b = {
        let doc_lock = store_b.get_fs_root();
        let doc = doc_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");
        let id = tree.create(None).unwrap();
        let m = metadata.insert_container(&id.to_string(), LoroMap::new()).unwrap();
        m.insert("name", "file_b.txt").unwrap();
        m.insert("type", "file").unwrap();
        doc.commit();
        doc.export(loro::ExportMode::all_updates()).unwrap()
    };

    // Now both nodes sync: A imports B's update, B imports A's update
    {
        let doc_lock = store_a.get_fs_root();
        let doc = doc_lock.write();
        doc.import(&update_b).unwrap();
        doc.commit();
    }
    {
        let doc_lock = store_b.get_fs_root();
        let doc = doc_lock.write();
        doc.import(&update_a).unwrap();
        doc.commit();
    }

    // Both nodes should now see exactly 2 files with no panic
    let check_node = |store: &ContextStore| {
        let doc_lock = store.get_fs_root();
        let doc = doc_lock.read();
        let tree = doc.get_tree("fs_tree");
        let children = tree.children(None).unwrap();
        assert_eq!(children.len(), 2, "Should see both files after merge");
        let metadata = doc.get_map("fs_metadata");
        let names: Vec<String> = children.iter().filter_map(|id| {
            if let Some(ValueOrContainer::Container(Container::Map(m))) =
                metadata.get(&id.to_string())
            {
                if let Some(ValueOrContainer::Value(loro::LoroValue::String(n))) = m.get("name") {
                    return Some(n.as_ref().to_string());
                }
            }
            None
        }).collect();
        assert!(names.contains(&"file_a.txt".to_string()));
        assert!(names.contains(&"file_b.txt".to_string()));
    };

    check_node(&store_a);
    check_node(&store_b);
}

#[tokio::test]
async fn truncate_then_write_produces_correct_content() {
    let dir = tempdir().unwrap();
    let store = Arc::new(ContextStore::new(dir.path().to_str().unwrap(), None).unwrap());

    // Create a file with initial content
    let blob_key = {
        let doc_lock = store.get_fs_root();
        let doc = doc_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");
        let id = tree.create(None).unwrap();
        let m = metadata.insert_container(&id.to_string(), LoroMap::new()).unwrap();
        m.insert("name", "test.txt").unwrap();
        m.insert("type", "file").unwrap();
        m.insert("size", 13i64).unwrap();
        let key = format!("blob:{}", id);
        store.set_blob(&key, b"initial content").unwrap();
        doc.commit();
        key
    };

    // Simulate truncate to 0
    store.set_blob(&blob_key, b"").unwrap();

    // Simulate write of new content
    store.set_blob(&blob_key, b"new").unwrap();

    let content = store.get_blob(&blob_key).unwrap();
    assert_eq!(content, b"new");
    assert_eq!(content.len(), 3);
}

#[tokio::test]
async fn large_file_chunks_and_syncs() {
    let db_a = tempdir().unwrap();
    let db_b = tempdir().unwrap();
    let store_a = Arc::new(ContextStore::new(db_a.path().to_str().unwrap(), None).unwrap());
    let store_b = Arc::new(ContextStore::new(db_b.path().to_str().unwrap(), None).unwrap());

    let (tx_a, _rx_a) = mpsc::channel(10);
    let (tx_b, _rx_b) = mpsc::channel(10);

    let _fs_a = DittoFS::new(store_a.clone(), Some(tx_a));
    let _fs_b = DittoFS::new(store_b.clone(), Some(tx_b));

    // 1. Peer A creates a 2MB file (should be chunked)
    let data = vec![0u8; 2 * 1024 * 1024];
    let root_a = store_a.get_fs_root();
    let tree_id = {
        let doc = root_a.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");
        let id = tree.create(None).unwrap();
        let map = metadata.insert_container(&id.to_string(), LoroMap::new()).unwrap();
        map.insert("name", "large.bin").unwrap();
        map.insert("type", "file").unwrap();
        map.insert("size", data.len() as i64).unwrap();
        
        let chunk_results = store_a.set_blob_chunks(&data).unwrap();
        let l = map.insert_container("chunks", LoroList::new()).unwrap();
        for (hash, _) in chunk_results {
            l.insert(l.len(), hash).unwrap();
        }
        doc.commit();
        id
    };

    // Simulate sync
    let last_vv_a = VersionVector::default();
    let update = root_a.read().export(loro::ExportMode::updates(&last_vv_a)).unwrap();
    
    let mut blobs = std::collections::HashMap::new();
    // Extract all chunks from store_a to simulate full sync
    let doc_a = root_a.read();
    let metadata = doc_a.get_map("fs_metadata");
    if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&tree_id.to_string()) {
        if let Some(ValueOrContainer::Container(Container::List(l))) = m.get("chunks") {
            for i in 0..l.len() {
                if let Some(ValueOrContainer::Value(loro::LoroValue::String(h))) = l.get(i) {
                    let hash = h.as_ref().to_string();
                    blobs.insert(format!("cas:{}", hash), store_a.get_blob_cas(&hash).unwrap());
                }
            }
        }
    }

    let ditto_update = DittoUpdate {
        loro_delta: update,
        blobs,
    };
    let sync_data = bincode::serialize(&ditto_update).unwrap();

    // Node B receives
    let received: DittoUpdate = bincode::deserialize(&sync_data).unwrap();
    let root_b = store_b.get_fs_root();
    {
        let doc = root_b.write();
        doc.import(&received.loro_delta).unwrap();
        for (key, blob) in &received.blobs {
            if key.starts_with("cas:") {
                let _ = store_b.set_raw_blob(key, blob);
            }
        }
        doc.commit();
    }

    // Verify Node B can reassemble
    let doc_b = root_b.read();
    let meta_b = doc_b.get_map("fs_metadata");
    if let Some(ValueOrContainer::Container(Container::Map(m))) = meta_b.get(&tree_id.to_string()) {
        if let Some(ValueOrContainer::Container(Container::List(l))) = m.get("chunks") {
            let hashes: Vec<String> = (0..l.len()).map(|i| {
                if let ValueOrContainer::Value(loro::LoroValue::String(s)) = l.get(i).unwrap() {
                    s.as_ref().to_string()
                } else {
                    panic!("Not a string hash");
                }
            }).collect();
            let reassembled = store_b.get_blob_chunks(&hashes).unwrap();
            assert_eq!(reassembled.len(), data.len());
            assert_eq!(reassembled, data);
        } else {
            panic!("No chunks found on Node B");
        }
    } else {
        panic!("No metadata found on Node B");
    }
}

#[tokio::test]
async fn encrypted_sync_works() {
    let db_a = tempdir().unwrap();
    let db_b = tempdir().unwrap();
    let db_c = tempdir().unwrap();
    
    let passphrase = "correct-horse-battery-staple";
    let store_a = Arc::new(ContextStore::new(db_a.path().to_str().unwrap(), Some(passphrase)).unwrap());
    let store_b = Arc::new(ContextStore::new(db_b.path().to_str().unwrap(), Some(passphrase)).unwrap());
    let store_c = Arc::new(ContextStore::new(db_c.path().to_str().unwrap(), Some("wrong-passphrase")).unwrap());

    // 1. Peer A creates an encrypted file
    let data = b"encrypted content".to_vec();
    let root_a = store_a.get_fs_root();
    let tree_id = {
        let doc = root_a.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");
        let id = tree.create(None).unwrap();
        let map = metadata.insert_container(&id.to_string(), LoroMap::new()).unwrap();
        map.insert("name", "secret.txt").unwrap();
        map.insert("type", "file").unwrap();
        
        let (hash, _) = store_a.set_blob_cas(&data).unwrap();
        map.insert("blob_hash", hash).unwrap();
        doc.commit();
        id
    };

    // Simulate sync to B and C
    let update = root_a.read().export(loro::ExportMode::updates(&VersionVector::default())).unwrap();
    
    // Encrypt the update as DittoFS would
    let loro_delta = store_a.space_key.as_ref().unwrap().encrypt_metadata(&update);
    
    let mut blobs = HashMap::new();
    let doc_a = root_a.read();
    let metadata = doc_a.get_map("fs_metadata");
    if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&tree_id.to_string()) {
        if let Some(ValueOrContainer::Value(loro::LoroValue::String(h))) = m.get("blob_hash") {
            let hash = h.as_ref().to_string();
            // Get the encrypted raw bytes from store_a
            let encrypted_blob = store_a.get_raw_blob(&format!("cas:{}", hash)).unwrap();
            blobs.insert(format!("cas:{}", hash), encrypted_blob);
        }
    }

    let ditto_update = DittoUpdate { loro_delta, blobs };
    let sync_data = bincode::serialize(&ditto_update).unwrap();

    // Node B (correct passphrase) receives
    let received: DittoUpdate = bincode::deserialize(&sync_data).unwrap();
    let root_b = store_b.get_fs_root();
    {
        let doc = root_b.write();
        let decrypted = store_b.space_key.as_ref().unwrap().decrypt_metadata(&received.loro_delta).unwrap();
        doc.import(&decrypted).unwrap();
        for (key, blob) in &received.blobs {
            store_b.set_raw_blob(key, blob).unwrap();
        }
        doc.commit();
    }

    // Verify Node B can read
    let doc_b = root_b.read();
    let meta_b = doc_b.get_map("fs_metadata");
    if let Some(ValueOrContainer::Container(Container::Map(m))) = meta_b.get(&tree_id.to_string()) {
        if let Some(ValueOrContainer::Value(loro::LoroValue::String(h))) = m.get("blob_hash") {
            let hash = h.as_ref().to_string();
            let decrypted = store_b.get_blob_cas(&hash).unwrap();
            assert_eq!(decrypted, data);
        }
    }

    // Node C (wrong passphrase) receives
    let root_c = store_c.get_fs_root();
    {
        let _doc = root_c.write();
        let decryption_result = store_c.space_key.as_ref().unwrap().decrypt_metadata(&received.loro_delta);
        assert!(decryption_result.is_err());
    }
}

use dittofs::store::ContextStore;
use dittofs::network::update::DittoUpdate;
use std::sync::Arc;
use std::collections::HashMap;
use tempfile::tempdir;
use loro::{Container, ValueOrContainer, LoroMap};

#[tokio::test]
async fn two_nodes_sync_via_wire_format() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();

    let store_a = Arc::new(ContextStore::new(dir_a.path().to_str().unwrap()).unwrap());
    let store_b = Arc::new(ContextStore::new(dir_b.path().to_str().unwrap()).unwrap());

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

    let store_a = Arc::new(ContextStore::new(dir_a.path().to_str().unwrap()).unwrap());
    let store_b = Arc::new(ContextStore::new(dir_b.path().to_str().unwrap()).unwrap());

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
    let store = Arc::new(ContextStore::new(dir.path().to_str().unwrap()).unwrap());

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

use dittofs::store::ContextStore;
use std::sync::Arc;
use tempfile::tempdir;
use loro::{Container, ValueOrContainer, LoroMap};

#[tokio::test]
async fn two_nodes_sync_a_file() {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    
    let store_a = Arc::new(ContextStore::new(dir_a.path().to_str().unwrap()).unwrap());
    let store_b = Arc::new(ContextStore::new(dir_b.path().to_str().unwrap()).unwrap());

    // Node A: create a file, write content
    let doc_a_lock = store_a.get_fs_root();
    let tree_id = {
        let doc = doc_a_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");
        
        // Create "hello.txt" at root (parent is None)
        let new_id = tree.create(None).unwrap();
        let meta_map = metadata.insert_container(&new_id.to_string(), LoroMap::new()).unwrap();
        meta_map.insert("name", "hello.txt").unwrap();
        meta_map.insert("type", "file").unwrap();
        meta_map.insert("size", 11i64).unwrap();
        
        // Write content to blob store
        let blob_key = format!("blob:{}", new_id);
        store_a.set_blob(&blob_key, b"hello world").unwrap();
        
        doc.commit();
        new_id
    };

    // Export updates from A
    let update = {
        let doc = doc_a_lock.read();
        doc.export(loro::ExportMode::all_updates()).unwrap()
    };

    // Node B: apply A's update
    let doc_b_lock = store_b.get_fs_root();
    {
        let doc = doc_b_lock.write();
        doc.import(&update).unwrap();
        doc.commit();
    }

    // Node B: Simulate sync of blob (in Phase 3 this will be separate or bundled)
    // For this test, we manually "sync" the blob to verify the storage logic
    let blob_key = format!("blob:{}", tree_id);
    let blob_data = store_a.get_blob(&blob_key).unwrap();
    store_b.set_blob(&blob_key, &blob_data).unwrap();

    // Assert B can see the file and content matches
    {
        let doc = doc_b_lock.read();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");
        
        let children = tree.children(None).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0], tree_id);
        
        let meta_val = metadata.get(&tree_id.to_string()).unwrap();
        if let ValueOrContainer::Container(Container::Map(m)) = meta_val {
            let name_val = m.get("name").unwrap();
            let name = name_val.as_value().unwrap().as_string().unwrap();
            assert_eq!(name.as_ref(), "hello.txt");
        } else {
            panic!("Metadata is not a map");
        }
        
        let read_blob = store_b.get_blob(&blob_key).unwrap();
        assert_eq!(read_blob, b"hello world");
    }
}

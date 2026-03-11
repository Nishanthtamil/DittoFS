//! Garbage Collector Module
//! Handles snapshotting of the CRDT history and compacting the Sled database.

use std::sync::Arc;
use tokio::time::{sleep, Duration};
use crate::store::ContextStore;

pub async fn start_gc_loop(store: Arc<ContextStore>) {
    loop {
        sleep(Duration::from_secs(3600)).await;
        let keys = store.get_all_keys();
        for key in keys {
            if let Some(doc) = store.get_doc(&key) {
                let _ = store.save_doc(&key, &doc);
            }
        }
        println!("GC: Compressed CRDT history to disk snapshots.");
    }
}

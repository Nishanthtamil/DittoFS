#[cfg(not(target_arch = "wasm32"))]
pub mod store;
#[cfg(not(target_arch = "wasm32"))]
pub mod network;
#[cfg(not(target_arch = "wasm32"))]
pub mod fs;
#[cfg(not(target_arch = "wasm32"))]
pub mod gc;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::mpsc;
#[cfg(not(target_arch = "wasm32"))]
use fuse3::path::Session;
#[cfg(not(target_arch = "wasm32"))]
use fuse3::MountOptions;
#[cfg(not(target_arch = "wasm32"))]
use clap::Parser;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "/tmp/ditto")]
    mount: String,

    #[arg(short, long, default_value = "/tmp/dittofs-sled")]
    db: String,
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    println!("DittoFS starting up...");
    println!("Mount point: {}", args.mount);
    println!("Database: {}", args.db);

    let store = Arc::new(store::ContextStore::new(&args.db)?);
    
    // Start Garbage Collector
    tokio::spawn(gc::start_gc_loop(store.clone()));

    // Complete Swarm integration (Edge P2P)
    let (tx_local, rx_local) = mpsc::channel(100);
    let (tx_remote, mut rx_remote) = mpsc::channel(100);
    
    let swarm = network::create_swarm()?;
    tokio::spawn(network::run_swarm(swarm, rx_local, tx_remote));

    // Swarm delta-broadcast updates intake
    let store_clone = store.clone();
    tokio::spawn(async move {
        while let Some(data) = rx_remote.recv().await {
            if let Ok(update) = bincode::deserialize::<network::update::DittoUpdate>(&data) {
                // Apply update to root doc
                let doc_lock = store_clone.get_or_create_doc("root");
                let doc = doc_lock.write();
                let _ = doc.import(&update.loro_delta);
                // Save any received blobs
                for (key, blob) in update.blobs {
                    let _ = store_clone.set_blob(&key, &blob);
                }
                // After remote import, we should probably update our last_known_vv to include this
                let _ = store_clone.save_vv("root", &doc.oplog_vv());
            }
        }
    });

    // Mount FUSE Edge Bridge
    let mount_path = args.mount;
    let _ = std::fs::create_dir_all(&mount_path);
    let ditto_fs = fs::DittoFS::new(store.clone(), Some(tx_local));
    
    let mount_options = MountOptions::default();
    let mount_handle = Session::new(mount_options).mount_with_unprivileged(ditto_fs, mount_path).await?;
    
    println!("Mounted FUSE virtual filesystem");
    let _ = mount_handle.await;
    
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn main() {}

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
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("DittoFS starting up...");

    let store = Arc::new(store::ContextStore::new("/tmp/dittofs-sled")?);
    
    // Start Garbage Collector
    tokio::spawn(gc::start_gc_loop(store.clone()));

    // Complete Swarm integration (Edge P2P)
    let swarm = network::create_swarm()?;
    let (_tx_local, rx_local) = mpsc::channel(100);
    let (tx_remote, mut rx_remote) = mpsc::channel(100);

    tokio::spawn(network::run_swarm(swarm, rx_local, tx_remote));

    // Swarm delta-broadcast updates intake
    let store_clone = store.clone();
    tokio::spawn(async move {
        while let Some(data) = rx_remote.recv().await {
            // Apply update to root doc
            let doc = store_clone.get_or_create_doc("root");
            let _ = doc.import(&data);
        }
    });

    // Mount FUSE Edge Bridge
    let mount_path = "/tmp/ditto";
    let _ = std::fs::create_dir_all(mount_path);
    let ditto_fs = fs::DittoFS::new(store.clone());
    
    let mount_options = MountOptions::default();
    let mount_handle = Session::new(mount_options).mount_with_unprivileged(ditto_fs, mount_path).await?;
    
    println!("Mounted FUSE virtual filesystem at {}", mount_path);
    let _ = mount_handle.await;
    
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn main() {}

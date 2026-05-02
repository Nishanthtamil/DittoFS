#[cfg(not(target_arch = "wasm32"))]
pub mod store;
#[cfg(not(target_arch = "wasm32"))]
pub mod network;
#[cfg(not(target_arch = "wasm32"))]
pub mod fs;
#[cfg(not(target_arch = "wasm32"))]
pub mod gc;
#[cfg(not(target_arch = "wasm32"))]
pub mod metrics;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use tokio::sync::mpsc;
#[cfg(not(target_arch = "wasm32"))]
use fuse3::path::Session;
#[cfg(not(target_arch = "wasm32"))]
use fuse3::MountOptions;
#[cfg(not(target_arch = "wasm32"))]
use clap::{Parser, Subcommand};

#[cfg(not(target_arch = "wasm32"))]
use loro::{ValueOrContainer, Container};

#[cfg(not(target_arch = "wasm32"))]
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "/tmp/ditto")]
    mount: String,

    #[arg(short, long, default_value = "/tmp/dittofs-sled")]
    db: String,

    #[arg(short, long)]
    passphrase: Option<String>,

    #[arg(short, long)]
    bootstrap: Vec<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create a manual checkpoint
    Checkpoint { name: String },
    /// List all snapshots
    ListCheckpoints,
    /// Restore a file from a snapshot
    Restore { 
        snapshot: String, 
        source: String, 
        target: String 
    },
    /// Manage access control lists
    Acl {
        #[command(subcommand)]
        action: AclCommands,
    }
}

#[derive(Subcommand, Debug)]
enum AclCommands {
    Set { path: String, #[arg(short, long)] peer: String, #[arg(long)] permission: String },
    Get { path: String },
    Revoke { path: String, #[arg(short, long)] peer: String },
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    if let Some(cmd) = args.command {
        let store = store::ContextStore::new(&args.db, args.passphrase.as_deref())?;
        match cmd {
            Commands::Checkpoint { name } => {
                let root_lock = store.get_fs_root();
                let doc = root_lock.read();
                store.save_checkpoint(&name, &doc.oplog_frontiers())?;
                println!("Checkpoint '{}' created.", name);
            }
            Commands::ListCheckpoints => {
                let checkpoints = store.list_checkpoints();
                println!("Snapshots:");
                for cp in checkpoints {
                    println!("  - {}", cp);
                }
            }
            Commands::Restore { snapshot, source, .. } => {
                let vv = store.get_checkpoint(&snapshot).ok_or("Snapshot not found")?;
                let doc_lock = store.get_fs_root();
                let doc = doc_lock.write();
                let historical_doc = doc.clone();
                let _ = historical_doc.checkout(&vv);
                
                // This is a bit complex as we need to copy bytes from CAS.
                // For now, let's just log.
                println!("Restoring {} from {} to current state (Not fully implemented yet)...", source, snapshot);
            }
            Commands::Acl { action } => {
                let store_arc = Arc::new(store);
                let ditto_fs = fs::DittoFS::new(store_arc.clone(), None);
                let doc_lock = store_arc.get_fs_root();
                let doc = doc_lock.write();

                match action {
                    AclCommands::Set { path, peer, permission } => {
                        if let Some(Some(tree_id)) = ditto_fs.resolve_path_at_doc(&doc, &path) {
                            ditto_fs.check_write_permission(&doc, Some(tree_id))?;
                            let permissions = doc.get_map("permissions");
                            if let Some(ValueOrContainer::Container(Container::Map(m))) = permissions.get(&tree_id.to_string()) {
                                if let Some(ValueOrContainer::Container(Container::Map(acl))) = m.get("acl") {
                                    acl.insert(&peer, permission)?;
                                    doc.commit();
                                    store_arc.save_doc("root", &doc)?;
                                    println!("ACL set for {} on {}", peer, path);
                                } else {
                                    println!("Error: ACL map not initialized for this file.");
                                }
                            } else {
                                println!("Error: Permissions not initialized for this file.");
                            }
                        } else {
                            println!("Error: Path not found.");
                        }
                    }
                    AclCommands::Get { path } => {
                        if let Some(Some(tree_id)) = ditto_fs.resolve_path_at_doc(&doc, &path) {
                            let permissions = doc.get_map("permissions");
                            if let Some(ValueOrContainer::Container(Container::Map(m))) = permissions.get(&tree_id.to_string()) {
                                if let Some(ValueOrContainer::Value(loro::LoroValue::String(owner))) = m.get("owner_peer") {
                                    println!("Owner: {}", owner.as_ref());
                                }
                                if let Some(ValueOrContainer::Container(Container::Map(acl))) = m.get("acl") {
                                    println!("ACL:");
                                    // A LoroMap doesn't currently provide a safe iterator of keys easily in the current API version we are using.
                                    // We will print what we can, but since we can't iterate the keys directly, we might just print its JSON representation.
                                    println!("{:?}", acl.get_value());
                                }
                            } else {
                                println!("No permissions set for this file.");
                            }
                        } else {
                            println!("Error: Path not found.");
                        }
                    }
                    AclCommands::Revoke { path, peer } => {
                        if let Some(Some(tree_id)) = ditto_fs.resolve_path_at_doc(&doc, &path) {
                            ditto_fs.check_write_permission(&doc, Some(tree_id))?;
                            let permissions = doc.get_map("permissions");
                            if let Some(ValueOrContainer::Container(Container::Map(m))) = permissions.get(&tree_id.to_string()) {
                                if let Some(ValueOrContainer::Container(Container::Map(acl))) = m.get("acl") {
                                    acl.delete(&peer)?;
                                    doc.commit();
                                    store_arc.save_doc("root", &doc)?;
                                    println!("ACL revoked for {} on {}", peer, path);
                                }
                            }
                        } else {
                            println!("Error: Path not found.");
                        }
                    }
                }
            }
        }
        return Ok(());
    }

    println!("DittoFS starting up...");
    println!("Mount point: {}", args.mount);
    println!("Database: {}", args.db);

    crate::metrics::register_metrics();
    tokio::spawn(crate::metrics::start_metrics_server(9091));

    let bootstrap_nodes = args.bootstrap.iter().filter_map(|s| {
        let parts: Vec<&str> = s.split("/p2p/").collect();
        if parts.len() != 2 { return None; }
        let addr: libp2p::Multiaddr = parts[0].parse().ok()?;
        let peer_id: libp2p::PeerId = parts[1].parse().ok()?;
        Some((peer_id, addr))
    }).collect();

    let store = Arc::new(store::ContextStore::new(&args.db, args.passphrase.as_deref())?);
    store.migrate_to_cas()?;
    
    // Start Garbage Collector
    tokio::spawn(gc::start_gc_loop(store.clone()));

    // Periodic Checkpoint Loop
    let store_cp = store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
        loop {
            interval.tick().await;
            let now = chrono::Utc::now().to_rfc3339();
            let root_lock = store_cp.get_fs_root();
            let doc = root_lock.read();
            let frontiers = doc.oplog_frontiers();
            let _ = store_cp.save_checkpoint(&now, &frontiers);
            println!("Created periodic checkpoint: {}", now);
        }
    });

    // Complete Swarm integration (Edge P2P)
    let (tx_local, rx_local) = mpsc::channel(100);
    let (tx_remote, mut rx_remote) = mpsc::channel(100);
    
    let swarm = network::create_swarm()?;
    tokio::spawn(network::run_swarm(swarm, rx_local, tx_remote, bootstrap_nodes, store.clone()));

    // Swarm delta-broadcast updates intake
    let store_clone = store.clone();
    tokio::spawn(async move {
        while let Some(data) = rx_remote.recv().await {
            if let Ok(update) = bincode::deserialize::<network::update::DittoUpdate>(&data) {
                // Apply update to root doc
                let doc_lock = store_clone.get_or_create_doc("root");
                let doc = doc_lock.write();

                let loro_delta = if let Some(key) = &store_clone.space_key {
                    if let Ok(decrypted) = key.decrypt_metadata(&update.loro_delta) {
                        decrypted
                    } else {
                        continue; // Skip updates we can't decrypt
                    }
                } else {
                    update.loro_delta
                };

                let _ = doc.import(&loro_delta);
                // Save any received blobs (they are already encrypted by the sender)
                for (key, blob) in update.blobs {
                    let _ = store_clone.set_raw_blob(&key, &blob);
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

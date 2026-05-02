use clap::Parser;
use std::sync::Arc;
use tokio::sync::mpsc;
use warp::Filter;
use dittofs::store::ContextStore;
use dittofs::network;
use dittofs::gc;
use loro::{LoroMap, ValueOrContainer, Container, TreeID};

#[derive(Parser, Debug)]
#[command(author, version, about = "DittoFS S3-Compatible Gateway", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "/tmp/dittofs-gateway-db")]
    db: String,

    #[arg(short, long)]
    passphrase: Option<String>,

    #[arg(short, long)]
    bootstrap: Vec<String>,

    #[arg(short, long, default_value = "0.0.0.0:9000")]
    listen: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    println!("DittoFS Gateway starting up...");

    let bootstrap_nodes = args.bootstrap.iter().filter_map(|s| {
        let parts: Vec<&str> = s.split("/p2p/").collect();
        if parts.len() != 2 { return None; }
        let addr: libp2p::Multiaddr = parts[0].parse().ok()?;
        let peer_id: libp2p::PeerId = parts[1].parse().ok()?;
        Some((peer_id, addr))
    }).collect();

    let store = Arc::new(ContextStore::new(&args.db, args.passphrase.as_deref())?);
    store.migrate_to_cas()?;
    tokio::spawn(gc::start_gc_loop(store.clone()));

    let (tx_local, rx_local) = mpsc::channel(100);
    let (tx_remote, mut rx_remote) = mpsc::channel(100);
    
    let swarm = network::create_swarm()?;
    tokio::spawn(network::run_swarm(swarm, rx_local, tx_remote, bootstrap_nodes, store.clone()));

    let store_clone = store.clone();
    tokio::spawn(async move {
        while let Some(data) = rx_remote.recv().await {
            if let Ok(update) = bincode::deserialize::<network::update::DittoUpdate>(&data) {
                let doc_lock = store_clone.get_or_create_doc("root");
                let doc = doc_lock.write();

                let loro_delta = if let Some(key) = &store_clone.space_key {
                    if let Ok(decrypted) = key.decrypt_metadata(&update.loro_delta) {
                        decrypted
                    } else {
                        continue;
                    }
                } else {
                    update.loro_delta
                };

                let _ = doc.import(&loro_delta);
                for (key, blob) in update.blobs {
                    let _ = store_clone.set_raw_blob(&key, &blob);
                }
                let _ = store_clone.save_vv("root", &doc.oplog_vv());
            }
        }
    });

    let store_filter = warp::any().map(move || store.clone());

    // Basic GET implementation for fetching files
    let get_route = warp::get()
        .and(warp::path::tail())
        .and(store_filter.clone())
        .map(|tail: warp::path::Tail, store: Arc<ContextStore>| {
            let path = format!("/{}", tail.as_str());
            let ditto_fs = dittofs::fs::DittoFS::new(store.clone(), None);
            
            if let Some(Some(tree_id)) = ditto_fs.resolve_path(std::ffi::OsStr::new(&path)) {
                let doc_lock = store.get_fs_root();
                let doc = doc_lock.read();
                let metadata = doc.get_map("fs_metadata");
                if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&tree_id.to_string()) {
                    let type_str = if let Some(ValueOrContainer::Value(loro::LoroValue::String(t))) = m.get("type") {
                        t.as_ref().to_string()
                    } else {
                        "file".to_string()
                    };

                    if type_str != "directory" {
                        let bytes = ditto_fs.get_file_bytes(&m, &tree_id);
                        return warp::reply::with_status(bytes, warp::http::StatusCode::OK);
                    }
                }
            }
            warp::reply::with_status(vec![], warp::http::StatusCode::NOT_FOUND)
        });

    let routes = get_route; // Additional routes (PUT, LIST) could be added here
    
    let addr: std::net::SocketAddr = args.listen.parse()?;
    println!("Listening on http://{}", addr);
    warp::serve(routes).run(addr).await;

    Ok(())
}
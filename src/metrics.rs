use lazy_static::lazy_static;
use prometheus::{IntCounter, IntGauge, HistogramVec, Registry, HistogramOpts, TextEncoder, Encoder};
use warp::Filter;
use std::sync::Arc;

lazy_static! {
    pub static ref REGISTRY: Arc<Registry> = Arc::new(Registry::new());

    pub static ref BLOB_READS: IntCounter = IntCounter::new("dittofs_blob_reads_total", "Total number of blob reads").unwrap();
    pub static ref BLOB_WRITES: IntCounter = IntCounter::new("dittofs_blob_writes_total", "Total number of blob writes").unwrap();
    pub static ref SYNC_BYTES_SENT: IntCounter = IntCounter::new("dittofs_sync_bytes_sent_total", "Total bytes sent over sync").unwrap();
    pub static ref SYNC_BYTES_RECEIVED: IntCounter = IntCounter::new("dittofs_sync_bytes_received_total", "Total bytes received over sync").unwrap();
    pub static ref CRDT_OPS_APPLIED: IntCounter = IntCounter::new("dittofs_crdt_ops_applied_total", "Total CRDT operations applied").unwrap();
    pub static ref PEER_COUNT: IntGauge = IntGauge::new("dittofs_peer_count", "Number of connected peers").unwrap();
    pub static ref STORAGE_BYTES: IntGauge = IntGauge::new("dittofs_storage_bytes", "Total storage used in bytes").unwrap();
    pub static ref FUSE_OP_DURATION: HistogramVec = HistogramVec::new(
        HistogramOpts::new("dittofs_fuse_op_duration_seconds", "Duration of FUSE operations"),
        &["op"]
    ).unwrap();
}

pub fn register_metrics() {
    let registry = &*REGISTRY;
    registry.register(Box::new(BLOB_READS.clone())).unwrap();
    registry.register(Box::new(BLOB_WRITES.clone())).unwrap();
    registry.register(Box::new(SYNC_BYTES_SENT.clone())).unwrap();
    registry.register(Box::new(SYNC_BYTES_RECEIVED.clone())).unwrap();
    registry.register(Box::new(CRDT_OPS_APPLIED.clone())).unwrap();
    registry.register(Box::new(PEER_COUNT.clone())).unwrap();
    registry.register(Box::new(STORAGE_BYTES.clone())).unwrap();
    registry.register(Box::new(FUSE_OP_DURATION.clone())).unwrap();
}

pub async fn start_metrics_server(port: u16) {
    let metrics_route = warp::path!("metrics").map(|| {
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = REGISTRY.gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    });

    println!("Starting metrics server on port {}", port);
    warp::serve(metrics_route).run(([0, 0, 0, 0], port)).await;
}

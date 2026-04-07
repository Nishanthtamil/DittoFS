use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct DittoUpdate {
    pub loro_delta: Vec<u8>,
    pub blobs: std::collections::HashMap<String, Vec<u8>>,
}

use wasm_bindgen::prelude::*;
use loro::LoroDoc;

#[wasm_bindgen]
pub struct DittoWasmClient {
    doc: LoroDoc,
}

#[wasm_bindgen]
impl DittoWasmClient {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        console_error_panic_hook::set_once();
        Self {
            doc: LoroDoc::new(),
        }
    }

    #[wasm_bindgen]
    pub fn get_version(&self) -> String {
        format!("{:?}", self.doc.oplog_vv())
    }

    #[wasm_bindgen]
    pub fn apply_update(&mut self, data: &[u8]) -> Result<(), JsValue> {
        self.doc.import(data).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(())
    }

    #[wasm_bindgen]
    pub fn create_file(&self, name: &str) -> Result<String, JsValue> {
        let tree = self.doc.get_tree("fs_tree");
        let metadata = self.doc.get_map("fs_metadata");
        
        let new_id = tree.create(None).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let map = metadata.insert_container(&new_id.to_string(), loro::LoroMap::new()).map_err(|e| JsValue::from_str(&e.to_string()))?;
        map.insert("name", name).map_err(|e| JsValue::from_str(&e.to_string()))?;
        map.insert("type", "file").map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        Ok(new_id.to_string())
    }

    #[wasm_bindgen]
    pub fn write_file(&self, file_id: &str, content: &[u8]) -> Result<(), JsValue> {
        let blob_map = self.doc.get_map("blobs");
        blob_map.insert(file_id, content).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(())
    }

    #[wasm_bindgen]
    pub fn read_file(&self, file_id: &str) -> Result<Vec<u8>, JsValue> {
        let blob_map = self.doc.get_map("blobs");
        match blob_map.get(file_id) {
            Some(loro::ValueOrContainer::Value(loro::LoroValue::Binary(b))) => Ok(b.to_vec()),
            _ => Err(JsValue::from_str("File not found or not a binary")),
        }
    }

    #[wasm_bindgen]
    pub fn list_files(&self) -> JsValue {
        let tree = self.doc.get_tree("fs_tree");
        let metadata = self.doc.get_map("fs_metadata");
        let children = tree.children(None).unwrap_or_default();
        
        let mut files = Vec::new();
        for child_id in children {
            if let Some(loro::ValueOrContainer::Container(loro::Container::Map(m))) = metadata.get(&child_id.to_string()) {
                if let Some(loro::ValueOrContainer::Value(loro::LoroValue::String(name))) = m.get("name") {
                    files.push(name.as_ref().to_string());
                }
            }
        }
        serde_wasm_bindgen::to_value(&files).unwrap()
    }

    #[wasm_bindgen]
    pub fn export_updates(&self, last_vv: &[u8]) -> Vec<u8> {
        let vv: loro::VersionVector = bincode::deserialize(last_vv).unwrap_or_default();
        self.doc.export(loro::ExportMode::updates(&vv)).unwrap()
    }
}

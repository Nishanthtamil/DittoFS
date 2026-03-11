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
    pub fn export_snapshot(&self) -> Vec<u8> {
        self.doc.export(loro::ExportMode::snapshot()).unwrap()
    }
}

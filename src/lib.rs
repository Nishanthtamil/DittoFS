pub mod store;

#[cfg(not(target_arch = "wasm32"))]
pub mod network;

#[cfg(not(target_arch = "wasm32"))]
pub mod fs;

#[cfg(not(target_arch = "wasm32"))]
pub mod gc;

#[cfg(not(target_arch = "wasm32"))]
pub mod metrics;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

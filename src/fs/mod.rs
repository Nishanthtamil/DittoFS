//! Edge Bridge Module
//! Implements FUSE interface via fuse3 over tokio-uring, mapping FUSE operations to Loro CRDT modifications.

use fuse3::{FileType, Result, Errno};
use fuse3::path::{PathFilesystem, Request};
use fuse3::path::prelude::*;
use std::ffi::OsStr;
use std::sync::Arc;
use crate::store::ContextStore;
use std::time::{SystemTime, UNIX_EPOCH};
use futures::stream::{self, StreamExt, BoxStream};

use std::collections::HashMap;
use parking_lot::RwLock;
use loro::{TreeID, Container, ValueOrContainer};

pub struct DittoFS {
    pub store: Arc<ContextStore>,
    pub inode_to_tree_id: RwLock<HashMap<u64, Option<TreeID>>>,
    pub tree_id_to_inode: RwLock<HashMap<Option<TreeID>, u64>>,
    pub next_inode: RwLock<u64>,
    pub path_cache: RwLock<HashMap<String, Option<TreeID>>>,
    pub local_updates: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
}

impl DittoFS {
    pub fn new(store: Arc<ContextStore>, local_updates: Option<tokio::sync::mpsc::Sender<Vec<u8>>>) -> Self {
        let mut inode_to_tree_id = HashMap::new();
        let mut tree_id_to_inode = HashMap::new();

        inode_to_tree_id.insert(1, None); // Root directory is inode 1 -> None
        tree_id_to_inode.insert(None, 1);

        Self {
            store,
            inode_to_tree_id: RwLock::new(inode_to_tree_id),
            tree_id_to_inode: RwLock::new(tree_id_to_inode),
            next_inode: RwLock::new(2),
            path_cache: RwLock::new(HashMap::new()),
            local_updates,
        }
    }

    pub fn get_tree_id(&self, inode: u64) -> Option<Option<TreeID>> {
        self.inode_to_tree_id.read().get(&inode).cloned()
    }

    pub fn get_inode(&self, tree_id: Option<TreeID>) -> u64 {
        if let Some(&inode) = self.tree_id_to_inode.read().get(&tree_id) {
            return inode;
        }

        let mut next = self.next_inode.write();
        let inode = *next;
        *next += 1;

        self.inode_to_tree_id.write().insert(inode, tree_id.clone());
        self.tree_id_to_inode.write().insert(tree_id, inode);

        inode
    }

    fn sync_local_delta(&self, doc: &loro::LoroDoc, changed_blobs: Vec<String>) {
        let last_vv = self.store.last_known_vv("root");
        let update = doc.export(loro::ExportMode::updates(&last_vv)).unwrap();

        let mut blobs = std::collections::HashMap::new();
        for blob_key in changed_blobs {
            if let Some(data) = self.store.get_blob(&blob_key) {
                blobs.insert(blob_key, data);
            }
        }

        if !update.is_empty() || !blobs.is_empty() {
            let ditto_update = crate::network::update::DittoUpdate {
                loro_delta: update,
                blobs,
            };
            if let Ok(encoded) = bincode::serialize(&ditto_update) {
                if let Some(tx) = &self.local_updates {
                    let _ = tx.try_send(encoded);
                }
            }
            let _ = self.store.save_vv("root", &doc.oplog_vv());
        }
    }

    pub fn resolve_path(&self, path: &OsStr) -> Option<Option<TreeID>> {
        let path_str = path.to_string_lossy().to_string();
        if path_str.is_empty() || path_str == "/" {
            return Some(None);
        }

        if let Some(res) = self.path_cache.read().get(&path_str) {
            return Some(res.clone());
        }

        let components: Vec<&str> = path_str.trim_start_matches('/').split('/').collect();

        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.read();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        let mut current_node: Option<TreeID> = None;

        for part in components {
            if part.is_empty() { continue; }

            let children = tree.children(current_node)?;

            let mut found = None;
            for child_id in children {
                if let Some(child_meta_val) = metadata.get(&child_id.to_string()) {
                    if let ValueOrContainer::Container(Container::Map(m)) = child_meta_val {
                        if let Some(ValueOrContainer::Value(loro::LoroValue::String(name))) = m.get("name") {
                            if name.as_ref() == part {
                                found = Some(child_id);
                                break;
                            }
                        }
                    }
                }
            }
            if found.is_none() {
                return None;
            }
            current_node = found;
        }

        self.path_cache.write().insert(path_str, current_node.clone());
        Some(current_node)
    }

    /// Helper to determine FileType from a type string
    fn file_type_from_str(type_str: &str) -> FileType {
        match type_str {
            "directory" => FileType::Directory,
            "symlink" => FileType::Symlink,
            _ => FileType::RegularFile,
        }
    }
}

impl PathFilesystem for DittoFS {
    type DirEntryStream<'a> = BoxStream<'a, Result<DirectoryEntry>>;
    type DirEntryPlusStream<'a> = BoxStream<'a, Result<DirectoryEntryPlus>>;

    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit { max_write: std::num::NonZeroU32::new(1024 * 1024).unwrap() })
    }

    async fn destroy(&self, _req: Request) {}

    async fn opendir(&self, _req: Request, _path: &OsStr, _flags: u32) -> Result<ReplyOpen> {
        Ok(ReplyOpen { fh: 0, flags: 0 })
    }

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<ReplyEntry> {
        let parent_id = self.resolve_path(parent).ok_or_else(|| Errno::from(libc::ENOENT))?;
        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.read();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        if let Some(children) = tree.children(parent_id) {
            for child_id in children {
                if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&child_id.to_string()) {
                    if let Some(ValueOrContainer::Value(loro::LoroValue::String(n))) = m.get("name") {
                        if n.as_ref() == name.to_string_lossy() {
                            let type_str = if let Some(ValueOrContainer::Value(loro::LoroValue::String(t))) = m.get("type") {
                                t.as_ref().to_string()
                            } else {
                                "file".to_string()
                            };
                            let kind = Self::file_type_from_str(&type_str);
                            let is_dir = type_str == "directory";

                            // For files/symlinks, always measure actual blob size
                            let size = if is_dir {
                                4096
                            } else {
                                let blob_key = format!("blob:{}", child_id);
                                self.store.get_blob(&blob_key)
                                    .map(|b| b.len() as u64)
                                    .unwrap_or(0)
                            };

                            let mode = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(v))) = m.get("mode") {
                                v as u16
                            } else {
                                0o755
                            };

                            let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
                            let ctime_val = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(c))) = m.get("ctime") { c as u64 } else { now_secs };
                            let mtime_val = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(mt))) = m.get("mtime") { mt as u64 } else { now_secs };

                            let ctime_sys = UNIX_EPOCH + std::time::Duration::from_secs(ctime_val);
                            let mtime_sys = UNIX_EPOCH + std::time::Duration::from_secs(mtime_val);

                            return Ok(ReplyEntry {
                                ttl: std::time::Duration::from_secs(1),
                                attr: FileAttr {
                                    size,
                                    blocks: 0,
                                    atime: mtime_sys,
                                    mtime: mtime_sys,
                                    ctime: ctime_sys,
                                    kind,
                                    perm: mode & 0o777,
                                    nlink: if is_dir { 2 } else { 1 },
                                    uid: 1000,
                                    gid: 1000,
                                    rdev: 0,
                                    blksize: 4096,
                                },
                            });
                        }
                    }
                }
            }
        }
        Err(libc::ENOENT.into())
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        let parent_id = self.resolve_path(parent).ok_or_else(|| Errno::from(libc::ENOENT))?;

        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        if let Some(children) = tree.children(parent_id) {
            for child_id in children {
                if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&child_id.to_string()) {
                    if let Some(ValueOrContainer::Value(loro::LoroValue::String(n))) = m.get("name") {
                        if n.as_ref() == name.to_string_lossy() {
                            return Err(libc::EEXIST.into());
                        }
                    }
                }
            }
        }

        let new_id = tree.create(parent_id).map_err(|_| Errno::from(libc::EIO))?;

        let map = metadata.insert_container(&new_id.to_string(), loro::LoroMap::new()).map_err(|_| Errno::from(libc::EIO))?;
        map.insert("name", name.to_string_lossy().to_string()).map_err(|_| Errno::from(libc::EIO))?;
        map.insert("type", "directory").map_err(|_| Errno::from(libc::EIO))?;

        // Ensure u32 maps properly into Loro (since it natively prefers i32/f64 for numbers, cast to i32)
        map.insert("mode", mode as i32).map_err(|_| Errno::from(libc::EIO))?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        map.insert("ctime", now as i64).map_err(|_| Errno::from(libc::EIO))?;
        map.insert("mtime", now as i64).map_err(|_| Errno::from(libc::EIO))?;

        self.get_inode(Some(new_id));

        #[cfg(not(target_arch = "wasm32"))]
        {
            doc.commit();
            let _ = self.store.save_doc("root", &doc);
            self.sync_local_delta(&doc, vec![]);
        }

        self.path_cache.write().clear();

        Ok(ReplyEntry {
            ttl: std::time::Duration::from_secs(1),
            attr: FileAttr {
                size: 4096,
                blocks: 0,
                atime: SystemTime::now().into(),
                mtime: SystemTime::now().into(),
                ctime: SystemTime::now().into(),
                kind: FileType::Directory,
                perm: (mode & 0o777) as u16,
                nlink: 2,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                blksize: 4096,
            },
        })
    }

    async fn create(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _flags: u32,
    ) -> Result<ReplyCreated> {
        let parent_id = self.resolve_path(parent).ok_or_else(|| Errno::from(libc::ENOENT))?;

        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        if let Some(children) = tree.children(parent_id) {
            for child_id in children {
                if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&child_id.to_string()) {
                    if let Some(ValueOrContainer::Value(loro::LoroValue::String(n))) = m.get("name") {
                        if n.as_ref() == name.to_string_lossy() {
                            return Err(libc::EEXIST.into());
                        }
                    }
                }
            }
        }

        let new_id = tree.create(parent_id).map_err(|_| Errno::from(libc::EIO))?;

        let map = metadata.insert_container(&new_id.to_string(), loro::LoroMap::new()).map_err(|_| Errno::from(libc::EIO))?;
        map.insert("name", name.to_string_lossy().to_string()).map_err(|_| Errno::from(libc::EIO))?;
        map.insert("type", "file").map_err(|_| Errno::from(libc::EIO))?;
        map.insert("mode", mode as i32).map_err(|_| Errno::from(libc::EIO))?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        map.insert("ctime", now as i64).map_err(|_| Errno::from(libc::EIO))?;
        map.insert("mtime", now as i64).map_err(|_| Errno::from(libc::EIO))?;

        self.get_inode(Some(new_id));

        #[cfg(not(target_arch = "wasm32"))]
        {
            doc.commit();
            let _ = self.store.save_doc("root", &doc);
            self.sync_local_delta(&doc, vec![]);
        }

        self.path_cache.write().clear();

        Ok(ReplyCreated {
            ttl: std::time::Duration::from_secs(1),
            attr: FileAttr {
                size: 0,
                blocks: 0,
                atime: SystemTime::now().into(),
                mtime: SystemTime::now().into(),
                ctime: SystemTime::now().into(),
                kind: FileType::RegularFile,
                perm: (mode & 0o777) as u16,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                blksize: 4096,
            },
            generation: 0,
            fh: 0,
            flags: 0,
        })
    }

    async fn open(&self, _req: Request, path: &OsStr, _flags: u32) -> Result<ReplyOpen> {
        let tree_id = self.resolve_path(path).flatten().ok_or_else(|| Errno::from(libc::ENOENT))?;

        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.read();
        let metadata = doc.get_map("fs_metadata");
        let meta_map = match metadata.get(&tree_id.to_string()) {
            Some(ValueOrContainer::Container(Container::Map(m))) => m,
            _ => return Err(libc::EIO.into()),
        };

        if let Some(ValueOrContainer::Value(loro::LoroValue::String(t))) = meta_map.get("type") {
            if t.as_ref() == "directory" {
                return Err(libc::EISDIR.into());
            }
        }

        Ok(ReplyOpen { fh: 0, flags: 0 })
    }

    async fn read(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        let path = path.ok_or_else(|| Errno::from(libc::EBADF))?;
        let tree_id = self.resolve_path(path).flatten().ok_or_else(|| Errno::from(libc::ENOENT))?;

        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.read();
        let metadata = doc.get_map("fs_metadata");
        let meta_map = match metadata.get(&tree_id.to_string()) {
            Some(ValueOrContainer::Container(Container::Map(m))) => m,
            _ => return Err(libc::EIO.into()),
        };

        if let Some(ValueOrContainer::Value(loro::LoroValue::String(t))) = meta_map.get("type") {
            if t.as_ref() == "directory" {
                return Err(libc::EISDIR.into());
            }
        }

        let blob_key = format!("blob:{}", tree_id);
        let bytes = self.store.get_blob(&blob_key).unwrap_or_default();

        let end = std::cmp::min((offset + size as u64) as usize, bytes.len());
        let start = std::cmp::min(offset as usize, bytes.len());
        let data = bytes[start..end].to_vec();

        Ok(ReplyData { data: data.into() })
    }

    #[allow(clippy::too_many_arguments)]
    async fn write(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> Result<ReplyWrite> {
        let path = path.ok_or_else(|| Errno::from(libc::EBADF))?;
        let tree_id = self.resolve_path(path).flatten().ok_or_else(|| Errno::from(libc::ENOENT))?;

        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.write();
        let metadata = doc.get_map("fs_metadata");
        let meta_map = match metadata.get(&tree_id.to_string()) {
            Some(ValueOrContainer::Container(Container::Map(m))) => m,
            _ => return Err(libc::EIO.into()),
        };

        let blob_key = format!("blob:{}", tree_id);
        let mut buf = self.store.get_blob(&blob_key).unwrap_or_default();
        let end = (offset as usize) + data.len();
        if buf.len() < end {
            buf.resize(end, 0);
        }
        buf[offset as usize..end].copy_from_slice(data);
        let len = buf.len();
        self.store.set_blob(&blob_key, &buf).map_err(|_| Errno::from(libc::EIO))?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        meta_map.insert("mtime", now as i64).map_err(|_| Errno::from(libc::EIO))?;

        // Update size logically
        meta_map.insert("size", len as i64).map_err(|_| Errno::from(libc::EIO))?;

        #[cfg(not(target_arch = "wasm32"))]
        {
            doc.commit();
            let _ = self.store.save_doc("root", &doc);
            self.sync_local_delta(&doc, vec![blob_key]);
        }

        Ok(ReplyWrite {
            written: data.len() as u32,
        })
    }

    async fn rename(
        &self,
        _req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<()> {
        let origin_parent_id = self.resolve_path(origin_parent).ok_or_else(|| Errno::from(libc::ENOENT))?;
        let target_parent_id = self.resolve_path(parent).ok_or_else(|| Errno::from(libc::ENOENT))?;

        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        let mut target_node: Option<TreeID> = None;
        if let Some(children) = tree.children(origin_parent_id) {
            for child_id in children {
                if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&child_id.to_string()) {
                    if let Some(ValueOrContainer::Value(loro::LoroValue::String(n))) = m.get("name") {
                        if n.as_ref() == origin_name.to_string_lossy() {
                            target_node = Some(child_id);
                            break;
                        }
                    }
                }
            }
        }

        let node_to_move = target_node.ok_or_else(|| Errno::from(libc::ENOENT))?;

        if let Some(children) = tree.children(target_parent_id) {
            for child_id in children {
                if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&child_id.to_string()) {
                    if let Some(ValueOrContainer::Value(loro::LoroValue::String(n))) = m.get("name") {
                        if n.as_ref() == name.to_string_lossy() {
                            // Clean up file if overriding
                            tree.delete(child_id).map_err(|_| Errno::from(libc::EIO))?;
                            break;
                        }
                    }
                }
            }
        }

        tree.mov(node_to_move, target_parent_id).map_err(|_| Errno::from(libc::EIO))?;

        if origin_name != name {
            if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&node_to_move.to_string()) {
                m.insert("name", name.to_string_lossy().to_string()).map_err(|_| Errno::from(libc::EIO))?;
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            doc.commit();
            let _ = self.store.save_doc("root", &doc);
            self.sync_local_delta(&doc, vec![]);
        }

        self.path_cache.write().clear();

        Ok(())
    }

    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        let parent_id = self.resolve_path(parent).ok_or_else(|| Errno::from(libc::ENOENT))?;
        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        if let Some(children) = tree.children(parent_id) {
            for child_id in children {
                if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&child_id.to_string()) {
                    if let Some(ValueOrContainer::Value(loro::LoroValue::String(n))) = m.get("name") {
                        if n.as_ref() == name.to_string_lossy() {
                            if let Some(ValueOrContainer::Value(loro::LoroValue::String(t))) = m.get("type") {
                                if t.as_ref() == "directory" {
                                    return Err(libc::EISDIR.into());
                                }
                            }
                            tree.delete(child_id).map_err(|_| Errno::from(libc::EIO))?;
                            #[cfg(not(target_arch = "wasm32"))]
                            {
                                doc.commit();
                                let _ = self.store.save_doc("root", &doc);
                                self.sync_local_delta(&doc, vec![]);
                            }
                            self.path_cache.write().clear();
                            return Ok(());
                        }
                    }
                }
            }
        }
        Err(libc::ENOENT.into())
    }

    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        let parent_id = self.resolve_path(parent).ok_or_else(|| Errno::from(libc::ENOENT))?;
        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        if let Some(children) = tree.children(parent_id) {
            for child_id in children {
                if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&child_id.to_string()) {
                    if let Some(ValueOrContainer::Value(loro::LoroValue::String(n))) = m.get("name") {
                        if n.as_ref() == name.to_string_lossy() {
                            if let Some(ValueOrContainer::Value(loro::LoroValue::String(t))) = m.get("type") {
                                if t.as_ref() != "directory" {
                                    return Err(libc::ENOTDIR.into());
                                }
                            }

                            // Check if directory is empty
                            if let Some(subchildren) = tree.children(Some(child_id)) {
                                if !subchildren.is_empty() {
                                    return Err(libc::ENOTEMPTY.into());
                                }
                            }

                            tree.delete(child_id).map_err(|_| Errno::from(libc::EIO))?;
                            #[cfg(not(target_arch = "wasm32"))]
                            {
                                doc.commit();
                                let _ = self.store.save_doc("root", &doc);
                                self.sync_local_delta(&doc, vec![]);
                            }
                            self.path_cache.write().clear();
                            return Ok(());
                        }
                    }
                }
            }
        }
        Err(libc::ENOENT.into())
    }

    async fn setattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        set_attr: fuse3::SetAttr,
    ) -> Result<ReplyAttr> {
        let path = path.ok_or_else(|| Errno::from(libc::EBADF))?;
        let node_id = self.resolve_path(path).ok_or_else(|| Errno::from(libc::ENOENT))?;

        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.write();
        let metadata = doc.get_map("fs_metadata");

        let meta_map = if let Some(id) = node_id {
            match metadata.get(&id.to_string()) {
                Some(ValueOrContainer::Container(Container::Map(m))) => m,
                _ => return Err(libc::ENOENT.into()),
            }
        } else {
            // Root directory — setattr on root is a no-op success
            let now = SystemTime::now();
            return Ok(ReplyAttr {
                ttl: std::time::Duration::from_secs(1),
                attr: FileAttr {
                    size: 4096,
                    blocks: 0,
                    atime: now.into(),
                    mtime: now.into(),
                    ctime: now.into(),
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: 1000,
                    gid: 1000,
                    rdev: 0,
                    blksize: 4096,
                },
            });
        };

        let mut changed_blobs = vec![];
        // Handle truncation (most important: this is what echo uses before writing)
        if let Some(new_size) = set_attr.size {
            let blob_key = format!("blob:{}", node_id.unwrap());
            let mut buf = self.store.get_blob(&blob_key).unwrap_or_default();
            buf.resize(new_size as usize, 0);
            self.store.set_blob(&blob_key, &buf).map_err(|_| Errno::from(libc::EIO))?;

            meta_map.insert("size", new_size as i64).map_err(|_| Errno::from(libc::EIO))?;
            changed_blobs.push(blob_key);
        }

        if let Some(mode) = set_attr.mode {
            meta_map.insert("mode", mode as i32).map_err(|_| Errno::from(libc::EIO))?;
        }

        let now = SystemTime::now();
        let ts = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        meta_map.insert("mtime", ts as i64).map_err(|_| Errno::from(libc::EIO))?;

        #[cfg(not(target_arch = "wasm32"))]
        {
            doc.commit();
            let _ = self.store.save_doc("root", &doc);
            self.sync_local_delta(&doc, changed_blobs);
        }

        // Re-read the actual values for the reply
        let type_str = if let Some(ValueOrContainer::Value(loro::LoroValue::String(t))) = meta_map.get("type") {
            t.as_ref().to_string()
        } else { "file".to_string() };
        let is_dir = type_str == "directory";
        let kind = Self::file_type_from_str(&type_str);
        let mode_val = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(v))) = meta_map.get("mode") { v as u16 } else { 0o755 };

        // For files, always measure the actual blob size
        let size_val = if is_dir {
            4096
        } else {
            let blob_key = format!("blob:{}", node_id.unwrap());
            self.store.get_blob(&blob_key)
                .map(|b| b.len() as u64)
                .unwrap_or(0)
        };

        let ctime_val = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(c))) = meta_map.get("ctime") { c as u64 } else { ts };
        let mtime_val = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(m))) = meta_map.get("mtime") { m as u64 } else { ts };

        let ctime_sys = UNIX_EPOCH + std::time::Duration::from_secs(ctime_val);
        let mtime_sys = UNIX_EPOCH + std::time::Duration::from_secs(mtime_val);

        Ok(ReplyAttr {
            ttl: std::time::Duration::from_secs(1),
            attr: FileAttr {
                size: size_val,
                blocks: 0,
                atime: mtime_sys,
                mtime: mtime_sys,
                ctime: ctime_sys,
                kind,
                perm: mode_val & 0o777,
                nlink: if is_dir { 2 } else { 1 },
                uid: 1000,
                gid: 1000,
                rdev: 0,
                blksize: 4096,
            },
        })
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        let path = path.unwrap_or_else(|| OsStr::new("/"));
        let node_id = self.resolve_path(path).ok_or_else(|| Errno::from(libc::ENOENT))?;

        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.read();
        let metadata = doc.get_map("fs_metadata");

        let mut is_dir = true;
        let mut size: u64 = 4096;
        let mut mode: u16 = 0o755;
        let now = SystemTime::now();
        let mut ctime = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let mut mtime = ctime;
        let mut kind = FileType::Directory;

        if let Some(id) = node_id {
            if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&id.to_string()) {
                let type_str = if let Some(ValueOrContainer::Value(loro::LoroValue::String(t))) = m.get("type") {
                    t.as_ref().to_string()
                } else {
                    "file".to_string()
                };
                is_dir = type_str == "directory";
                kind = Self::file_type_from_str(&type_str);

                mode = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(v))) = m.get("mode") { v as u16 } else { 0o755 };

                // For files, always measure the actual blob size — never trust metadata
                size = if is_dir {
                    4096
                } else {
                    let blob_key = format!("blob:{}", id);
                    self.store.get_blob(&blob_key)
                        .map(|b| b.len() as u64)
                        .unwrap_or(0)
                };

                ctime = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(c))) = m.get("ctime") { c as u64 } else { ctime };
                mtime = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(mt))) = m.get("mtime") { mt as u64 } else { mtime };
            } else {
                return Err(libc::ENOENT.into());
            }
        }

        let ctime_dur = UNIX_EPOCH + std::time::Duration::from_secs(ctime);
        let mtime_dur = UNIX_EPOCH + std::time::Duration::from_secs(mtime);

        let attr = FileAttr {
            size,
            blocks: 0,
            atime: mtime_dur,
            mtime: mtime_dur,
            ctime: ctime_dur,
            kind,
            perm: mode & 0o777,
            nlink: if is_dir { 2 } else { 1 },
            uid: 1000,
            gid: 1000,
            rdev: 0,
            blksize: 4096,
        };

        Ok(ReplyAttr {
            attr,
            ttl: std::time::Duration::from_secs(1),
        })
    }

    // Fix 3: readdir now returns actual children from the Loro tree
    async fn readdir<'a>(
        &'a self,
        _req: Request,
        path: &'a OsStr,
        _fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let mut entries = vec![];

        if offset < 1 {
            entries.push(Ok(DirectoryEntry {
                kind: FileType::Directory,
                name: ".".into(),
                offset: 1,
            }));
        }
        if offset < 2 {
            entries.push(Ok(DirectoryEntry {
                kind: FileType::Directory,
                name: "..".into(),
                offset: 2,
            }));
        }

        let parent_id = self.resolve_path(path).unwrap_or(None);
        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.read();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        if let Some(children) = tree.children(parent_id) {
            let mut current_offset: i64 = 3;
            for child_id in children {
                if current_offset > offset {
                    if let Some(ValueOrContainer::Container(Container::Map(m))) =
                        metadata.get(&child_id.to_string())
                    {
                        if let Some(ValueOrContainer::Value(loro::LoroValue::String(n))) =
                            m.get("name")
                        {
                            let type_str = if let Some(ValueOrContainer::Value(
                                loro::LoroValue::String(t),
                            )) = m.get("type")
                            {
                                t.as_ref().to_string()
                            } else {
                                "file".to_string()
                            };
                            let kind = Self::file_type_from_str(&type_str);
                            entries.push(Ok(DirectoryEntry {
                                kind,
                                name: n.as_ref().into(),
                                offset: current_offset,
                            }));
                        }
                    }
                }
                current_offset += 1;
            }
        }

        let stream = stream::iter(entries).boxed();
        Ok(ReplyDirectory { entries: stream })
    }

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        path: &'a OsStr,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        let mut entries = vec![];
        let now = SystemTime::now();
        let attr = FileAttr {
            size: 4096,
            blocks: 0,
            atime: now.into(),
            mtime: now.into(),
            ctime: now.into(),
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            blksize: 4096,
        };

        if offset < 1 {
            entries.push(Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: ".".into(),
                offset: 1,
                attr,
                entry_ttl: std::time::Duration::from_secs(1),
                attr_ttl: std::time::Duration::from_secs(1),
            }));
        }
        if offset < 2 {
            entries.push(Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: "..".into(),
                offset: 2,
                attr,
                entry_ttl: std::time::Duration::from_secs(1),
                attr_ttl: std::time::Duration::from_secs(1),
            }));
        }

        // Fetch dynamic children from Loro
        let parent_id = self.resolve_path(path).unwrap_or(None);
        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.read();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        if let Some(children) = tree.children(parent_id) {
            let mut current_offset = 3;
            for child_id in children {
                if current_offset > offset {
                    if let Some(ValueOrContainer::Container(Container::Map(m))) = metadata.get(&child_id.to_string()) {
                        if let Some(ValueOrContainer::Value(loro::LoroValue::String(n))) = m.get("name") {
                            let type_str = if let Some(ValueOrContainer::Value(loro::LoroValue::String(t))) = m.get("type") {
                                t.as_ref().to_string()
                            } else {
                                "file".to_string()
                            };
                            let is_dir = type_str == "directory";
                            let kind = Self::file_type_from_str(&type_str);
                            let mode = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(v))) = m.get("mode") { v as u16 } else { 0o755 };

                            // For files, always measure actual blob size
                            let size = if is_dir {
                                4096
                            } else {
                                let blob_key = format!("blob:{}", child_id);
                                self.store.get_blob(&blob_key)
                                    .map(|b| b.len() as u64)
                                    .unwrap_or(0)
                            };

                            let ct = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(c))) = m.get("ctime") { c as u64 } else { now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() };
                            let mt = if let Some(ValueOrContainer::Value(loro::LoroValue::I64(m))) = m.get("mtime") { m as u64 } else { now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() };

                            let itime = UNIX_EPOCH + std::time::Duration::from_secs(ct);
                            let ytime = UNIX_EPOCH + std::time::Duration::from_secs(mt);

                            entries.push(Ok(DirectoryEntryPlus {
                                kind,
                                name: n.as_ref().into(),
                                offset: current_offset as i64,
                                attr: FileAttr {
                                    size,
                                    blocks: 0,
                                    atime: ytime,
                                    mtime: ytime,
                                    ctime: itime,
                                    kind,
                                    perm: mode & 0o777,
                                    nlink: if is_dir { 2 } else { 1 },
                                    uid: 1000,
                                    gid: 1000,
                                    rdev: 0,
                                    blksize: 4096,
                                },
                                entry_ttl: std::time::Duration::from_secs(1),
                                attr_ttl: std::time::Duration::from_secs(1),
                            }));
                        }
                    }
                }
                current_offset += 1;
            }
        }

        let stream = stream::iter(entries).boxed();
        Ok(ReplyDirectoryPlus { entries: stream })
    }

    // A1: fsync — no-op, Sled flushes synchronously on every set_blob call
    async fn fsync(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        _datasync: bool,
    ) -> Result<()> {
        let _ = path;
        Ok(())
    }

    async fn flush(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        _lock_owner: u64,
    ) -> Result<()> {
        let _ = path;
        Ok(())
    }

    // A2: release and releasedir — no-op stubs
    async fn release(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        let _ = path;
        Ok(())
    }

    async fn releasedir(
        &self,
        _req: Request,
        _path: &OsStr,
        _fh: u64,
        _flags: u32,
    ) -> Result<()> {
        Ok(())
    }

    // A4: symlink — store symlink target as blob, type="symlink"
    async fn symlink(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        link_path: &OsStr,
    ) -> Result<ReplyEntry> {
        let parent_id = self.resolve_path(parent)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;

        let doc_lock = self.store.get_fs_root();
        let doc = doc_lock.write();
        let tree = doc.get_tree("fs_tree");
        let metadata = doc.get_map("fs_metadata");

        let new_id = tree.create(parent_id)
            .map_err(|_| Errno::from(libc::EIO))?;
        let map = metadata
            .insert_container(&new_id.to_string(), loro::LoroMap::new())
            .map_err(|_| Errno::from(libc::EIO))?;
        map.insert("name", name.to_string_lossy().to_string())
            .map_err(|_| Errno::from(libc::EIO))?;
        map.insert("type", "symlink")
            .map_err(|_| Errno::from(libc::EIO))?;
        map.insert("mode", 0o777i32)
            .map_err(|_| Errno::from(libc::EIO))?;

        // Store symlink target as blob
        let blob_key = format!("blob:{}", new_id);
        let target = link_path.to_string_lossy().into_owned().into_bytes();
        let target_len = target.len() as u64;
        self.store.set_blob(&blob_key, &target)
            .map_err(|_| Errno::from(libc::EIO))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        map.insert("ctime", now as i64).map_err(|_| Errno::from(libc::EIO))?;
        map.insert("mtime", now as i64).map_err(|_| Errno::from(libc::EIO))?;

        self.get_inode(Some(new_id));

        #[cfg(not(target_arch = "wasm32"))]
        {
            doc.commit();
            let _ = self.store.save_doc("root", &doc);
            self.sync_local_delta(&doc, vec![blob_key]);
        }

        self.path_cache.write().clear();

        Ok(ReplyEntry {
            ttl: std::time::Duration::from_secs(1),
            attr: FileAttr {
                size: target_len,
                blocks: 0,
                atime: SystemTime::now().into(),
                mtime: SystemTime::now().into(),
                ctime: SystemTime::now().into(),
                kind: FileType::Symlink,
                perm: 0o777,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                rdev: 0,
                blksize: 4096,
            },
        })
    }

    // A4: readlink — read symlink target from blob
    async fn readlink(&self, _req: Request, path: &OsStr) -> Result<ReplyData> {
        let tree_id = self.resolve_path(path)
            .flatten()
            .ok_or_else(|| Errno::from(libc::ENOENT))?;

        let blob_key = format!("blob:{}", tree_id);
        let target = self.store
            .get_blob(&blob_key)
            .ok_or_else(|| Errno::from(libc::EINVAL))?;

        Ok(ReplyData { data: target.into() })
    }

    // A5: link — DittoFS does not support hard links, return ENOSYS
    async fn link(
        &self,
        _req: Request,
        path: &OsStr,
        new_parent: &OsStr,
        new_name: &OsStr,
    ) -> Result<ReplyEntry> {
        let _ = (path, new_parent, new_name);
        Err(Errno::from(libc::ENOSYS).into())
    }

    // A7: access — if the path resolves, we grant access (single-user)
    async fn access(&self, _req: Request, path: &OsStr, _mask: u32) -> Result<()> {
        self.resolve_path(path)
            .ok_or_else(|| Errno::from(libc::ENOENT))?;
        Ok(())
    }
}

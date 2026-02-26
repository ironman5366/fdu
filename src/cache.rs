use crate::tree::FileTree;
use anyhow::Result;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

fn cache_dir() -> Result<PathBuf> {
    let base = dirs::cache_dir()
        .ok_or_else(|| anyhow::anyhow!("Could not determine cache directory"))?;
    Ok(base.join("fdu"))
}

fn cache_path_for(scanned_path: &Path) -> Result<PathBuf> {
    let canonical = scanned_path.canonicalize()?;
    let mut hasher = DefaultHasher::new();
    canonical.hash(&mut hasher);
    let hash = hasher.finish();
    Ok(cache_dir()?.join(format!("{:016x}.bin", hash)))
}

pub fn load_cache(scanned_path: &Path) -> Result<Option<FileTree>> {
    let path = cache_path_for(scanned_path)?;
    if !path.exists() {
        return Ok(None);
    }
    let data = fs::read(&path)?;
    let tree: FileTree = match bincode::deserialize(&data) {
        Ok(t) => t,
        Err(_) => {
            // Corrupt or incompatible cache, remove it
            let _ = fs::remove_file(&path);
            return Ok(None);
        }
    };

    // Shallow staleness check: compare root dir mtime with scan_time
    if let Ok(metadata) = fs::metadata(scanned_path) {
        if let Ok(mtime) = metadata.modified() {
            if mtime > tree.scan_time {
                return Ok(None); // Cache is stale
            }
        }
    }

    Ok(Some(tree))
}

pub fn save_cache(tree: &FileTree) -> Result<()> {
    let dir = cache_dir()?;
    fs::create_dir_all(&dir)?;
    let path = cache_path_for(&tree.root_path)?;
    let data = bincode::serialize(tree)?;
    fs::write(&path, &data)?;
    Ok(())
}

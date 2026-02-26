use crate::tree::{FileNode, FileTree};
use jwalk::WalkDir;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum ScanMessage {
    Progress {
        tree: FileTree,
        current_path: String,
    },
    Complete(FileTree),
    Error(String),
}

pub struct ScanOptions {
    pub path: PathBuf,
    pub same_filesystem: bool,
}

pub fn start_scan(
    options: ScanOptions,
    tx: mpsc::UnboundedSender<ScanMessage>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        scan_directory(&options, &tx);
    })
}

fn scan_directory(options: &ScanOptions, tx: &mpsc::UnboundedSender<ScanMessage>) {
    let root_path = match options.path.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            let _ = tx.send(ScanMessage::Error(format!(
                "Cannot access {}: {}",
                options.path.display(),
                e
            )));
            return;
        }
    };

    #[cfg(unix)]
    let root_dev = if options.same_filesystem {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(&root_path).ok().map(|m| m.dev())
    } else {
        None
    };

    let mut files_count: u64 = 0;
    let mut dirs_count: u64 = 0;
    let mut errors_count: u64 = 0;
    let mut entry_count: u64 = 0;

    // Map from directory path to its accumulated children
    let mut dir_children: HashMap<PathBuf, Vec<FileNode>> = HashMap::new();
    // Track directory metadata (mtime) separately
    let mut dir_mtimes: HashMap<PathBuf, Option<SystemTime>> = HashMap::new();

    // Initialize root
    let root_mtime = std::fs::metadata(&root_path)
        .ok()
        .and_then(|m| m.modified().ok());
    dir_children.insert(root_path.clone(), Vec::new());
    dir_mtimes.insert(root_path.clone(), root_mtime);

    let walker = WalkDir::new(&root_path)
        .skip_hidden(false)
        .follow_links(false);

    let mut current_path_str = String::new();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => {
                errors_count += 1;
                continue;
            }
        };

        let path = entry.path();

        // Skip the root entry itself
        if path == root_path {
            continue;
        }

        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(_) => {
                errors_count += 1;
                continue;
            }
        };

        // Same filesystem check
        #[cfg(unix)]
        if let Some(dev) = root_dev {
            use std::os::unix::fs::MetadataExt;
            if metadata.dev() != dev {
                continue;
            }
        }

        let parent = match path.parent() {
            Some(p) => p.to_path_buf(),
            None => continue,
        };

        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let mtime = metadata.modified().ok();

        if metadata.is_dir() {
            dirs_count += 1;
            dir_children.insert(path.clone(), Vec::new());
            dir_mtimes.insert(path.clone(), mtime);
            dir_children
                .entry(parent)
                .or_default()
                .push(FileNode::new_dir(name, mtime));
        } else {
            files_count += 1;
            let size = metadata.len();
            dir_children
                .entry(parent)
                .or_default()
                .push(FileNode::new_file(name, size, mtime));
        }

        entry_count += 1;
        if entry_count % 500 == 0 {
            current_path_str = path.to_string_lossy().to_string();
        }
        if entry_count % 2000 == 0 {
            // Build a snapshot of the top-level children for incremental display
            let snapshot = build_snapshot(
                &root_path,
                &dir_children,
                &dir_mtimes,
                files_count,
                dirs_count,
                errors_count,
            );
            let _ = tx.send(ScanMessage::Progress {
                tree: snapshot,
                current_path: current_path_str.clone(),
            });
        }
    }

    // Assemble the final tree bottom-up
    let root = assemble_tree(&root_path, &mut dir_children, &dir_mtimes);

    let tree = FileTree {
        root_path: root_path.clone(),
        root,
        total_files: files_count,
        total_dirs: dirs_count,
        total_errors: errors_count,
        scan_time: SystemTime::now(),
        complete: true,
    };

    let _ = tx.send(ScanMessage::Complete(tree));
}

/// Build a quick snapshot of top-level entries for incremental display.
/// Only assembles direct children of root (1 level deep), summing known sizes.
fn build_snapshot(
    root_path: &Path,
    dir_children: &HashMap<PathBuf, Vec<FileNode>>,
    dir_mtimes: &HashMap<PathBuf, Option<SystemTime>>,
    total_files: u64,
    total_dirs: u64,
    total_errors: u64,
) -> FileTree {
    let root_entries = match dir_children.get(root_path) {
        Some(entries) => entries,
        None => {
            return FileTree {
                root_path: root_path.to_path_buf(),
                root: FileNode::new_dir(
                    root_path
                        .file_name()
                        .unwrap_or(root_path.as_os_str())
                        .to_string_lossy()
                        .to_string(),
                    dir_mtimes.get(root_path).copied().flatten(),
                ),
                total_files,
                total_dirs,
                total_errors,
                scan_time: SystemTime::now(),
                complete: false,
            };
        }
    };

    let mut children: Vec<FileNode> = root_entries
        .iter()
        .map(|entry| {
            if entry.is_dir {
                // Sum up all known files under this directory
                let child_path = root_path.join(&entry.name);
                let size = sum_known_sizes(&child_path, dir_children);
                let child_count = dir_children
                    .get(&child_path)
                    .map(|c| c.len())
                    .unwrap_or(0);
                FileNode {
                    name: entry.name.clone(),
                    size,
                    is_dir: true,
                    child_count,
                    mtime: entry.mtime,
                    children: Vec::new(), // Don't recurse for snapshot
                    error_count: 0,
                }
            } else {
                entry.clone()
            }
        })
        .collect();

    children.sort_by(|a, b| b.size.cmp(&a.size));

    let total_size: u64 = children.iter().map(|c| c.size).sum();
    let child_count = children.len();
    let root_name = root_path
        .file_name()
        .unwrap_or(root_path.as_os_str())
        .to_string_lossy()
        .to_string();

    let root = FileNode {
        name: root_name,
        size: total_size,
        is_dir: true,
        child_count,
        mtime: dir_mtimes.get(root_path).copied().flatten(),
        children,
        error_count: 0,
    };

    FileTree {
        root_path: root_path.to_path_buf(),
        root,
        total_files,
        total_dirs,
        total_errors,
        scan_time: SystemTime::now(),
        complete: false,
    }
}

/// Recursively sum sizes of all known file entries under a directory path.
fn sum_known_sizes(path: &Path, dir_children: &HashMap<PathBuf, Vec<FileNode>>) -> u64 {
    let entries = match dir_children.get(path) {
        Some(e) => e,
        None => return 0,
    };

    let mut total: u64 = 0;
    for entry in entries {
        if entry.is_dir {
            total += sum_known_sizes(&path.join(&entry.name), dir_children);
        } else {
            total += entry.size;
        }
    }
    total
}

fn assemble_tree(
    path: &Path,
    dir_children: &mut HashMap<PathBuf, Vec<FileNode>>,
    dir_mtimes: &HashMap<PathBuf, Option<SystemTime>>,
) -> FileNode {
    let children = dir_children.remove(path).unwrap_or_default();
    let mtime = dir_mtimes.get(path).copied().flatten();
    let name = path
        .file_name()
        .unwrap_or(path.as_os_str())
        .to_string_lossy()
        .to_string();

    let mut assembled_children = Vec::new();

    for child in children {
        if child.is_dir {
            let child_path = path.join(&child.name);
            let assembled = assemble_tree(&child_path, dir_children, dir_mtimes);
            assembled_children.push(assembled);
        } else {
            assembled_children.push(child);
        }
    }

    let size: u64 = assembled_children.iter().map(|c| c.size).sum();
    let child_count = assembled_children.len();
    let error_count: u64 = assembled_children.iter().map(|c| c.error_count).sum();

    let mut node = FileNode {
        name,
        size,
        is_dir: true,
        child_count,
        mtime,
        children: assembled_children,
        error_count,
    };
    node.sort_recursive_by_size();
    node
}

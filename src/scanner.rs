use crate::tree::{FileNode, FileTree};
use jwalk::WalkDir;
use rayon::prelude::*;
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

/// Number of entries to collect before doing a parallel stat batch.
/// Larger = more stat parallelism, smaller = more responsive progress.
const STAT_BATCH_SIZE: usize = 8192;
/// Number of threads for parallel stat calls.
/// More threads help on high-performance network filesystems like WekaFS.
const STAT_THREADS: usize = 128;

pub fn start_scan(
    options: ScanOptions,
    tx: mpsc::UnboundedSender<ScanMessage>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        scan_directory(&options, &tx);
    })
}

/// Entry collected from jwalk (readdir only, no stat yet).
struct PendingEntry {
    path: PathBuf,
    parent: PathBuf,
    name: String,
    is_dir: bool,
}

/// Result of a parallel stat call.
struct StatResult {
    size: u64,
    mtime: Option<SystemTime>,
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
    let root_dev: Option<u64> = if options.same_filesystem {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(&root_path).ok().map(|m| m.dev())
    } else {
        None
    };

    #[cfg(not(unix))]
    let _root_dev: Option<u64> = None;

    let mut files_count: u64 = 0;
    let mut dirs_count: u64 = 0;
    let mut errors_count: u64 = 0;
    let mut entry_count: u64 = 0;

    let mut dir_children: HashMap<PathBuf, Vec<FileNode>> = HashMap::new();
    let mut dir_mtimes: HashMap<PathBuf, Option<SystemTime>> = HashMap::new();
    // Incremental size tracking per top-level directory (avoids O(n²) recomputation)
    let mut top_level_sizes: HashMap<String, u64> = HashMap::new();

    let root_mtime = std::fs::metadata(&root_path)
        .ok()
        .and_then(|m| m.modified().ok());
    dir_children.insert(root_path.clone(), Vec::new());
    dir_mtimes.insert(root_path.clone(), root_mtime);

    // Build a dedicated thread pool for stat calls. On network filesystems,
    // too many concurrent stat calls cause contention — 32 threads is a
    // good balance between parallelism and network friendliness.
    let stat_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(STAT_THREADS)
        .build()
        .unwrap();

    // Use regular WalkDir (no process_read_dir) so entries stream immediately
    // after readdir completes. This gives responsive progress updates even
    // for directories with millions of files.
    let walker = WalkDir::new(&root_path)
        .skip_hidden(false)
        .follow_links(false);

    let mut pending: Vec<PendingEntry> = Vec::with_capacity(STAT_BATCH_SIZE);
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
        if path == root_path {
            continue;
        }

        let is_dir = entry.file_type().is_dir();

        let parent = match path.parent() {
            Some(p) => p.to_path_buf(),
            None => continue,
        };

        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        pending.push(PendingEntry {
            path,
            parent,
            name,
            is_dir,
        });

        if pending.len() >= STAT_BATCH_SIZE {
            process_batch(
                &stat_pool,
                &pending,
                &root_path,
                #[cfg(unix)]
                root_dev,
                &mut dir_children,
                &mut dir_mtimes,
                &mut top_level_sizes,
                &mut files_count,
                &mut dirs_count,
                &mut errors_count,
                &mut entry_count,
                &mut current_path_str,
            );
            pending.clear();

            // Send progress after each batch
            let snapshot = build_snapshot(
                &root_path,
                &dir_children,
                &dir_mtimes,
                &top_level_sizes,
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

    // Process remaining entries
    if !pending.is_empty() {
        process_batch(
            &stat_pool,
            &pending,
            &root_path,
            #[cfg(unix)]
            root_dev,
            &mut dir_children,
            &mut dir_mtimes,
            &mut top_level_sizes,
            &mut files_count,
            &mut dirs_count,
            &mut errors_count,
            &mut entry_count,
            &mut current_path_str,
        );
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

/// Stat a batch of entries in parallel, then process results sequentially.
#[allow(clippy::too_many_arguments)]
fn process_batch(
    pool: &rayon::ThreadPool,
    pending: &[PendingEntry],
    root_path: &Path,
    #[cfg(unix)] root_dev: Option<u64>,
    dir_children: &mut HashMap<PathBuf, Vec<FileNode>>,
    dir_mtimes: &mut HashMap<PathBuf, Option<SystemTime>>,
    top_level_sizes: &mut HashMap<String, u64>,
    files_count: &mut u64,
    dirs_count: &mut u64,
    errors_count: &mut u64,
    entry_count: &mut u64,
    current_path_str: &mut String,
) {
    // Parallel stat calls using dedicated thread pool
    let stat_results: Vec<Option<StatResult>> = pool.install(|| {
        pending
            .par_iter()
            .map(|entry| {
                let meta = std::fs::symlink_metadata(&entry.path).ok()?;

                #[cfg(unix)]
                if let Some(rd) = root_dev {
                    use std::os::unix::fs::MetadataExt;
                    if meta.dev() != rd {
                        return None;
                    }
                }

                let size = if entry.is_dir { 0 } else { meta.len() };
                let mtime = meta.modified().ok();

                Some(StatResult { size, mtime })
            })
            .collect()
    });

    // Process results sequentially (update HashMap, counters)
    for (entry, stat) in pending.iter().zip(stat_results.iter()) {
        let (size, mtime) = match stat {
            Some(s) => (s.size, s.mtime),
            None => {
                *errors_count += 1;
                continue;
            }
        };

        if entry.is_dir {
            *dirs_count += 1;
            dir_children.insert(entry.path.clone(), Vec::new());
            dir_mtimes.insert(entry.path.clone(), mtime);
            dir_children
                .entry(entry.parent.clone())
                .or_default()
                .push(FileNode::new_dir(entry.name.clone(), mtime));
        } else {
            *files_count += 1;
            dir_children
                .entry(entry.parent.clone())
                .or_default()
                .push(FileNode::new_file(entry.name.clone(), size, mtime));

            if let Some(top_name) = get_top_level_name(&entry.path, root_path) {
                *top_level_sizes.entry(top_name).or_insert(0) += size;
            }
        }

        *entry_count += 1;
        if *entry_count % 500 == 0 {
            *current_path_str = entry.path.to_string_lossy().to_string();
        }
    }
}

/// Get the name of the top-level directory that a path falls under.
fn get_top_level_name(path: &Path, root_path: &Path) -> Option<String> {
    let relative = path.strip_prefix(root_path).ok()?;
    let first = relative.components().next()?;
    Some(first.as_os_str().to_string_lossy().to_string())
}

/// Build a quick snapshot of top-level entries for incremental display.
/// Uses pre-computed top_level_sizes for O(1) lookups.
fn build_snapshot(
    root_path: &Path,
    dir_children: &HashMap<PathBuf, Vec<FileNode>>,
    dir_mtimes: &HashMap<PathBuf, Option<SystemTime>>,
    top_level_sizes: &HashMap<String, u64>,
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
                let size = top_level_sizes.get(&entry.name).copied().unwrap_or(0);
                let child_path = root_path.join(&entry.name);
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
                    children: Vec::new(),
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

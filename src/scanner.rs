use crate::tree::{FileNode, FileTree};
use rayon::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};
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
const STAT_BATCH_SIZE: usize = 8192;
/// Number of threads for parallel stat calls.
const STAT_THREADS: usize = 128;
/// How often to send progress updates (in entries from readdir).
const PROGRESS_INTERVAL: u64 = 2000;
/// Max file children per directory in progress snapshots (dirs always included).
/// Keeps snapshot size bounded for directories with millions of files.
const MAX_SNAPSHOT_FILES_PER_DIR: usize = 500;
/// Minimum interval between full tree snapshots.
const SNAPSHOT_INTERVAL_MS: u128 = 500;

pub fn start_scan(
    options: ScanOptions,
    tx: mpsc::UnboundedSender<ScanMessage>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        scan_directory(&options, &tx);
    })
}

/// Entry collected from readdir (no stat yet).
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

    let mut dir_children: HashMap<PathBuf, Vec<FileNode>> = HashMap::new();
    let mut dir_mtimes: HashMap<PathBuf, Option<SystemTime>> = HashMap::new();
    // Cumulative size for every directory (propagated from stat'd files)
    let mut dir_sizes: HashMap<PathBuf, u64> = HashMap::new();

    let root_mtime = std::fs::metadata(&root_path)
        .ok()
        .and_then(|m| m.modified().ok());
    dir_children.insert(root_path.clone(), Vec::new());
    dir_mtimes.insert(root_path.clone(), root_mtime);

    let stat_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(STAT_THREADS)
        .build()
        .unwrap();

    // BFS queue — use streaming read_dir instead of jwalk so entries flow
    // immediately from the OS rather than being buffered per-directory.
    let mut dirs_to_scan: VecDeque<PathBuf> = VecDeque::new();
    dirs_to_scan.push_back(root_path.clone());

    let mut pending: Vec<PendingEntry> = Vec::with_capacity(STAT_BATCH_SIZE);
    let mut current_path_str = String::new();
    let mut last_progress_count: u64 = 0;
    let mut last_snapshot_time = Instant::now();

    while let Some(dir_path) = dirs_to_scan.pop_front() {
        // Same-filesystem check: stat the directory before reading it
        #[cfg(unix)]
        if let Some(rd) = root_dev {
            use std::os::unix::fs::MetadataExt;
            match std::fs::metadata(&dir_path) {
                Ok(m) if m.dev() != rd => continue,
                Err(_) => {
                    errors_count += 1;
                    continue;
                }
                _ => {}
            }
        }

        // Ensure this directory has storage for its children
        dir_children.entry(dir_path.clone()).or_default();

        let read_dir = match std::fs::read_dir(&dir_path) {
            Ok(rd) => rd,
            Err(_) => {
                errors_count += 1;
                continue;
            }
        };

        for entry_result in read_dir {
            let entry = match entry_result {
                Ok(e) => e,
                Err(_) => {
                    errors_count += 1;
                    continue;
                }
            };

            let file_type = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => {
                    errors_count += 1;
                    continue;
                }
            };

            // Skip symlinks (equivalent to follow_links(false))
            if file_type.is_symlink() {
                continue;
            }

            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let is_dir = file_type.is_dir();

            if is_dir {
                dirs_count += 1;
                dirs_to_scan.push_back(path.clone());
            } else {
                files_count += 1;
            }

            pending.push(PendingEntry {
                path,
                parent: dir_path.clone(),
                name,
                is_dir,
            });

            if (files_count + dirs_count) % 500 == 0 {
                current_path_str = pending
                    .last()
                    .map(|e| e.path.to_string_lossy().to_string())
                    .unwrap_or_default();
            }

            // Stat batch when full
            if pending.len() >= STAT_BATCH_SIZE {
                process_batch(
                    &stat_pool,
                    &pending,
                    &root_path,
                    #[cfg(unix)]
                    root_dev,
                    &mut dir_children,
                    &mut dir_mtimes,
                    &mut dir_sizes,
                    &mut errors_count,
                );
                pending.clear();
            }

            // Send progress frequently based on readdir entry count
            if (files_count + dirs_count) - last_progress_count >= PROGRESS_INTERVAL {
                last_progress_count = files_count + dirs_count;

                // Rate-limit full tree snapshots to avoid excessive work
                let now = Instant::now();
                if now.duration_since(last_snapshot_time).as_millis() >= SNAPSHOT_INTERVAL_MS {
                    last_snapshot_time = now;
                    let snapshot = build_snapshot(
                        &root_path,
                        &dir_children,
                        &dir_mtimes,
                        &dir_sizes,
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
            &mut dir_sizes,
            &mut errors_count,
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
    dir_sizes: &mut HashMap<PathBuf, u64>,
    errors_count: &mut u64,
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
            dir_children.entry(entry.path.clone()).or_default();
            dir_mtimes.insert(entry.path.clone(), mtime);
            dir_children
                .entry(entry.parent.clone())
                .or_default()
                .push(FileNode::new_dir(entry.name.clone(), mtime));
        } else {
            dir_children
                .entry(entry.parent.clone())
                .or_default()
                .push(FileNode::new_file(entry.name.clone(), size, mtime));

            // Propagate file size to all ancestor directories
            let mut ancestor = entry.parent.clone();
            loop {
                *dir_sizes.entry(ancestor.clone()).or_insert(0) += size;
                if ancestor == *root_path {
                    break;
                }
                match ancestor.parent() {
                    Some(p) => ancestor = p.to_path_buf(),
                    None => break,
                }
            }
        }
    }
}

/// Build a recursive tree snapshot from the current scan state.
/// Includes all directories (for navigation) but caps file children
/// per directory to keep the snapshot size bounded.
fn build_snapshot(
    root_path: &Path,
    dir_children: &HashMap<PathBuf, Vec<FileNode>>,
    dir_mtimes: &HashMap<PathBuf, Option<SystemTime>>,
    dir_sizes: &HashMap<PathBuf, u64>,
    total_files: u64,
    total_dirs: u64,
    total_errors: u64,
) -> FileTree {
    let root = build_snapshot_node(root_path, dir_children, dir_mtimes, dir_sizes);

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

/// Recursively build a FileNode from the dir_children HashMap.
/// Always includes subdirectories (for deep navigation), but limits
/// file children to MAX_SNAPSHOT_FILES_PER_DIR per directory.
fn build_snapshot_node(
    path: &Path,
    dir_children: &HashMap<PathBuf, Vec<FileNode>>,
    dir_mtimes: &HashMap<PathBuf, Option<SystemTime>>,
    dir_sizes: &HashMap<PathBuf, u64>,
) -> FileNode {
    let entries = dir_children.get(path);
    let mtime = dir_mtimes.get(path).copied().flatten();
    let name = path
        .file_name()
        .unwrap_or(path.as_os_str())
        .to_string_lossy()
        .to_string();

    let total_child_count = entries.map(|e| e.len()).unwrap_or(0);
    let total_size = dir_sizes.get(path).copied().unwrap_or(0);

    let mut children = Vec::new();

    if let Some(entries) = entries {
        let mut file_count = 0;
        for entry in entries {
            if entry.is_dir {
                // Always recurse into subdirectories
                let child_path = path.join(&entry.name);
                let child =
                    build_snapshot_node(&child_path, dir_children, dir_mtimes, dir_sizes);
                children.push(child);
            } else if file_count < MAX_SNAPSHOT_FILES_PER_DIR {
                children.push(entry.clone());
                file_count += 1;
            }
        }
    }

    children.sort_by(|a, b| b.size.cmp(&a.size));

    FileNode {
        name,
        size: total_size,
        is_dir: true,
        child_count: total_child_count,
        mtime,
        children,
        error_count: 0,
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

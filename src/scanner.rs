use crate::tree::{FileNode, FileTree};
use rayon::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};
use tokio::sync::mpsc;

/// Shared thread activity slots — each stat thread updates its slot with the
/// path it's currently stat'ing. The UI reads these directly for live display.
pub type ThreadActivities = Arc<Vec<Mutex<String>>>;

pub fn new_thread_activities(num_threads: usize) -> ThreadActivities {
    Arc::new(
        (0..num_threads)
            .map(|_| Mutex::new(String::new()))
            .collect(),
    )
}

#[derive(Debug, Clone)]
pub enum ScanMessage {
    Progress {
        tree: FileTree,
        current_path: String,
        elapsed_secs: f64,
        entries_per_sec: u64,
        stat_threads: usize,
        dirs_queued: usize,
    },
    Complete(FileTree),
    Error(String),
}

pub struct ScanOptions {
    pub path: PathBuf,
    pub same_filesystem: bool,
    pub stat_threads: usize,
    pub thread_activities: ThreadActivities,
}

/// Number of entries to collect before sending a batch to the stat pipeline.
const STAT_BATCH_SIZE: usize = 8192;
/// Max file children per directory in progress snapshots.
const MAX_SNAPSHOT_FILES_PER_DIR: usize = 500;
/// Minimum interval between full tree snapshots.
const SNAPSHOT_INTERVAL_MS: u128 = 500;

/// A batch of entries sent from the readdir stage to the stat stage.
struct Batch {
    entries: Vec<PendingEntry>,
    files_count: u64,
    dirs_count: u64,
    dirs_queued: usize,
    current_path: String,
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

pub fn start_scan(
    options: ScanOptions,
    progress_tx: mpsc::UnboundedSender<ScanMessage>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_scan_pipeline(options, progress_tx).await;
    })
}

async fn run_scan_pipeline(
    options: ScanOptions,
    progress_tx: mpsc::UnboundedSender<ScanMessage>,
) {
    let root_path = match options.path.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            let _ = progress_tx.send(ScanMessage::Error(format!(
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
    let root_dev: Option<u64> = None;

    let stat_threads = options.stat_threads;
    let thread_activities = options.thread_activities;

    // Bounded channel: readdir fills batch N+1 while stat processes batch N.
    // Capacity 2 gives double-buffering with natural backpressure.
    let (batch_tx, batch_rx) = std::sync::mpsc::sync_channel::<Batch>(2);

    // === Stage 1: Readdir producer (runs in spawn_blocking) ===
    let readdir_root = root_path.clone();
    let readdir_handle = tokio::task::spawn_blocking(move || {
        readdir_producer(readdir_root, root_dev, batch_tx);
    });

    // === Stage 2: Stat consumer (runs in spawn_blocking) ===
    let stat_root = root_path.clone();
    let stat_handle = tokio::task::spawn_blocking(move || {
        stat_consumer(
            stat_root,
            root_dev,
            stat_threads,
            thread_activities,
            batch_rx,
            progress_tx,
        );
    });

    // Wait for both stages to complete
    let _ = readdir_handle.await;
    let _ = stat_handle.await;
}

/// Stage 1: BFS readdir, classifies entries via d_type, sends batches.
fn readdir_producer(
    root_path: PathBuf,
    #[allow(unused_variables)] root_dev: Option<u64>,
    batch_tx: std::sync::mpsc::SyncSender<Batch>,
) {
    let mut dirs_to_scan: VecDeque<PathBuf> = VecDeque::new();
    dirs_to_scan.push_back(root_path.clone());

    let mut pending: Vec<PendingEntry> = Vec::with_capacity(STAT_BATCH_SIZE);
    let mut files_count: u64 = 0;
    let mut dirs_count: u64 = 0;
    let mut current_path_str = String::new();

    while let Some(dir_path) = dirs_to_scan.pop_front() {
        // Same-filesystem check for directories
        #[cfg(unix)]
        if let Some(rd) = root_dev {
            use std::os::unix::fs::MetadataExt;
            match std::fs::metadata(&dir_path) {
                Ok(m) if m.dev() != rd => continue,
                Err(_) => continue,
                _ => {}
            }
        }

        let read_dir = match std::fs::read_dir(&dir_path) {
            Ok(rd) => rd,
            Err(_) => continue,
        };

        for entry_result in read_dir {
            let entry = match entry_result {
                Ok(e) => e,
                Err(_) => continue,
            };

            let file_type = match entry.file_type() {
                Ok(ft) => ft,
                Err(_) => continue,
            };

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

            // Send batch when full — this may block if the stat stage is busy
            // (bounded channel provides backpressure)
            if pending.len() >= STAT_BATCH_SIZE {
                let batch = Batch {
                    entries: std::mem::replace(
                        &mut pending,
                        Vec::with_capacity(STAT_BATCH_SIZE),
                    ),
                    files_count,
                    dirs_count,
                    dirs_queued: dirs_to_scan.len(),
                    current_path: current_path_str.clone(),
                };
                if batch_tx.send(batch).is_err() {
                    return; // stat stage dropped, abort
                }
            }
        }
    }

    // Send remaining entries
    if !pending.is_empty() {
        let batch = Batch {
            entries: pending,
            files_count,
            dirs_count,
            dirs_queued: 0,
            current_path: current_path_str,
        };
        let _ = batch_tx.send(batch);
    }
    // batch_tx drops here, signaling the stat stage that readdir is done
}

/// Stage 2: Receives batches, does parallel stat, updates tree, sends progress.
fn stat_consumer(
    root_path: PathBuf,
    #[allow(unused_variables)] root_dev: Option<u64>,
    stat_threads: usize,
    thread_activities: ThreadActivities,
    batch_rx: std::sync::mpsc::Receiver<Batch>,
    progress_tx: mpsc::UnboundedSender<ScanMessage>,
) {
    let stat_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(stat_threads)
        .build()
        .unwrap();

    let mut dir_children: HashMap<PathBuf, Vec<FileNode>> = HashMap::new();
    let mut dir_mtimes: HashMap<PathBuf, Option<SystemTime>> = HashMap::new();
    let mut dir_sizes: HashMap<PathBuf, u64> = HashMap::new();
    let mut errors_count: u64 = 0;

    let root_mtime = std::fs::metadata(&root_path)
        .ok()
        .and_then(|m| m.modified().ok());
    dir_children.insert(root_path.clone(), Vec::new());
    dir_mtimes.insert(root_path.clone(), root_mtime);

    let scan_start = Instant::now();
    let mut last_snapshot_time = Instant::now();
    let mut last_files_count: u64 = 0;
    let mut last_dirs_count: u64 = 0;

    // Receive batches until readdir stage is done (channel closed)
    while let Ok(batch) = batch_rx.recv() {
        last_files_count = batch.files_count;
        last_dirs_count = batch.dirs_count;
        // Ensure parent dirs exist in the tree for all entries in this batch
        for entry in &batch.entries {
            if entry.is_dir {
                dir_children.entry(entry.path.clone()).or_default();
            }
        }

        // Parallel stat
        let activities = thread_activities.clone();
        let stat_results: Vec<Option<StatResult>> = stat_pool.install(|| {
            batch
                .entries
                .par_iter()
                .map(|entry| {
                    // Update thread activity slot
                    if let Some(idx) = rayon::current_thread_index() {
                        if let Some(slot) = activities.get(idx) {
                            if let Ok(mut s) = slot.try_lock() {
                                *s = entry.path.to_string_lossy().to_string();
                            }
                        }
                    }

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

        // Process results sequentially
        for (entry, stat) in batch.entries.iter().zip(stat_results.iter()) {
            let (size, mtime) = match stat {
                Some(s) => (s.size, s.mtime),
                None => {
                    errors_count += 1;
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
                    if ancestor == root_path {
                        break;
                    }
                    match ancestor.parent() {
                        Some(p) => ancestor = p.to_path_buf(),
                        None => break,
                    }
                }
            }
        }

        // Send progress (rate-limited snapshots)
        let now = Instant::now();
        if now.duration_since(last_snapshot_time).as_millis() >= SNAPSHOT_INTERVAL_MS {
            last_snapshot_time = now;
            let elapsed = scan_start.elapsed().as_secs_f64();
            let total = batch.files_count + batch.dirs_count;
            let eps = if elapsed > 0.0 {
                (total as f64 / elapsed) as u64
            } else {
                0
            };
            let snapshot = build_snapshot(
                &root_path,
                &dir_children,
                &dir_mtimes,
                &dir_sizes,
                batch.files_count,
                batch.dirs_count,
                errors_count,
            );
            let _ = progress_tx.send(ScanMessage::Progress {
                tree: snapshot,
                current_path: batch.current_path.clone(),
                elapsed_secs: elapsed,
                entries_per_sec: eps,
                stat_threads,
                dirs_queued: batch.dirs_queued,
            });
        }
    }

    // Clear thread activities now that all stat work is done
    for slot in thread_activities.iter() {
        if let Ok(mut s) = slot.try_lock() {
            s.clear();
        }
    }

    // Assemble the final tree
    let root = assemble_tree(&root_path, &mut dir_children, &dir_mtimes);

    let tree = FileTree {
        root_path: root_path.clone(),
        root,
        total_files: last_files_count,
        total_dirs: last_dirs_count,
        total_errors: errors_count,
        scan_time: SystemTime::now(),
        complete: true,
    };

    let _ = progress_tx.send(ScanMessage::Complete(tree));
}

/// Build a recursive tree snapshot from the current scan state.
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

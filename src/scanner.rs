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
    /// Result of an expand_directory request (user pressed 'e')
    ExpandResult {
        breadcrumbs: Vec<usize>,
        children: Vec<FileNode>,
    },
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
/// Directories with more than this many files get aggregated.
const LARGE_DIR_FILE_THRESHOLD: usize = 10_000;
/// Number of top files by size to keep for aggregated directories.
const TOP_FILES_TO_KEEP: usize = 100;

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

/// Tracks aggregated files for directories exceeding LARGE_DIR_FILE_THRESHOLD.
/// Keeps the top N files by size so the UI shows the most important ones.
struct DirAggregation {
    total_file_count: usize,
    /// Top files by size (sorted descending, max TOP_FILES_TO_KEEP entries).
    top_files: Vec<FileNode>,
}

impl DirAggregation {
    fn new(initial_count: usize) -> Self {
        Self {
            total_file_count: initial_count,
            top_files: Vec::with_capacity(TOP_FILES_TO_KEEP),
        }
    }

    /// Offer a file for inclusion in the top-N. Only keeps the largest.
    fn offer(&mut self, node: FileNode) {
        self.total_file_count += 1;
        let pos = self.top_files.partition_point(|f| f.size > node.size);
        if pos < TOP_FILES_TO_KEEP {
            self.top_files.insert(pos, node);
            self.top_files.truncate(TOP_FILES_TO_KEEP);
        }
    }
}

pub fn start_scan(
    options: ScanOptions,
    progress_tx: mpsc::UnboundedSender<ScanMessage>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_scan_pipeline(options, progress_tx).await;
    })
}

/// Re-scan a single directory to expand all files (called when user presses 'e').
pub fn expand_directory(path: &Path, stat_threads: usize) -> Vec<FileNode> {
    let raw_entries: Vec<_> = match std::fs::read_dir(path) {
        Ok(rd) => rd.filter_map(|e| e.ok()).collect(),
        Err(_) => return Vec::new(),
    };

    let pending: Vec<_> = raw_entries
        .iter()
        .filter_map(|entry| {
            let ft = entry.file_type().ok()?;
            if ft.is_symlink() {
                return None;
            }
            Some((
                entry.path(),
                entry.file_name().to_string_lossy().to_string(),
                ft.is_dir(),
            ))
        })
        .collect();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(stat_threads)
        .build()
        .unwrap();

    let mut children: Vec<FileNode> = pool.install(|| {
        pending
            .par_iter()
            .filter_map(|(path, name, is_dir)| {
                let meta = std::fs::symlink_metadata(path).ok()?;
                let mtime = meta.modified().ok();
                if *is_dir {
                    Some(FileNode::new_dir(name.clone(), mtime))
                } else {
                    Some(FileNode::new_file(name.clone(), meta.len(), mtime))
                }
            })
            .collect()
    });

    children.sort_by(|a, b| b.is_dir.cmp(&a.is_dir).then_with(|| b.size.cmp(&a.size)));
    children
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

    let (batch_tx, batch_rx) = std::sync::mpsc::sync_channel::<Batch>(2);

    let readdir_root = root_path.clone();
    let readdir_handle = tokio::task::spawn_blocking(move || {
        readdir_producer(readdir_root, root_dev, batch_tx);
    });

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

    let _ = readdir_handle.await;
    let _ = stat_handle.await;
}

/// Number of entries to read from one directory before rotating to the next.
/// Keeps all directories making progress simultaneously.
const READDIR_CHUNK_SIZE: usize = 1024;

/// Stage 1: Round-robin readdir across all directories.
/// Instead of exhausting one directory before moving to the next, reads a
/// chunk from each directory in rotation. This prevents billion-entry
/// directories from blocking discovery of the rest of the tree.
fn readdir_producer(
    root_path: PathBuf,
    #[allow(unused_variables)] root_dev: Option<u64>,
    batch_tx: std::sync::mpsc::SyncSender<Batch>,
) {
    let mut dirs_to_scan: VecDeque<PathBuf> = VecDeque::new();
    dirs_to_scan.push_back(root_path.clone());

    // Active ReadDir iterators being round-robined
    let mut active_readers: VecDeque<(PathBuf, std::fs::ReadDir)> = VecDeque::new();

    let mut pending: Vec<PendingEntry> = Vec::with_capacity(STAT_BATCH_SIZE);
    let mut files_count: u64 = 0;
    let mut dirs_count: u64 = 0;
    let mut current_path_str = String::new();

    loop {
        // Promote queued directories into active readers
        while let Some(dir_path) = dirs_to_scan.pop_front() {
            #[cfg(unix)]
            {
                #[allow(unused_variables)]
                let skip = if let Some(rd) = root_dev {
                    use std::os::unix::fs::MetadataExt;
                    match std::fs::metadata(&dir_path) {
                        Ok(m) if m.dev() != rd => true,
                        Err(_) => true,
                        _ => false,
                    }
                } else {
                    false
                };
                #[cfg(unix)]
                if skip {
                    continue;
                }
            }

            match std::fs::read_dir(&dir_path) {
                Ok(rd) => active_readers.push_back((dir_path, rd)),
                Err(_) => continue,
            }
        }

        if active_readers.is_empty() {
            break;
        }

        // Round-robin: read READDIR_CHUNK_SIZE entries from the front reader
        let (dir_path, mut reader) = active_readers.pop_front().unwrap();
        let mut chunk_count = 0;
        let mut exhausted = false;

        loop {
            let entry_result = match reader.next() {
                Some(r) => r,
                None => {
                    exhausted = true;
                    break;
                }
            };

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

            if pending.len() >= STAT_BATCH_SIZE {
                let batch = Batch {
                    entries: std::mem::replace(
                        &mut pending,
                        Vec::with_capacity(STAT_BATCH_SIZE),
                    ),
                    files_count,
                    dirs_count,
                    dirs_queued: dirs_to_scan.len() + active_readers.len(),
                    current_path: current_path_str.clone(),
                };
                if batch_tx.send(batch).is_err() {
                    return;
                }
            }

            chunk_count += 1;
            if chunk_count >= READDIR_CHUNK_SIZE {
                break;
            }
        }

        if !exhausted {
            // This directory has more entries — put it back for next round
            active_readers.push_back((dir_path, reader));
        }
    }

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

    // Aggregation tracking
    let mut dir_file_counts: HashMap<PathBuf, usize> = HashMap::new();
    let mut aggregated_dirs: HashMap<PathBuf, DirAggregation> = HashMap::new();

    let root_mtime = std::fs::metadata(&root_path)
        .ok()
        .and_then(|m| m.modified().ok());
    dir_children.insert(root_path.clone(), Vec::new());
    dir_mtimes.insert(root_path.clone(), root_mtime);

    let scan_start = Instant::now();
    let mut last_snapshot_time = Instant::now();
    let mut last_files_count: u64 = 0;
    let mut last_dirs_count: u64 = 0;

    while let Ok(batch) = batch_rx.recv() {
        last_files_count = batch.files_count;
        last_dirs_count = batch.dirs_count;

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
                let file_count = dir_file_counts.entry(entry.parent.clone()).or_insert(0);
                let node = FileNode::new_file(entry.name.clone(), size, mtime);

                if *file_count < LARGE_DIR_FILE_THRESHOLD {
                    // Under threshold: store normally
                    dir_children
                        .entry(entry.parent.clone())
                        .or_default()
                        .push(node);
                    *file_count += 1;
                } else {
                    // Over threshold: aggregate — only keep top N by size
                    let agg = aggregated_dirs
                        .entry(entry.parent.clone())
                        .or_insert_with(|| DirAggregation::new(*file_count));
                    agg.offer(node);
                }

                // Size propagation always runs
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

        // Send progress (rate-limited snapshots).
        // Skip building if we're still within the interval — building the
        // recursive snapshot is O(directories) and must not stall the pipeline.
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
                &aggregated_dirs,
                &dir_file_counts,
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

    // Clear thread activities
    for slot in thread_activities.iter() {
        if let Ok(mut s) = slot.try_lock() {
            s.clear();
        }
    }

    // Assemble the final tree
    let root = assemble_tree(
        &root_path,
        &mut dir_children,
        &dir_mtimes,
        &dir_sizes,
        &aggregated_dirs,
        &dir_file_counts,
    );

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
#[allow(clippy::too_many_arguments)]
fn build_snapshot(
    root_path: &Path,
    dir_children: &HashMap<PathBuf, Vec<FileNode>>,
    dir_mtimes: &HashMap<PathBuf, Option<SystemTime>>,
    dir_sizes: &HashMap<PathBuf, u64>,
    aggregated_dirs: &HashMap<PathBuf, DirAggregation>,
    dir_file_counts: &HashMap<PathBuf, usize>,
    total_files: u64,
    total_dirs: u64,
    total_errors: u64,
) -> FileTree {
    let root = build_snapshot_node(
        root_path,
        dir_children,
        dir_mtimes,
        dir_sizes,
        aggregated_dirs,
        dir_file_counts,
    );

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
    aggregated_dirs: &HashMap<PathBuf, DirAggregation>,
    dir_file_counts: &HashMap<PathBuf, usize>,
) -> FileNode {
    let entries = dir_children.get(path);
    let mtime = dir_mtimes.get(path).copied().flatten();
    let name = path
        .file_name()
        .unwrap_or(path.as_os_str())
        .to_string_lossy()
        .to_string();

    let total_size = dir_sizes.get(path).copied().unwrap_or(0);

    // Compute accurate child count including aggregated files
    let total_child_count = if let Some(agg) = aggregated_dirs.get(path) {
        let dir_count = entries
            .map(|e| e.iter().filter(|c| c.is_dir).count())
            .unwrap_or(0);
        dir_count + agg.total_file_count
    } else {
        // Use dir_file_counts if available (more accurate than entries.len()
        // since entries might have files AND dirs mixed)
        let dir_count = entries
            .map(|e| e.iter().filter(|c| c.is_dir).count())
            .unwrap_or(0);
        let file_count = dir_file_counts
            .get(path)
            .copied()
            .unwrap_or_else(|| entries.map(|e| e.len()).unwrap_or(0).saturating_sub(dir_count));
        dir_count + file_count
    };

    let mut children = Vec::new();

    if let Some(entries) = entries {
        let mut file_count = 0;
        for entry in entries {
            if entry.is_dir {
                let child_path = path.join(&entry.name);
                let child = build_snapshot_node(
                    &child_path,
                    dir_children,
                    dir_mtimes,
                    dir_sizes,
                    aggregated_dirs,
                    dir_file_counts,
                );
                children.push(child);
            } else if file_count < MAX_SNAPSHOT_FILES_PER_DIR {
                children.push(entry.clone());
                file_count += 1;
            }
        }
    }

    // For aggregated dirs, merge in top files from the heap
    // (these are files beyond the 10K threshold that were the largest)
    if let Some(agg) = aggregated_dirs.get(path) {
        for top_file in &agg.top_files {
            children.push(top_file.clone());
        }
    }

    children.sort_by(|a, b| b.is_dir.cmp(&a.is_dir).then_with(|| b.size.cmp(&a.size)));

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
    dir_sizes: &HashMap<PathBuf, u64>,
    aggregated_dirs: &HashMap<PathBuf, DirAggregation>,
    dir_file_counts: &HashMap<PathBuf, usize>,
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
            let assembled = assemble_tree(
                &child_path,
                dir_children,
                dir_mtimes,
                dir_sizes,
                aggregated_dirs,
                dir_file_counts,
            );
            assembled_children.push(assembled);
        } else {
            assembled_children.push(child);
        }
    }

    // Merge top files from aggregation heap (these are the globally largest
    // files beyond the 10K threshold — not already in assembled_children)
    if let Some(agg) = aggregated_dirs.get(path) {
        for top_file in &agg.top_files {
            assembled_children.push(top_file.clone());
        }
    }

    // For aggregated dirs, use dir_sizes (which includes ALL files, not just stored ones)
    let size = if aggregated_dirs.contains_key(path) {
        dir_sizes.get(path).copied().unwrap_or(0)
    } else {
        assembled_children.iter().map(|c| c.size).sum()
    };
    let error_count: u64 = assembled_children.iter().map(|c| c.error_count).sum();

    let child_count = if let Some(agg) = aggregated_dirs.get(path) {
        let dir_count = assembled_children.iter().filter(|c| c.is_dir).count();
        dir_count + agg.total_file_count
    } else {
        assembled_children.len()
    };

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

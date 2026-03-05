use crate::tree::{FileNode, FileTree};
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
    /// Lightweight counter update (no tree clone — just numbers for the status bar)
    Counting {
        files_count: u64,
        dirs_count: u64,
        current_path: String,
        elapsed_secs: f64,
        entries_per_sec: u64,
    },
    Complete(FileTree),
    Error(String),
    ExpandResult {
        breadcrumbs: Vec<usize>,
        children: Vec<FileNode>,
    },
}

pub struct ScanOptions {
    pub path: PathBuf,
    pub same_filesystem: bool,
    pub stat_threads: usize,
    pub queue_multiplier: usize,
    pub thread_activities: ThreadActivities,
}

/// Max file children per directory in progress snapshots.
const MAX_SNAPSHOT_FILES_PER_DIR: usize = 500;
/// Minimum interval between full tree snapshots.
const SNAPSHOT_INTERVAL_MS: u128 = 500;
/// Directories with more than this many files get aggregated.
const LARGE_DIR_FILE_THRESHOLD: usize = 10_000;
/// Number of top files by size to keep for aggregated directories.
const TOP_FILES_TO_KEEP: usize = 100;
/// Entries to read from one directory before rotating to the next.
const READDIR_CHUNK_SIZE: usize = 1024;

/// Entry sent from readdir → stat workers.
struct PendingEntry {
    path: PathBuf,
    parent: PathBuf,
    name: String,
    is_dir: bool,
}

/// Result sent from stat workers → tree builder.
struct StatEntry {
    parent: PathBuf,
    name: String,
    is_dir: bool,
    size: u64,
    mtime: Option<SystemTime>,
}

/// Tracks aggregated files for directories exceeding LARGE_DIR_FILE_THRESHOLD.
struct DirAggregation {
    total_file_count: usize,
    top_files: Vec<FileNode>,
}

impl DirAggregation {
    fn new(initial_count: usize) -> Self {
        Self {
            total_file_count: initial_count,
            top_files: Vec::with_capacity(TOP_FILES_TO_KEEP),
        }
    }

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

    use rayon::prelude::*;
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
    let queue_cap = stat_threads * options.queue_multiplier;

    // Stage 1 → Stage 2: entries from readdir to stat workers
    let (entry_tx, entry_rx) = async_channel::bounded::<PendingEntry>(queue_cap);
    // Stage 2 → Stage 3: stat results to tree builder
    let (result_tx, result_rx) = async_channel::bounded::<StatEntry>(queue_cap);

    // === Stage 1: Readdir producer ===
    let readdir_root = root_path.clone();
    let readdir_handle = tokio::task::spawn_blocking(move || {
        readdir_producer(readdir_root, root_dev, entry_tx);
    });

    // === Stage 2: Stat workers (N independent workers) ===
    let mut worker_handles = Vec::with_capacity(stat_threads);
    for worker_id in 0..stat_threads {
        let rx = entry_rx.clone();
        let tx = result_tx.clone();
        let activities = thread_activities.clone();
        #[allow(unused_variables)]
        let root_dev = root_dev;

        worker_handles.push(tokio::task::spawn_blocking(move || {
            stat_worker(worker_id, rx, tx, activities, root_dev);
        }));
    }
    // Drop our copies — workers own the channel ends now
    drop(entry_rx);
    drop(result_tx);

    // === Stage 3: Tree builder ===
    let builder_root = root_path.clone();
    let builder_handle = tokio::task::spawn_blocking(move || {
        tree_builder(builder_root, stat_threads, result_rx, progress_tx);
    });

    // Wait for pipeline to drain
    let _ = readdir_handle.await;
    for h in worker_handles {
        let _ = h.await;
    }
    let _ = builder_handle.await;
}

// ─── Stage 1: Readdir Producer ───────────────────────────────────────────────

fn readdir_producer(
    root_path: PathBuf,
    #[allow(unused_variables)] root_dev: Option<u64>,
    entry_tx: async_channel::Sender<PendingEntry>,
) {
    let mut dirs_to_scan: VecDeque<PathBuf> = VecDeque::new();
    dirs_to_scan.push_back(root_path.clone());

    let mut active_readers: VecDeque<(PathBuf, std::fs::ReadDir)> = VecDeque::new();

    loop {
        // Promote a few queued dirs into active readers
        while active_readers.len() < 16 {
            let dir_path = match dirs_to_scan.pop_front() {
                Some(d) => d,
                None => break,
            };

            #[cfg(unix)]
            if let Some(rd) = root_dev {
                use std::os::unix::fs::MetadataExt;
                match std::fs::metadata(&dir_path) {
                    Ok(m) if m.dev() != rd => continue,
                    Err(_) => continue,
                    _ => {}
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

        // Round-robin: read a chunk from the front reader
        let (dir_path, mut reader) = active_readers.pop_front().unwrap();
        let mut chunk_count = 0;
        let mut exhausted = false;

        loop {
            let entry = match reader.next() {
                Some(Ok(e)) => e,
                Some(Err(_)) => continue,
                None => {
                    exhausted = true;
                    break;
                }
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
                dirs_to_scan.push_back(path.clone());
            }

            // This blocks when the queue is full — natural backpressure
            if entry_tx
                .send_blocking(PendingEntry {
                    path,
                    parent: dir_path.clone(),
                    name,
                    is_dir,
                })
                .is_err()
            {
                return; // channel closed
            }

            chunk_count += 1;
            if chunk_count >= READDIR_CHUNK_SIZE {
                break;
            }
        }

        if !exhausted {
            active_readers.push_back((dir_path, reader));
        }
    }
    // entry_tx drops here → channel closes → workers exit
}

// ─── Stage 2: Stat Workers ──────────────────────────────────────────────────

fn stat_worker(
    worker_id: usize,
    entry_rx: async_channel::Receiver<PendingEntry>,
    result_tx: async_channel::Sender<StatEntry>,
    activities: ThreadActivities,
    #[allow(unused_variables)] root_dev: Option<u64>,
) {
    while let Ok(entry) = entry_rx.recv_blocking() {
        // Update thread activity slot
        if let Some(slot) = activities.get(worker_id) {
            if let Ok(mut s) = slot.try_lock() {
                *s = entry.path.to_string_lossy().to_string();
            }
        }

        let meta = match std::fs::symlink_metadata(&entry.path) {
            Ok(m) => m,
            Err(_) => continue, // skip errors
        };

        #[cfg(unix)]
        if let Some(rd) = root_dev {
            use std::os::unix::fs::MetadataExt;
            if meta.dev() != rd {
                continue;
            }
        }

        let size = if entry.is_dir { 0 } else { meta.len() };
        let mtime = meta.modified().ok();

        if result_tx
            .send_blocking(StatEntry {
                parent: entry.parent,
                name: entry.name,
                is_dir: entry.is_dir,
                size,
                mtime,
            })
            .is_err()
        {
            return; // channel closed
        }
    }

    // Clear our activity slot
    if let Some(slot) = activities.get(worker_id) {
        if let Ok(mut s) = slot.try_lock() {
            s.clear();
        }
    }
}

// ─── Stage 3: Tree Builder ──────────────────────────────────────────────────

fn tree_builder(
    root_path: PathBuf,
    stat_threads: usize,
    result_rx: async_channel::Receiver<StatEntry>,
    progress_tx: mpsc::UnboundedSender<ScanMessage>,
) {
    let mut dir_children: HashMap<PathBuf, Vec<FileNode>> = HashMap::new();
    let mut dir_mtimes: HashMap<PathBuf, Option<SystemTime>> = HashMap::new();
    let mut dir_sizes: HashMap<PathBuf, u64> = HashMap::new();
    let mut dir_file_counts: HashMap<PathBuf, usize> = HashMap::new();
    let mut aggregated_dirs: HashMap<PathBuf, DirAggregation> = HashMap::new();

    let mut files_count: u64 = 0;
    let mut dirs_count: u64 = 0;
    let mut errors_count: u64 = 0;

    let root_mtime = std::fs::metadata(&root_path)
        .ok()
        .and_then(|m| m.modified().ok());
    dir_children.insert(root_path.clone(), Vec::new());
    dir_mtimes.insert(root_path.clone(), root_mtime);

    let scan_start = Instant::now();
    let mut last_snapshot_time = Instant::now();
    let mut current_path = String::new();
    let mut entry_count: u64 = 0;

    while let Ok(entry) = result_rx.recv_blocking() {
        if entry.is_dir {
            dirs_count += 1;
            dir_children.entry(PathBuf::from(&entry.parent).join(&entry.name)).or_default();
            dir_mtimes.insert(
                entry.parent.join(&entry.name),
                entry.mtime,
            );
            dir_children
                .entry(entry.parent.clone())
                .or_default()
                .push(FileNode::new_dir(entry.name.clone(), entry.mtime));
        } else {
            files_count += 1;

            let file_count = dir_file_counts.entry(entry.parent.clone()).or_insert(0);
            let node = FileNode::new_file(entry.name.clone(), entry.size, entry.mtime);

            if *file_count < LARGE_DIR_FILE_THRESHOLD {
                dir_children
                    .entry(entry.parent.clone())
                    .or_default()
                    .push(node);
                *file_count += 1;
            } else {
                let agg = aggregated_dirs
                    .entry(entry.parent.clone())
                    .or_insert_with(|| DirAggregation::new(*file_count));
                agg.offer(node);
            }

            // Propagate size to ancestors
            let mut ancestor = entry.parent.clone();
            loop {
                *dir_sizes.entry(ancestor.clone()).or_insert(0) += entry.size;
                if ancestor == root_path {
                    break;
                }
                match ancestor.parent() {
                    Some(p) => ancestor = p.to_path_buf(),
                    None => break,
                }
            }
        }

        entry_count += 1;
        if entry_count % 200 == 0 {
            current_path = format!("{}/{}", entry.parent.display(), entry.name);

            // Send lightweight counter update frequently (no tree clone)
            let elapsed = scan_start.elapsed().as_secs_f64();
            let total = files_count + dirs_count;
            let eps = if elapsed > 0.0 {
                (total as f64 / elapsed) as u64
            } else {
                0
            };
            let _ = progress_tx.send(ScanMessage::Counting {
                files_count,
                dirs_count,
                current_path: current_path.clone(),
                elapsed_secs: elapsed,
                entries_per_sec: eps,
            });
        }

        // Rate-limited full tree snapshots (expensive)
        let now = Instant::now();
        if now.duration_since(last_snapshot_time).as_millis() >= SNAPSHOT_INTERVAL_MS {
            last_snapshot_time = now;
            let elapsed = scan_start.elapsed().as_secs_f64();
            let total = files_count + dirs_count;
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
                files_count,
                dirs_count,
                errors_count,
            );
            let _ = progress_tx.send(ScanMessage::Progress {
                tree: snapshot,
                current_path: current_path.clone(),
                elapsed_secs: elapsed,
                entries_per_sec: eps,
                stat_threads,
                dirs_queued: 0, // not easily available from this stage
            });
        }
    }

    // Build final tree
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
        total_files: files_count,
        total_dirs: dirs_count,
        total_errors: errors_count,
        scan_time: SystemTime::now(),
        complete: true,
    };

    let _ = progress_tx.send(ScanMessage::Complete(tree));
}

// ─── Snapshot Building ──────────────────────────────────────────────────────

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

    let total_child_count = if let Some(agg) = aggregated_dirs.get(path) {
        let dir_count = entries
            .map(|e| e.iter().filter(|c| c.is_dir).count())
            .unwrap_or(0);
        dir_count + agg.total_file_count
    } else {
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

// ─── Final Tree Assembly ────────────────────────────────────────────────────

fn assemble_tree(
    path: &Path,
    dir_children: &mut HashMap<PathBuf, Vec<FileNode>>,
    dir_mtimes: &HashMap<PathBuf, Option<SystemTime>>,
    dir_sizes: &HashMap<PathBuf, u64>,
    aggregated_dirs: &HashMap<PathBuf, DirAggregation>,
    _dir_file_counts: &HashMap<PathBuf, usize>,
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
                _dir_file_counts,
            );
            assembled_children.push(assembled);
        } else {
            assembled_children.push(child);
        }
    }

    if let Some(agg) = aggregated_dirs.get(path) {
        for top_file in &agg.top_files {
            assembled_children.push(top_file.clone());
        }
    }

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

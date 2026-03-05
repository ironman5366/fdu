use crate::scanner::{ScanMessage, ThreadActivities};
use crate::tree::{FileNode, FileTree};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
pub enum View {
    Scanning,
    Browser,
    Help,
    DeleteConfirm,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    SizeDesc,
    SizeAsc,
    NameAsc,
    NameDesc,
}

#[derive(Debug, Clone, Default)]
pub struct ScanProgress {
    pub files_count: u64,
    pub dirs_count: u64,
    pub total_size: u64,
    pub errors_count: u64,
    pub current_path: String,
    pub elapsed_secs: f64,
    pub entries_per_sec: u64,
    pub stat_threads: usize,
    pub dirs_queued: usize,
}

pub struct App {
    pub view: View,
    pub tree: Option<FileTree>,
    pub breadcrumbs: Vec<usize>,
    pub selected_index: usize,
    pub sort_order: SortOrder,
    pub show_bars: bool,
    pub scan_progress: ScanProgress,
    pub scanning: bool,
    pub should_quit: bool,
    pub scroll_offset: usize,
    pub error_message: Option<String>,
    pub show_threads: bool,
    pub thread_activities: Option<ThreadActivities>,
    pub expanding: bool,
}

impl App {
    pub fn new() -> Self {
        Self {
            view: View::Scanning,
            tree: None,
            breadcrumbs: Vec::new(),
            selected_index: 0,
            sort_order: SortOrder::SizeDesc,
            show_bars: true,
            scan_progress: ScanProgress::default(),
            scanning: true,
            should_quit: false,
            scroll_offset: 0,
            error_message: None,
            show_threads: false,
            thread_activities: None,
            expanding: false,
        }
    }

    pub fn current_node(&self) -> Option<&FileNode> {
        self.tree.as_ref().map(|t| {
            if self.breadcrumbs.is_empty() {
                &t.root
            } else {
                t.node_at(&self.breadcrumbs).unwrap_or(&t.root)
            }
        })
    }

    pub fn current_node_mut(&mut self) -> Option<&mut FileNode> {
        if self.tree.is_none() {
            return None;
        }
        let tree = self.tree.as_mut().unwrap();
        if self.breadcrumbs.is_empty() {
            Some(&mut tree.root)
        } else {
            tree.node_at_mut(&self.breadcrumbs)
        }
    }

    pub fn child_count(&self) -> usize {
        self.current_node()
            .map(|n| n.children.len())
            .unwrap_or(0)
    }

    pub fn enter_directory(&mut self) {
        let node = match self.current_node() {
            Some(n) => n,
            None => return,
        };
        if self.selected_index >= node.children.len() {
            return;
        }
        if !node.children[self.selected_index].is_dir {
            return;
        }
        self.breadcrumbs.push(self.selected_index);
        self.selected_index = 0;
        self.scroll_offset = 0;
    }

    pub fn go_up(&mut self) {
        if let Some(parent_idx) = self.breadcrumbs.pop() {
            self.selected_index = parent_idx;
            self.ensure_visible();
        }
    }

    pub fn select_next(&mut self) {
        let count = self.child_count();
        if count == 0 {
            return;
        }
        if self.selected_index < count - 1 {
            self.selected_index += 1;
            self.ensure_visible();
        }
    }

    pub fn select_previous(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
            self.ensure_visible();
        }
    }

    pub fn page_down(&mut self, page_size: usize) {
        let count = self.child_count();
        if count == 0 {
            return;
        }
        self.selected_index = (self.selected_index + page_size).min(count - 1);
        self.ensure_visible();
    }

    pub fn page_up(&mut self, page_size: usize) {
        self.selected_index = self.selected_index.saturating_sub(page_size);
        self.ensure_visible();
    }

    pub fn select_first(&mut self) {
        self.selected_index = 0;
        self.scroll_offset = 0;
    }

    pub fn select_last(&mut self) {
        let count = self.child_count();
        if count > 0 {
            self.selected_index = count - 1;
            self.ensure_visible();
        }
    }

    pub fn cycle_sort(&mut self) {
        self.sort_order = match self.sort_order {
            SortOrder::SizeDesc => SortOrder::SizeAsc,
            SortOrder::SizeAsc => SortOrder::NameAsc,
            SortOrder::NameAsc => SortOrder::NameDesc,
            SortOrder::NameDesc => SortOrder::SizeDesc,
        };
        self.apply_sort();
    }

    pub fn sort_by_name(&mut self) {
        self.sort_order = match self.sort_order {
            SortOrder::NameAsc => SortOrder::NameDesc,
            _ => SortOrder::NameAsc,
        };
        self.apply_sort();
    }

    fn apply_sort(&mut self) {
        let sort_order = self.sort_order.clone();
        if let Some(node) = self.current_node_mut() {
            match sort_order {
                SortOrder::SizeDesc => node.sort_by_size_desc(),
                SortOrder::SizeAsc => node.sort_by_size_asc(),
                SortOrder::NameAsc => node.sort_by_name_asc(),
                SortOrder::NameDesc => node.sort_by_name_desc(),
            }
        }
        self.selected_index = 0;
        self.scroll_offset = 0;
    }

    pub fn handle_scan_message(&mut self, msg: ScanMessage) {
        match msg {
            ScanMessage::Progress {
                tree,
                current_path,
                elapsed_secs,
                entries_per_sec,
                stat_threads,
                dirs_queued,
            } => {
                self.scan_progress = ScanProgress {
                    files_count: tree.total_files,
                    dirs_count: tree.total_dirs,
                    total_size: tree.root.size,
                    errors_count: tree.total_errors,
                    current_path,
                    elapsed_secs,
                    entries_per_sec,
                    stat_threads,
                    dirs_queued,
                };
                self.tree = Some(tree);
                if self.view == View::Scanning {
                    self.view = View::Browser;
                }
                // Validate breadcrumbs — if the tree changed and our navigation
                // path is no longer valid, pop back to the deepest valid level
                while !self.breadcrumbs.is_empty() {
                    if self.tree.as_ref().and_then(|t| t.node_at(&self.breadcrumbs)).is_none() {
                        self.breadcrumbs.pop();
                        self.selected_index = 0;
                    } else {
                        break;
                    }
                }
                // Clamp selected index to valid range
                let count = self.child_count();
                if count > 0 && self.selected_index >= count {
                    self.selected_index = count - 1;
                }
            }
            ScanMessage::Counting {
                files_count,
                dirs_count,
                current_path,
                elapsed_secs,
                entries_per_sec,
            } => {
                self.scan_progress.files_count = files_count;
                self.scan_progress.dirs_count = dirs_count;
                self.scan_progress.current_path = current_path;
                self.scan_progress.elapsed_secs = elapsed_secs;
                self.scan_progress.entries_per_sec = entries_per_sec;
                // Switch from Scanning screen to Browser as soon as we have any data
                if self.view == View::Scanning && self.tree.is_some() {
                    self.view = View::Browser;
                }
            }
            ScanMessage::Complete(tree) => {
                self.tree = Some(tree);
                self.scanning = false;
                self.view = View::Browser;
                // Clamp selection
                let count = self.child_count();
                if count > 0 && self.selected_index >= count {
                    self.selected_index = count - 1;
                }
            }
            ScanMessage::ExpandResult {
                breadcrumbs,
                children,
            } => {
                self.expanding = false;
                // Apply expand result if navigation path is still valid
                if let Some(tree) = self.tree.as_mut() {
                    let node = if breadcrumbs.is_empty() {
                        Some(&mut tree.root)
                    } else {
                        tree.node_at_mut(&breadcrumbs)
                    };
                    if let Some(node) = node {
                        node.child_count = children.len();
                        node.children = children;
                    }
                }
                let count = self.child_count();
                if count > 0 && self.selected_index >= count {
                    self.selected_index = count - 1;
                }
            }
            ScanMessage::Error(msg) => {
                self.error_message = Some(msg);
                self.should_quit = true;
            }
        }
    }

    /// Returns true if the current directory has aggregated (hidden) children.
    pub fn is_aggregated(&self) -> bool {
        self.current_node()
            .map(|n| n.child_count > n.children.len())
            .unwrap_or(false)
    }

    /// Get the filesystem path of the currently viewed directory.
    pub fn current_dir_path(&self) -> Option<PathBuf> {
        let tree = self.tree.as_ref()?;
        let mut path = tree.root_path.clone();
        let mut current = &tree.root;
        for &idx in &self.breadcrumbs {
            let child = current.children.get(idx)?;
            path = path.join(&child.name);
            current = child;
        }
        Some(path)
    }

    fn ensure_visible(&mut self) {
        // Will be properly calibrated when we know the viewport height.
        // For now, keep a basic invariant.
        if self.selected_index < self.scroll_offset {
            self.scroll_offset = self.selected_index;
        }
        // The upper bound check happens during rendering when we know the height.
    }

    pub fn ensure_visible_with_height(&mut self, visible_height: usize) {
        if visible_height == 0 {
            return;
        }
        if self.selected_index < self.scroll_offset {
            self.scroll_offset = self.selected_index;
        } else if self.selected_index >= self.scroll_offset + visible_height {
            self.scroll_offset = self.selected_index - visible_height + 1;
        }
    }
}

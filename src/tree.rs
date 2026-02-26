use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileNode {
    pub name: String,
    pub size: u64,
    pub is_dir: bool,
    pub child_count: usize,
    pub mtime: Option<SystemTime>,
    pub children: Vec<FileNode>,
    pub error_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileTree {
    pub root_path: PathBuf,
    pub root: FileNode,
    pub total_files: u64,
    pub total_dirs: u64,
    pub total_errors: u64,
    pub scan_time: SystemTime,
    pub complete: bool,
}

impl FileNode {
    pub fn new_dir(name: String, mtime: Option<SystemTime>) -> Self {
        Self {
            name,
            size: 0,
            is_dir: true,
            child_count: 0,
            mtime,
            children: Vec::new(),
            error_count: 0,
        }
    }

    pub fn new_file(name: String, size: u64, mtime: Option<SystemTime>) -> Self {
        Self {
            name,
            size,
            is_dir: false,
            child_count: 0,
            mtime,
            children: Vec::new(),
            error_count: 0,
        }
    }

    pub fn sort_by_size_desc(&mut self) {
        self.children.sort_by(|a, b| b.size.cmp(&a.size));
    }

    pub fn sort_by_size_asc(&mut self) {
        self.children.sort_by(|a, b| a.size.cmp(&b.size));
    }

    pub fn sort_by_name_asc(&mut self) {
        self.children
            .sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    }

    pub fn sort_by_name_desc(&mut self) {
        self.children
            .sort_by(|a, b| b.name.to_lowercase().cmp(&a.name.to_lowercase()));
    }

    pub fn sort_recursive_by_size(&mut self) {
        self.sort_by_size_desc();
        for child in &mut self.children {
            if child.is_dir {
                child.sort_recursive_by_size();
            }
        }
    }

}

impl FileTree {
    pub fn node_at(&self, path: &[usize]) -> Option<&FileNode> {
        let mut current = &self.root;
        for &idx in path {
            current = current.children.get(idx)?;
        }
        Some(current)
    }

    pub fn node_at_mut(&mut self, path: &[usize]) -> Option<&mut FileNode> {
        let mut current = &mut self.root;
        for &idx in path {
            current = current.children.get_mut(idx)?;
        }
        Some(current)
    }
}

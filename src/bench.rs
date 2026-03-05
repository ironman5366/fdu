#![allow(dead_code, unused_imports)]
use std::path::PathBuf;
use std::time::Instant;

mod cli;
mod scanner;
mod tree;

#[tokio::main]
async fn main() {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let options = scanner::ScanOptions {
        path: path.clone(),
        same_filesystem: false,
        stat_threads: 128,
        queue_multiplier: 32,
        thread_activities: scanner::new_thread_activities(128),
    };

    let start = Instant::now();
    let _handle = scanner::start_scan(options, tx);

    let mut last_progress = Instant::now();
    loop {
        match rx.recv().await {
            Some(scanner::ScanMessage::Progress {
                tree,
                current_path,
                entries_per_sec,
                ..
            }) => {
                if last_progress.elapsed().as_millis() > 200 {
                    eprintln!(
                        "[{:.1}s] {} files, {} dirs, {} errors | {}/s | {}",
                        start.elapsed().as_secs_f64(),
                        tree.total_files,
                        tree.total_dirs,
                        tree.total_errors,
                        entries_per_sec,
                        current_path,
                    );
                    last_progress = Instant::now();
                }
            }
            Some(scanner::ScanMessage::Complete(tree)) => {
                let elapsed = start.elapsed();
                println!("=== SCAN COMPLETE ===");
                println!("Path: {}", path.display());
                println!("Files: {}", tree.total_files);
                println!("Dirs: {}", tree.total_dirs);
                println!("Errors: {}", tree.total_errors);
                println!("Total size: {} bytes", tree.root.size);
                println!("Time: {:.2}s", elapsed.as_secs_f64());
                println!(
                    "Rate: {:.0} entries/sec",
                    (tree.total_files + tree.total_dirs) as f64 / elapsed.as_secs_f64()
                );
                break;
            }
            Some(scanner::ScanMessage::Counting {
                files_count,
                dirs_count,
                entries_per_sec,
                current_path,
                ..
            }) => {
                if last_progress.elapsed().as_millis() > 200 {
                    eprintln!(
                        "[{:.1}s] {} files, {} dirs | {}/s | {}",
                        start.elapsed().as_secs_f64(),
                        files_count,
                        dirs_count,
                        entries_per_sec,
                        current_path,
                    );
                    last_progress = Instant::now();
                }
            }
            Some(scanner::ScanMessage::ExpandResult { .. })
            | Some(scanner::ScanMessage::DeleteProgress { .. })
            | Some(scanner::ScanMessage::DeleteComplete { .. }) => {}
            Some(scanner::ScanMessage::Error(e)) => {
                eprintln!("Error: {}", e);
                break;
            }
            None => break,
        }
    }
}

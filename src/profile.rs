#![allow(dead_code, unused_imports)]
/// Headless profiler — runs the same scan pipeline and handle_scan_message
/// as the TUI but without rendering or crossterm. Logs slow frames to stdout.
use std::path::PathBuf;
use std::time::{Duration, Instant};

mod app;
mod cli;
mod events;
mod scanner;
mod tree;
mod ui;

use app::App;
use scanner::{ScanMessage, ScanOptions, new_thread_activities, start_scan};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let stat_threads: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(128);
    let duration_secs: u64 = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);

    eprintln!(
        "Profiling {} for {}s with {} threads",
        path.display(),
        duration_secs,
        stat_threads
    );

    let mut app = App::new();
    let (scan_tx, mut scan_rx) = mpsc::unbounded_channel();

    let thread_activities = new_thread_activities(stat_threads);
    app.thread_activities = Some(thread_activities.clone());

    let scan_options = ScanOptions {
        path: path.clone(),
        same_filesystem: false,
        stat_threads,
        queue_multiplier: 32,
        thread_activities,
    };
    let _scan_handle = start_scan(scan_options, scan_tx);

    let start = Instant::now();
    let mut frame_count: u64 = 0;
    let mut slow_frames: u64 = 0;
    let mut max_frame_us: u128 = 0;
    let mut last_report = Instant::now();
    let mut scan_handle_times: Vec<(u128, &'static str)> = Vec::new();

    let mut last_msg_time = Instant::now();
    let mut gap_reported = false;

    loop {
        if start.elapsed().as_secs() >= duration_secs {
            break;
        }

        let msg = tokio::select! {
            msg = scan_rx.recv() => msg,
            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                // Check for stall
                let gap = last_msg_time.elapsed();
                if gap.as_secs() >= 1 && !gap_reported {
                    gap_reported = true;
                    println!(
                        "[{:.1}s] *** FREEZE *** no messages for {:.1}s — files={} dirs={}",
                        start.elapsed().as_secs_f64(),
                        gap.as_secs_f64(),
                        app.scan_progress.files_count,
                        app.scan_progress.dirs_count,
                    );
                }
                continue;
            },
        };
        if gap_reported {
            let gap = last_msg_time.elapsed();
            println!(
                "[{:.1}s] *** THAW *** resumed after {:.1}s gap",
                start.elapsed().as_secs_f64(),
                gap.as_secs_f64(),
            );
            gap_reported = false;
        }
        last_msg_time = Instant::now();

        if let Some(msg) = msg {
            // Coalesce: drain any queued messages, keep latest progress/counting
            let mut latest = msg;
            while let Ok(next) = scan_rx.try_recv() {
                match (&latest, &next) {
                    (ScanMessage::Progress { .. }, ScanMessage::Progress { .. })
                    | (ScanMessage::Counting { .. }, ScanMessage::Counting { .. }) => {
                        latest = next;
                    }
                    (ScanMessage::Counting { .. }, ScanMessage::Progress { .. }) => {
                        latest = next;
                    }
                    (ScanMessage::Progress { .. }, ScanMessage::Counting { .. }) => {
                        // keep progress
                    }
                    _ => {
                        // Process current, continue with next
                        let t = Instant::now();
                        app.handle_scan_message(latest);
                        let _ = t.elapsed().as_micros();
                        latest = next;
                    }
                }
            }

            let event_name = match &latest {
                ScanMessage::Progress { .. } => "progress",
                ScanMessage::Counting { .. } => "counting",
                ScanMessage::Complete(_) => "complete",
                ScanMessage::ExpandResult { .. } => "expand",
                ScanMessage::DeleteProgress { .. } => "del_prog",
                ScanMessage::DeleteComplete { .. } => "del_done",
                ScanMessage::Error(_) => "error",
            };

            let t = Instant::now();
            app.handle_scan_message(latest);
            // Simulate what rendering does: iterate children, build ext stats
            if let Some(node) = app.current_node() {
                let mut _sum = 0u64;
                for child in &node.children {
                    _sum += child.size;
                    // Simulate extension stats
                    let _ext = child.name.rsplit('.').next();
                }
            }
            let elapsed_us = t.elapsed().as_micros();
            frame_count += 1;

            if elapsed_us > max_frame_us {
                max_frame_us = elapsed_us;
            }

            scan_handle_times.push((elapsed_us, event_name));

            // Log any frame > 16ms
            if elapsed_us > 16_000 {
                slow_frames += 1;
                let node_count = app
                    .tree
                    .as_ref()
                    .map(|t| count_tree_nodes(&t.root))
                    .unwrap_or(0);
                println!(
                    "[{:.1}s] SLOW {:.1}ms event={} tree_nodes={} files={} dirs={}",
                    start.elapsed().as_secs_f64(),
                    elapsed_us as f64 / 1000.0,
                    event_name,
                    node_count,
                    app.scan_progress.files_count,
                    app.scan_progress.dirs_count,
                );
            }

            if matches!(event_name, "complete") {
                eprintln!("[{:.1}s] Scan complete.", start.elapsed().as_secs_f64());
            }
            if matches!(event_name, "error") {
                eprintln!("Scan error.");
                break;
            }
        }

        // Periodic summary every 5s
        if last_report.elapsed().as_secs() >= 5 {
            last_report = Instant::now();
            let node_count = app
                .tree
                .as_ref()
                .map(|t| count_tree_nodes(&t.root))
                .unwrap_or(0);

            let progress_times: Vec<u128> = scan_handle_times
                .iter()
                .filter(|(_, name)| *name == "progress")
                .map(|(us, _)| *us)
                .collect();
            let counting_times: Vec<u128> = scan_handle_times
                .iter()
                .filter(|(_, name)| *name == "counting")
                .map(|(us, _)| *us)
                .collect();

            let avg_p = if progress_times.is_empty() {
                0
            } else {
                progress_times.iter().sum::<u128>() / progress_times.len() as u128
            };
            let max_p = progress_times.iter().max().copied().unwrap_or(0);
            let avg_c = if counting_times.is_empty() {
                0
            } else {
                counting_times.iter().sum::<u128>() / counting_times.len() as u128
            };

            eprintln!(
                "[{:.0}s] frames={} slow={} max={:.1}ms | progress: n={} avg={:.1}ms max={:.1}ms | counting: n={} avg={:.0}µs | nodes={} files={} dirs={}",
                start.elapsed().as_secs_f64(),
                frame_count, slow_frames, max_frame_us as f64 / 1000.0,
                progress_times.len(), avg_p as f64 / 1000.0, max_p as f64 / 1000.0,
                counting_times.len(), avg_c as f64,
                node_count, app.scan_progress.files_count, app.scan_progress.dirs_count,
            );
            scan_handle_times.clear();
            max_frame_us = 0;
            slow_frames = 0;
        }
    }

    eprintln!(
        "=== DONE === files={} dirs={} size={}",
        app.scan_progress.files_count, app.scan_progress.dirs_count, app.scan_progress.total_size,
    );
}

fn count_tree_nodes(node: &tree::FileNode) -> usize {
    1 + node
        .children
        .iter()
        .map(|c| count_tree_nodes(c))
        .sum::<usize>()
}

mod app;
mod cli;
mod events;
mod scanner;
mod tree;
mod ui;

use anyhow::Result;
use app::App;
use clap::Parser;
use cli::Cli;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use events::{AppEvent, EventHandler};
use scanner::{ScanOptions, new_thread_activities, start_scan};
use std::time::Duration;
use tokio::sync::mpsc;

/// State needed for expand operations (persists after scan completes)
struct ExpandState {
    tx: mpsc::UnboundedSender<scanner::ScanMessage>,
    stat_threads: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut app = App::new();

    let (scan_tx, scan_rx) = mpsc::unbounded_channel();
    let stat_threads = cli.threads.unwrap_or(128);
    let expand_state = ExpandState {
        tx: scan_tx.clone(),
        stat_threads,
    };

    // Start background scan
    let thread_activities = new_thread_activities(stat_threads);
    app.thread_activities = Some(thread_activities.clone());
    let scan_options = ScanOptions {
        path: cli.path.clone(),
        same_filesystem: cli.same_filesystem,
        stat_threads,
        queue_multiplier: cli.queue_multiplier.unwrap_or(32),
        thread_activities,
    };
    let _scan_handle = start_scan(scan_options, scan_tx);

    // Initialize terminal
    let mut terminal = ratatui::init();

    // Event handler with 100ms tick rate
    let mut events = EventHandler::new(Duration::from_millis(100), scan_rx);

    // Main event loop
    loop {
        terminal.draw(|frame| ui::render(frame, &mut app))?;

        if let Some(event) = events.next().await {
            match event {
                AppEvent::Key(key) => handle_key(&mut app, key, &expand_state),
                AppEvent::Scan(msg) => app.handle_scan_message(msg),
                AppEvent::Resize => {}
                AppEvent::Tick => {}
            }
        }

        if app.should_quit {
            break;
        }
    }

    // Restore terminal
    ratatui::restore();

    // Print any error that caused quit
    if let Some(ref msg) = app.error_message {
        eprintln!("Error: {}", msg);
    }

    // Export if requested
    if let Some(ref export_path) = cli.export {
        if let Some(ref tree) = app.tree {
            let json = serde_json::to_string_pretty(tree)?;
            std::fs::write(export_path, json)?;
            eprintln!("Exported to {}", export_path.display());
        }
    }

    Ok(())
}

fn handle_key(app: &mut App, key: KeyEvent, expand_state: &ExpandState) {
    // Ctrl+C always quits
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.should_quit = true;
        return;
    }

    match &app.view {
        app::View::Scanning => match key.code {
            KeyCode::Char('q') => app.should_quit = true,
            _ => {}
        },
        app::View::Browser => match key.code {
            KeyCode::Char('q') => app.should_quit = true,
            KeyCode::Up | KeyCode::Char('k') => app.select_previous(),
            KeyCode::Down | KeyCode::Char('j') => app.select_next(),
            KeyCode::Right | KeyCode::Enter | KeyCode::Char('l') => app.enter_directory(),
            KeyCode::Left | KeyCode::Backspace | KeyCode::Char('h') => app.go_up(),
            KeyCode::Char('s') => app.cycle_sort(),
            KeyCode::Char('n') => app.sort_by_name(),
            KeyCode::Char('g') => app.show_bars = !app.show_bars,
            KeyCode::Char('d') => {
                if app.child_count() > 0 && app.active_deletion.is_none() {
                    let info = app.current_node().and_then(|node| {
                        node.children.get(app.selected_index).map(|c| (c.name.clone(), c.is_dir))
                    });
                    if let Some((name, is_dir)) = info {
                        app.delete_target_name = name;
                        app.delete_is_dir = is_dir;
                        app.delete_input.clear();
                        app.view = app::View::DeleteConfirm;
                    }
                }
            }
            KeyCode::Char(' ') => {
                if app.scanning || app.active_deletion.is_some() {
                    app.show_threads = !app.show_threads;
                }
            }
            KeyCode::Char('e') => {
                if app.is_aggregated() && !app.expanding {
                    if let Some(dir_path) = app.current_dir_path() {
                        app.expanding = true;
                        let breadcrumbs = app.breadcrumbs.clone();
                        let tx = expand_state.tx.clone();
                        let threads = expand_state.stat_threads;
                        tokio::task::spawn_blocking(move || {
                            let children = scanner::expand_directory(&dir_path, threads);
                            let _ = tx.send(scanner::ScanMessage::ExpandResult {
                                breadcrumbs,
                                children,
                            });
                        });
                    }
                }
            }
            KeyCode::Char('?') => app.view = app::View::Help,
            KeyCode::PageDown => app.page_down(20),
            KeyCode::PageUp => app.page_up(20),
            KeyCode::Home => app.select_first(),
            KeyCode::End => app.select_last(),
            _ => {}
        },
        app::View::Help => match key.code {
            KeyCode::Char('?') | KeyCode::Esc | KeyCode::Char('q') => {
                app.view = app::View::Browser
            }
            _ => {}
        },
        app::View::DeleteConfirm => {
            if app.delete_is_dir {
                // Directory: type-to-confirm
                match key.code {
                    KeyCode::Char(c) => app.delete_input.push(c),
                    KeyCode::Backspace => { app.delete_input.pop(); }
                    KeyCode::Enter => {
                        if app.delete_input == app.delete_target_name {
                            start_async_delete(app, expand_state);
                            app.view = app::View::Browser;
                        }
                    }
                    KeyCode::Esc => {
                        app.delete_input.clear();
                        app.view = app::View::Browser;
                    }
                    _ => {}
                }
            } else {
                // File: y/n confirm
                match key.code {
                    KeyCode::Char('y') => {
                        delete_selected(app);
                        app.view = app::View::Browser;
                    }
                    KeyCode::Char('n') | KeyCode::Esc => app.view = app::View::Browser,
                    _ => {}
                }
            }
        }
    }
}

/// Build the full filesystem path for the currently selected child.
fn build_selected_path(app: &App) -> Option<std::path::PathBuf> {
    let tree = app.tree.as_ref()?;
    let node = if app.breadcrumbs.is_empty() {
        &tree.root
    } else {
        tree.node_at(&app.breadcrumbs)?
    };
    node.children.get(app.selected_index)?;

    let mut path = tree.root_path.clone();
    let mut current = &tree.root;
    for &idx in &app.breadcrumbs {
        if let Some(c) = current.children.get(idx) {
            path = path.join(&c.name);
            current = c;
        }
    }
    path = path.join(&current.children[app.selected_index].name);
    Some(path)
}

/// Synchronous file deletion (for non-directory items).
fn delete_selected(app: &mut App) {
    let item_path = match build_selected_path(app) {
        Some(p) => p,
        None => return,
    };

    let result = std::fs::remove_file(&item_path);

    if result.is_ok() {
        let selected = app.selected_index;
        let deleted_size = {
            let node = app.current_node_mut().unwrap();
            let size = node.children[selected].size;
            node.children.remove(selected);
            node.child_count = node.children.len();
            size
        };

        if let Some(tree) = app.tree.as_mut() {
            tree.root.size = tree.root.size.saturating_sub(deleted_size);
            tree.total_files = tree.total_files.saturating_sub(1);
            for depth in 0..app.breadcrumbs.len() {
                let node = {
                    let mut n = &mut tree.root;
                    for &idx in &app.breadcrumbs[..=depth] {
                        n = &mut n.children[idx];
                    }
                    n
                };
                node.size = node.size.saturating_sub(deleted_size);
            }
        }

        let count = app.child_count();
        if count == 0 {
            app.selected_index = 0;
        } else if app.selected_index >= count {
            app.selected_index = count - 1;
        }
    }
}

/// Start async directory deletion in the background.
fn start_async_delete(app: &mut App, expand_state: &ExpandState) {
    let item_path = match build_selected_path(app) {
        Some(p) => p,
        None => return,
    };

    let original_size = app
        .current_node()
        .and_then(|n| n.children.get(app.selected_index))
        .map(|c| c.size)
        .unwrap_or(0);

    app.delete_breadcrumbs = app.breadcrumbs.clone();
    app.delete_child_index = app.selected_index;
    app.active_deletion = Some(app::DeletionProgress {
        name: app.delete_target_name.clone(),
        bytes_deleted: 0,
        files_deleted: 0,
        dirs_deleted: 0,
        original_size,
    });

    // Ensure thread activity panel is visible
    if app.thread_activities.is_none() {
        let activities = scanner::new_thread_activities(1);
        app.thread_activities = Some(activities);
    }
    app.show_threads = true;

    let tx = expand_state.tx.clone();
    let thread_activities = app.thread_activities.clone();
    tokio::task::spawn_blocking(move || {
        scanner::delete_directory_async(item_path, tx, thread_activities);
    });
}

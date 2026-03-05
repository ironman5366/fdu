mod app;
mod cache;
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut app = App::new();

    // Try loading from cache first (unless --no-cache)
    let cached_tree = if !cli.no_cache {
        cache::load_cache(&cli.path).unwrap_or(None)
    } else {
        None
    };

    let (scan_tx, scan_rx) = mpsc::unbounded_channel();

    if let Some(tree) = cached_tree {
        // Use cached tree, skip scanning
        app.tree = Some(tree);
        app.view = app::View::Browser;
    } else {
        // Start background scan
        let stat_threads = cli.threads.unwrap_or(128);
        let thread_activities = new_thread_activities(stat_threads);
        app.thread_activities = Some(thread_activities.clone());
        let scan_options = ScanOptions {
            path: cli.path.clone(),
            same_filesystem: cli.same_filesystem,
            stat_threads,
            thread_activities,
        };
        let _scan_handle = start_scan(scan_options, scan_tx);
    }

    // Initialize terminal
    let mut terminal = ratatui::init();

    // Event handler with 100ms tick rate
    let mut events = EventHandler::new(Duration::from_millis(100), scan_rx);

    // Main event loop
    loop {
        terminal.draw(|frame| ui::render(frame, &mut app))?;

        if let Some(event) = events.next().await {
            match event {
                AppEvent::Key(key) => handle_key(&mut app, key),
                AppEvent::Scan(msg) => app.handle_scan_message(msg),
                AppEvent::Resize => {} // ratatui handles resize on next draw
                AppEvent::Tick => {}         // just triggers a redraw
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

    // Auto-save to cache (unless --no-cache)
    if !cli.no_cache {
        if let Some(ref tree) = app.tree {
            if tree.complete {
                if let Err(e) = cache::save_cache(tree) {
                    eprintln!("Warning: could not save cache: {}", e);
                }
            }
        }
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

fn handle_key(app: &mut App, key: KeyEvent) {
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
                if app.child_count() > 0 {
                    app.view = app::View::DeleteConfirm;
                }
            }
            KeyCode::Char(' ') => {
                if app.scanning {
                    app.show_threads = !app.show_threads;
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
        app::View::DeleteConfirm => match key.code {
            KeyCode::Char('y') => {
                delete_selected(app);
                app.view = app::View::Browser;
            }
            KeyCode::Char('n') | KeyCode::Esc => app.view = app::View::Browser,
            _ => {}
        },
    }
}

fn delete_selected(app: &mut App) {
    // Get the full path of the item to delete
    let (item_path, is_dir) = {
        let tree = match app.tree.as_ref() {
            Some(t) => t,
            None => return,
        };
        let node = if app.breadcrumbs.is_empty() {
            &tree.root
        } else {
            match tree.node_at(&app.breadcrumbs) {
                Some(n) => n,
                None => return,
            }
        };
        let child = match node.children.get(app.selected_index) {
            Some(c) => c,
            None => return,
        };

        // Build full path
        let mut path = tree.root_path.clone();
        let mut current = &tree.root;
        for &idx in &app.breadcrumbs {
            if let Some(c) = current.children.get(idx) {
                path = path.join(&c.name);
                current = c;
            }
        }
        path = path.join(&child.name);
        (path, child.is_dir)
    };

    // Perform deletion
    let result = if is_dir {
        std::fs::remove_dir_all(&item_path)
    } else {
        std::fs::remove_file(&item_path)
    };

    if result.is_ok() {
        // Remove from tree and recalculate sizes
        let selected = app.selected_index;
        let deleted_size = {
            let node = app.current_node_mut().unwrap();
            let size = node.children[selected].size;
            node.children.remove(selected);
            node.child_count = node.children.len();
            size
        };

        // Update sizes up the breadcrumb chain
        if let Some(tree) = app.tree.as_mut() {
            tree.root.size = tree.root.size.saturating_sub(deleted_size);
            // Walk down the breadcrumb chain, subtracting size at each level
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

        // Fix selected index
        let count = app.child_count();
        if count == 0 {
            app.selected_index = 0;
        } else if app.selected_index >= count {
            app.selected_index = count - 1;
        }
    }
}

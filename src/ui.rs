use crate::app::{App, SortOrder, View};
use crate::tree::FileNode;
use humansize::{format_size, BINARY};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph};
use ratatui::Frame;

fn format_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result
}

pub fn render(frame: &mut Frame, app: &mut App) {
    match app.view {
        View::Scanning => render_scanning(frame, app),
        View::Browser => render_browser(frame, app),
        View::Help => {
            render_browser(frame, app);
            render_help_overlay(frame);
        }
        View::DeleteConfirm => {
            render_browser(frame, app);
            render_delete_confirm(frame, app);
        }
    }
}

fn render_scanning(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let block = Block::default()
        .title(" fdu - Scanning... ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let progress = &app.scan_progress;

    // Animated spinner
    let spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
    let spinner_idx = (progress.elapsed_secs * 10.0) as usize % spinner_chars.len();
    let spinner = spinner_chars[spinner_idx];

    // Thread activity visualization: show stat threads as a mini bar
    let thread_bar = render_thread_bar(progress.stat_threads, inner.width.saturating_sub(22) as usize);

    let elapsed_str = format_elapsed(progress.elapsed_secs);

    let text = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled(format!("  {} ", spinner), Style::default().fg(Color::Cyan)),
            Span::styled("Files: ", Style::default().fg(Color::Yellow)),
            Span::styled(
                format_num(progress.files_count),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ),
            Span::styled("  Dirs: ", Style::default().fg(Color::Yellow)),
            Span::styled(
                format_num(progress.dirs_count),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ),
            Span::styled("  Errors: ", Style::default().fg(Color::Yellow)),
            Span::raw(format_num(progress.errors_count)),
        ]),
        Line::from(vec![
            Span::styled("    Size: ", Style::default().fg(Color::Yellow)),
            Span::styled(
                format_size(progress.total_size, BINARY),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ),
            Span::styled("  Time: ", Style::default().fg(Color::Yellow)),
            Span::raw(&elapsed_str),
            Span::styled("  Rate: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{}/s", format_num(progress.entries_per_sec))),
        ]),
        Line::from(vec![
            Span::styled("    Dirs queued: ", Style::default().fg(Color::Yellow)),
            Span::raw(format_num(progress.dirs_queued as u64)),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("    Stat threads: ", Style::default().fg(Color::DarkGray)),
            Span::styled(thread_bar, Style::default().fg(Color::Green)),
            Span::styled(
                format!(" {}", progress.stat_threads),
                Style::default().fg(Color::DarkGray),
            ),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("    Scanning: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                truncate_path(&progress.current_path, inner.width.saturating_sub(16) as usize),
                Style::default().fg(Color::DarkGray),
            ),
        ]),
    ];

    let paragraph = Paragraph::new(text);
    frame.render_widget(paragraph, inner);
}

fn format_elapsed(secs: f64) -> String {
    if secs < 60.0 {
        format!("{:.1}s", secs)
    } else {
        let m = secs as u64 / 60;
        let s = secs as u64 % 60;
        format!("{}m{:02}s", m, s)
    }
}

fn render_thread_bar(threads: usize, max_width: usize) -> String {
    let bar_width = threads.min(max_width);
    "▮".repeat(bar_width)
}

fn render_browser(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // header
            Constraint::Min(1),   // list
            Constraint::Length(1), // footer
        ])
        .split(area);

    render_header(frame, app, chunks[0]);

    let visible_height = chunks[1].height as usize;
    app.ensure_visible_with_height(visible_height);

    render_list(frame, app, chunks[1]);
    render_footer(frame, app, chunks[2]);
}

fn render_header(frame: &mut Frame, app: &App, area: Rect) {
    let node = app.current_node();
    let tree = app.tree.as_ref();

    let (path, size, files, dirs) = match (tree, node) {
        (Some(tree), Some(node)) => {
            let mut path = tree.root_path.to_string_lossy().to_string();
            // Build full path from breadcrumbs
            if let Some(t) = app.tree.as_ref() {
                let mut current = &t.root;
                for &idx in &app.breadcrumbs {
                    if let Some(child) = current.children.get(idx) {
                        path = format!("{}/{}", path, child.name);
                        current = child;
                    }
                }
            }
            (
                path,
                format_size(node.size, BINARY),
                tree.total_files,
                tree.total_dirs,
            )
        }
        _ => ("/".to_string(), "0 B".to_string(), 0, 0),
    };

    let sort_indicator = match app.sort_order {
        SortOrder::SizeDesc => "size↓",
        SortOrder::SizeAsc => "size↑",
        SortOrder::NameAsc => "name↑",
        SortOrder::NameDesc => "name↓",
    };

    let title = format!(" {} ", path);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let mut spans = vec![
        Span::styled("  Total: ", Style::default().fg(Color::Yellow)),
        Span::styled(&size, Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
        Span::styled("  Files: ", Style::default().fg(Color::Yellow)),
        Span::raw(format_num(files)),
        Span::styled("  Dirs: ", Style::default().fg(Color::Yellow)),
        Span::raw(format_num(dirs)),
        Span::styled("  Sort: ", Style::default().fg(Color::Yellow)),
        Span::raw(sort_indicator.to_string()),
    ];
    if app.scanning {
        let p = &app.scan_progress;
        let spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        let idx = (p.elapsed_secs * 10.0) as usize % spinner_chars.len();
        spans.push(Span::styled(
            format!("  {} scanning ", spinner_chars[idx]),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(
            format!("{}/s ", format_num(p.entries_per_sec)),
            Style::default().fg(Color::Green),
        ));
        spans.push(Span::styled(
            format!("{}t ", p.stat_threads),
            Style::default().fg(Color::DarkGray),
        ));
    }
    let info = Line::from(spans);

    frame.render_widget(Paragraph::new(info), inner);
}

fn render_list(frame: &mut Frame, app: &App, area: Rect) {
    let node = match app.current_node() {
        Some(n) => n,
        None => return,
    };

    let parent_size = node.size;
    let visible_height = area.height as usize;

    let items: Vec<ListItem> = node
        .children
        .iter()
        .enumerate()
        .skip(app.scroll_offset)
        .take(visible_height)
        .map(|(i, child)| render_entry(child, parent_size, app.show_bars, i == app.selected_index))
        .collect();

    let list = List::new(items);
    frame.render_widget(list, area);
}

fn render_entry(
    node: &FileNode,
    parent_size: u64,
    show_bars: bool,
    is_selected: bool,
) -> ListItem<'static> {
    let size_str = format!("{:>10}", format_size(node.size, BINARY));
    let name = if node.is_dir {
        format!("{}/", node.name)
    } else {
        node.name.clone()
    };

    let bar = if show_bars {
        let ratio = if parent_size > 0 {
            node.size as f64 / parent_size as f64
        } else {
            0.0
        };
        let filled = (ratio * 16.0).round() as usize;
        let empty = 16 - filled;
        format!("[{}{}]", "#".repeat(filled), " ".repeat(empty))
    } else {
        String::new()
    };

    let base_style = if is_selected {
        Style::default()
            .bg(Color::DarkGray)
            .fg(Color::White)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };

    let name_style = if node.is_dir {
        base_style.fg(Color::Blue).add_modifier(Modifier::BOLD)
    } else {
        base_style
    };

    let bar_style = if is_selected {
        base_style.fg(Color::Green)
    } else {
        Style::default().fg(Color::Green)
    };

    let spans = if show_bars {
        vec![
            Span::styled(format!("  {} ", size_str), base_style),
            Span::styled(format!("{} ", bar), bar_style),
            Span::styled(name, name_style),
        ]
    } else {
        vec![
            Span::styled(format!("  {} ", size_str), base_style),
            Span::styled(name, name_style),
        ]
    };

    ListItem::new(Line::from(spans))
}

fn render_footer(frame: &mut Frame, app: &App, area: Rect) {
    let count = app.child_count();
    let pos = if count > 0 {
        format!("{}/{}", app.selected_index + 1, count)
    } else {
        "empty".to_string()
    };

    let footer = Line::from(vec![
        Span::styled(
            " ↑↓/jk:nav  →/enter:open  ←/bs:back  s:sort  g:bars  d:del  ?:help  q:quit ",
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            format!(" {} ", pos),
            Style::default().fg(Color::Yellow),
        ),
    ]);

    frame.render_widget(Paragraph::new(footer), area);
}

fn render_help_overlay(frame: &mut Frame) {
    let area = centered_rect(50, 70, frame.area());
    frame.render_widget(Clear, area);

    let block = Block::default()
        .title(" Help ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let help = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  ↑/k         ", Style::default().fg(Color::Cyan)),
            Span::raw("Move up"),
        ]),
        Line::from(vec![
            Span::styled("  ↓/j         ", Style::default().fg(Color::Cyan)),
            Span::raw("Move down"),
        ]),
        Line::from(vec![
            Span::styled("  →/Enter/l   ", Style::default().fg(Color::Cyan)),
            Span::raw("Open directory"),
        ]),
        Line::from(vec![
            Span::styled("  ←/Backspace/h", Style::default().fg(Color::Cyan)),
            Span::raw(" Go back"),
        ]),
        Line::from(vec![
            Span::styled("  s           ", Style::default().fg(Color::Cyan)),
            Span::raw("Cycle sort (size↓ → size↑ → name↑ → name↓)"),
        ]),
        Line::from(vec![
            Span::styled("  n           ", Style::default().fg(Color::Cyan)),
            Span::raw("Sort by name"),
        ]),
        Line::from(vec![
            Span::styled("  g           ", Style::default().fg(Color::Cyan)),
            Span::raw("Toggle size bars"),
        ]),
        Line::from(vec![
            Span::styled("  d           ", Style::default().fg(Color::Cyan)),
            Span::raw("Delete selected"),
        ]),
        Line::from(vec![
            Span::styled("  PgUp/PgDn   ", Style::default().fg(Color::Cyan)),
            Span::raw("Scroll page"),
        ]),
        Line::from(vec![
            Span::styled("  Home/End    ", Style::default().fg(Color::Cyan)),
            Span::raw("Jump to first/last"),
        ]),
        Line::from(vec![
            Span::styled("  ?/Esc       ", Style::default().fg(Color::Cyan)),
            Span::raw("Close help"),
        ]),
        Line::from(vec![
            Span::styled("  q           ", Style::default().fg(Color::Cyan)),
            Span::raw("Quit"),
        ]),
    ];

    frame.render_widget(Paragraph::new(help), inner);
}

fn render_delete_confirm(frame: &mut Frame, app: &App) {
    let area = centered_rect(50, 20, frame.area());
    frame.render_widget(Clear, area);

    let block = Block::default()
        .title(" Confirm Delete ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Red));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let name = app
        .current_node()
        .and_then(|n| n.children.get(app.selected_index))
        .map(|c| {
            let suffix = if c.is_dir { "/" } else { "" };
            format!("{}{} ({})", c.name, suffix, format_size(c.size, BINARY))
        })
        .unwrap_or_default();

    let text = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  Delete ", Style::default().fg(Color::Red)),
            Span::styled(&name, Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
            Span::styled("?", Style::default().fg(Color::Red)),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Press ", Style::default().fg(Color::DarkGray)),
            Span::styled("y", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
            Span::styled(" to confirm, ", Style::default().fg(Color::DarkGray)),
            Span::styled("n/Esc", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::styled(" to cancel", Style::default().fg(Color::DarkGray)),
        ]),
    ];

    frame.render_widget(Paragraph::new(text), inner);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn truncate_path(path: &str, max_len: usize) -> String {
    if path.len() <= max_len {
        path.to_string()
    } else if max_len > 3 {
        format!("...{}", &path[path.len() - (max_len - 3)..])
    } else {
        "...".to_string()
    }
}

//! Rendering. Pure functions of `App` state (plus ratatui's table state).

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Cell, Clear, List, ListItem, Paragraph, Row, Table, Tabs, Wrap};

use crate::activity::Severity;
use crate::api::{Connector, Kind};
use crate::app::{
    App, Conn, Focus, Level, Overlay, Panel, Preview, Screen, StateFilter, TypeFilter, Validation,
    preview_json,
};
use crate::templates::TEMPLATES;
use crate::{setup, splash};

const ACCENT: Color = Color::Cyan;
const DIM: Color = Color::DarkGray;
const OK: Color = Color::Green;
const WARN: Color = Color::Yellow;
const ERR: Color = Color::Red;

fn state_color(state: &str) -> Color {
    match state {
        "RUNNING" => OK,
        "PAUSED" | "STOPPED" => WARN,
        "FAILED" => ERR,
        _ => DIM,
    }
}

fn state_symbol(state: &str) -> &'static str {
    match state {
        "RUNNING" => "●",
        "PAUSED" | "STOPPED" => "‖",
        "FAILED" => "✖",
        _ => "○",
    }
}

fn key_hint<'a>(key: &'a str, what: &'a str) -> [Span<'a>; 2] {
    [
        Span::styled(format!(" {key} "), Style::new().fg(Color::Black).bg(DIM)),
        Span::styled(format!("{what} "), Style::new().fg(Color::Gray)),
    ]
}

fn hints(items: &[(&str, &str)]) -> Line<'static> {
    let mut spans = Vec::new();
    for (k, w) in items {
        spans.extend(key_hint(k, w).map(|s| Span::styled(s.content.to_string(), s.style)));
    }
    Line::from(spans)
}

pub fn draw(f: &mut Frame, app: &mut App) {
    match &app.screen {
        Screen::Splash(started) => return splash::draw(f, started.elapsed()),
        Screen::Setup(form) => return setup::draw(f, form),
        Screen::Main => {}
    }
    let [header, main, footer] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(4), Constraint::Length(2)]).areas(f.area());
    draw_header(f, app, header);

    let (lp, dp) = if app.focus == Focus::Details { (35, 65) } else { (55, 45) };
    let [list_area, detail_area] =
        Layout::vertical([Constraint::Percentage(lp), Constraint::Percentage(dp)]).areas(main);
    draw_list(f, app, list_area);
    draw_details(f, app, detail_area);
    draw_footer(f, app, footer);
    draw_overlay(f, app);
}

fn draw_header(f: &mut Frame, app: &App, area: Rect) {
    let left = Line::from(vec![
        Span::styled(" Tuizium ", Style::new().fg(Color::Black).bg(ACCENT).add_modifier(Modifier::BOLD)),
        Span::raw("  cluster "),
        Span::styled(app.cluster.clone(), Style::new().fg(ACCENT).add_modifier(Modifier::BOLD)),
        Span::styled(
            format!(
                "  ({} profile{})",
                app.config.clusters.len(),
                if app.config.clusters.len() == 1 { "" } else { "s" }
            ),
            Style::new().fg(DIM),
        ),
    ]);
    let status = match &app.conn {
        Conn::Connecting => Span::styled("◌ connecting…", Style::new().fg(WARN)),
        Conn::Connected(root) => {
            let v = root.as_ref().map(|r| r.version.as_str()).filter(|v| !v.is_empty());
            Span::styled(
                match v {
                    Some(v) => format!("● connected · Kafka Connect {v}"),
                    None => "● connected".to_string(),
                },
                Style::new().fg(OK),
            )
        }
        Conn::Failed(e) => {
            let max = (area.width as usize / 2).saturating_sub(4).max(10);
            Span::styled(format!("✖ {}", truncate(e.lines().next().unwrap_or(""), max)), Style::new().fg(ERR))
        }
    };
    f.render_widget(Paragraph::new(left), area);
    f.render_widget(
        Paragraph::new(Line::from(vec![status, Span::raw(" ")])).alignment(Alignment::Right),
        area,
    );
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut t: String = s.chars().take(max.saturating_sub(1)).collect();
        t.push('…');
        t
    }
}

fn draw_list(f: &mut Frame, app: &mut App, area: Rect) {
    let mut title = vec![Span::styled(
        format!(" Connectors {}/{} ", app.visible.len(), app.connectors.len()),
        Style::new().add_modifier(Modifier::BOLD),
    )];
    let mut tag = |t: String| title.push(Span::styled(format!("[{t}] "), Style::new().fg(WARN)));
    if !app.marked.is_empty() {
        tag(format!("{} marked", app.marked.len()));
    }
    if app.type_filter != TypeFilter::All {
        tag(format!("type:{}", app.type_filter.label()));
    }
    if app.state_filter != StateFilter::All {
        tag(format!("state:{}", app.state_filter.label()));
    }
    if !app.query.is_empty() {
        tag(format!("/{}", app.query));
    }
    title.push(Span::styled(
        format!("sort:{}{} ", app.sort.label(), if app.sort_desc { "↓" } else { "↑" }),
        Style::new().fg(DIM),
    ));
    let border = if app.focus == Focus::List { ACCENT } else { DIM };
    let block = Block::bordered().border_style(Style::new().fg(border)).title(Line::from(title));

    if app.visible.is_empty() {
        let msg = match (&app.conn, app.connectors.is_empty()) {
            (Conn::Connecting, _) => "Connecting…".to_string(),
            (Conn::Failed(e), true) => {
                format!("Cannot load connectors.\n\n{e}\n\nPress r to retry, c to switch cluster.")
            }
            (_, true) => "No connectors. Press n to import JSON or t to use a template.".to_string(),
            _ => "No connectors match the current filters. Press Esc to clear them.".to_string(),
        };
        f.render_widget(
            Paragraph::new(msg).wrap(Wrap { trim: false }).style(Style::new().fg(DIM)).block(block),
            area,
        );
        return;
    }

    let header = Row::new(["NAME", "TYPE", "STATE", "TASKS", "CONNECTOR PLUGIN"])
        .style(Style::new().fg(DIM).add_modifier(Modifier::BOLD));
    let rows: Vec<Row> = app
        .visible
        .iter()
        .map(|&i| {
            let c = &app.connectors[i];
            let failed = c.is_failed();
            let sc = if failed { ERR } else { state_color(&c.state) };
            let kind_color = match c.kind {
                Kind::Source => Color::Blue,
                Kind::Sink => Color::Magenta,
                Kind::Unknown => DIM,
            };
            let row = Row::new(vec![
                Cell::from(if app.marked.contains(&c.name) {
                    format!("✓ {}", c.name)
                } else {
                    c.name.clone()
                }),
                Cell::from(c.kind.label()).style(Style::new().fg(kind_color)),
                Cell::from(format!("{} {}", state_symbol(if failed { "FAILED" } else { &c.state }), c.state))
                    .style(Style::new().fg(sc)),
                Cell::from(format!("{}/{}", c.running_tasks(), c.tasks.len())),
                Cell::from(c.class.clone()).style(Style::new().fg(DIM)),
            ]);
            if failed { row.style(Style::new().fg(ERR)) } else { row }
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Fill(2),
            Constraint::Length(7),
            Constraint::Length(14),
            Constraint::Length(6),
            Constraint::Fill(3),
        ],
    )
    .header(header)
    .block(block)
    .column_spacing(2)
    .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED))
    .highlight_symbol("▶ ");
    f.render_stateful_widget(table, area, &mut app.table);
}

fn draw_details(f: &mut Frame, app: &App, area: Rect) {
    let border = if app.focus == Focus::Details { ACCENT } else { DIM };
    let name = app.selected().map(|c| c.name.as_str()).unwrap_or("—");
    let block = Block::bordered()
        .border_style(Style::new().fg(border))
        .title(Line::from(Span::styled(format!(" {name} "), Style::new().add_modifier(Modifier::BOLD))));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let [tabs_area, body] = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).areas(inner);
    let titles: Vec<String> =
        Panel::ALL.iter().enumerate().map(|(i, p)| format!("{} {}", i + 1, p.title())).collect();
    f.render_widget(
        Tabs::new(titles)
            .select(Panel::ALL.iter().position(|p| *p == app.panel).unwrap_or(0))
            .style(Style::new().fg(DIM))
            .highlight_style(Style::new().fg(ACCENT).add_modifier(Modifier::BOLD | Modifier::UNDERLINED)),
        tabs_area,
    );
    let Some(c) = app.selected() else {
        f.render_widget(
            Paragraph::new("Select a connector to see its details.").style(Style::new().fg(DIM)),
            body,
        );
        return;
    };
    match app.panel {
        Panel::Overview => f.render_widget(
            Paragraph::new(overview(c, app)).wrap(Wrap { trim: false }).scroll((app.scroll, 0)),
            body,
        ),
        Panel::Tasks => draw_tasks(f, app, c, body),
        Panel::Config => {
            let json = serde_json::to_string_pretty(&c.config).unwrap_or_default();
            f.render_widget(Paragraph::new(json).scroll((app.scroll, 0)), body);
        }
        Panel::Trace => f.render_widget(
            Paragraph::new(trace_text(c)).wrap(Wrap { trim: false }).scroll((app.scroll, 0)),
            body,
        ),
    }
}

/// Widest label shown in the overview (`database.hostname`), so values never touch their key.
const KEY_WIDTH: usize = 17;

fn kv<'a>(k: &'a str, v: Span<'a>) -> Line<'a> {
    Line::from(vec![Span::styled(format!("{k:<KEY_WIDTH$} "), Style::new().fg(DIM)), v])
}

fn overview(c: &Connector, app: &App) -> Text<'static> {
    let sc = state_color(&c.state);
    let mut lines = vec![
        kv("Name", Span::raw(c.name.clone())),
        kv("State", Span::styled(format!("{} {}", state_symbol(&c.state), c.state), Style::new().fg(sc))),
        kv("Type", Span::raw(c.kind.label())),
        kv("Class", Span::raw(c.class.clone())),
        kv(
            "Plugin ver.",
            Span::raw(
                app.plugins
                    .iter()
                    .find(|p| p.class == c.class)
                    .map(|p| p.version.clone())
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| "unknown".into()),
            ),
        ),
        kv("Worker", Span::raw(c.worker_id.clone())),
        kv("Tasks", Span::raw(format!("{} running / {} total", c.running_tasks(), c.tasks.len()))),
    ];
    if let Some(v) = c.config.get("tasks.max").and_then(|v| v.as_str()) {
        lines.push(kv("tasks.max", Span::raw(v.to_string())));
    }
    for key in ["topics", "topic.prefix", "database.hostname", "connection.url"] {
        if let Some(v) = c.config.get(key).and_then(|v| v.as_str()) {
            lines.push(kv(key, Span::raw(v.to_string())));
        }
    }
    let failed = c.tasks.iter().filter(|t| t.state == "FAILED").count();
    if c.state == "FAILED" || failed > 0 {
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            format!(
                "⚠ {} failed task(s){} — see Trace (4)",
                failed,
                if c.state == "FAILED" { " and failed connector" } else { "" }
            ),
            Style::new().fg(ERR),
        ));
    }
    Text::from(lines)
}

fn draw_tasks(f: &mut Frame, app: &App, c: &Connector, area: Rect) {
    if c.tasks.is_empty() {
        f.render_widget(Paragraph::new("No tasks assigned.").style(Style::new().fg(DIM)), area);
        return;
    }
    let rows: Vec<Row> = c
        .tasks
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let err = t.trace.as_deref().and_then(|s| s.lines().next()).unwrap_or("");
            let mut row = Row::new(vec![
                Cell::from(t.id.to_string()),
                Cell::from(format!("{} {}", state_symbol(&t.state), t.state))
                    .style(Style::new().fg(state_color(&t.state))),
                Cell::from(t.worker_id.clone()),
                Cell::from(err.to_string()).style(Style::new().fg(ERR)),
            ]);
            if app.focus == Focus::Details && i == app.task_sel {
                row = row.style(Style::new().add_modifier(Modifier::REVERSED));
            } else if i == app.task_sel {
                row = row.style(Style::new().add_modifier(Modifier::BOLD));
            }
            row
        })
        .collect();
    let table = Table::new(
        rows,
        [Constraint::Length(4), Constraint::Length(13), Constraint::Length(24), Constraint::Fill(1)],
    )
    .header(
        Row::new(["ID", "STATE", "WORKER", "ERROR"]).style(Style::new().fg(DIM).add_modifier(Modifier::BOLD)),
    )
    .column_spacing(2);
    f.render_widget(table, area);
}

fn trace_text(c: &Connector) -> Text<'static> {
    let mut lines: Vec<Line> = Vec::new();
    let mut section = |title: String, trace: &str| {
        lines.push(Line::styled(title, Style::new().fg(ERR).add_modifier(Modifier::BOLD)));
        lines.extend(trace.lines().map(|l| Line::raw(l.replace('\t', "    "))));
        lines.push(Line::raw(""));
    };
    if let Some(t) = &c.trace {
        section("connector".into(), t);
    }
    for t in &c.tasks {
        if let Some(tr) = &t.trace {
            section(format!("task {}", t.id), tr);
        }
    }
    if lines.is_empty() {
        lines.push(Line::styled("No failures reported. 🎉", Style::new().fg(OK)));
    }
    Text::from(lines)
}

fn draw_footer(f: &mut Frame, app: &App, area: Rect) {
    let [l1, l2] = Layout::vertical([Constraint::Length(1), Constraint::Length(1)]).areas(area);
    let line = if app.focus == Focus::Details {
        let mut h = vec![("j/k", "scroll"), ("Tab", "panel"), ("Esc", "back")];
        h.push(("R", if app.panel == Panel::Tasks { "restart task" } else { "restart" }));
        h.extend([("p/u", "pause/resume"), ("e", "edit"), ("?", "all keys")]);
        hints(&h)
    } else {
        // Only the everyday keys; everything else is one `?` away.
        hints(&[
            ("⏎", "details"),
            ("/", "search"),
            ("␣", "mark"),
            ("p/u", "pause/resume"),
            ("R", "restart"),
            ("d", "delete"),
            ("n", "new"),
            ("L", "activity"),
            ("?", "all keys"),
            ("q", "quit"),
        ])
    };
    f.render_widget(Paragraph::new(line), l1);

    let second = if app.searching {
        Line::from(vec![
            Span::styled("/", Style::new().fg(ACCENT).add_modifier(Modifier::BOLD)),
            Span::raw(app.query.clone()),
            Span::styled("▌", Style::new().fg(ACCENT)),
            Span::styled("   Enter keep · Esc clear", Style::new().fg(DIM)),
        ])
    } else if let Some(t) = &app.toast {
        let color = match t.level {
            Level::Error => ERR,
            Level::Warn => WARN,
            Level::Info => ACCENT,
        };
        Line::styled(
            format!(" {}", truncate(&t.text, (area.width as usize).saturating_sub(2))),
            Style::new().fg(color),
        )
    } else {
        let refreshed = match app.last_refresh {
            Some(t) => format!("refreshed {}s ago", t.elapsed().as_secs()),
            None => "not refreshed yet".into(),
        };
        let auto = if app.auto_refresh { "auto-refresh on (a)" } else { "auto-refresh off (a)" };
        let spin = if app.loading { " ⟳" } else { "" };
        let mut spans = vec![Span::styled(format!(" {refreshed}{spin} · {auto}"), Style::new().fg(DIM))];
        // The last journal entry stays visible after toasts expire.
        if let Some(e) = app.log.last() {
            let used: usize = spans.iter().map(|s| s.content.chars().count()).sum();
            let room = (area.width as usize).saturating_sub(used + 12);
            if room > 8 {
                spans.push(Span::styled(
                    format!(" · {} {}", e.time, truncate(&e.text, room)),
                    Style::new().fg(severity_color(e.severity)),
                ));
            }
        }
        Line::from(spans)
    };
    f.render_widget(Paragraph::new(second), l2);
}

// ---- overlays -------------------------------------------------------------

fn centered(area: Rect, w: u16, h: u16) -> Rect {
    let w = w.min(area.width);
    let h = h.min(area.height);
    Rect::new(area.x + (area.width - w) / 2, area.y + (area.height - h) / 2, w, h)
}

fn popup(f: &mut Frame, w: u16, h: u16, title: &str, color: Color) -> Rect {
    let area = centered(f.area(), w, h);
    f.render_widget(Clear, area);
    let block = Block::bordered().border_style(Style::new().fg(color)).title(Line::from(Span::styled(
        format!(" {title} "),
        Style::new().fg(color).add_modifier(Modifier::BOLD),
    )));
    let inner = block.inner(area);
    f.render_widget(block, area);
    inner
}

fn draw_overlay(f: &mut Frame, app: &App) {
    match &app.overlay {
        Overlay::None => {}
        Overlay::Help => draw_help(f),
        Overlay::Error(e) => {
            let inner = popup(f, 90, 18, "Error", ERR);
            let [body, foot] = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(inner);
            f.render_widget(Paragraph::new(e.as_str()).wrap(Wrap { trim: false }), body);
            f.render_widget(hints(&[("Esc", "close")]), foot);
        }
        Overlay::Confirm(c) => {
            let h = (c.body.lines().count().min(100) as u16 + 4).clamp(8, 24);
            let inner = popup(f, 64, h, &c.title, ERR);
            let [body, foot] = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(inner);
            f.render_widget(Paragraph::new(c.body.as_str()).wrap(Wrap { trim: false }), body);
            f.render_widget(hints(&[("y", "confirm delete"), ("Esc", "cancel")]), foot);
        }
        Overlay::Clusters(sel) => {
            let h = app.config.clusters.len() as u16 + 4;
            let inner = popup(f, 70, h.max(6), "Switch cluster", ACCENT);
            let [body, foot] = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(inner);
            let items: Vec<ListItem> = app
                .config
                .clusters
                .iter()
                .enumerate()
                .map(|(i, (name, c))| {
                    let cur = if *name == app.cluster { "●" } else { " " };
                    let auth = if c.token_env.is_some() {
                        " [token]"
                    } else if c.username.is_some() {
                        " [basic]"
                    } else {
                        ""
                    };
                    let style =
                        if i == *sel { Style::new().add_modifier(Modifier::REVERSED) } else { Style::new() };
                    ListItem::new(format!("{cur} {} {name:<14} {}{auth}", i + 1, c.url)).style(style)
                })
                .collect();
            f.render_widget(List::new(items), body);
            f.render_widget(hints(&[("⏎/1-9", "switch"), ("a", "add profile"), ("Esc", "close")]), foot);
        }
        Overlay::AddCluster(a) => {
            let inner = popup(f, 78, 8, "Add cluster profile", ACCENT);
            let lines = vec![
                Line::styled("<name> <url> [username password_env]", Style::new().fg(DIM)),
                Line::from(vec![
                    Span::raw("> "),
                    Span::raw(a.input.clone()),
                    Span::styled("▌", Style::new().fg(ACCENT)),
                ]),
                Line::styled(
                    "Passwords are never stored; password_env is an env var NAME.",
                    Style::new().fg(DIM),
                ),
                Line::styled(a.error.clone().unwrap_or_default(), Style::new().fg(ERR)),
                hints(&[("⏎", "save"), ("Esc", "back")]),
            ];
            f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
        }
        Overlay::Templates(sel) => {
            let inner = popup(f, 80, 10, "New from template", ACCENT);
            let [body, foot] = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(inner);
            let items: Vec<ListItem> = TEMPLATES
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    let style =
                        if i == *sel { Style::new().add_modifier(Modifier::REVERSED) } else { Style::new() };
                    ListItem::new(vec![
                        Line::styled(format!(" {}", t.title), style.add_modifier(Modifier::BOLD)),
                        Line::styled(format!("   {}", t.description), style.fg(DIM)),
                    ])
                })
                .collect();
            f.render_widget(List::new(items), body);
            f.render_widget(hints(&[("⏎", "select"), ("j/k", "move"), ("Esc", "close")]), foot);
        }
        Overlay::Form(form) => {
            let t = &TEMPLATES[form.template];
            let h = form.fields.len() as u16 + 5;
            let inner = popup(f, 88, h, t.title, ACCENT);
            let [body, foot] = Layout::vertical([Constraint::Min(1), Constraint::Length(2)]).areas(inner);
            let lines: Vec<Line> = form
                .fields
                .iter()
                .enumerate()
                .map(|(i, fld)| {
                    let sel = i == form.selected;
                    let shown =
                        if fld.secret { "•".repeat(fld.value.chars().count()) } else { fld.value.clone() };
                    let mut spans = vec![
                        Span::styled(if sel { "▶ " } else { "  " }, Style::new().fg(ACCENT)),
                        Span::styled(
                            format!("{:<28}", fld.label),
                            Style::new().fg(if sel { ACCENT } else { Color::Gray }),
                        ),
                        Span::styled(
                            shown,
                            Style::new().add_modifier(if sel { Modifier::BOLD } else { Modifier::empty() }),
                        ),
                    ];
                    if sel {
                        spans.push(Span::styled("▌", Style::new().fg(ACCENT)));
                    }
                    Line::from(spans)
                })
                .collect();
            f.render_widget(Paragraph::new(lines), body);
            let err = form.error.clone().unwrap_or_default();
            f.render_widget(
                Paragraph::new(vec![
                    Line::styled(err, Style::new().fg(ERR)),
                    hints(&[("Tab/↑↓", "field"), ("⏎", "preview"), ("^U", "clear"), ("Esc", "back")]),
                ]),
                foot,
            );
        }
        Overlay::Import(i) => {
            let inner = popup(f, 90, 9, "Create connector from JSON", ACCENT);
            let lines = vec![
                Line::styled(
                    "Path to a .json file (or paste inline JSON starting with '{').",
                    Style::new().fg(DIM),
                ),
                Line::styled(
                    "Accepts {\"name\":..,\"config\":{..}} or a flat config containing \"name\".",
                    Style::new().fg(DIM),
                ),
                Line::from(vec![
                    Span::raw("> "),
                    Span::raw(i.input.clone()),
                    Span::styled("▌", Style::new().fg(ACCENT)),
                ]),
                Line::styled(i.error.clone().unwrap_or_default(), Style::new().fg(ERR)),
                hints(&[("⏎", "preview"), ("^U", "clear"), ("Esc", "cancel")]),
            ];
            f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
        }
        Overlay::Preview(p) => draw_preview(f, app, p),
        Overlay::Plugins(sel) => draw_plugins(f, app, *sel),
        Overlay::Activity(off) => draw_activity(f, app, *off),
    }
}

fn draw_preview(f: &mut Frame, app: &App, p: &Preview) {
    let verb = if p.update { "Update" } else { "Create" };
    let inner = popup(f, 100, 32, &format!("{verb} \"{}\" on {} ?", p.name, app.cluster), WARN);
    let (status, status_color, details): (String, Color, &[String]) = match &p.validation {
        Validation::Pending => ("… validating against the connector plugin".into(), DIM, &[]),
        Validation::Ok => ("✔ configuration is valid".into(), OK, &[]),
        Validation::Errors(e) => {
            (format!("✖ {} validation error(s) — fix them before saving", e.len()), ERR, e)
        }
        Validation::Unavailable(e) => {
            (format!("! could not validate ({}) — saving is still allowed", truncate(e, 60)), WARN, &[])
        }
    };
    let err_rows = details.len().min(6) as u16;
    let [body, val, foot] =
        Layout::vertical([Constraint::Min(1), Constraint::Length(1 + err_rows), Constraint::Length(2)])
            .areas(inner);
    f.render_widget(Paragraph::new(preview_json(&p.name, &p.config)).scroll((p.scroll, 0)), body);
    let mut lines = vec![Line::styled(status, Style::new().fg(status_color))];
    lines.extend(details.iter().take(6).map(|e| Line::styled(format!("  {e}"), Style::new().fg(ERR))));
    f.render_widget(Paragraph::new(lines), val);
    let (endpoint, action) =
        if p.update { ("PUT /connectors/{name}/config", "save") } else { ("POST /connectors", "create") };
    f.render_widget(
        Paragraph::new(vec![
            Line::styled(
                format!("{endpoint} — secrets are masked in this preview only."),
                Style::new().fg(DIM),
            ),
            hints(&[("⏎", action), ("j/k", "scroll"), ("Esc", "back")]),
        ]),
        foot,
    );
}

fn draw_plugins(f: &mut Frame, app: &App, sel: usize) {
    let inner = popup(f, 100, 24, &format!("Plugins on {}", app.cluster), ACCENT);
    let [body, foot] = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).areas(inner);
    if app.plugins.is_empty() {
        f.render_widget(
            Paragraph::new("No plugin list available (yet). Press r to refresh.").style(Style::new().fg(DIM)),
            body,
        );
    } else {
        let deb = app.plugins.iter().filter(|p| p.is_debezium()).count();
        let rows: Vec<Row> = app
            .plugins
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let color = if p.is_debezium() { ACCENT } else { Color::Gray };
                let row = Row::new(vec![
                    Cell::from(p.class.clone()).style(Style::new().fg(color)),
                    Cell::from(p.kind.clone()),
                    Cell::from(p.version.clone()),
                ]);
                if i == sel { row.style(Style::new().add_modifier(Modifier::REVERSED)) } else { row }
            })
            .collect();
        let offset = sel.saturating_sub(body.height.saturating_sub(2) as usize);
        let table = Table::new(rows, [Constraint::Fill(1), Constraint::Length(7), Constraint::Length(16)])
            .header(
                Row::new([format!("CLASS ({deb} Debezium)"), "TYPE".into(), "VERSION".into()])
                    .style(Style::new().fg(DIM).add_modifier(Modifier::BOLD)),
            )
            .column_spacing(2);
        let mut state = ratatui::widgets::TableState::default().with_offset(offset);
        f.render_stateful_widget(table, body, &mut state);
    }
    f.render_widget(hints(&[("j/k", "move"), ("Esc", "close")]), foot);
}

fn severity_color(s: Severity) -> Color {
    match s {
        Severity::Info => DIM,
        Severity::Ok => OK,
        Severity::Warn => WARN,
        Severity::Error => ERR,
    }
}

fn draw_activity(f: &mut Frame, app: &App, off: usize) {
    let inner = popup(f, 120, 30, &format!("Activity on {} (newest first)", app.cluster), ACCENT);
    let [body, foot] = Layout::vertical([Constraint::Min(1), Constraint::Length(2)]).areas(inner);
    if app.log.entries.is_empty() {
        f.render_widget(
            Paragraph::new(
                "Nothing yet. Pause, resume, restart, edit or delete something and it is recorded here.",
            )
            .style(Style::new().fg(DIM)),
            body,
        );
    } else {
        let width = body.width.saturating_sub(12) as usize;
        let lines: Vec<Line> = app
            .log
            .entries
            .iter()
            .rev()
            .skip(off)
            .take(body.height as usize)
            .map(|e| {
                let color = severity_color(e.severity);
                let mark = match e.severity {
                    Severity::Ok => "✔",
                    Severity::Warn => "!",
                    Severity::Error => "✖",
                    Severity::Info => "·",
                };
                Line::from(vec![
                    Span::styled(format!("{} ", e.time), Style::new().fg(DIM)),
                    Span::styled(format!("{mark} "), Style::new().fg(color)),
                    Span::styled(truncate(&e.text, width), Style::new().fg(color)),
                ])
            })
            .collect();
        f.render_widget(Paragraph::new(lines), body);
    }
    let file = match app.log.path() {
        Some(p) if app.log.file_failed => format!("could not write {}", p.display()),
        Some(p) => format!("also saved to {}", p.display()),
        None => "not saved to disk".to_string(),
    };
    let color = if app.log.file_failed { ERR } else { DIM };
    f.render_widget(
        Paragraph::new(vec![
            Line::styled(truncate(&file, foot.width as usize), Style::new().fg(color)),
            hints(&[("j/k", "scroll"), ("g/G", "newest/oldest"), ("Esc", "close")]),
        ]),
        foot,
    );
}

fn draw_help(f: &mut Frame) {
    let rows = [
        ("j/k ↑/↓", "Navigate (scroll in details)"),
        ("g / G", "Top / bottom"),
        ("Enter", "Focus connector details"),
        ("Esc", "Back / clear filters"),
        ("Tab, 1-4", "Details panel: overview, tasks, config, trace"),
        ("/", "Search by name"),
        ("f / s", "Cycle type / state filter"),
        ("o / O", "Cycle sort column / reverse"),
        ("r / a", "Refresh now / toggle auto refresh"),
        ("Space", "Mark connector (batch p/u/R/d/F act on marks)"),
        ("p / u", "Pause / resume connector(s)"),
        ("R", "Restart connector(s) (task, in Tasks panel)"),
        ("F", "Restart only failed parts (marked, or all failed)"),
        ("e", "Edit config in $EDITOR, validate, then save"),
        ("P", "Installed plugins (Debezium versions)"),
        ("L", "Activity log: what was done, and what was observed"),
        ("d", "Delete (with confirmation)"),
        ("n", "New connector from JSON file / inline"),
        ("t", "New connector from template"),
        ("c", "Switch cluster (a: add profile)"),
        ("?", "This help"),
        ("q, Ctrl+C", "Quit"),
    ];
    let inner = popup(f, 70, rows.len() as u16 + 4, "Help — Tuizium", ACCENT);
    let mut lines: Vec<Line> = rows
        .iter()
        .map(|(k, d)| {
            Line::from(vec![Span::styled(format!("{k:<12}"), Style::new().fg(ACCENT)), Span::raw(*d)])
        })
        .collect();
    lines.push(Line::raw(""));
    lines.push(Line::styled("Config: see README (TUIZIUM_CONFIG overrides the path)", Style::new().fg(DIM)));
    f.render_widget(Paragraph::new(lines), inner);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::Severity;
    use crate::api::{Snapshot, Task};
    use crate::app::Msg;
    use crate::config::Config;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::crossterm::event::{KeyCode, KeyEvent};

    fn screen(term: &Terminal<TestBackend>) -> String {
        term.backend().buffer().content().iter().map(|c| c.symbol()).collect()
    }

    #[tokio::test]
    async fn renders_every_screen_at_all_sizes_without_panicking() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut cfg = Config::with_local_default();
        cfg.clusters.get_mut("local").unwrap().url = "http://127.0.0.1:1".into();
        let mut app = App::new(cfg, None, "local".into(), tx);
        let generation = 1;
        let mut c = Connector {
            name: "orders-cdc".into(),
            kind: Kind::Source,
            state: "RUNNING".into(),
            worker_id: "w:8083".into(),
            class: "io.debezium.connector.postgresql.PostgresConnector".into(),
            config: serde_json::Map::new(),
            tasks: vec![Task {
                id: 0,
                state: "FAILED".into(),
                worker_id: "w".into(),
                trace: Some("boom\n\tat x".into()),
            }],
            trace: None,
        };
        c.config.insert("tasks.max".into(), "1".into());
        app.update(Msg::Loaded {
            generation,
            result: Ok(Snapshot { root: None, connectors: vec![c], plugins: None }),
        });

        for (w, h) in [(120, 40), (80, 24), (40, 12), (10, 4), (1, 1)] {
            let mut term = Terminal::new(TestBackend::new(w, h)).unwrap();
            for keys in [
                "", "\n", "2", "3", "4", "?", "\x1b", "t", "\n", "\n", "\x1b\x1b", "c", "a", "\x1b", "n",
                "\x1b", "d",
            ] {
                for ch in keys.chars() {
                    let code = match ch {
                        '\n' => KeyCode::Enter,
                        '\x1b' => KeyCode::Esc,
                        c => KeyCode::Char(c),
                    };
                    app.update(Msg::Key(KeyEvent::from(code)));
                }
                term.draw(|f| draw(f, &mut app)).unwrap();
            }
        }
        let mut term = Terminal::new(TestBackend::new(120, 40)).unwrap();
        app.update(Msg::Key(KeyEvent::from(KeyCode::Esc)));
        app.update(Msg::Key(KeyEvent::from(KeyCode::Esc)));
        term.draw(|f| draw(f, &mut app)).unwrap();
        let s = screen(&term);
        assert!(s.contains("Tuizium") && s.contains("orders-cdc") && s.contains("PostgresConnect"));
    }

    #[tokio::test]
    async fn server_supplied_text_cannot_inject_terminal_escapes_and_activity_renders() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut cfg = Config::with_local_default();
        cfg.clusters.get_mut("local").unwrap().url = "http://127.0.0.1:1".into();
        let mut app = App::new(cfg, None, "local".into(), tx);
        let evil = "\x1b[2J\x1b]0;pwned\x07\x1b[31mred\r\x08";
        let c = Connector {
            name: format!("n{evil}"),
            kind: Kind::Sink,
            state: format!("S{evil}"),
            worker_id: format!("w{evil}"),
            class: format!("c{evil}"),
            config: [("k".to_string(), serde_json::Value::String(format!("v{evil}")))].into_iter().collect(),
            tasks: vec![Task {
                id: 0,
                state: format!("T{evil}"),
                worker_id: evil.into(),
                trace: Some(format!("t{evil}")),
            }],
            trace: Some(format!("c{evil}")),
        };
        app.update(Msg::Loaded {
            generation: 1,
            result: Ok(Snapshot { root: None, connectors: vec![c], plugins: None }),
        });
        for keys in ['1', '2', '3', '4'] {
            app.update(Msg::Key(KeyEvent::from(KeyCode::Char(keys))));
            let mut term = Terminal::new(TestBackend::new(120, 40)).unwrap();
            term.draw(|f| draw(f, &mut app)).unwrap();
            let s = screen(&term);
            assert!(!s.contains('\x1b') && !s.contains('\x07') && !s.contains('\r') && !s.contains('\x08'));
        }
        // The journal overlay at every size, including one with hostile and very long text.
        for i in 0..60 {
            app.log.record("local", Severity::Ok, &format!("entry {i} {evil}{}", "x".repeat(300)));
        }
        app.update(Msg::Key(KeyEvent::from(KeyCode::Char('L'))));
        for (w, h) in [(120, 40), (80, 24), (40, 12), (10, 4), (1, 1)] {
            let mut term = Terminal::new(TestBackend::new(w, h)).unwrap();
            term.draw(|f| draw(f, &mut app)).unwrap();
            let s = screen(&term);
            assert!(!s.contains('\x1b') && !s.contains('\x07'));
        }
        let mut term = Terminal::new(TestBackend::new(120, 40)).unwrap();
        term.draw(|f| draw(f, &mut app)).unwrap();
        assert!(screen(&term).contains("Activity on local"));
    }
}

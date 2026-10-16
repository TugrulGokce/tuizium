//! Opening screen: the colored logo mark with the "Tuizium" wordmark under it,
//! both rippling like a wave. The mark is the Tuizium logo (`assets/logo.png`), pre-sampled into
//! `assets/logo.bin` (width, height, then RGBA per pixel) and drawn with
//! half-block characters, two pixels per terminal cell.

use std::time::Duration;

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

const LOGO: &[u8] = include_bytes!("../assets/logo.bin");

use std::sync::atomic::AtomicBool;

/// True while the splash is on screen; the frame ticker stops once it is cleared.
pub static ACTIVE: AtomicBool = AtomicBool::new(false);

const LIME: (u8, u8, u8) = (170, 240, 20);
const MINT: (u8, u8, u8) = (0, 205, 130);
const TEAL: (u8, u8, u8) = (0, 120, 120);

type Rgb = (u8, u8, u8);

struct Bitmap {
    w: usize,
    h: usize,
    /// Row-major; `None` = transparent.
    px: Vec<Option<Rgb>>,
}

fn decode(data: &[u8]) -> Bitmap {
    let (w, h) = (usize::from(data[0]), usize::from(data[1]));
    let px = data[2..]
        .as_chunks::<4>()
        .0
        .iter()
        .take(w * h)
        .map(|p| (p[3] > 0).then_some((p[0], p[1], p[2])))
        .collect();
    Bitmap { w, h, px }
}

fn lerp(a: Rgb, b: Rgb, t: f32) -> Rgb {
    let t = t.clamp(0.0, 1.0);
    let m = |x: u8, y: u8| (f32::from(x) + (f32::from(y) - f32::from(x)) * t).round() as u8;
    (m(a.0, b.0), m(a.1, b.1), m(a.2, b.2))
}

fn color(c: Rgb) -> Color {
    Color::Rgb(c.0, c.1, c.2)
}

/// Two vertically stacked pixels as one cell.
fn cell(top: Option<Rgb>, bottom: Option<Rgb>) -> Span<'static> {
    match (top, bottom) {
        (None, None) => Span::raw(" "),
        (Some(t), None) => Span::styled("▀", Style::new().fg(color(t))),
        (None, Some(b)) => Span::styled("▄", Style::new().fg(color(b))),
        (Some(t), Some(b)) => Span::styled("▀", Style::new().fg(color(t)).bg(color(b))),
    }
}

/// Terminal rows of the logo mark.
fn logo_rows() -> Vec<Line<'static>> {
    let bmp = decode(LOGO);
    (0..bmp.h / 2)
        .map(|r| {
            Line::from(
                (0..bmp.w)
                    .map(|x| cell(bmp.px[2 * r * bmp.w + x], bmp.px[(2 * r + 1) * bmp.w + x]))
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

// ---- wordmark -----------------------------------------------------------

const GLYPH_H: usize = 7;
const GLYPH_W: usize = 5;

fn glyph(c: char) -> [&'static str; GLYPH_H] {
    match c {
        'T' => ["#####", "..#..", "..#..", "..#..", "..#..", "..#..", "..#.."],
        'U' => ["#...#", "#...#", "#...#", "#...#", "#...#", "#...#", ".###."],
        'I' => ["#####", "..#..", "..#..", "..#..", "..#..", "..#..", "#####"],
        'Z' => ["#####", "....#", "...#.", "..#..", ".#...", "#....", "#####"],
        'M' => ["#...#", "##.##", "#.#.#", "#.#.#", "#...#", "#...#", "#...#"],
        _ => ["....."; GLYPH_H],
    }
}

/// Pixel rows of `text` (true = ink), letters separated by one empty column.
fn text_pixels(text: &str) -> Vec<Vec<bool>> {
    let mut rows = vec![Vec::new(); GLYPH_H + 1];
    for (i, ch) in text.chars().enumerate() {
        for (r, row) in rows.iter_mut().enumerate().take(GLYPH_H) {
            if i > 0 {
                row.push(false);
            }
            row.extend(glyph(ch)[r].chars().map(|c| c == '#'));
        }
        if i > 0 {
            rows[GLYPH_H].push(false);
        }
        rows[GLYPH_H].extend([false; GLYPH_W]);
    }
    rows
}

/// Wordmark as terminal rows, in a steady lime → mint gradient (it does not move).
fn wordmark_rows(text: &str) -> Vec<Line<'static>> {
    let px = text_pixels(text);
    let width = px[0].len();
    (0..px.len() / 2)
        .map(|r| {
            Line::from(
                (0..width)
                    .map(|x| {
                        let (t, b) = (px[2 * r][x], px[2 * r + 1][x]);
                        let wave = x as f32 / width.max(1) as f32;
                        let c = color(lerp(MINT, LIME, wave));
                        match (t, b) {
                            (false, false) => Span::raw(" "),
                            (true, false) => Span::styled("▀", Style::new().fg(c)),
                            (false, true) => Span::styled("▄", Style::new().fg(c)),
                            (true, true) => Span::styled("█", Style::new().fg(c)),
                        }
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .collect()
}

/// Horizontal ripple, in columns, for row `row` at time `t` seconds.
pub fn ripple(row: usize, t: f32, amplitude: f32) -> i32 {
    ((t * 3.2 - row as f32 * 0.55).sin() * amplitude).round() as i32
}

/// Draws one line at `x + dx`, clipped to `area`.
fn put(f: &mut Frame, area: Rect, line: Line<'static>, x: i32, y: u16) {
    let width = line.width() as i32;
    let left = x.max(area.x as i32);
    let right = (x + width).min((area.x + area.width) as i32);
    if y >= area.y + area.height || right <= left {
        return;
    }
    // Skip the part that fell off the left edge.
    let skip = (left - x) as usize;
    let mut spans = Vec::new();
    let mut seen = 0;
    for s in line.spans {
        let n = s.content.chars().count();
        if seen + n > skip {
            let from = skip.saturating_sub(seen);
            let content: String = s.content.chars().skip(from).collect();
            spans.push(Span::styled(content, s.style));
        }
        seen += n;
    }
    f.render_widget(Paragraph::new(Line::from(spans)), Rect::new(left as u16, y, (right - left) as u16, 1));
}

pub fn draw(f: &mut Frame, elapsed: Duration) {
    let area = f.area();
    let t = elapsed.as_secs_f32();
    let logo = logo_rows();
    let word = wordmark_rows("TUIZIUM");
    let tagline = "Debezium · Kafka Connect, in your terminal";
    let hint = "▸ press Enter to continue";

    let logo_w = logo.first().map_or(0, Line::width) as i32;
    let word_w = word.first().map_or(0, Line::width) as i32;
    let total_h = (logo.len() + 1 + word.len() + 2 + 1) as i32;
    let top = ((area.height as i32 - total_h) / 2).max(0);
    let center = |w: i32| (area.width as i32 - w) / 2;

    // Too small for the artwork: fall back to plain centered text.
    if area.width < logo_w as u16 + 6 || (area.height as i32) < total_h {
        let y = area.y + area.height.saturating_sub(1) / 2;
        let title = Line::styled("T U I Z I U M", Style::new().fg(color(LIME)));
        put(f, area, title.clone(), area.x as i32 + center(title.width() as i32), y);
        return;
    }

    // Entrance: the ripple is strongest at the start and calms down to a gentle sway.
    let amp = 2.4 - 1.4 * (t / 2.0).min(1.0);
    let mut y = area.y + top as u16;
    for (r, line) in logo.into_iter().enumerate() {
        put(f, area, line, area.x as i32 + center(logo_w) + ripple(r, t, amp), y);
        y += 1;
    }
    y += 1;
    for line in word {
        put(f, area, line, area.x as i32 + center(word_w), y);
        y += 1;
    }
    y += 1;
    let tag = Line::styled(tagline, Style::new().fg(color(lerp(MINT, TEAL, 0.35))));
    put(f, area, tag.clone(), area.x as i32 + center(tag.width() as i32), y);
    y += 1;
    let blink = ((t * 2.0) as u32).is_multiple_of(2);
    let hint_style = Style::new().fg(if blink { Color::Gray } else { Color::DarkGray });
    let h = Line::styled(hint, hint_style);
    put(f, area, h.clone(), area.x as i32 + center(h.width() as i32), y);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn asset_is_well_formed_and_not_empty() {
        let b = decode(LOGO);
        assert_eq!(LOGO.len(), 2 + b.w * b.h * 4);
        assert!(b.h.is_multiple_of(2), "needs an even pixel height for half blocks");
        let ink = b.px.iter().flatten().count();
        assert!(ink > b.w * b.h / 5, "logo must have visible pixels, had {ink}");
        assert!(b.px.iter().flatten().any(|c| c.1 > 180), "has the bright green of the mark");
    }

    #[test]
    fn wordmark_is_seven_letters_wide_and_four_rows_high() {
        let rows = wordmark_rows("TUIZIUM");
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|r| r.width() == 7 * GLYPH_W + 6));
        assert!(rows.iter().any(|r| r.spans.iter().any(|s| s.content != " ")));
    }

    #[test]
    fn ripple_is_bounded_and_actually_moves() {
        let vals: Vec<i32> = (0..40).map(|i| ripple(i % 20, i as f32 * 0.1, 2.0)).collect();
        assert!(vals.iter().all(|v| v.abs() <= 2));
        assert!(vals.iter().any(|v| *v != vals[0]));
    }

    #[test]
    fn draws_at_every_size_and_time_without_panicking() {
        for (w, h) in [(120, 40), (80, 24), (60, 30), (46, 24), (20, 6), (10, 4), (1, 1), (0, 0)] {
            let mut term = Terminal::new(TestBackend::new(w, h)).unwrap();
            for ms in [0u64, 50, 400, 1700, 3599, 10_000] {
                term.draw(|f| draw(f, Duration::from_millis(ms))).unwrap();
            }
        }
    }

    #[test]
    fn full_size_screen_shows_logo_wordmark_and_tagline() {
        let mut term = Terminal::new(TestBackend::new(100, 34)).unwrap();
        term.draw(|f| draw(f, Duration::from_millis(500))).unwrap();
        let s: String = term.backend().buffer().content().iter().map(|c| c.symbol()).collect();
        assert!(s.contains("Kafka Connect, in your terminal"));
        assert!(s.contains('▀') && s.contains('█'));
        assert!(s.contains("press Enter to continue"));
        if std::env::var_os("TUIZIUM_DUMP").is_some() {
            let buf = term.backend().buffer();
            for y in 0..buf.area.height {
                let row: String = (0..buf.area.width).map(|x| buf[(x, y)].symbol().to_string()).collect();
                println!("{}", row.trim_end());
            }
        }
    }
}

//! Startup connection screen: asks where Kafka Connect (the Debezium REST API)
//! can be reached. Passwords and tokens are never typed here; like everywhere
//! else, a profile only stores the *name* of an environment variable.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph, Wrap};

use crate::config::{Cluster, Config};

const LIME: Color = Color::Rgb(170, 240, 20);
const MINT: Color = Color::Rgb(0, 205, 130);
const DIM: Color = Color::DarkGray;
const ERR: Color = Color::Red;

pub const NAME: usize = 0;
pub const URL: usize = 1;
pub const USER: usize = 2;
pub const PASSWORD_ENV: usize = 3;
pub const TOKEN_ENV: usize = 4;

const LABELS: [&str; 5] = [
    "Profile name",
    "Kafka Connect URL",
    "Username (optional)",
    "Password env var (optional)",
    "Bearer token env var (optional)",
];

const HELP: [&str; 5] = [
    "A short name for this connection. ←/→ switches between saved profiles.",
    "Where the Debezium / Kafka Connect REST API lives, e.g. http://localhost:8083",
    "For HTTP basic auth. Leave empty if the API is open.",
    "NAME of the environment variable that holds the password (never the password itself).",
    "NAME of the environment variable that holds a bearer token (instead of username/password).",
];

pub struct SetupForm {
    pub fields: [String; 5],
    pub selected: usize,
    pub error: Option<String>,
    /// Profile names for ←/→, and which one the form currently mirrors.
    profiles: Vec<String>,
    current: Option<usize>,
}

/// What the user decided.
pub enum Outcome {
    Stay,
    Quit,
    /// Connect to this profile; `save` is set when it is new or was edited.
    Connect {
        name: String,
        cluster: Cluster,
        save: bool,
    },
}

impl SetupForm {
    pub fn new(config: &Config, preferred: &str) -> Self {
        let profiles: Vec<String> = config.clusters.keys().cloned().collect();
        let current =
            profiles.iter().position(|p| p == preferred).or(if profiles.is_empty() { None } else { Some(0) });
        let mut form = Self { fields: Default::default(), selected: URL, error: None, profiles, current };
        match current {
            Some(i) => form.load_profile(config, i),
            None => {
                form.fields[NAME] = "local".into();
                form.fields[URL] = "http://localhost:8083".into();
            }
        }
        form
    }

    fn load_profile(&mut self, config: &Config, index: usize) {
        let name = self.profiles[index].clone();
        let c = config.clusters.get(&name).cloned().unwrap_or_default();
        self.fields = [
            name,
            c.url,
            c.username.unwrap_or_default(),
            c.password_env.unwrap_or_default(),
            c.token_env.unwrap_or_default(),
        ];
        self.current = Some(index);
        self.error = None;
    }

    fn cycle_profile(&mut self, config: &Config, delta: isize) {
        if self.profiles.is_empty() {
            return;
        }
        let n = self.profiles.len() as isize;
        let i = self.current.map_or(0, |i| (i as isize + delta).rem_euclid(n) as usize);
        self.load_profile(config, i);
    }

    /// Builds the profile from the form, validating everything that can be checked offline.
    pub fn build(&self, config: &Config) -> Result<(String, Cluster, bool), String> {
        let f = |i: usize| self.fields[i].trim();
        let opt = |i: usize| (!f(i).is_empty()).then(|| f(i).to_string());
        let name = f(NAME);
        if name.is_empty() {
            return Err("give the profile a name".into());
        }
        if name.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err("the profile name must not contain spaces".into());
        }
        if f(URL).is_empty() {
            return Err("enter the Kafka Connect URL, e.g. http://localhost:8083".into());
        }
        let cluster = Cluster {
            url: f(URL).to_string(),
            username: opt(USER),
            password_env: opt(PASSWORD_ENV),
            token_env: opt(TOKEN_ENV),
            ..config.clusters.get(name).cloned().unwrap_or_default()
        };
        if cluster.username.is_some() && cluster.password_env.is_none() && cluster.token_env.is_none() {
            return Err("a username needs the NAME of the env var holding its password".into());
        }
        if cluster.password_env.is_some() && cluster.username.is_none() {
            return Err("a password env var needs a username (or use a bearer token instead)".into());
        }
        for var in [&cluster.password_env, &cluster.token_env].into_iter().flatten() {
            if !var.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err("env var names may only contain letters, digits and _ — type the NAME of the variable, never the value itself".into());
            }
        }
        // Surfaces bad URLs, credentials in the URL, unset variables.
        cluster.client().map_err(|e| format!("{e:#}"))?;
        let save = config.clusters.get(name) != Some(&cluster);
        Ok((name.to_string(), cluster, save))
    }

    pub fn on_key(&mut self, config: &Config, key: ratatui::crossterm::event::KeyEvent) -> Outcome {
        use ratatui::crossterm::event::{KeyCode, KeyModifiers};
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let n = self.fields.len();
        match key.code {
            KeyCode::Esc => return Outcome::Quit,
            KeyCode::Tab | KeyCode::Down => self.selected = (self.selected + 1) % n,
            KeyCode::BackTab | KeyCode::Up => self.selected = (self.selected + n - 1) % n,
            KeyCode::Left if self.selected == NAME => self.cycle_profile(config, -1),
            KeyCode::Right if self.selected == NAME => self.cycle_profile(config, 1),
            KeyCode::Backspace => {
                self.fields[self.selected].pop();
                self.error = None;
            }
            KeyCode::Char('u') if ctrl => {
                self.fields[self.selected].clear();
                self.error = None;
            }
            KeyCode::Char(c) if !ctrl && !c.is_control() => {
                if self.fields[self.selected].chars().count() < 300 {
                    self.fields[self.selected].push(c);
                }
                self.error = None;
            }
            KeyCode::Enter => match self.build(config) {
                Ok((name, cluster, save)) => return Outcome::Connect { name, cluster, save },
                Err(e) => self.error = Some(e),
            },
            _ => {}
        }
        Outcome::Stay
    }
}

pub fn draw(f: &mut Frame, form: &SetupForm) {
    let area = f.area();
    let w = area.width.min(86);
    let h = area.height.min(22);
    let rect = Rect::new(area.x + (area.width - w) / 2, area.y + (area.height - h) / 2, w, h);
    f.render_widget(Clear, rect);
    let block = Block::bordered().border_style(Style::new().fg(MINT)).title(Line::from(Span::styled(
        " Tuizium · connect to Debezium ",
        Style::new().fg(LIME).add_modifier(Modifier::BOLD),
    )));
    let inner = block.inner(rect);
    f.render_widget(block, rect);
    let [intro, body, helpline, errline, foot] = Layout::vertical([
        Constraint::Length(2),
        Constraint::Min(5),
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Length(1),
    ])
    .areas(inner);

    f.render_widget(
        Paragraph::new("Where can Tuizium reach the Debezium API (Kafka Connect REST, usually port 8083)?")
            .style(Style::new().fg(Color::Gray))
            .wrap(Wrap { trim: true }),
        intro,
    );

    let lines: Vec<Line> = form
        .fields
        .iter()
        .enumerate()
        .map(|(i, value)| {
            let sel = i == form.selected;
            let mut spans = vec![
                Span::styled(if sel { "▶ " } else { "  " }, Style::new().fg(LIME)),
                Span::styled(
                    format!("{:<33}", LABELS[i]),
                    Style::new().fg(if sel { LIME } else { Color::Gray }),
                ),
                Span::styled(
                    value.clone(),
                    Style::new().add_modifier(if sel { Modifier::BOLD } else { Modifier::empty() }),
                ),
            ];
            if sel {
                spans.push(Span::styled("▌", Style::new().fg(LIME)));
            }
            Line::from(spans)
        })
        .collect();
    f.render_widget(Paragraph::new(lines), body);

    f.render_widget(
        Paragraph::new(HELP[form.selected]).style(Style::new().fg(DIM)).wrap(Wrap { trim: true }),
        helpline,
    );
    if let Some(e) = &form.error {
        f.render_widget(
            Paragraph::new(format!("✖ {e}")).style(Style::new().fg(ERR)).wrap(Wrap { trim: true }),
            errline,
        );
    }
    let key = |k: &str, what: &str| {
        [
            Span::styled(format!(" {k} "), Style::new().fg(Color::Black).bg(DIM)),
            Span::styled(format!("{what} "), Style::new().fg(Color::Gray)),
        ]
    };
    let mut spans = Vec::new();
    for (k, what) in [("⏎", "connect"), ("Tab", "next field"), ("^U", "clear"), ("Esc", "quit")] {
        spans.extend(key(k, what));
    }
    f.render_widget(Paragraph::new(Line::from(spans)).alignment(Alignment::Left), foot);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn cfg() -> Config {
        let mut c = Config::with_local_default();
        c.clusters.insert(
            "prod".into(),
            Cluster {
                url: "https://connect.example.com".into(),
                username: Some("admin".into()),
                password_env: Some("TUIZIUM_SETUP_TEST_PW".into()),
                ..Default::default()
            },
        );
        c
    }

    fn type_into(form: &mut SetupForm, config: &Config, text: &str) {
        for ch in text.chars() {
            form.on_key(config, KeyEvent::from(KeyCode::Char(ch)));
        }
    }

    #[test]
    fn prefills_from_the_preferred_profile_and_defaults_when_there_is_none() {
        let c = cfg();
        let f = SetupForm::new(&c, "prod");
        assert_eq!(f.fields[NAME], "prod");
        assert_eq!(f.fields[URL], "https://connect.example.com");
        assert_eq!(f.fields[PASSWORD_ENV], "TUIZIUM_SETUP_TEST_PW");
        let empty = Config::default();
        let f = SetupForm::new(&empty, "x");
        assert_eq!((f.fields[NAME].as_str(), f.fields[URL].as_str()), ("local", "http://localhost:8083"));
    }

    #[test]
    fn enter_on_an_untouched_saved_profile_connects_without_saving() {
        let c = cfg();
        // SAFETY: unique test-only variable name.
        unsafe { std::env::set_var("TUIZIUM_SETUP_TEST_PW", "x") };
        let mut f = SetupForm::new(&c, "prod");
        match f.on_key(&c, KeyEvent::from(KeyCode::Enter)) {
            Outcome::Connect { name, save, .. } => assert!((name.as_str(), save) == ("prod", false)),
            _ => panic!("expected to connect"),
        }
    }

    #[test]
    fn editing_the_url_marks_the_profile_for_saving() {
        let c = Config::with_local_default();
        let mut f = SetupForm::new(&c, "local");
        f.selected = URL;
        f.on_key(&c, KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
        type_into(&mut f, &c, "http://connect.internal:8083");
        match f.on_key(&c, KeyEvent::from(KeyCode::Enter)) {
            Outcome::Connect { cluster, save, .. } => {
                assert!(save);
                assert_eq!(cluster.url, "http://connect.internal:8083");
            }
            _ => panic!("expected to connect"),
        }
    }

    #[test]
    fn arrows_switch_saved_profiles_only_on_the_name_field() {
        let c = cfg();
        let mut f = SetupForm::new(&c, "local");
        f.selected = NAME;
        f.on_key(&c, KeyEvent::from(KeyCode::Right));
        assert_eq!(f.fields[NAME], "prod");
        f.on_key(&c, KeyEvent::from(KeyCode::Right));
        assert_eq!(f.fields[NAME], "local", "wraps around");
        f.on_key(&c, KeyEvent::from(KeyCode::Left));
        assert_eq!(f.fields[NAME], "prod");
        f.selected = URL;
        f.on_key(&c, KeyEvent::from(KeyCode::Right));
        assert_eq!(f.fields[NAME], "prod", "other fields ignore the arrows");
    }

    #[test]
    fn validation_catches_mistakes_before_connecting() {
        let c = Config::with_local_default();
        type Case<'a> = (&'a str, &'a dyn Fn(&mut SetupForm), &'a str);
        let cases: [Case; 9] = [
            ("empty name", &|f| f.fields[NAME].clear(), "name"),
            ("space in name", &|f| f.fields[NAME] = "my cluster".into(), "spaces"),
            ("empty url", &|f| f.fields[URL].clear(), "URL"),
            ("bad scheme", &|f| f.fields[URL] = "ftp://x".into(), "http"),
            ("creds in url", &|f| f.fields[URL] = "http://a:p4ssw0rd@h:8083".into(), "credentials"),
            ("user only", &|f| f.fields[USER] = "bob".into(), "password"),
            ("pw env only", &|f| f.fields[PASSWORD_ENV] = "PW".into(), "username"),
            (
                "secret typed",
                &|f| {
                    f.fields[USER] = "bob".into();
                    f.fields[PASSWORD_ENV] = "hunter2!".into();
                },
                "NAME",
            ),
            (
                "unset env",
                &|f| f.fields[TOKEN_ENV] = "TUIZIUM_SETUP_DEFINITELY_UNSET".into(),
                "TUIZIUM_SETUP_DEFINITELY_UNSET",
            ),
        ];
        for (label, mutate, want) in cases {
            let mut base = SetupForm::new(&c, "local");
            mutate(&mut base);
            let err = base.build(&c).unwrap_err();
            assert!(err.contains(want), "{label}: {err}");
            assert!(
                !err.contains("p4ssw0rd") && !err.contains("hunter2"),
                "{label}: must not echo secrets: {err}"
            );
            match base.on_key(&c, KeyEvent::from(KeyCode::Enter)) {
                Outcome::Stay => assert!(base.error.is_some(), "{label}"),
                _ => panic!("{label}: must not connect"),
            }
        }
    }

    #[test]
    fn esc_quits_and_input_is_bounded() {
        let c = Config::with_local_default();
        let mut f = SetupForm::new(&c, "local");
        f.selected = USER;
        type_into(&mut f, &c, &"x".repeat(1000));
        assert_eq!(f.fields[USER].chars().count(), 300);
        f.on_key(&c, KeyEvent::from(KeyCode::Char('\u{1b}'))); // control chars are ignored
        assert_eq!(f.fields[USER].chars().count(), 300);
        assert!(matches!(f.on_key(&c, KeyEvent::from(KeyCode::Esc)), Outcome::Quit));
    }

    #[test]
    fn renders_at_every_size_with_and_without_errors() {
        let c = cfg();
        let mut f = SetupForm::new(&c, "prod");
        for (w, h) in [(120, 40), (80, 24), (50, 12), (10, 4), (1, 1), (0, 0)] {
            let mut term = Terminal::new(TestBackend::new(w, h)).unwrap();
            for sel in 0..5 {
                f.selected = sel;
                f.error = (sel % 2 == 0).then(|| "something is wrong".to_string());
                term.draw(|fr| draw(fr, &f)).unwrap();
            }
        }
        let mut term = Terminal::new(TestBackend::new(100, 30)).unwrap();
        f.error = None;
        term.draw(|fr| draw(fr, &f)).unwrap();
        let s: String = term.backend().buffer().content().iter().map(|c| c.symbol()).collect();
        assert!(s.contains("Kafka Connect URL") && s.contains("https://connect.example.com"));
    }
}

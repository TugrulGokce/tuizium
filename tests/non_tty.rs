//! Without a terminal Tuizium must say so and exit, instead of writing a screen's worth of
//! escape sequences into a pipe or file.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

struct Outcome {
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: String,
    timed_out: bool,
}

fn run(args: &[&str]) -> Outcome {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tuizium"))
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start tuizium");
    let start = Instant::now();
    let mut timed_out = false;
    let status = loop {
        if let Some(s) = child.try_wait().expect("wait") {
            break s.code();
        }
        if start.elapsed() > Duration::from_secs(10) {
            let _ = child.kill();
            let _ = child.wait();
            timed_out = true;
            break None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let (mut stdout, mut stderr) = (Vec::new(), String::new());
    child.stdout.take().unwrap().read_to_end(&mut stdout).unwrap();
    child.stderr.take().unwrap().read_to_string(&mut stderr).unwrap();
    Outcome { code: status, stdout, stderr, timed_out }
}

#[test]
fn refuses_to_start_the_tui_when_not_attached_to_a_terminal() {
    let o = run(&["--no-splash", "--url", "http://127.0.0.1:9"]);
    assert!(!o.timed_out, "it started a TUI and kept running");
    assert_eq!(o.code, Some(1));
    assert!(o.stdout.is_empty(), "stdout must stay clean, got {} bytes", o.stdout.len());
    assert!(o.stderr.contains("interactive terminal"), "stderr: {}", o.stderr);
}

#[test]
fn help_and_version_still_work_without_a_terminal() {
    for flag in ["--help", "--version"] {
        let o = run(&[flag]);
        assert_eq!(o.code, Some(0), "{flag}");
        assert!(!o.stdout.is_empty() && !o.stdout.contains(&0x1b), "{flag} output has escape codes");
    }
}

#[test]
fn argument_errors_are_still_reported_first() {
    let o = run(&["--bogus"]);
    assert_eq!(o.code, Some(2), "usage errors exit with 2");
    assert!(o.stderr.contains("unknown argument") && o.stdout.is_empty());
}

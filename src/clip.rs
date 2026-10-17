//! Copy to the system clipboard through the terminal (OSC 52). No clipboard crate, no process
//! spawning; the terminal decides whether to honour it, and Tuizium cannot tell whether it did.

use std::io::Write;

use crate::templates;

/// Largest payload sent; many terminals refuse more than ~100 kB anyway.
const MAX_BYTES: usize = 64 * 1024;

/// Masks secrets and drops control characters (keeping newlines), so what lands in the
/// clipboard is safe to paste into a chat or ticket.
pub fn sanitize(text: &str) -> String {
    templates::mask_text(text)
        .chars()
        .map(|c| if c == '\t' { ' ' } else { c })
        .filter(|c| *c == '\n' || !c.is_control())
        .collect()
}

pub fn copy(text: &str) -> Result<(), String> {
    let mut end = text.len().min(MAX_BYTES);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let seq = osc52(&text[..end]);
    let mut out = std::io::stdout().lock();
    out.write_all(seq.as_bytes()).and_then(|()| out.flush()).map_err(|e| e.to_string())
}

fn osc52(text: &str) -> String {
    format!("\x1b]52;c;{}\x07", base64(text.as_bytes()))
}

fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = chunk.iter().enumerate().fold(0u32, |acc, (i, b)| acc | (*b as u32) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_rfc4648_vectors() {
        for (input, want) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(input.as_bytes()), want, "{input:?}");
        }
    }

    #[test]
    fn sanitize_masks_secrets_and_strips_escapes() {
        let s = sanitize("jdbc:x?user=u&password=hunter2\n\x1b[31mred\x1b[0m\tend");
        assert!(!s.contains("hunter2"), "{s}");
        assert!(!s.contains('\x1b'));
        assert!(s.contains('\n'));
    }

    #[test]
    fn osc52_frame_is_well_formed() {
        assert_eq!(osc52("foo"), "\x1b]52;c;Zm9v\x07");
    }
}

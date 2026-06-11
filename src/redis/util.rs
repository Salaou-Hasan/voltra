//! Shared helpers for the Redis layer: glob matching (KEYS / SCAN MATCH /
//! PSUBSCRIBE) and Redis-style number parsing.

use bytes::Bytes;

/// Redis glob matcher: `*`, `?`, `[abc]`, `[^abc]`, `[a-z]`, `\x` escape.
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    glob_inner(pattern, text)
}

fn glob_inner(p: &[u8], t: &[u8]) -> bool {
    let (mut pi, mut ti) = (0usize, 0usize);
    // Backtracking state for the most recent '*'.
    let (mut star_pi, mut star_ti) = (usize::MAX, 0usize);

    while ti < t.len() {
        if pi < p.len() {
            match p[pi] {
                b'*' => {
                    star_pi = pi;
                    star_ti = ti;
                    pi += 1;
                    continue;
                }
                b'?' => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                b'[' => {
                    if let Some((matched, next_pi)) = match_class(p, pi, t[ti]) {
                        if matched {
                            pi = next_pi;
                            ti += 1;
                            continue;
                        }
                    }
                }
                b'\\' if pi + 1 < p.len() => {
                    if p[pi + 1] == t[ti] {
                        pi += 2;
                        ti += 1;
                        continue;
                    }
                }
                c => {
                    if c == t[ti] {
                        pi += 1;
                        ti += 1;
                        continue;
                    }
                }
            }
        }
        // Mismatch: backtrack to the last '*' if any.
        if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

/// Match a `[...]` class starting at `p[start] == b'['` against byte `c`.
/// Returns (matched, index just past the closing `]`).
fn match_class(p: &[u8], start: usize, c: u8) -> Option<(bool, usize)> {
    let mut i = start + 1;
    let negate = i < p.len() && p[i] == b'^';
    if negate {
        i += 1;
    }
    let mut matched = false;
    let mut first = true;
    while i < p.len() {
        if p[i] == b']' && !first {
            return Some((matched != negate, i + 1));
        }
        first = false;
        if p[i] == b'\\' && i + 1 < p.len() {
            if p[i + 1] == c {
                matched = true;
            }
            i += 2;
            continue;
        }
        if i + 2 < p.len() && p[i + 1] == b'-' && p[i + 2] != b']' {
            let (lo, hi) = (p[i].min(p[i + 2]), p[i].max(p[i + 2]));
            if (lo..=hi).contains(&c) {
                matched = true;
            }
            i += 3;
            continue;
        }
        if p[i] == c {
            matched = true;
        }
        i += 1;
    }
    None // unterminated class — treat as no match
}

/// Parse an i64 the way Redis does (entire string must be a valid integer).
pub fn parse_i64(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// Parse a float; accepts `inf`, `+inf`, `-inf`, `infinity` (case-insensitive).
pub fn parse_f64(b: &Bytes) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?.trim();
    match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => return Some(f64::INFINITY),
        "-inf" | "-infinity" => return Some(f64::NEG_INFINITY),
        _ => {}
    }
    let f: f64 = s.parse().ok()?;
    if f.is_nan() {
        None
    } else {
        Some(f)
    }
}

/// Uppercase ASCII copy of a command name.
pub fn upper(b: &Bytes) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

/// Byte slice as UTF-8 (lossy) string — for error messages and key echoing.
pub fn lossy(b: &Bytes) -> String {
    String::from_utf8_lossy(b).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(p: &str, t: &str) -> bool {
        glob_match(p.as_bytes(), t.as_bytes())
    }

    #[test]
    fn glob_basics() {
        assert!(m("*", "anything"));
        assert!(m("h?llo", "hello"));
        assert!(m("h*llo", "heeeello"));
        assert!(m("h[ae]llo", "hallo"));
        assert!(m("h[ae]llo", "hello"));
        assert!(!m("h[ae]llo", "hillo"));
        assert!(m("h[^e]llo", "hallo"));
        assert!(!m("h[^e]llo", "hello"));
        assert!(m("h[a-c]llo", "hbllo"));
        assert!(!m("h[a-c]llo", "hdllo"));
        assert!(m("user:*:name", "user:42:name"));
        assert!(!m("user:*:name", "user:42:email"));
        assert!(m("", ""));
        assert!(!m("", "x"));
        assert!(m("**", "x"));
        assert!(m("\\*", "*"));
        assert!(!m("\\*", "x"));
    }

    #[test]
    fn float_parsing() {
        assert_eq!(parse_f64(&Bytes::from_static(b"3.5")), Some(3.5));
        assert_eq!(parse_f64(&Bytes::from_static(b"+inf")), Some(f64::INFINITY));
        assert_eq!(parse_f64(&Bytes::from_static(b"-inf")), Some(f64::NEG_INFINITY));
        assert_eq!(parse_f64(&Bytes::from_static(b"nan")), None);
        assert_eq!(parse_f64(&Bytes::from_static(b"abc")), None);
    }
}

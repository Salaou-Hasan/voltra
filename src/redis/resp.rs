//! RESP (REdis Serialization Protocol) — RESP2 and RESP3.
//!
//! Inbound: clients send commands as arrays of bulk strings (or inline text).
//! Outbound: full RESP2/RESP3 value encoding; RESP3-only types degrade
//! gracefully when the connection negotiated protocol 2.

use bytes::Bytes;

/// A RESP value (server → client).
#[derive(Clone, Debug, PartialEq)]
pub enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Bytes),
    /// RESP2: `$-1\r\n` · RESP3: `_\r\n`
    Null,
    /// RESP2: `*-1\r\n` (EXEC abort) · RESP3: `_\r\n`
    NullArray,
    Array(Vec<Resp>),
    /// RESP3 double · RESP2 fallback: bulk string
    Double(f64),
    /// RESP3 boolean · RESP2 fallback: :1 / :0
    Bool(bool),
    /// RESP3 big number · RESP2 fallback: bulk string
    BigNum(String),
    /// RESP3 verbatim string (format, body) · RESP2 fallback: bulk string
    Verbatim(&'static str, Bytes),
    /// RESP3 map · RESP2 fallback: flat array
    Map(Vec<(Resp, Resp)>),
    /// RESP3 set · RESP2 fallback: array
    SetReply(Vec<Resp>),
    /// RESP3 push frame (pub/sub) · RESP2 fallback: array
    Push(Vec<Resp>),
}

impl Resp {
    pub fn ok() -> Resp {
        Resp::Simple("OK".into())
    }
    pub fn bulk(s: impl Into<Bytes>) -> Resp {
        Resp::Bulk(s.into())
    }
    pub fn bulk_str(s: impl AsRef<str>) -> Resp {
        Resp::Bulk(Bytes::copy_from_slice(s.as_ref().as_bytes()))
    }
    pub fn err(msg: impl Into<String>) -> Resp {
        Resp::Error(msg.into())
    }
    pub fn wrong_type() -> Resp {
        Resp::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
    }
    pub fn not_int() -> Resp {
        Resp::Error("ERR value is not an integer or out of range".into())
    }
    pub fn not_float() -> Resp {
        Resp::Error("ERR value is not a valid float".into())
    }
    pub fn syntax() -> Resp {
        Resp::Error("ERR syntax error".into())
    }
    pub fn arity(cmd: &str) -> Resp {
        Resp::Error(format!("ERR wrong number of arguments for '{}' command", cmd.to_lowercase()))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Encoder
// ─────────────────────────────────────────────────────────────────────────────

/// Format a float the way Redis does (`%.17g`-equivalent, minimal digits).
pub fn fmt_f64(f: f64) -> String {
    if f.is_nan() {
        "nan".into()
    } else if f.is_infinite() {
        if f > 0.0 { "inf".into() } else { "-inf".into() }
    } else if f == f.trunc() && f.abs() < 1e17 {
        format!("{}", f as i64)
    } else {
        let s = format!("{f}");
        s
    }
}

/// Encode a value for a connection that negotiated RESP `proto` (2 or 3).
pub fn encode(out: &mut Vec<u8>, v: &Resp, proto: u8) {
    match v {
        Resp::Simple(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Resp::Error(s) => {
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Resp::Int(i) => {
            out.push(b':');
            out.extend_from_slice(i.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Resp::Bulk(b) => {
            out.push(b'$');
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
        }
        Resp::Null => {
            if proto >= 3 {
                out.extend_from_slice(b"_\r\n");
            } else {
                out.extend_from_slice(b"$-1\r\n");
            }
        }
        Resp::NullArray => {
            if proto >= 3 {
                out.extend_from_slice(b"_\r\n");
            } else {
                out.extend_from_slice(b"*-1\r\n");
            }
        }
        Resp::Array(items) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for it in items {
                encode(out, it, proto);
            }
        }
        Resp::Double(f) => {
            if proto >= 3 {
                out.push(b',');
                out.extend_from_slice(fmt_f64(*f).as_bytes());
                out.extend_from_slice(b"\r\n");
            } else {
                encode(out, &Resp::bulk_str(fmt_f64(*f)), proto);
            }
        }
        Resp::Bool(b) => {
            if proto >= 3 {
                out.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
            } else {
                encode(out, &Resp::Int(if *b { 1 } else { 0 }), proto);
            }
        }
        Resp::BigNum(s) => {
            if proto >= 3 {
                out.push(b'(');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            } else {
                encode(out, &Resp::bulk_str(s), proto);
            }
        }
        Resp::Verbatim(fmt, body) => {
            if proto >= 3 {
                out.push(b'=');
                out.extend_from_slice((body.len() + 4).to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(fmt.as_bytes());
                out.push(b':');
                out.extend_from_slice(body);
                out.extend_from_slice(b"\r\n");
            } else {
                encode(out, &Resp::Bulk(body.clone()), proto);
            }
        }
        Resp::Map(pairs) => {
            if proto >= 3 {
                out.push(b'%');
                out.extend_from_slice(pairs.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for (k, val) in pairs {
                    encode(out, k, proto);
                    encode(out, val, proto);
                }
            } else {
                out.push(b'*');
                out.extend_from_slice((pairs.len() * 2).to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for (k, val) in pairs {
                    encode(out, k, proto);
                    encode(out, val, proto);
                }
            }
        }
        Resp::SetReply(items) => {
            if proto >= 3 {
                out.push(b'~');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for it in items {
                    encode(out, it, proto);
                }
            } else {
                encode(out, &Resp::Array(items.clone()), proto);
            }
        }
        Resp::Push(items) => {
            if proto >= 3 {
                out.push(b'>');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for it in items {
                    encode(out, it, proto);
                }
            } else {
                encode(out, &Resp::Array(items.clone()), proto);
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Command parser (client → server)
// ─────────────────────────────────────────────────────────────────────────────

/// Parse one inbound command from `buf`.
///
/// Returns `Ok(None)` when the buffer holds an incomplete frame,
/// `Ok(Some((args, consumed)))` for a complete command,
/// `Err(msg)` on a protocol violation (the connection should be closed).
pub fn parse_command(buf: &[u8]) -> Result<Option<(Vec<Bytes>, usize)>, String> {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] == b'*' {
        parse_array_command(buf)
    } else {
        parse_inline_command(buf)
    }
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn parse_array_command(buf: &[u8]) -> Result<Option<(Vec<Bytes>, usize)>, String> {
    let line_end = match find_crlf(buf, 1) {
        Some(i) => i,
        None => return Ok(None),
    };
    let count: i64 = std::str::from_utf8(&buf[1..line_end])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or("Protocol error: invalid multibulk length")?;
    if count < 0 {
        return Ok(Some((Vec::new(), line_end + 2)));
    }
    if count > 1024 * 1024 {
        return Err("Protocol error: invalid multibulk length".into());
    }
    let mut pos = line_end + 2;
    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if pos >= buf.len() {
            return Ok(None);
        }
        if buf[pos] != b'$' {
            return Err(format!("Protocol error: expected '$', got '{}'", buf[pos] as char));
        }
        let lend = match find_crlf(buf, pos + 1) {
            Some(i) => i,
            None => return Ok(None),
        };
        let len: i64 = std::str::from_utf8(&buf[pos + 1..lend])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or("Protocol error: invalid bulk length")?;
        if !(0..=512 * 1024 * 1024).contains(&len) {
            return Err("Protocol error: invalid bulk length".into());
        }
        let dstart = lend + 2;
        let dend = dstart + len as usize;
        if buf.len() < dend + 2 {
            return Ok(None);
        }
        if &buf[dend..dend + 2] != b"\r\n" {
            return Err("Protocol error: bulk string missing CRLF".into());
        }
        args.push(Bytes::copy_from_slice(&buf[dstart..dend]));
        pos = dend + 2;
    }
    Ok(Some((args, pos)))
}

/// Inline commands (`PING\r\n` from telnet/netcat). Splits on whitespace;
/// supports double-quoted segments with backslash escapes.
fn parse_inline_command(buf: &[u8]) -> Result<Option<(Vec<Bytes>, usize)>, String> {
    let line_end = match find_crlf(buf, 0) {
        Some(i) => i,
        None => {
            // also accept bare \n line endings from sloppy clients
            match buf.iter().position(|&b| b == b'\n') {
                Some(nl) => {
                    let line = &buf[..nl];
                    let line = line.strip_suffix(b"\r").unwrap_or(line);
                    let args = split_inline(line)?;
                    return Ok(Some((args, nl + 1)));
                }
                None => return Ok(None),
            }
        }
    };
    let args = split_inline(&buf[..line_end])?;
    Ok(Some((args, line_end + 2)))
}

fn split_inline(line: &[u8]) -> Result<Vec<Bytes>, String> {
    let mut args = Vec::new();
    let mut cur: Vec<u8> = Vec::new();
    let mut in_quote = false;
    let mut i = 0;
    while i < line.len() {
        let c = line[i];
        if in_quote {
            if c == b'\\' && i + 1 < line.len() {
                cur.push(line[i + 1]);
                i += 2;
                continue;
            }
            if c == b'"' {
                in_quote = false;
                i += 1;
                continue;
            }
            cur.push(c);
        } else if c == b'"' {
            in_quote = true;
        } else if c == b' ' || c == b'\t' {
            if !cur.is_empty() {
                args.push(Bytes::from(std::mem::take(&mut cur)));
            }
        } else {
            cur.push(c);
        }
        i += 1;
    }
    if in_quote {
        return Err("Protocol error: unbalanced quotes in request".into());
    }
    if !cur.is_empty() {
        args.push(Bytes::from(cur));
    }
    Ok(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: &Resp, proto: u8) -> Vec<u8> {
        let mut out = Vec::new();
        encode(&mut out, v, proto);
        out
    }

    #[test]
    fn encodes_basic_types() {
        assert_eq!(enc(&Resp::ok(), 2), b"+OK\r\n");
        assert_eq!(enc(&Resp::Int(42), 2), b":42\r\n");
        assert_eq!(enc(&Resp::bulk_str("hi"), 2), b"$2\r\nhi\r\n");
        assert_eq!(enc(&Resp::Null, 2), b"$-1\r\n");
        assert_eq!(enc(&Resp::Null, 3), b"_\r\n");
        assert_eq!(enc(&Resp::Bool(true), 2), b":1\r\n");
        assert_eq!(enc(&Resp::Bool(true), 3), b"#t\r\n");
        assert_eq!(enc(&Resp::Double(3.5), 3), b",3.5\r\n");
        assert_eq!(enc(&Resp::Double(3.0), 3), b",3\r\n");
    }

    #[test]
    fn map_degrades_to_flat_array_in_resp2() {
        let m = Resp::Map(vec![(Resp::bulk_str("k"), Resp::Int(1))]);
        assert_eq!(enc(&m, 2), b"*2\r\n$1\r\nk\r\n:1\r\n");
        assert_eq!(enc(&m, 3), b"%1\r\n$1\r\nk\r\n:1\r\n");
    }

    #[test]
    fn parses_array_command() {
        let buf = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n";
        let (args, used) = parse_command(buf).unwrap().unwrap();
        assert_eq!(used, buf.len());
        assert_eq!(args.len(), 3);
        assert_eq!(&args[0][..], b"SET");
        assert_eq!(&args[2][..], b"hello");
    }

    #[test]
    fn partial_frame_returns_none() {
        let buf = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhel";
        assert!(parse_command(buf).unwrap().is_none());
    }

    #[test]
    fn pipelined_commands_consume_exactly_one() {
        let buf = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        let (args, used) = parse_command(buf).unwrap().unwrap();
        assert_eq!(&args[0][..], b"PING");
        assert_eq!(used, 14);
        let (args2, used2) = parse_command(&buf[used..]).unwrap().unwrap();
        assert_eq!(&args2[0][..], b"PING");
        assert_eq!(used2, 14);
    }

    #[test]
    fn parses_inline_command() {
        let (args, _) = parse_command(b"PING\r\n").unwrap().unwrap();
        assert_eq!(&args[0][..], b"PING");
        let (args, _) = parse_command(b"SET k \"a b\"\r\n").unwrap().unwrap();
        assert_eq!(args.len(), 3);
        assert_eq!(&args[2][..], b"a b");
    }

    #[test]
    fn rejects_allocation_bomb() {
        assert!(parse_command(b"*99999999\r\n").is_err());
        assert!(parse_command(b"*1\r\n$999999999999\r\n").is_err());
    }

    #[test]
    fn float_formatting_matches_redis() {
        assert_eq!(fmt_f64(3.0), "3");
        assert_eq!(fmt_f64(3.5), "3.5");
        assert_eq!(fmt_f64(f64::INFINITY), "inf");
        assert_eq!(fmt_f64(f64::NEG_INFINITY), "-inf");
        assert_eq!(fmt_f64(-0.5), "-0.5");
    }
}

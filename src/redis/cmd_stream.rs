//! Stream commands (XADD/XLEN/XRANGE/XREVRANGE/XREAD/XDEL/XTRIM).
//!
//! Consumer groups (XGROUP/XREADGROUP/XACK/XCLAIM/XPENDING) are intentionally
//! NOT implemented — basic append/read is the priority for this pass. Every
//! command here follows the same `Db`-trait pattern as the other families:
//! one implementation shared by `SnapDb` reads and `mvcc::Writer` writes.

use super::engine::{read_stream, Db};
use super::resp::Resp;
use super::util::{parse_i64, upper};
use crate::mvcc::{Datum, StreamId, XStream};
use bytes::Bytes;

// ─────────────────────────────────────────────────────────────────────────────
// ID parsing / formatting
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a stream ID for XADD's explicit-ID form: `<ms>` or `<ms>-<seq>`.
/// `*` and `<ms>-*` are handled by the caller (need access to `last_id`/now).
fn parse_explicit_id(b: &Bytes, default_seq: u64) -> Option<StreamId> {
    let s = std::str::from_utf8(b).ok()?;
    match s.split_once('-') {
        Some((ms, seq)) => Some(StreamId {
            ms: ms.parse().ok()?,
            seq: seq.parse().ok()?,
        }),
        None => Some(StreamId {
            ms: s.parse().ok()?,
            seq: default_seq,
        }),
    }
}

/// Parse a range boundary for XRANGE/XREVRANGE: `-`, `+`, `<ms>`, `<ms>-<seq>`,
/// with optional leading `(` for exclusive bounds. `is_start` selects the
/// default seq (0 for a range start, MAX for a range end) when only ms is given.
fn parse_range_bound(b: &Bytes, is_start: bool) -> Option<(StreamId, bool)> {
    let s = std::str::from_utf8(b).ok()?;
    if s == "-" {
        return Some((StreamId::MIN, false));
    }
    if s == "+" {
        return Some((StreamId::MAX, false));
    }
    let (exclusive, rest) = match s.strip_prefix('(') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let default_seq = if is_start { 0 } else { u64::MAX };
    let id = parse_explicit_id(&Bytes::copy_from_slice(rest.as_bytes()), default_seq)?;
    Some((id, exclusive))
}

fn id_to_resp(id: StreamId, fields: &im::Vector<(Bytes, Bytes)>) -> Resp {
    let mut flat = Vec::with_capacity(fields.len() * 2);
    for (k, v) in fields.iter() {
        flat.push(Resp::Bulk(k.clone()));
        flat.push(Resp::Bulk(v.clone()));
    }
    Resp::Array(vec![Resp::bulk_str(id.to_string()), Resp::Array(flat)])
}

// ─────────────────────────────────────────────────────────────────────────────
// XADD
// ─────────────────────────────────────────────────────────────────────────────

pub fn xadd(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 4 {
        return Resp::arity("xadd");
    }
    let key = &args[0];
    let mut i = 1;
    // Optional NOMKSTREAM.
    let mut nomkstream = false;
    if upper(&args[i]) == "NOMKSTREAM" {
        nomkstream = true;
        i += 1;
    }
    // Optional trimming clause: MAXLEN [~|=] N  or  MINID [~|=] id — best-effort,
    // applied after the append, mirroring real Redis's inline-trim option.
    let mut trim_maxlen: Option<u64> = None;
    let mut trim_minid: Option<StreamId> = None;
    if i < args.len() && matches!(upper(&args[i]).as_str(), "MAXLEN" | "MINID") {
        let is_maxlen = upper(&args[i]) == "MAXLEN";
        i += 1;
        if i < args.len() && matches!(args[i].as_ref(), b"~" | b"=") {
            i += 1;
        }
        let Some(bound) = args.get(i) else {
            return Resp::syntax();
        };
        if is_maxlen {
            let Some(n) = parse_i64(bound).filter(|n| *n >= 0) else {
                return Resp::not_int();
            };
            trim_maxlen = Some(n as u64);
        } else {
            let Some(id) = parse_explicit_id(bound, 0) else {
                return Resp::err("ERR Invalid stream ID specified as stream command argument");
            };
            trim_minid = Some(id);
        }
        i += 1;
        // Optional LIMIT N after MAXLEN/MINID — accepted and ignored (no
        // approximate-trim/lazy-radix-tree semantics to bound here).
        if i < args.len() && upper(&args[i]) == "LIMIT" {
            i += 2;
        }
    }
    let Some(id_arg) = args.get(i) else {
        return Resp::arity("xadd");
    };
    i += 1;
    let field_args = &args[i..];
    if field_args.is_empty() || !field_args.len().is_multiple_of(2) {
        return Resp::arity("xadd");
    }

    let existed = db.get(ns, key).is_some();
    if nomkstream && !existed {
        return Resp::Null;
    }
    let (mut s, exp) = match read_stream(db, ns, key) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let new_id = if id_arg.as_ref() == b"*" {
        let ms = db.now_ms();
        if ms > s.last_id.ms {
            StreamId { ms, seq: 0 }
        } else {
            s.last_id.next()
        }
    } else {
        let arg_str = std::str::from_utf8(id_arg).unwrap_or("");
        if let Some(ms_part) = arg_str.strip_suffix("-*") {
            let Ok(ms) = ms_part.parse::<u64>() else {
                return Resp::err("ERR Invalid stream ID specified as stream command argument");
            };
            if ms == s.last_id.ms {
                s.last_id.next()
            } else {
                StreamId { ms, seq: 0 }
            }
        } else {
            match parse_explicit_id(id_arg, 0) {
                Some(id) => id,
                None => {
                    return Resp::err("ERR Invalid stream ID specified as stream command argument")
                }
            }
        }
    };
    if new_id <= s.last_id && (s.entries_added > 0 || s.last_id != StreamId::MIN) {
        return Resp::err(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        );
    }
    if new_id == StreamId::MIN {
        return Resp::err("ERR The ID specified in XADD must be greater than 0-0");
    }

    let fields: im::Vector<(Bytes, Bytes)> = field_args
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    s.entries.insert(new_id, fields);
    s.last_id = new_id;
    s.entries_added += 1;

    if let Some(maxlen) = trim_maxlen {
        trim_to_maxlen(&mut s, maxlen);
    }
    if let Some(minid) = trim_minid {
        trim_to_minid(&mut s, minid);
    }

    db.put(ns, key.clone(), Datum::Stream(s), exp);
    Resp::bulk_str(new_id.to_string())
}

fn trim_to_maxlen(s: &mut XStream, maxlen: u64) {
    while s.entries.len() as u64 > maxlen {
        if let Some((id, _)) = s.entries.get_min().map(|(k, v)| (*k, v.clone())) {
            s.entries.remove(&id);
            if id > s.max_deleted_id {
                s.max_deleted_id = id;
            }
        } else {
            break;
        }
    }
}

fn trim_to_minid(s: &mut XStream, minid: StreamId) {
    loop {
        match s.entries.get_min() {
            Some((id, _)) if *id < minid => {
                let id = *id;
                s.entries.remove(&id);
                if id > s.max_deleted_id {
                    s.max_deleted_id = id;
                }
            }
            _ => break,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// XLEN / XDEL / XTRIM
// ─────────────────────────────────────────────────────────────────────────────

pub fn xlen(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("xlen");
    }
    match db.get(ns, &args[0]) {
        Some(Datum::Stream(s)) => Resp::Int(s.len() as i64),
        Some(_) => Resp::wrong_type(),
        None => Resp::Int(0),
    }
}

pub fn xdel(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("xdel");
    }
    let (mut s, exp) = match read_stream(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut removed = 0i64;
    for id_arg in &args[1..] {
        let Some(id) = parse_explicit_id(id_arg, 0) else {
            return Resp::err("ERR Invalid stream ID specified as stream command argument");
        };
        if s.entries.remove(&id).is_some() {
            removed += 1;
            if id > s.max_deleted_id {
                s.max_deleted_id = id;
            }
        }
    }
    db.put(ns, args[0].clone(), Datum::Stream(s), exp);
    Resp::Int(removed)
}

pub fn xtrim(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("xtrim");
    }
    let (mut s, exp) = match read_stream(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let before = s.len();
    let mut i = 1;
    match upper(&args[i]).as_str() {
        "MAXLEN" => {
            i += 1;
            if i < args.len() && matches!(args[i].as_ref(), b"~" | b"=") {
                i += 1;
            }
            let Some(n) = args.get(i).and_then(parse_i64).filter(|n| *n >= 0) else {
                return Resp::not_int();
            };
            trim_to_maxlen(&mut s, n as u64);
        }
        "MINID" => {
            i += 1;
            if i < args.len() && matches!(args[i].as_ref(), b"~" | b"=") {
                i += 1;
            }
            let Some(id) = args.get(i).and_then(|b| parse_explicit_id(b, 0)) else {
                return Resp::err("ERR Invalid stream ID specified as stream command argument");
            };
            trim_to_minid(&mut s, id);
        }
        _ => return Resp::syntax(),
    }
    let removed = (before - s.len()) as i64;
    db.put(ns, args[0].clone(), Datum::Stream(s), exp);
    Resp::Int(removed)
}

// ─────────────────────────────────────────────────────────────────────────────
// XRANGE / XREVRANGE
// ─────────────────────────────────────────────────────────────────────────────

pub fn xrange(db: &mut dyn Db, ns: u32, args: &[Bytes], rev: bool) -> Resp {
    if args.len() < 3 {
        return Resp::arity(if rev { "xrevrange" } else { "xrange" });
    }
    let (start_arg, end_arg) = if rev {
        (&args[2], &args[1])
    } else {
        (&args[1], &args[2])
    };
    let Some((lo, lo_ex)) = parse_range_bound(start_arg, true) else {
        return Resp::err("ERR Invalid stream ID specified as stream command argument");
    };
    let Some((hi, hi_ex)) = parse_range_bound(end_arg, false) else {
        return Resp::err("ERR Invalid stream ID specified as stream command argument");
    };
    let mut count: Option<usize> = None;
    if args.len() >= 5 && upper(&args[3]) == "COUNT" {
        match parse_i64(&args[4]) {
            Some(n) if n >= 0 => count = Some(n as usize),
            _ => return Resp::not_int(),
        }
    } else if args.len() != 3 {
        return Resp::syntax();
    }

    let (s, _) = match read_stream(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let lo_bound = if lo_ex { lo.next() } else { lo };
    let hi_bound = if hi_ex { hi.prev() } else { hi };
    if lo_bound > hi_bound {
        return Resp::Array(vec![]);
    }
    let mut items: Vec<Resp> = s
        .entries
        .range(lo_bound..=hi_bound)
        .map(|(id, fields)| id_to_resp(*id, fields))
        .collect();
    if rev {
        items.reverse();
    }
    if let Some(c) = count {
        items.truncate(c);
    }
    Resp::Array(items)
}

// ─────────────────────────────────────────────────────────────────────────────
// XREAD
// ─────────────────────────────────────────────────────────────────────────────

pub fn xread(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    let mut i = 0;
    let mut count: Option<usize> = None;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "COUNT" => {
                let Some(n) = args.get(i + 1).and_then(parse_i64).filter(|n| *n >= 0) else {
                    return Resp::not_int();
                };
                count = Some(n as usize);
                i += 2;
            }
            "BLOCK" => {
                // Blocking XREAD is not implemented; accept + ignore the
                // timeout arg and behave as a non-blocking read (matches
                // Voltra's "no Lua/no cluster" style of documented gaps
                // rather than silently hanging forever).
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return Resp::syntax(),
        }
    }
    let rest = &args[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(2) {
        return Resp::err("ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.");
    }
    let n = rest.len() / 2;
    let keys = &rest[..n];
    let id_args = &rest[n..];

    let mut out = Vec::with_capacity(n);
    for (key, id_arg) in keys.iter().zip(id_args.iter()) {
        let (s, _) = match read_stream(db, ns, key) {
            Ok(v) => v,
            Err(e) => return e,
        };
        // '$' means "only new entries after now" — with no blocking support
        // that is always empty, matching a BLOCK-less XREAD $ in real Redis
        // (it never returns anything on the first, non-blocking call).
        let after = if id_arg.as_ref() == b"$" {
            s.last_id
        } else {
            match parse_explicit_id(id_arg, u64::MAX) {
                Some(id) => id,
                None => {
                    return Resp::err("ERR Invalid stream ID specified as stream command argument")
                }
            }
        };
        let mut items: Vec<Resp> = s
            .entries
            .range(after.next()..)
            .map(|(id, fields)| id_to_resp(*id, fields))
            .collect();
        if let Some(c) = count {
            items.truncate(c);
        }
        if !items.is_empty() {
            out.push(Resp::Array(vec![
                Resp::Bulk(key.clone()),
                Resp::Array(items),
            ]));
        }
    }
    if out.is_empty() {
        Resp::NullArray
    } else {
        Resp::Array(out)
    }
}

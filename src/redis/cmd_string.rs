//! String, bitmap, and key-management commands.

use super::engine::{norm_range, read_str, scan_page, Db, ExpireUnit, Rng};
use super::resp::{fmt_f64, Resp};
use super::util::{glob_match, lossy, parse_f64, parse_i64, upper};
use crate::mvcc::Datum;
use bytes::Bytes;

fn str_or_empty(db: &mut dyn Db, ns: u32, key: &Bytes) -> Result<(Bytes, Option<u64>), Resp> {
    let (v, e) = read_str(db, ns, key)?;
    Ok((v.unwrap_or_default(), e))
}

// ─────────────────────────────────────────────────────────────────────────────
// GET / SET family
// ─────────────────────────────────────────────────────────────────────────────

pub fn get(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("get");
    }
    match read_str(db, ns, &args[0]) {
        Ok((Some(v), _)) => Resp::Bulk(v),
        Ok((None, _)) => Resp::Null,
        Err(e) => e,
    }
}

pub fn set(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("set");
    }
    let (key, val) = (args[0].clone(), args[1].clone());
    let mut expires: Option<Option<u64>> = None; // None = clear TTL (default), Some(e) = set
    let mut keep_ttl = false;
    let (mut nx, mut xx, mut want_get) = (false, false, false);
    let now = db.now_ms();

    let mut i = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "EX" | "PX" | "EXAT" | "PXAT" => {
                let unit = upper(&args[i]);
                let Some(n) = args.get(i + 1).and_then(parse_i64) else {
                    return Resp::not_int();
                };
                if n <= 0 && (unit == "EX" || unit == "PX") {
                    return Resp::err("ERR invalid expire time in 'set' command");
                }
                let at = match unit.as_str() {
                    "EX" => now + (n as u64) * 1000,
                    "PX" => now + n as u64,
                    "EXAT" => (n as u64) * 1000,
                    _ => n as u64,
                };
                expires = Some(Some(at));
                i += 2;
            }
            "KEEPTTL" => {
                keep_ttl = true;
                i += 1;
            }
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GET" => {
                want_get = true;
                i += 1;
            }
            _ => return Resp::syntax(),
        }
    }
    if nx && xx {
        return Resp::syntax();
    }

    let (old, old_exp) = match read_str(db, ns, &key) {
        Ok(v) => v,
        Err(e) => return e, // WRONGTYPE — SET..GET on a non-string errors
    };
    if (nx && old.is_some()) || (xx && old.is_none()) {
        return if want_get {
            old.map(Resp::Bulk).unwrap_or(Resp::Null)
        } else {
            Resp::Null
        };
    }
    let exp = if keep_ttl { old_exp } else { expires.flatten() };
    db.put(ns, key, Datum::Str(val), exp);
    if want_get {
        old.map(Resp::Bulk).unwrap_or(Resp::Null)
    } else {
        Resp::ok()
    }
}

pub fn setnx(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("setnx");
    }
    if db.get(ns, &args[0]).is_some() {
        return Resp::Int(0);
    }
    db.put(ns, args[0].clone(), Datum::Str(args[1].clone()), None);
    Resp::Int(1)
}

pub fn setex(db: &mut dyn Db, ns: u32, args: &[Bytes], ms: bool) -> Resp {
    if args.len() != 3 {
        return Resp::arity(if ms { "psetex" } else { "setex" });
    }
    let Some(n) = parse_i64(&args[1]) else {
        return Resp::not_int();
    };
    if n <= 0 {
        return Resp::err("ERR invalid expire time");
    }
    let at = db.now_ms() + if ms { n as u64 } else { n as u64 * 1000 };
    db.put(ns, args[0].clone(), Datum::Str(args[2].clone()), Some(at));
    Resp::ok()
}

pub fn mget(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() {
        return Resp::arity("mget");
    }
    Resp::Array(
        args.iter()
            .map(|k| match db.get(ns, k) {
                Some(Datum::Str(s)) => Resp::Bulk(s),
                _ => Resp::Null, // wrong type reads as nil in MGET
            })
            .collect(),
    )
}

pub fn mset(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() || args.len() % 2 != 0 {
        return Resp::arity("mset");
    }
    for pair in args.chunks(2) {
        db.put(ns, pair[0].clone(), Datum::Str(pair[1].clone()), None);
    }
    Resp::ok()
}

pub fn msetnx(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() || args.len() % 2 != 0 {
        return Resp::arity("msetnx");
    }
    for pair in args.chunks(2) {
        if db.get(ns, &pair[0]).is_some() {
            return Resp::Int(0);
        }
    }
    for pair in args.chunks(2) {
        db.put(ns, pair[0].clone(), Datum::Str(pair[1].clone()), None);
    }
    Resp::Int(1)
}

pub fn append(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("append");
    }
    let (old, exp) = match str_or_empty(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut buf = Vec::with_capacity(old.len() + args[1].len());
    buf.extend_from_slice(&old);
    buf.extend_from_slice(&args[1]);
    let len = buf.len();
    db.put(ns, args[0].clone(), Datum::Str(Bytes::from(buf)), exp);
    Resp::Int(len as i64)
}

pub fn strlen(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("strlen");
    }
    match read_str(db, ns, &args[0]) {
        Ok((v, _)) => Resp::Int(v.map(|b| b.len()).unwrap_or(0) as i64),
        Err(e) => e,
    }
}

/// INCR / DECR / INCRBY / DECRBY. `sign` flips for DECR*; `implicit` means
/// the delta is 1 (INCR/DECR) instead of parsed from args[1].
pub fn incr_by(db: &mut dyn Db, ns: u32, args: &[Bytes], sign: i64, implicit: bool) -> Resp {
    let want = if implicit { 1 } else { 2 };
    if args.len() != want {
        return Resp::arity("incr");
    }
    let delta = if implicit {
        sign
    } else {
        match parse_i64(&args[1]) {
            Some(d) => sign * d,
            None => return Resp::not_int(),
        }
    };
    let (old, exp) = match read_str(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let cur: i64 = match &old {
        Some(b) => match parse_i64(b) {
            Some(v) => v,
            None => return Resp::not_int(),
        },
        None => 0,
    };
    let Some(newv) = cur.checked_add(delta) else {
        return Resp::not_int();
    };
    db.put(ns, args[0].clone(), Datum::Str(Bytes::from(newv.to_string())), exp);
    Resp::Int(newv)
}

pub fn incr_by_float(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("incrbyfloat");
    }
    let Some(delta) = parse_f64(&args[1]) else {
        return Resp::not_float();
    };
    let (old, exp) = match read_str(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let cur: f64 = match &old {
        Some(b) => match parse_f64(b) {
            Some(v) => v,
            None => return Resp::not_float(),
        },
        None => 0.0,
    };
    let newv = cur + delta;
    if newv.is_nan() || newv.is_infinite() {
        return Resp::err("ERR increment would produce NaN or Infinity");
    }
    let s = fmt_f64(newv);
    db.put(ns, args[0].clone(), Datum::Str(Bytes::from(s.clone())), exp);
    Resp::bulk_str(s)
}

pub fn getset(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("getset");
    }
    let (old, _) = match read_str(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    db.put(ns, args[0].clone(), Datum::Str(args[1].clone()), None);
    old.map(Resp::Bulk).unwrap_or(Resp::Null)
}

pub fn getdel(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("getdel");
    }
    let (old, _) = match read_str(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if old.is_some() {
        db.del(ns, args[0].clone());
    }
    old.map(Resp::Bulk).unwrap_or(Resp::Null)
}

pub fn getex(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() {
        return Resp::arity("getex");
    }
    let (old, cur_exp) = match read_str(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let Some(val) = old else {
        return Resp::Null;
    };
    let now = db.now_ms();
    let mut new_exp = cur_exp;
    let mut changed = false;
    let mut i = 1;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "PERSIST" => {
                new_exp = None;
                changed = true;
                i += 1;
            }
            u @ ("EX" | "PX" | "EXAT" | "PXAT") => {
                let Some(n) = args.get(i + 1).and_then(parse_i64) else {
                    return Resp::not_int();
                };
                new_exp = Some(match u {
                    "EX" => now + n as u64 * 1000,
                    "PX" => now + n as u64,
                    "EXAT" => n as u64 * 1000,
                    _ => n as u64,
                });
                changed = true;
                i += 2;
            }
            _ => return Resp::syntax(),
        }
    }
    if changed {
        db.put(ns, args[0].clone(), Datum::Str(val.clone()), new_exp);
    }
    Resp::Bulk(val)
}

pub fn getrange(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("getrange");
    }
    let (Some(start), Some(stop)) = (parse_i64(&args[1]), parse_i64(&args[2])) else {
        return Resp::not_int();
    };
    let (v, _) = match str_or_empty(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    match norm_range(start, stop, v.len()) {
        Some((s, e)) => Resp::Bulk(v.slice(s..=e)),
        None => Resp::bulk_str(""),
    }
}

pub fn setrange(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("setrange");
    }
    let Some(offset) = parse_i64(&args[1]).filter(|o| *o >= 0) else {
        return Resp::err("ERR offset is out of range");
    };
    let offset = offset as usize;
    if offset + args[2].len() > 512 * 1024 * 1024 {
        return Resp::err("ERR string exceeds maximum allowed size (512MB)");
    }
    let (old, exp) = match str_or_empty(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut buf = old.to_vec();
    if buf.len() < offset + args[2].len() {
        buf.resize(offset + args[2].len(), 0);
    }
    buf[offset..offset + args[2].len()].copy_from_slice(&args[2]);
    let len = buf.len();
    db.put(ns, args[0].clone(), Datum::Str(Bytes::from(buf)), exp);
    Resp::Int(len as i64)
}

// ─────────────────────────────────────────────────────────────────────────────
// Bitmaps
// ─────────────────────────────────────────────────────────────────────────────

pub fn setbit(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("setbit");
    }
    let Some(pos) = parse_i64(&args[1]).filter(|p| (0..4_294_967_296).contains(p)) else {
        return Resp::err("ERR bit offset is not an integer or out of range");
    };
    let bit = match parse_i64(&args[2]) {
        Some(0) => 0u8,
        Some(1) => 1u8,
        _ => return Resp::err("ERR bit is not an integer or out of range"),
    };
    let (old, exp) = match str_or_empty(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let byte_idx = (pos / 8) as usize;
    let bit_idx = 7 - (pos % 8) as u8;
    let mut buf = old.to_vec();
    if buf.len() <= byte_idx {
        buf.resize(byte_idx + 1, 0);
    }
    let old_bit = (buf[byte_idx] >> bit_idx) & 1;
    if bit == 1 {
        buf[byte_idx] |= 1 << bit_idx;
    } else {
        buf[byte_idx] &= !(1 << bit_idx);
    }
    db.put(ns, args[0].clone(), Datum::Str(Bytes::from(buf)), exp);
    Resp::Int(old_bit as i64)
}

pub fn getbit(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("getbit");
    }
    let Some(pos) = parse_i64(&args[1]).filter(|p| *p >= 0) else {
        return Resp::err("ERR bit offset is not an integer or out of range");
    };
    let (v, _) = match str_or_empty(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let byte_idx = (pos / 8) as usize;
    if byte_idx >= v.len() {
        return Resp::Int(0);
    }
    Resp::Int(((v[byte_idx] >> (7 - (pos % 8) as u8)) & 1) as i64)
}

pub fn bitcount(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() {
        return Resp::arity("bitcount");
    }
    let (v, _) = match str_or_empty(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if args.len() == 1 {
        return Resp::Int(v.iter().map(|b| b.count_ones() as i64).sum());
    }
    if args.len() < 3 {
        return Resp::syntax();
    }
    let (Some(start), Some(stop)) = (parse_i64(&args[1]), parse_i64(&args[2])) else {
        return Resp::not_int();
    };
    let bit_mode = args.len() == 4 && upper(&args[3]) == "BIT";
    if args.len() == 4 && !bit_mode && upper(&args[3]) != "BYTE" {
        return Resp::syntax();
    }
    if bit_mode {
        let total_bits = v.len() * 8;
        let Some((s, e)) = norm_range(start, stop, total_bits) else {
            return Resp::Int(0);
        };
        let mut count = 0i64;
        for bit in s..=e {
            if (v[bit / 8] >> (7 - (bit % 8) as u8)) & 1 == 1 {
                count += 1;
            }
        }
        Resp::Int(count)
    } else {
        match norm_range(start, stop, v.len()) {
            Some((s, e)) => Resp::Int(v[s..=e].iter().map(|b| b.count_ones() as i64).sum()),
            None => Resp::Int(0),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Key management
// ─────────────────────────────────────────────────────────────────────────────

pub fn del(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() {
        return Resp::arity("del");
    }
    let mut n = 0;
    for k in args {
        if db.del(ns, k.clone()) {
            n += 1;
        }
    }
    Resp::Int(n)
}

pub fn exists(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() {
        return Resp::arity("exists");
    }
    Resp::Int(args.iter().filter(|k| db.get(ns, k).is_some()).count() as i64)
}

pub fn type_cmd(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("type");
    }
    match db.get(ns, &args[0]) {
        Some(d) => Resp::Simple(d.type_name().into()),
        None => Resp::Simple("none".into()),
    }
}

pub fn ttl(db: &mut dyn Db, ns: u32, args: &[Bytes], ms: bool) -> Resp {
    if args.len() != 1 {
        return Resp::arity("ttl");
    }
    if db.get(ns, &args[0]).is_none() {
        return Resp::Int(-2);
    }
    match db.expiry(ns, &args[0]) {
        Some(at) => {
            let rem = at.saturating_sub(db.now_ms());
            Resp::Int(if ms { rem as i64 } else { ((rem + 999) / 1000) as i64 })
        }
        None => Resp::Int(-1),
    }
}

pub fn expiretime(db: &mut dyn Db, ns: u32, args: &[Bytes], ms: bool) -> Resp {
    if args.len() != 1 {
        return Resp::arity("expiretime");
    }
    if db.get(ns, &args[0]).is_none() {
        return Resp::Int(-2);
    }
    match db.expiry(ns, &args[0]) {
        Some(at) => Resp::Int(if ms { at as i64 } else { (at / 1000) as i64 }),
        None => Resp::Int(-1),
    }
}

pub fn expire(db: &mut dyn Db, ns: u32, args: &[Bytes], unit: ExpireUnit, absolute: bool) -> Resp {
    if args.len() < 2 {
        return Resp::arity("expire");
    }
    let Some(n) = parse_i64(&args[1]) else {
        return Resp::not_int();
    };
    let mut cond: Option<&str> = None;
    if args.len() == 3 {
        match upper(&args[2]).as_str() {
            "NX" => cond = Some("NX"),
            "XX" => cond = Some("XX"),
            "GT" => cond = Some("GT"),
            "LT" => cond = Some("LT"),
            _ => return Resp::syntax(),
        }
    } else if args.len() > 3 {
        return Resp::syntax();
    }

    let Some(val) = db.get(ns, &args[0]) else {
        return Resp::Int(0);
    };
    let now = db.now_ms();
    let at = match (unit, absolute) {
        (ExpireUnit::Sec, false) => now.saturating_add_signed(n.saturating_mul(1000)),
        (ExpireUnit::Ms, false) => now.saturating_add_signed(n),
        (ExpireUnit::Sec, true) => (n.max(0) as u64).saturating_mul(1000),
        (ExpireUnit::Ms, true) => n.max(0) as u64,
    };
    let cur = db.expiry(ns, &args[0]);
    let allowed = match cond {
        Some("NX") => cur.is_none(),
        Some("XX") => cur.is_some(),
        Some("GT") => cur.map(|c| at > c).unwrap_or(false),
        Some("LT") => cur.map(|c| at < c).unwrap_or(true),
        _ => true,
    };
    if !allowed {
        return Resp::Int(0);
    }
    if at <= now {
        db.del(ns, args[0].clone()); // expiring in the past deletes the key
    } else {
        db.put(ns, args[0].clone(), val, Some(at));
    }
    Resp::Int(1)
}

pub fn persist(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("persist");
    }
    let Some(val) = db.get(ns, &args[0]) else {
        return Resp::Int(0);
    };
    if db.expiry(ns, &args[0]).is_none() {
        return Resp::Int(0);
    }
    db.put(ns, args[0].clone(), val, None);
    Resp::Int(1)
}

pub fn rename(db: &mut dyn Db, ns: u32, args: &[Bytes], nx: bool) -> Resp {
    if args.len() != 2 {
        return Resp::arity("rename");
    }
    let Some(val) = db.get(ns, &args[0]) else {
        return Resp::err("ERR no such key");
    };
    if nx && db.get(ns, &args[1]).is_some() {
        return Resp::Int(0);
    }
    let exp = db.expiry(ns, &args[0]);
    db.del(ns, args[0].clone());
    db.put(ns, args[1].clone(), val, exp);
    if nx {
        Resp::Int(1)
    } else {
        Resp::ok()
    }
}

pub fn copy(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("copy");
    }
    let mut dst_ns = ns;
    let mut replace = false;
    let mut i = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "DB" => {
                let Some(n) = args.get(i + 1).and_then(parse_i64).filter(|n| (0..16).contains(n))
                else {
                    return Resp::err("ERR DB index is out of range");
                };
                dst_ns = n as u32;
                i += 2;
            }
            "REPLACE" => {
                replace = true;
                i += 1;
            }
            _ => return Resp::syntax(),
        }
    }
    let Some(val) = db.get(ns, &args[0]) else {
        return Resp::Int(0);
    };
    if !replace && db.get(dst_ns, &args[1]).is_some() {
        return Resp::Int(0);
    }
    let exp = db.expiry(ns, &args[0]);
    db.put(dst_ns, args[1].clone(), val, exp);
    Resp::Int(1)
}

pub fn keys(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("keys");
    }
    let out: Vec<Resp> = db
        .keys(ns)
        .into_iter()
        .filter(|k| glob_match(&args[0], k))
        .map(Resp::Bulk)
        .collect();
    Resp::Array(out)
}

pub fn scan(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() {
        return Resp::arity("scan");
    }
    let Some(cursor) = parse_i64(&args[0]).filter(|c| *c >= 0) else {
        return Resp::err("ERR invalid cursor");
    };
    let mut pattern: Option<Bytes> = None;
    let mut count = 10usize;
    let mut type_filter: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "MATCH" => {
                let Some(p) = args.get(i + 1) else {
                    return Resp::syntax();
                };
                pattern = Some(p.clone());
                i += 2;
            }
            "COUNT" => {
                let Some(c) = args.get(i + 1).and_then(parse_i64).filter(|c| *c > 0) else {
                    return Resp::syntax();
                };
                count = c as usize;
                i += 2;
            }
            "TYPE" => {
                let Some(t) = args.get(i + 1) else {
                    return Resp::syntax();
                };
                type_filter = Some(lossy(t));
                i += 2;
            }
            _ => return Resp::syntax(),
        }
    }
    let all = db.keys(ns);
    let (next, page) = scan_page(&all, cursor as u64, count);
    let mut out = Vec::new();
    for k in page {
        if let Some(p) = &pattern {
            if !glob_match(p, &k) {
                continue;
            }
        }
        if let Some(t) = &type_filter {
            let matches = db.get(ns, &k).map(|d| d.type_name() == t).unwrap_or(false);
            if !matches {
                continue;
            }
        }
        out.push(Resp::Bulk(k));
    }
    Resp::Array(vec![Resp::bulk_str(next.to_string()), Resp::Array(out)])
}

pub fn randomkey(db: &mut dyn Db, ns: u32, _args: &[Bytes]) -> Resp {
    let keys = db.keys(ns);
    if keys.is_empty() {
        return Resp::Null;
    }
    let mut rng = Rng::seeded(db.now_ms() ^ keys.len() as u64);
    Resp::Bulk(keys[rng.below(keys.len())].clone())
}

pub fn flushdb(db: &mut dyn Db, ns: u32, _args: &[Bytes]) -> Resp {
    for k in db.keys(ns) {
        db.del(ns, k);
    }
    Resp::ok()
}

pub fn flushall(db: &mut dyn Db, _args: &[Bytes]) -> Resp {
    for ns in 0..16 {
        for k in db.keys(ns) {
            db.del(ns, k);
        }
    }
    Resp::ok()
}

pub fn swapdb(db: &mut dyn Db, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("swapdb");
    }
    let (Some(a), Some(b)) = (parse_i64(&args[0]), parse_i64(&args[1])) else {
        return Resp::not_int();
    };
    if !(0..16).contains(&a) || !(0..16).contains(&b) {
        return Resp::err("ERR DB index is out of range");
    }
    let (a, b) = (a as u32, b as u32);
    if a == b {
        return Resp::ok();
    }
    // Physical swap so the AOF stays a pure effect log.
    let keys_a = db.keys(a);
    let keys_b = db.keys(b);
    let vals_a: Vec<(Bytes, Datum, Option<u64>)> = keys_a
        .iter()
        .filter_map(|k| db.get(a, k).map(|v| (k.clone(), v, db.expiry(a, k))))
        .collect();
    let vals_b: Vec<(Bytes, Datum, Option<u64>)> = keys_b
        .iter()
        .filter_map(|k| db.get(b, k).map(|v| (k.clone(), v, db.expiry(b, k))))
        .collect();
    for k in keys_a {
        db.del(a, k);
    }
    for k in keys_b {
        db.del(b, k);
    }
    for (k, v, e) in vals_a {
        db.put(b, k, v, e);
    }
    for (k, v, e) in vals_b {
        db.put(a, k, v, e);
    }
    Resp::ok()
}

pub fn object(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("object");
    }
    let sub = upper(&args[0]);
    let key = &args[1];
    let Some(d) = db.get(ns, key) else {
        return Resp::err("ERR no such key");
    };
    match sub.as_str() {
        "ENCODING" => Resp::bulk_str(match d {
            Datum::Str(_) => "embstr",
            Datum::Hash(_) => "hashtable",
            Datum::List(_) => "quicklist",
            Datum::Set(_) => "hashtable",
            Datum::ZSet(_) => "skiplist",
            Datum::Row(_) => "raw",
        }),
        "REFCOUNT" => Resp::Int(1),
        "IDLETIME" => Resp::Int(0),
        "FREQ" => Resp::Int(0),
        _ => Resp::err(format!("ERR Unknown OBJECT subcommand '{}'", lossy(&args[0]))),
    }
}

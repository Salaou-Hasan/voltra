//! Hash and list commands.

use super::engine::{norm_range, read_hash, read_list, scan_page, store_coll, Db, Rng};
use super::resp::{fmt_f64, Resp};
use super::util::{glob_match, parse_f64, parse_i64, upper};
use crate::mvcc::Datum;
use bytes::Bytes;

// ─────────────────────────────────────────────────────────────────────────────
// Hashes
// ─────────────────────────────────────────────────────────────────────────────

pub fn hset(db: &mut dyn Db, ns: u32, args: &[Bytes], hmset_compat: bool) -> Resp {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Resp::arity(if hmset_compat { "hmset" } else { "hset" });
    }
    let (mut h, exp) = match read_hash(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut added = 0i64;
    for pair in args[1..].chunks(2) {
        if h.insert(pair[0].clone(), pair[1].clone()).is_none() {
            added += 1;
        }
    }
    db.put(ns, args[0].clone(), Datum::Hash(h), exp);
    if hmset_compat {
        Resp::ok()
    } else {
        Resp::Int(added)
    }
}

pub fn hsetnx(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("hsetnx");
    }
    let (mut h, exp) = match read_hash(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if h.contains_key(&args[1]) {
        return Resp::Int(0);
    }
    h.insert(args[1].clone(), args[2].clone());
    db.put(ns, args[0].clone(), Datum::Hash(h), exp);
    Resp::Int(1)
}

pub fn hget(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("hget");
    }
    match read_hash(db, ns, &args[0]) {
        Ok((h, _)) => h.get(&args[1]).cloned().map(Resp::Bulk).unwrap_or(Resp::Null),
        Err(e) => e,
    }
}

pub fn hmget(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("hmget");
    }
    match read_hash(db, ns, &args[0]) {
        Ok((h, _)) => Resp::Array(
            args[1..]
                .iter()
                .map(|f| h.get(f).cloned().map(Resp::Bulk).unwrap_or(Resp::Null))
                .collect(),
        ),
        Err(e) => e,
    }
}

pub fn hdel(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("hdel");
    }
    let (mut h, exp) = match read_hash(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut removed = 0i64;
    for f in &args[1..] {
        if h.remove(f).is_some() {
            removed += 1;
        }
    }
    store_coll(db, ns, args[0].clone(), Datum::Hash(h), exp);
    Resp::Int(removed)
}

pub fn hlen(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("hlen");
    }
    match read_hash(db, ns, &args[0]) {
        Ok((h, _)) => Resp::Int(h.len() as i64),
        Err(e) => e,
    }
}

pub fn hexists(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("hexists");
    }
    match read_hash(db, ns, &args[0]) {
        Ok((h, _)) => Resp::Int(h.contains_key(&args[1]) as i64),
        Err(e) => e,
    }
}

/// HKEYS (want_keys=true) / HVALS (false) — sorted for deterministic output.
pub fn hkeys(db: &mut dyn Db, ns: u32, args: &[Bytes], want_keys: bool) -> Resp {
    if args.len() != 1 {
        return Resp::arity(if want_keys { "hkeys" } else { "hvals" });
    }
    match read_hash(db, ns, &args[0]) {
        Ok((h, _)) => {
            let mut entries: Vec<(Bytes, Bytes)> =
                h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Resp::Array(
                entries
                    .into_iter()
                    .map(|(k, v)| Resp::Bulk(if want_keys { k } else { v }))
                    .collect(),
            )
        }
        Err(e) => e,
    }
}

pub fn hgetall(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("hgetall");
    }
    match read_hash(db, ns, &args[0]) {
        Ok((h, _)) => {
            let mut entries: Vec<(Bytes, Bytes)> =
                h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Resp::Map(
                entries
                    .into_iter()
                    .map(|(k, v)| (Resp::Bulk(k), Resp::Bulk(v)))
                    .collect(),
            )
        }
        Err(e) => e,
    }
}

pub fn hstrlen(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("hstrlen");
    }
    match read_hash(db, ns, &args[0]) {
        Ok((h, _)) => Resp::Int(h.get(&args[1]).map(|v| v.len()).unwrap_or(0) as i64),
        Err(e) => e,
    }
}

pub fn hincrby(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("hincrby");
    }
    let Some(delta) = parse_i64(&args[2]) else {
        return Resp::not_int();
    };
    let (mut h, exp) = match read_hash(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let cur = match h.get(&args[1]) {
        Some(v) => match parse_i64(v) {
            Some(n) => n,
            None => return Resp::err("ERR hash value is not an integer"),
        },
        None => 0,
    };
    let Some(newv) = cur.checked_add(delta) else {
        return Resp::not_int();
    };
    h.insert(args[1].clone(), Bytes::from(newv.to_string()));
    db.put(ns, args[0].clone(), Datum::Hash(h), exp);
    Resp::Int(newv)
}

pub fn hincrbyfloat(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("hincrbyfloat");
    }
    let Some(delta) = parse_f64(&args[2]) else {
        return Resp::not_float();
    };
    let (mut h, exp) = match read_hash(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let cur = match h.get(&args[1]) {
        Some(v) => match parse_f64(v) {
            Some(n) => n,
            None => return Resp::err("ERR hash value is not a float"),
        },
        None => 0.0,
    };
    let newv = cur + delta;
    if newv.is_nan() || newv.is_infinite() {
        return Resp::err("ERR increment would produce NaN or Infinity");
    }
    let s = fmt_f64(newv);
    h.insert(args[1].clone(), Bytes::from(s.clone()));
    db.put(ns, args[0].clone(), Datum::Hash(h), exp);
    Resp::bulk_str(s)
}

pub fn hrandfield(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() || args.len() > 3 {
        return Resp::arity("hrandfield");
    }
    let (h, _) = match read_hash(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut fields: Vec<(Bytes, Bytes)> = h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    fields.sort_by(|a, b| a.0.cmp(&b.0));
    let mut rng = Rng::seeded(db.now_ms() ^ fields.len() as u64);

    if args.len() == 1 {
        if fields.is_empty() {
            return Resp::Null;
        }
        return Resp::Bulk(fields[rng.below(fields.len())].0.clone());
    }
    let Some(count) = parse_i64(&args[1]) else {
        return Resp::not_int();
    };
    let with_values = args.len() == 3 && upper(&args[2]) == "WITHVALUES";
    if args.len() == 3 && !with_values {
        return Resp::syntax();
    }
    let mut picked: Vec<(Bytes, Bytes)> = Vec::new();
    if count >= 0 {
        // distinct fields, up to hash size
        let mut pool = fields.clone();
        let n = (count as usize).min(pool.len());
        for _ in 0..n {
            picked.push(pool.remove(rng.below(pool.len())));
        }
    } else {
        // may repeat
        for _ in 0..(-count) as usize {
            if fields.is_empty() {
                break;
            }
            picked.push(fields[rng.below(fields.len())].clone());
        }
    }
    let mut out = Vec::new();
    for (k, v) in picked {
        out.push(Resp::Bulk(k));
        if with_values {
            out.push(Resp::Bulk(v));
        }
    }
    Resp::Array(out)
}

pub fn hscan(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("hscan");
    }
    let Some(cursor) = parse_i64(&args[1]).filter(|c| *c >= 0) else {
        return Resp::err("ERR invalid cursor");
    };
    let mut pattern: Option<Bytes> = None;
    let mut count = 10usize;
    let mut novalues = false;
    let mut i = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "MATCH" => {
                pattern = args.get(i + 1).cloned();
                if pattern.is_none() {
                    return Resp::syntax();
                }
                i += 2;
            }
            "COUNT" => {
                match args.get(i + 1).and_then(parse_i64).filter(|c| *c > 0) {
                    Some(c) => count = c as usize,
                    None => return Resp::syntax(),
                }
                i += 2;
            }
            "NOVALUES" => {
                novalues = true;
                i += 1;
            }
            _ => return Resp::syntax(),
        }
    }
    let (h, _) = match read_hash(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut entries: Vec<(Bytes, Bytes)> = h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let (next, page) = scan_page(&entries, cursor as u64, count);
    let mut out = Vec::new();
    for (k, v) in page {
        if let Some(p) = &pattern {
            if !glob_match(p, &k) {
                continue;
            }
        }
        out.push(Resp::Bulk(k));
        if !novalues {
            out.push(Resp::Bulk(v));
        }
    }
    Resp::Array(vec![Resp::bulk_str(next.to_string()), Resp::Array(out)])
}

// ─────────────────────────────────────────────────────────────────────────────
// Lists
// ─────────────────────────────────────────────────────────────────────────────

pub fn push(db: &mut dyn Db, ns: u32, args: &[Bytes], left: bool, exists_only: bool) -> Resp {
    if args.len() < 2 {
        return Resp::arity(if left { "lpush" } else { "rpush" });
    }
    let (mut l, exp) = match read_list(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if exists_only && l.is_empty() {
        return Resp::Int(0);
    }
    for v in &args[1..] {
        if left {
            l.push_front(v.clone());
        } else {
            l.push_back(v.clone());
        }
    }
    let len = l.len();
    db.put(ns, args[0].clone(), Datum::List(l), exp);
    Resp::Int(len as i64)
}

pub fn pop(db: &mut dyn Db, ns: u32, args: &[Bytes], left: bool) -> Resp {
    if args.is_empty() || args.len() > 2 {
        return Resp::arity(if left { "lpop" } else { "rpop" });
    }
    let count = match args.get(1) {
        Some(c) => match parse_i64(c).filter(|n| *n >= 0) {
            Some(n) => Some(n as usize),
            None => return Resp::err("ERR value is out of range, must be positive"),
        },
        None => None,
    };
    let (mut l, exp) = match read_list(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if l.is_empty() {
        return if count.is_some() { Resp::NullArray } else { Resp::Null };
    }
    let n = count.unwrap_or(1).min(l.len());
    let mut popped = Vec::with_capacity(n);
    for _ in 0..n {
        let v = if left { l.pop_front() } else { l.pop_back() };
        if let Some(v) = v {
            popped.push(v);
        }
    }
    store_coll(db, ns, args[0].clone(), Datum::List(l), exp);
    if count.is_some() {
        Resp::Array(popped.into_iter().map(Resp::Bulk).collect())
    } else {
        Resp::Bulk(popped.into_iter().next().unwrap())
    }
}

pub fn llen(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("llen");
    }
    match read_list(db, ns, &args[0]) {
        Ok((l, _)) => Resp::Int(l.len() as i64),
        Err(e) => e,
    }
}

pub fn lrange(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("lrange");
    }
    let (Some(start), Some(stop)) = (parse_i64(&args[1]), parse_i64(&args[2])) else {
        return Resp::not_int();
    };
    let (l, _) = match read_list(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    match norm_range(start, stop, l.len()) {
        Some((s, e)) => Resp::Array(
            l.iter()
                .skip(s)
                .take(e - s + 1)
                .map(|v| Resp::Bulk(v.clone()))
                .collect(),
        ),
        None => Resp::Array(vec![]),
    }
}

pub fn lindex(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("lindex");
    }
    let Some(idx) = parse_i64(&args[1]) else {
        return Resp::not_int();
    };
    let (l, _) = match read_list(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let real = if idx < 0 { l.len() as i64 + idx } else { idx };
    if real < 0 || real >= l.len() as i64 {
        return Resp::Null;
    }
    l.get(real as usize).cloned().map(Resp::Bulk).unwrap_or(Resp::Null)
}

pub fn lset(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("lset");
    }
    let Some(idx) = parse_i64(&args[1]) else {
        return Resp::not_int();
    };
    let (mut l, exp) = match read_list(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if l.is_empty() {
        return Resp::err("ERR no such key");
    }
    let real = if idx < 0 { l.len() as i64 + idx } else { idx };
    if real < 0 || real >= l.len() as i64 {
        return Resp::err("ERR index out of range");
    }
    l.set(real as usize, args[2].clone());
    db.put(ns, args[0].clone(), Datum::List(l), exp);
    Resp::ok()
}

pub fn lrem(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("lrem");
    }
    let Some(count) = parse_i64(&args[1]) else {
        return Resp::not_int();
    };
    let (l, exp) = match read_list(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let target = &args[2];
    let mut removed = 0i64;
    let limit = count.unsigned_abs() as usize;
    let items: Vec<Bytes> = l.iter().cloned().collect();
    let mut keep: Vec<Bytes> = Vec::with_capacity(items.len());
    if count >= 0 {
        for v in items {
            if (&v == target) && (limit == 0 || (removed as usize) < limit) {
                removed += 1;
            } else {
                keep.push(v);
            }
        }
    } else {
        for v in items.into_iter().rev() {
            if (&v == target) && ((removed as usize) < limit) {
                removed += 1;
            } else {
                keep.push(v);
            }
        }
        keep.reverse();
    }
    let new_list: im::Vector<Bytes> = keep.into_iter().collect();
    store_coll(db, ns, args[0].clone(), Datum::List(new_list), exp);
    Resp::Int(removed)
}

pub fn ltrim(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("ltrim");
    }
    let (Some(start), Some(stop)) = (parse_i64(&args[1]), parse_i64(&args[2])) else {
        return Resp::not_int();
    };
    let (l, exp) = match read_list(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let new_list: im::Vector<Bytes> = match norm_range(start, stop, l.len()) {
        Some((s, e)) => l.iter().skip(s).take(e - s + 1).cloned().collect(),
        None => im::Vector::new(),
    };
    store_coll(db, ns, args[0].clone(), Datum::List(new_list), exp);
    Resp::ok()
}

pub fn linsert(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 4 {
        return Resp::arity("linsert");
    }
    let before = match upper(&args[1]).as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return Resp::syntax(),
    };
    let (mut l, exp) = match read_list(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if l.is_empty() {
        return Resp::Int(0);
    }
    let pos = l.iter().position(|v| v == &args[2]);
    match pos {
        Some(i) => {
            l.insert(if before { i } else { i + 1 }, args[3].clone());
            let len = l.len();
            db.put(ns, args[0].clone(), Datum::List(l), exp);
            Resp::Int(len as i64)
        }
        None => Resp::Int(-1),
    }
}

pub fn lmove(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 4 {
        return Resp::arity("lmove");
    }
    let from_left = match upper(&args[2]).as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return Resp::syntax(),
    };
    let to_left = match upper(&args[3]).as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return Resp::syntax(),
    };
    do_lmove(db, ns, &args[0], &args[1], from_left, to_left)
}

/// RPOPLPUSH src dst == LMOVE src dst RIGHT LEFT
pub fn lmove_compat(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("rpoplpush");
    }
    do_lmove(db, ns, &args[0], &args[1], false, true)
}

fn do_lmove(
    db: &mut dyn Db,
    ns: u32,
    src: &Bytes,
    dst: &Bytes,
    from_left: bool,
    to_left: bool,
) -> Resp {
    let (mut s, s_exp) = match read_list(db, ns, src) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let Some(val) = (if from_left { s.pop_front() } else { s.pop_back() }) else {
        return Resp::Null;
    };
    if src == dst {
        if to_left {
            s.push_front(val.clone());
        } else {
            s.push_back(val.clone());
        }
        db.put(ns, src.clone(), Datum::List(s), s_exp);
        return Resp::Bulk(val);
    }
    store_coll(db, ns, src.clone(), Datum::List(s), s_exp);
    let (mut d, d_exp) = match read_list(db, ns, dst) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if to_left {
        d.push_front(val.clone());
    } else {
        d.push_back(val.clone());
    }
    db.put(ns, dst.clone(), Datum::List(d), d_exp);
    Resp::Bulk(val)
}

pub fn lpos(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("lpos");
    }
    let mut rank = 1i64;
    let mut count: Option<usize> = None;
    let mut i = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "RANK" => {
                match args.get(i + 1).and_then(parse_i64).filter(|r| *r != 0) {
                    Some(r) => rank = r,
                    None => return Resp::err("ERR RANK can't be zero"),
                }
                i += 2;
            }
            "COUNT" => {
                match args.get(i + 1).and_then(parse_i64).filter(|c| *c >= 0) {
                    Some(c) => count = Some(c as usize),
                    None => return Resp::err("ERR COUNT can't be negative"),
                }
                i += 2;
            }
            _ => return Resp::syntax(),
        }
    }
    let (l, _) = match read_list(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let items: Vec<Bytes> = l.iter().cloned().collect();
    let mut matches: Vec<i64> = Vec::new();
    let limit = count.unwrap_or(1);
    let mut skip = rank.unsigned_abs() as usize - 1;
    if rank > 0 {
        for (idx, v) in items.iter().enumerate() {
            if v == &args[1] {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                matches.push(idx as i64);
                if limit != 0 && matches.len() >= limit {
                    break;
                }
            }
        }
    } else {
        for (idx, v) in items.iter().enumerate().rev() {
            if v == &args[1] {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                matches.push(idx as i64);
                if limit != 0 && matches.len() >= limit {
                    break;
                }
            }
        }
    }
    if count.is_some() {
        Resp::Array(matches.into_iter().map(Resp::Int).collect())
    } else {
        matches.first().map(|i| Resp::Int(*i)).unwrap_or(Resp::Null)
    }
}

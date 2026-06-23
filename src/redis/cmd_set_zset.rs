//! Set and sorted-set commands.

use super::engine::{read_set, read_zset, scan_page, store_coll, Db, Rng, SetOp};
use super::resp::{fmt_f64, Resp};
use super::util::{glob_match, parse_f64, parse_i64, upper};
use crate::mvcc::{Datum, ZSet};
use bytes::Bytes;

// ─────────────────────────────────────────────────────────────────────────────
// Sets
// ─────────────────────────────────────────────────────────────────────────────

pub fn sadd(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("sadd");
    }
    let (mut s, exp) = match read_set(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut added = 0i64;
    for m in &args[1..] {
        if s.insert(m.clone()).is_none() {
            added += 1;
        }
    }
    db.put(ns, args[0].clone(), Datum::Set(s), exp);
    Resp::Int(added)
}

pub fn srem(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("srem");
    }
    let (mut s, exp) = match read_set(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut removed = 0i64;
    for m in &args[1..] {
        if s.remove(m).is_some() {
            removed += 1;
        }
    }
    store_coll(db, ns, args[0].clone(), Datum::Set(s), exp);
    Resp::Int(removed)
}

fn sorted_members(s: &im::HashSet<Bytes>) -> Vec<Bytes> {
    let mut v: Vec<Bytes> = s.iter().cloned().collect();
    v.sort();
    v
}

pub fn smembers(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("smembers");
    }
    match read_set(db, ns, &args[0]) {
        Ok((s, _)) => Resp::SetReply(sorted_members(&s).into_iter().map(Resp::Bulk).collect()),
        Err(e) => e,
    }
}

pub fn sismember(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("sismember");
    }
    match read_set(db, ns, &args[0]) {
        Ok((s, _)) => Resp::Int(s.contains(&args[1]) as i64),
        Err(e) => e,
    }
}

pub fn smismember(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("smismember");
    }
    match read_set(db, ns, &args[0]) {
        Ok((s, _)) => Resp::Array(
            args[1..]
                .iter()
                .map(|m| Resp::Int(s.contains(m) as i64))
                .collect(),
        ),
        Err(e) => e,
    }
}

pub fn scard(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("scard");
    }
    match read_set(db, ns, &args[0]) {
        Ok((s, _)) => Resp::Int(s.len() as i64),
        Err(e) => e,
    }
}

pub fn spop(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() || args.len() > 2 {
        return Resp::arity("spop");
    }
    let count = match args.get(1) {
        Some(c) => match parse_i64(c).filter(|n| *n >= 0) {
            Some(n) => Some(n as usize),
            None => return Resp::err("ERR value is out of range, must be positive"),
        },
        None => None,
    };
    let (mut s, exp) = match read_set(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if s.is_empty() {
        return if count.is_some() { Resp::SetReply(vec![]) } else { Resp::Null };
    }
    let mut pool = sorted_members(&s);
    let mut rng = Rng::seeded(db.now_ms() ^ pool.len() as u64);
    let n = count.unwrap_or(1).min(pool.len());
    let mut popped = Vec::with_capacity(n);
    for _ in 0..n {
        let m = pool.remove(rng.below(pool.len()));
        s.remove(&m);
        popped.push(m);
    }
    store_coll(db, ns, args[0].clone(), Datum::Set(s), exp);
    if count.is_some() {
        Resp::SetReply(popped.into_iter().map(Resp::Bulk).collect())
    } else {
        Resp::Bulk(popped.into_iter().next().unwrap())
    }
}

pub fn srandmember(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() || args.len() > 2 {
        return Resp::arity("srandmember");
    }
    let (s, _) = match read_set(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let pool = sorted_members(&s);
    let mut rng = Rng::seeded(db.now_ms() ^ pool.len() as u64 ^ 0x5eed);
    match args.get(1) {
        None => {
            if pool.is_empty() {
                Resp::Null
            } else {
                Resp::Bulk(pool[rng.below(pool.len())].clone())
            }
        }
        Some(c) => {
            let Some(count) = parse_i64(c) else {
                return Resp::not_int();
            };
            let mut out = Vec::new();
            if count >= 0 {
                let mut p = pool.clone();
                for _ in 0..(count as usize).min(p.len()) {
                    out.push(p.remove(rng.below(p.len())));
                }
            } else {
                for _ in 0..(-count) as usize {
                    if pool.is_empty() {
                        break;
                    }
                    out.push(pool[rng.below(pool.len())].clone());
                }
            }
            Resp::Array(out.into_iter().map(Resp::Bulk).collect())
        }
    }
}

pub fn smove(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("smove");
    }
    let (mut src, src_exp) = match read_set(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if src.remove(&args[2]).is_none() {
        return Resp::Int(0);
    }
    if args[0] == args[1] {
        src.insert(args[2].clone());
        db.put(ns, args[0].clone(), Datum::Set(src), src_exp);
        return Resp::Int(1);
    }
    let (mut dst, dst_exp) = match read_set(db, ns, &args[1]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    store_coll(db, ns, args[0].clone(), Datum::Set(src), src_exp);
    dst.insert(args[2].clone());
    db.put(ns, args[1].clone(), Datum::Set(dst), dst_exp);
    Resp::Int(1)
}

/// SUNION/SINTER/SDIFF and their STORE variants.
pub fn setop(db: &mut dyn Db, ns: u32, args: &[Bytes], op: SetOp, store: bool) -> Resp {
    let min_args = if store { 2 } else { 1 };
    if args.len() < min_args {
        return Resp::arity("sunion");
    }
    let keys = if store { &args[1..] } else { args };
    let mut acc: Option<im::HashSet<Bytes>> = None;
    for k in keys {
        let (s, _) = match read_set(db, ns, k) {
            Ok(v) => v,
            Err(e) => return e,
        };
        acc = Some(match acc {
            None => s,
            Some(a) => match op {
                SetOp::Union => {
                    let mut u = a;
                    for m in s.iter() {
                        u.insert(m.clone());
                    }
                    u
                }
                SetOp::Inter => {
                    let mut r = im::HashSet::new();
                    for m in a.iter() {
                        if s.contains(m) {
                            r.insert(m.clone());
                        }
                    }
                    r
                }
                SetOp::Diff => {
                    let mut r = a;
                    for m in s.iter() {
                        r.remove(m);
                    }
                    r
                }
            },
        });
    }
    let result = acc.unwrap_or_default();
    if store {
        let card = result.len() as i64;
        store_coll(db, ns, args[0].clone(), Datum::Set(result), None);
        Resp::Int(card)
    } else {
        Resp::SetReply(sorted_members(&result).into_iter().map(Resp::Bulk).collect())
    }
}

pub fn sintercard(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("sintercard");
    }
    let Some(numkeys) = parse_i64(&args[0]).filter(|n| *n > 0) else {
        return Resp::err("ERR numkeys should be greater than 0");
    };
    let numkeys = numkeys as usize;
    if args.len() < 1 + numkeys {
        return Resp::syntax();
    }
    let mut limit = usize::MAX;
    if args.len() > 1 + numkeys {
        if args.len() != 3 + numkeys || upper(&args[1 + numkeys]) != "LIMIT" {
            return Resp::syntax();
        }
        match parse_i64(&args[2 + numkeys]).filter(|l| *l >= 0) {
            Some(0) => limit = usize::MAX,
            Some(l) => limit = l as usize,
            None => return Resp::err("ERR LIMIT can't be negative"),
        }
    }
    let mut acc: Option<im::HashSet<Bytes>> = None;
    for k in &args[1..1 + numkeys] {
        let (s, _) = match read_set(db, ns, k) {
            Ok(v) => v,
            Err(e) => return e,
        };
        acc = Some(match acc {
            None => s,
            Some(a) => {
                let mut r = im::HashSet::new();
                for m in a.iter() {
                    if s.contains(m) {
                        r.insert(m.clone());
                    }
                }
                r
            }
        });
    }
    Resp::Int(acc.map(|s| s.len().min(limit)).unwrap_or(0) as i64)
}

pub fn sscan(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("sscan");
    }
    let Some(cursor) = parse_i64(&args[1]).filter(|c| *c >= 0) else {
        return Resp::err("ERR invalid cursor");
    };
    let (pattern, count) = match scan_opts(&args[2..]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (s, _) = match read_set(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let members = sorted_members(&s);
    let (next, page) = scan_page(&members, cursor as u64, count);
    let out: Vec<Resp> = page
        .into_iter()
        .filter(|m| pattern.as_ref().map(|p| glob_match(p, m)).unwrap_or(true))
        .map(Resp::Bulk)
        .collect();
    Resp::Array(vec![Resp::bulk_str(next.to_string()), Resp::Array(out)])
}

fn scan_opts(rest: &[Bytes]) -> Result<(Option<Bytes>, usize), Resp> {
    let mut pattern = None;
    let mut count = 10usize;
    let mut i = 0;
    while i < rest.len() {
        match upper(&rest[i]).as_str() {
            "MATCH" => {
                pattern = rest.get(i + 1).cloned();
                if pattern.is_none() {
                    return Err(Resp::syntax());
                }
                i += 2;
            }
            "COUNT" => {
                match rest.get(i + 1).and_then(parse_i64).filter(|c| *c > 0) {
                    Some(c) => count = c as usize,
                    None => return Err(Resp::syntax()),
                }
                i += 2;
            }
            _ => return Err(Resp::syntax()),
        }
    }
    Ok((pattern, count))
}

// ─────────────────────────────────────────────────────────────────────────────
// Sorted sets
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a score bound: `5`, `(5`, `-inf`, `+inf`. Returns (value, exclusive).
fn parse_score_bound(b: &Bytes) -> Option<(f64, bool)> {
    if b.first() == Some(&b'(') {
        parse_f64(&b.slice(1..)).map(|f| (f, true))
    } else {
        parse_f64(b).map(|f| (f, false))
    }
}

/// Parse a lex bound: `-`, `+`, `[member`, `(member`.
enum LexBound {
    NegInf,
    PosInf,
    Incl(Bytes),
    Excl(Bytes),
}

fn parse_lex_bound(b: &Bytes) -> Option<LexBound> {
    match b.first() {
        Some(b'-') if b.len() == 1 => Some(LexBound::NegInf),
        Some(b'+') if b.len() == 1 => Some(LexBound::PosInf),
        Some(b'[') => Some(LexBound::Incl(b.slice(1..))),
        Some(b'(') => Some(LexBound::Excl(b.slice(1..))),
        _ => None,
    }
}

fn lex_ge(m: &Bytes, bound: &LexBound) -> bool {
    match bound {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Incl(b) => m >= b,
        LexBound::Excl(b) => m > b,
    }
}

fn lex_le(m: &Bytes, bound: &LexBound) -> bool {
    match bound {
        LexBound::NegInf => false,
        LexBound::PosInf => true,
        LexBound::Incl(b) => m <= b,
        LexBound::Excl(b) => m < b,
    }
}

/// Score-ordered (score, member) pairs.
fn ordered(z: &ZSet) -> Vec<(f64, Bytes)> {
    z.by_score.iter().map(|(s, m)| (s.0, m.clone())).collect()
}

fn with_scores_reply(items: Vec<(f64, Bytes)>, with_scores: bool) -> Resp {
    let mut out = Vec::with_capacity(items.len() * if with_scores { 2 } else { 1 });
    for (s, m) in items {
        out.push(Resp::Bulk(m));
        if with_scores {
            out.push(Resp::bulk_str(fmt_f64(s)));
        }
    }
    Resp::Array(out)
}

pub fn zadd(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 3 {
        return Resp::arity("zadd");
    }
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) = (false, false, false, false, false, false);
    let mut i = 1;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            "CH" => ch = true,
            "INCR" => incr = true,
            _ => break,
        }
        i += 1;
    }
    if nx && xx {
        return Resp::err("ERR XX and NX options at the same time are not compatible");
    }
    if (gt && lt) || (nx && (gt || lt)) {
        return Resp::err("ERR GT, LT, and/or NX options at the same time are not compatible");
    }
    let rest = &args[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(2) {
        return Resp::syntax();
    }
    if incr && rest.len() != 2 {
        return Resp::err("ERR INCR option supports a single increment-element pair");
    }
    let (mut z, exp) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut added = 0i64;
    let mut changed = 0i64;
    let mut incr_result: Option<f64> = None;
    for pair in rest.chunks(2) {
        let Some(score) = parse_f64(&pair[0]) else {
            return Resp::not_float();
        };
        let member = pair[1].clone();
        let cur = z.score(&member);
        let new_score = if incr { cur.unwrap_or(0.0) + score } else { score };
        let allowed = match (cur, nx, xx, gt, lt) {
            (Some(_), true, _, _, _) => false,         // NX: only add new
            (None, _, true, _, _) => false,            // XX: only update existing
            (Some(c), _, _, true, _) => new_score > c, // GT
            (Some(c), _, _, _, true) => new_score < c, // LT
            _ => true,
        };
        if !allowed {
            if incr {
                return Resp::Null;
            }
            continue;
        }
        if cur.is_none() {
            added += 1;
            changed += 1;
        } else if cur != Some(new_score) {
            changed += 1;
        }
        z.insert(member, new_score);
        if incr {
            incr_result = Some(new_score);
        }
    }
    store_coll(db, ns, args[0].clone(), Datum::ZSet(z), exp);
    if incr {
        return incr_result
            .map(|s| Resp::bulk_str(fmt_f64(s)))
            .unwrap_or(Resp::Null);
    }
    Resp::Int(if ch { changed } else { added })
}

pub fn zrem(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("zrem");
    }
    let (mut z, exp) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut removed = 0i64;
    for m in &args[1..] {
        if z.remove(m).is_some() {
            removed += 1;
        }
    }
    store_coll(db, ns, args[0].clone(), Datum::ZSet(z), exp);
    Resp::Int(removed)
}

pub fn zscore(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 2 {
        return Resp::arity("zscore");
    }
    match read_zset(db, ns, &args[0]) {
        Ok((z, _)) => z
            .score(&args[1])
            .map(|s| Resp::bulk_str(fmt_f64(s)))
            .unwrap_or(Resp::Null),
        Err(e) => e,
    }
}

pub fn zmscore(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("zmscore");
    }
    match read_zset(db, ns, &args[0]) {
        Ok((z, _)) => Resp::Array(
            args[1..]
                .iter()
                .map(|m| {
                    z.score(m)
                        .map(|s| Resp::bulk_str(fmt_f64(s)))
                        .unwrap_or(Resp::Null)
                })
                .collect(),
        ),
        Err(e) => e,
    }
}

pub fn zcard(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 1 {
        return Resp::arity("zcard");
    }
    match read_zset(db, ns, &args[0]) {
        Ok((z, _)) => Resp::Int(z.len() as i64),
        Err(e) => e,
    }
}

pub fn zcount(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("zcount");
    }
    let (Some((min, min_ex)), Some((max, max_ex))) =
        (parse_score_bound(&args[1]), parse_score_bound(&args[2]))
    else {
        return Resp::err("ERR min or max is not a float");
    };
    match read_zset(db, ns, &args[0]) {
        Ok((z, _)) => {
            let n = ordered(&z)
                .into_iter()
                .filter(|(s, _)| {
                    (if min_ex { *s > min } else { *s >= min })
                        && (if max_ex { *s < max } else { *s <= max })
                })
                .count();
            Resp::Int(n as i64)
        }
        Err(e) => e,
    }
}

pub fn zincrby(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("zincrby");
    }
    let Some(delta) = parse_f64(&args[1]) else {
        return Resp::not_float();
    };
    let (mut z, exp) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let newv = z.score(&args[2]).unwrap_or(0.0) + delta;
    if newv.is_nan() {
        return Resp::err("ERR resulting score is not a number (NaN)");
    }
    z.insert(args[2].clone(), newv);
    db.put(ns, args[0].clone(), Datum::ZSet(z), exp);
    Resp::bulk_str(fmt_f64(newv))
}

pub fn zrank(db: &mut dyn Db, ns: u32, args: &[Bytes], rev: bool) -> Resp {
    if args.len() < 2 || args.len() > 3 {
        return Resp::arity("zrank");
    }
    let with_score = args.len() == 3 && upper(&args[2]) == "WITHSCORE";
    if args.len() == 3 && !with_score {
        return Resp::syntax();
    }
    match read_zset(db, ns, &args[0]) {
        Ok((z, _)) => {
            let items = ordered(&z);
            let pos = items.iter().position(|(_, m)| m == &args[1]);
            match pos {
                Some(i) => {
                    let rank = if rev { items.len() - 1 - i } else { i } as i64;
                    if with_score {
                        Resp::Array(vec![Resp::Int(rank), Resp::bulk_str(fmt_f64(items[i].0))])
                    } else {
                        Resp::Int(rank)
                    }
                }
                None => {
                    if with_score {
                        Resp::NullArray
                    } else {
                        Resp::Null
                    }
                }
            }
        }
        Err(e) => e,
    }
}

/// Unified ZRANGE (Redis 6.2+): index, BYSCORE, or BYLEX ranges, REV, LIMIT.
pub fn zrange(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 3 {
        return Resp::arity("zrange");
    }
    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut with_scores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 3;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "BYSCORE" => {
                by_score = true;
                i += 1;
            }
            "BYLEX" => {
                by_lex = true;
                i += 1;
            }
            "REV" => {
                rev = true;
                i += 1;
            }
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" => {
                let (Some(off), Some(cnt)) = (
                    args.get(i + 1).and_then(parse_i64),
                    args.get(i + 2).and_then(parse_i64),
                ) else {
                    return Resp::syntax();
                };
                limit = Some((off, cnt));
                i += 3;
            }
            _ => return Resp::syntax(),
        }
    }
    if by_score && by_lex {
        return Resp::syntax();
    }
    if limit.is_some() && !by_score && !by_lex {
        return Resp::err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
    }
    if by_lex && with_scores {
        return Resp::syntax();
    }
    let (z, _) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut items = ordered(&z);

    if by_score {
        // In REV mode the bounds come as (max, min).
        let (lo_raw, hi_raw) = if rev { (&args[2], &args[1]) } else { (&args[1], &args[2]) };
        let (Some((min, min_ex)), Some((max, max_ex))) =
            (parse_score_bound(lo_raw), parse_score_bound(hi_raw))
        else {
            return Resp::err("ERR min or max is not a float");
        };
        items.retain(|(s, _)| {
            (if min_ex { *s > min } else { *s >= min })
                && (if max_ex { *s < max } else { *s <= max })
        });
        if rev {
            items.reverse();
        }
        if let Some((off, cnt)) = limit {
            items = apply_limit(items, off, cnt);
        }
        return with_scores_reply(items, with_scores);
    }

    if by_lex {
        let (lo_raw, hi_raw) = if rev { (&args[2], &args[1]) } else { (&args[1], &args[2]) };
        let (Some(min), Some(max)) = (parse_lex_bound(lo_raw), parse_lex_bound(hi_raw)) else {
            return Resp::err("ERR min or max not valid string range item");
        };
        items.retain(|(_, m)| lex_ge(m, &min) && lex_le(m, &max));
        if rev {
            items.reverse();
        }
        if let Some((off, cnt)) = limit {
            items = apply_limit(items, off, cnt);
        }
        return with_scores_reply(items, false);
    }

    // Index range.
    let (Some(start), Some(stop)) = (parse_i64(&args[1]), parse_i64(&args[2])) else {
        return Resp::not_int();
    };
    if rev {
        items.reverse();
    }
    match super::engine::norm_range(start, stop, items.len()) {
        Some((s, e)) => with_scores_reply(items[s..=e].to_vec(), with_scores),
        None => Resp::Array(vec![]),
    }
}

fn apply_limit(items: Vec<(f64, Bytes)>, off: i64, cnt: i64) -> Vec<(f64, Bytes)> {
    let off = off.max(0) as usize;
    if off >= items.len() {
        return Vec::new();
    }
    let take = if cnt < 0 { items.len() } else { cnt as usize };
    items.into_iter().skip(off).take(take).collect()
}

pub fn zrevrange(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 3 || args.len() > 4 {
        return Resp::arity("zrevrange");
    }
    let with_scores = args.len() == 4 && upper(&args[3]) == "WITHSCORES";
    if args.len() == 4 && !with_scores {
        return Resp::syntax();
    }
    let (Some(start), Some(stop)) = (parse_i64(&args[1]), parse_i64(&args[2])) else {
        return Resp::not_int();
    };
    let (z, _) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut items = ordered(&z);
    items.reverse();
    match super::engine::norm_range(start, stop, items.len()) {
        Some((s, e)) => with_scores_reply(items[s..=e].to_vec(), with_scores),
        None => Resp::Array(vec![]),
    }
}

pub fn zrangebyscore(db: &mut dyn Db, ns: u32, args: &[Bytes], rev: bool) -> Resp {
    if args.len() < 3 {
        return Resp::arity("zrangebyscore");
    }
    let mut with_scores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 3;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" => {
                let (Some(off), Some(cnt)) = (
                    args.get(i + 1).and_then(parse_i64),
                    args.get(i + 2).and_then(parse_i64),
                ) else {
                    return Resp::syntax();
                };
                limit = Some((off, cnt));
                i += 3;
            }
            _ => return Resp::syntax(),
        }
    }
    // ZREVRANGEBYSCORE takes (max, min).
    let (lo_raw, hi_raw) = if rev { (&args[2], &args[1]) } else { (&args[1], &args[2]) };
    let (Some((min, min_ex)), Some((max, max_ex))) =
        (parse_score_bound(lo_raw), parse_score_bound(hi_raw))
    else {
        return Resp::err("ERR min or max is not a float");
    };
    let (z, _) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut items: Vec<(f64, Bytes)> = ordered(&z)
        .into_iter()
        .filter(|(s, _)| {
            (if min_ex { *s > min } else { *s >= min })
                && (if max_ex { *s < max } else { *s <= max })
        })
        .collect();
    if rev {
        items.reverse();
    }
    if let Some((off, cnt)) = limit {
        items = apply_limit(items, off, cnt);
    }
    with_scores_reply(items, with_scores)
}

pub fn zrangebylex(db: &mut dyn Db, ns: u32, args: &[Bytes], rev: bool) -> Resp {
    if args.len() < 3 {
        return Resp::arity("zrangebylex");
    }
    let mut limit: Option<(i64, i64)> = None;
    if args.len() > 3 {
        if args.len() != 6 || upper(&args[3]) != "LIMIT" {
            return Resp::syntax();
        }
        let (Some(off), Some(cnt)) = (parse_i64(&args[4]), parse_i64(&args[5])) else {
            return Resp::syntax();
        };
        limit = Some((off, cnt));
    }
    let (lo_raw, hi_raw) = if rev { (&args[2], &args[1]) } else { (&args[1], &args[2]) };
    let (Some(min), Some(max)) = (parse_lex_bound(lo_raw), parse_lex_bound(hi_raw)) else {
        return Resp::err("ERR min or max not valid string range item");
    };
    let (z, _) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut items: Vec<(f64, Bytes)> = ordered(&z)
        .into_iter()
        .filter(|(_, m)| lex_ge(m, &min) && lex_le(m, &max))
        .collect();
    if rev {
        items.reverse();
    }
    if let Some((off, cnt)) = limit {
        items = apply_limit(items, off, cnt);
    }
    with_scores_reply(items, false)
}

pub fn zlexcount(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("zlexcount");
    }
    let (Some(min), Some(max)) = (parse_lex_bound(&args[1]), parse_lex_bound(&args[2])) else {
        return Resp::err("ERR min or max not valid string range item");
    };
    match read_zset(db, ns, &args[0]) {
        Ok((z, _)) => Resp::Int(
            ordered(&z)
                .into_iter()
                .filter(|(_, m)| lex_ge(m, &min) && lex_le(m, &max))
                .count() as i64,
        ),
        Err(e) => e,
    }
}

pub fn zpop(db: &mut dyn Db, ns: u32, args: &[Bytes], min: bool) -> Resp {
    if args.is_empty() || args.len() > 2 {
        return Resp::arity(if min { "zpopmin" } else { "zpopmax" });
    }
    let count = match args.get(1) {
        Some(c) => match parse_i64(c).filter(|n| *n >= 0) {
            Some(n) => n as usize,
            None => return Resp::not_int(),
        },
        None => 1,
    };
    let (mut z, exp) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut items = ordered(&z);
    if !min {
        items.reverse();
    }
    let mut out = Vec::new();
    for (s, m) in items.into_iter().take(count) {
        z.remove(&m);
        out.push(Resp::Bulk(m));
        out.push(Resp::bulk_str(fmt_f64(s)));
    }
    store_coll(db, ns, args[0].clone(), Datum::ZSet(z), exp);
    Resp::Array(out)
}

pub fn zrandmember(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() || args.len() > 3 {
        return Resp::arity("zrandmember");
    }
    let (z, _) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let pool = ordered(&z);
    let mut rng = Rng::seeded(db.now_ms() ^ pool.len() as u64 ^ 0x2a11d);
    if args.len() == 1 {
        if pool.is_empty() {
            return Resp::Null;
        }
        return Resp::Bulk(pool[rng.below(pool.len())].1.clone());
    }
    let Some(count) = parse_i64(&args[1]) else {
        return Resp::not_int();
    };
    let with_scores = args.len() == 3 && upper(&args[2]) == "WITHSCORES";
    if args.len() == 3 && !with_scores {
        return Resp::syntax();
    }
    let mut picked = Vec::new();
    if count >= 0 {
        let mut p = pool.clone();
        for _ in 0..(count as usize).min(p.len()) {
            picked.push(p.remove(rng.below(p.len())));
        }
    } else {
        for _ in 0..(-count) as usize {
            if pool.is_empty() {
                break;
            }
            picked.push(pool[rng.below(pool.len())].clone());
        }
    }
    let mut out = Vec::new();
    for (s, m) in picked {
        out.push(Resp::Bulk(m));
        if with_scores {
            out.push(Resp::bulk_str(fmt_f64(s)));
        }
    }
    Resp::Array(out)
}

pub fn zremrangebyrank(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("zremrangebyrank");
    }
    let (Some(start), Some(stop)) = (parse_i64(&args[1]), parse_i64(&args[2])) else {
        return Resp::not_int();
    };
    let (mut z, exp) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let items = ordered(&z);
    let Some((s, e)) = super::engine::norm_range(start, stop, items.len()) else {
        return Resp::Int(0);
    };
    let mut removed = 0i64;
    for (_, m) in &items[s..=e] {
        z.remove(m);
        removed += 1;
    }
    store_coll(db, ns, args[0].clone(), Datum::ZSet(z), exp);
    Resp::Int(removed)
}

pub fn zremrangebyscore(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("zremrangebyscore");
    }
    let (Some((min, min_ex)), Some((max, max_ex))) =
        (parse_score_bound(&args[1]), parse_score_bound(&args[2]))
    else {
        return Resp::err("ERR min or max is not a float");
    };
    let (mut z, exp) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let victims: Vec<Bytes> = ordered(&z)
        .into_iter()
        .filter(|(s, _)| {
            (if min_ex { *s > min } else { *s >= min })
                && (if max_ex { *s < max } else { *s <= max })
        })
        .map(|(_, m)| m)
        .collect();
    let removed = victims.len() as i64;
    for m in victims {
        z.remove(&m);
    }
    store_coll(db, ns, args[0].clone(), Datum::ZSet(z), exp);
    Resp::Int(removed)
}

pub fn zremrangebylex(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() != 3 {
        return Resp::arity("zremrangebylex");
    }
    let (Some(min), Some(max)) = (parse_lex_bound(&args[1]), parse_lex_bound(&args[2])) else {
        return Resp::err("ERR min or max not valid string range item");
    };
    let (mut z, exp) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let victims: Vec<Bytes> = ordered(&z)
        .into_iter()
        .filter(|(_, m)| lex_ge(m, &min) && lex_le(m, &max))
        .map(|(_, m)| m)
        .collect();
    let removed = victims.len() as i64;
    for m in victims {
        z.remove(&m);
    }
    store_coll(db, ns, args[0].clone(), Datum::ZSet(z), exp);
    Resp::Int(removed)
}

pub fn zscan(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 2 {
        return Resp::arity("zscan");
    }
    let Some(cursor) = parse_i64(&args[1]).filter(|c| *c >= 0) else {
        return Resp::err("ERR invalid cursor");
    };
    let (pattern, count) = match scan_opts(&args[2..]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (z, _) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let items = ordered(&z);
    let (next, page) = scan_page(&items, cursor as u64, count);
    let mut out = Vec::new();
    for (s, m) in page {
        if let Some(p) = &pattern {
            if !glob_match(p, &m) {
                continue;
            }
        }
        out.push(Resp::Bulk(m));
        out.push(Resp::bulk_str(fmt_f64(s)));
    }
    Resp::Array(vec![Resp::bulk_str(next.to_string()), Resp::Array(out)])
}

/// ZUNIONSTORE / ZINTERSTORE / ZDIFFSTORE
/// `dst numkeys key... [WEIGHTS w...] [AGGREGATE SUM|MIN|MAX]`
pub fn zstore(db: &mut dyn Db, ns: u32, args: &[Bytes], op: SetOp) -> Resp {
    if args.len() < 3 {
        return Resp::arity("zunionstore");
    }
    let Some(numkeys) = parse_i64(&args[1]).filter(|n| *n > 0) else {
        return Resp::err("ERR at least 1 input key is needed");
    };
    let numkeys = numkeys as usize;
    if args.len() < 2 + numkeys {
        return Resp::syntax();
    }
    let keys = &args[2..2 + numkeys];
    let mut weights = vec![1.0f64; numkeys];
    let mut aggregate = "SUM".to_string();
    let mut i = 2 + numkeys;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "WEIGHTS" => {
                if op == SetOp::Diff {
                    return Resp::syntax();
                }
                for w in 0..numkeys {
                    match args.get(i + 1 + w).and_then(parse_f64) {
                        Some(f) => weights[w] = f,
                        None => return Resp::err("ERR weight value is not a float"),
                    }
                }
                i += 1 + numkeys;
            }
            "AGGREGATE" => {
                if op == SetOp::Diff {
                    return Resp::syntax();
                }
                match args.get(i + 1).map(upper).as_deref() {
                    Some(a @ ("SUM" | "MIN" | "MAX")) => aggregate = a.to_string(),
                    _ => return Resp::syntax(),
                }
                i += 2;
            }
            _ => return Resp::syntax(),
        }
    }

    // Inputs may be sets (score 1.0) or zsets.
    let mut inputs: Vec<Vec<(Bytes, f64)>> = Vec::with_capacity(numkeys);
    for k in keys {
        match db.get(ns, k) {
            Some(Datum::ZSet(z)) => {
                inputs.push(z.by_member.iter().map(|(m, s)| (m.clone(), *s)).collect())
            }
            Some(Datum::Set(s)) => inputs.push(s.iter().map(|m| (m.clone(), 1.0)).collect()),
            Some(_) => return Resp::wrong_type(),
            None => inputs.push(Vec::new()),
        }
    }

    let mut acc: std::collections::HashMap<Bytes, f64> = std::collections::HashMap::new();
    match op {
        SetOp::Union => {
            for (idx, input) in inputs.iter().enumerate() {
                for (m, s) in input {
                    let w = s * weights[idx];
                    acc.entry(m.clone())
                        .and_modify(|cur| {
                            *cur = match aggregate.as_str() {
                                "MIN" => cur.min(w),
                                "MAX" => cur.max(w),
                                _ => *cur + w,
                            }
                        })
                        .or_insert(w);
                }
            }
        }
        SetOp::Inter => {
            if let Some(first) = inputs.first() {
                'member: for (m, s) in first {
                    let mut agg = s * weights[0];
                    for (idx, input) in inputs.iter().enumerate().skip(1) {
                        match input.iter().find(|(im, _)| im == m) {
                            Some((_, is)) => {
                                let w = is * weights[idx];
                                agg = match aggregate.as_str() {
                                    "MIN" => agg.min(w),
                                    "MAX" => agg.max(w),
                                    _ => agg + w,
                                };
                            }
                            None => continue 'member,
                        }
                    }
                    acc.insert(m.clone(), agg);
                }
            }
        }
        SetOp::Diff => {
            if let Some(first) = inputs.first() {
                for (m, s) in first {
                    let in_others = inputs[1..]
                        .iter()
                        .any(|input| input.iter().any(|(im, _)| im == m));
                    if !in_others {
                        acc.insert(m.clone(), *s);
                    }
                }
            }
        }
    }

    let mut z = ZSet::default();
    for (m, s) in acc {
        z.insert(m, s);
    }
    let card = z.len() as i64;
    store_coll(db, ns, args[0].clone(), Datum::ZSet(z), None);
    Resp::Int(card)
}

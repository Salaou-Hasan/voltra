//! Redis command engine core.
//!
//! Every data command is implemented once against the [`Db`] trait:
//!
//! * `SnapDb` — lock-free snapshot reads, runs on the connection task.
//!   Read-only commands execute here in parallel across all cores.
//! * `mvcc::Writer` — runs inside the single sequencer thread. Write commands
//!   and MULTI/EXEC bodies execute here, giving Redis-exact linearizable
//!   read-modify-write semantics with zero lock contention.

use crate::mvcc::{Datum, MvccStore, TxnId, Writer};
use crate::redis::resp::Resp;
use bytes::Bytes;

// ─────────────────────────────────────────────────────────────────────────────
// Db abstraction
// ─────────────────────────────────────────────────────────────────────────────

pub trait Db {
    fn get(&mut self, ns: u32, key: &Bytes) -> Option<Datum>;
    /// Live TTL (epoch ms). None if no TTL or key missing/expired.
    fn expiry(&mut self, ns: u32, key: &Bytes) -> Option<u64>;
    fn put(&mut self, ns: u32, key: Bytes, val: Datum, exp: Option<u64>);
    fn del(&mut self, ns: u32, key: Bytes) -> bool;
    fn keys(&mut self, ns: u32) -> Vec<Bytes>;
    fn dbsize(&mut self, ns: u32) -> u64;
    fn now_ms(&self) -> u64;
}

impl Db for Writer<'_> {
    fn get(&mut self, ns: u32, key: &Bytes) -> Option<Datum> {
        Writer::get(self, ns, key)
    }
    fn expiry(&mut self, ns: u32, key: &Bytes) -> Option<u64> {
        let now = Writer::now_ms(self);
        Writer::get_expiry(self, ns, key).filter(|e| *e > now)
    }
    fn put(&mut self, ns: u32, key: Bytes, val: Datum, exp: Option<u64>) {
        Writer::put(self, ns, key, val, exp)
    }
    fn del(&mut self, ns: u32, key: Bytes) -> bool {
        Writer::del(self, ns, key)
    }
    fn keys(&mut self, ns: u32) -> Vec<Bytes> {
        let mut k = Writer::live_keys(self, ns);
        k.sort();
        k
    }
    fn dbsize(&mut self, ns: u32) -> u64 {
        Writer::dbsize(self, ns)
    }
    fn now_ms(&self) -> u64 {
        Writer::now_ms(self)
    }
}

/// Read-only view at a pinned timestamp. Write methods are unreachable —
/// the dispatcher routes every write command to the sequencer.
pub struct SnapDb<'a> {
    pub store: &'a MvccStore,
    pub ts: TxnId,
}

impl Db for SnapDb<'_> {
    fn get(&mut self, ns: u32, key: &Bytes) -> Option<Datum> {
        self.store.get_at(ns, key, self.ts)
    }
    fn expiry(&mut self, ns: u32, key: &Bytes) -> Option<u64> {
        let now = crate::mvcc::now_ms();
        // get_expiry is "latest" — fine: read commands use it only for TTL report.
        self.store.get_expiry(ns, key).filter(|e| *e > now)
    }
    fn put(&mut self, _ns: u32, _key: Bytes, _val: Datum, _exp: Option<u64>) {
        debug_assert!(false, "write command dispatched to read-only snapshot");
    }
    fn del(&mut self, _ns: u32, _key: Bytes) -> bool {
        debug_assert!(false, "write command dispatched to read-only snapshot");
        false
    }
    fn keys(&mut self, ns: u32) -> Vec<Bytes> {
        self.store.visible_keys_sorted(ns, self.ts)
    }
    fn dbsize(&mut self, ns: u32) -> u64 {
        self.store.ns_len(ns)
    }
    fn now_ms(&self) -> u64 {
        crate::mvcc::now_ms()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Typed read helpers (lazy-expiry-safe: value read happens before TTL read)
// ─────────────────────────────────────────────────────────────────────────────

pub type CmdResult<T> = Result<T, Resp>;

pub fn read_str(db: &mut dyn Db, ns: u32, key: &Bytes) -> CmdResult<(Option<Bytes>, Option<u64>)> {
    match db.get(ns, key) {
        Some(Datum::Str(s)) => {
            let e = db.expiry(ns, key);
            Ok((Some(s), e))
        }
        Some(_) => Err(Resp::wrong_type()),
        None => Ok((None, None)),
    }
}

pub fn read_hash(
    db: &mut dyn Db,
    ns: u32,
    key: &Bytes,
) -> CmdResult<(im::HashMap<Bytes, Bytes>, Option<u64>)> {
    match db.get(ns, key) {
        Some(Datum::Hash(h)) => {
            let e = db.expiry(ns, key);
            Ok((h, e))
        }
        Some(_) => Err(Resp::wrong_type()),
        None => Ok((im::HashMap::new(), None)),
    }
}

pub fn read_list(
    db: &mut dyn Db,
    ns: u32,
    key: &Bytes,
) -> CmdResult<(im::Vector<Bytes>, Option<u64>)> {
    match db.get(ns, key) {
        Some(Datum::List(l)) => {
            let e = db.expiry(ns, key);
            Ok((l, e))
        }
        Some(_) => Err(Resp::wrong_type()),
        None => Ok((im::Vector::new(), None)),
    }
}

pub fn read_set(
    db: &mut dyn Db,
    ns: u32,
    key: &Bytes,
) -> CmdResult<(im::HashSet<Bytes>, Option<u64>)> {
    match db.get(ns, key) {
        Some(Datum::Set(s)) => {
            let e = db.expiry(ns, key);
            Ok((s, e))
        }
        Some(_) => Err(Resp::wrong_type()),
        None => Ok((im::HashSet::new(), None)),
    }
}

pub fn read_zset(
    db: &mut dyn Db,
    ns: u32,
    key: &Bytes,
) -> CmdResult<(crate::mvcc::ZSet, Option<u64>)> {
    match db.get(ns, key) {
        Some(Datum::ZSet(z)) => {
            let e = db.expiry(ns, key);
            Ok((z, e))
        }
        Some(_) => Err(Resp::wrong_type()),
        None => Ok((crate::mvcc::ZSet::default(), None)),
    }
}

/// Store a collection back; Redis deletes keys whose collection became empty.
pub fn store_coll(db: &mut dyn Db, ns: u32, key: Bytes, val: Datum, exp: Option<u64>) {
    let empty = match &val {
        Datum::Hash(h) => h.is_empty(),
        Datum::List(l) => l.is_empty(),
        Datum::Set(s) => s.is_empty(),
        Datum::ZSet(z) => z.is_empty(),
        _ => false,
    };
    if empty {
        db.del(ns, key);
    } else {
        db.put(ns, key, val, exp);
    }
}

/// Normalize a Redis (start, stop) index pair against a collection length.
/// Returns None when the range is empty.
pub fn norm_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || len == 0 {
        None
    } else {
        Some((s as usize, e as usize))
    }
}

/// xorshift PRNG for SPOP / SRANDMEMBER / HRANDFIELD / ZRANDMEMBER.
/// Effects are resolved before AOF logging, so replay stays deterministic.
pub struct Rng(u64);

impl Rng {
    pub fn seeded(seed: u64) -> Self {
        Rng(seed | 1)
    }
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next() % n as u64) as usize
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Dispatch
// ─────────────────────────────────────────────────────────────────────────────

/// True if a command mutates data and must run inside the sequencer.
pub fn is_write(cmd: &str) -> bool {
    matches!(
        cmd,
        // strings
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "MSET" | "MSETNX" | "APPEND" | "INCR" | "DECR"
            | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "GETSET" | "GETDEL" | "GETEX" | "SETRANGE"
            | "SETBIT"
        // keys
            | "DEL" | "UNLINK" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST"
            | "RENAME" | "RENAMENX" | "COPY" | "FLUSHDB" | "FLUSHALL" | "SWAPDB"
        // hashes
            | "HSET" | "HSETNX" | "HMSET" | "HDEL" | "HINCRBY" | "HINCRBYFLOAT"
        // lists
            | "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LPOP" | "RPOP" | "LSET" | "LREM"
            | "LTRIM" | "LINSERT" | "RPOPLPUSH" | "LMOVE"
        // sets
            | "SADD" | "SREM" | "SPOP" | "SMOVE" | "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE"
        // zsets
            | "ZADD" | "ZREM" | "ZINCRBY" | "ZPOPMIN" | "ZPOPMAX" | "ZREMRANGEBYRANK"
            | "ZREMRANGEBYSCORE" | "ZREMRANGEBYLEX" | "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE"
    )
}

/// True for commands implemented in the data plane (this dispatcher).
pub fn is_data_command(cmd: &str) -> bool {
    is_write(cmd)
        || matches!(
            cmd,
            "GET" | "MGET" | "STRLEN" | "GETRANGE" | "SUBSTR" | "GETBIT" | "BITCOUNT"
                | "EXISTS" | "TYPE" | "TTL" | "PTTL" | "EXPIRETIME" | "PEXPIRETIME" | "KEYS"
                | "SCAN" | "RANDOMKEY" | "TOUCH" | "DBSIZE" | "OBJECT"
                | "HGET" | "HMGET" | "HLEN" | "HEXISTS" | "HKEYS" | "HVALS" | "HGETALL"
                | "HSTRLEN" | "HRANDFIELD" | "HSCAN"
                | "LLEN" | "LRANGE" | "LINDEX" | "LPOS"
                | "SMEMBERS" | "SISMEMBER" | "SMISMEMBER" | "SCARD" | "SRANDMEMBER" | "SUNION"
                | "SINTER" | "SDIFF" | "SINTERCARD" | "SSCAN"
                | "ZSCORE" | "ZMSCORE" | "ZCARD" | "ZCOUNT" | "ZRANK" | "ZREVRANK" | "ZRANGE"
                | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZRANGEBYLEX"
                | "ZREVRANGEBYLEX" | "ZLEXCOUNT" | "ZRANDMEMBER" | "ZSCAN"
        )
}

/// Execute a data-plane command. `cmd` must already be uppercase.
/// `dbi` is the logical Redis database (namespace).
pub fn dispatch_data(db: &mut dyn Db, dbi: u32, cmd: &str, args: &[Bytes]) -> Resp {
    use crate::redis::{cmd_hash_list as hl, cmd_set_zset as sz, cmd_string as st};
    match cmd {
        // ── strings ──────────────────────────────────────────────────────────
        "GET" => st::get(db, dbi, args),
        "SET" => st::set(db, dbi, args),
        "SETNX" => st::setnx(db, dbi, args),
        "SETEX" => st::setex(db, dbi, args, false),
        "PSETEX" => st::setex(db, dbi, args, true),
        "MGET" => st::mget(db, dbi, args),
        "MSET" => st::mset(db, dbi, args),
        "MSETNX" => st::msetnx(db, dbi, args),
        "APPEND" => st::append(db, dbi, args),
        "STRLEN" => st::strlen(db, dbi, args),
        "INCR" => st::incr_by(db, dbi, args, 1, true),
        "DECR" => st::incr_by(db, dbi, args, -1, true),
        "INCRBY" => st::incr_by(db, dbi, args, 1, false),
        "DECRBY" => st::incr_by(db, dbi, args, -1, false),
        "INCRBYFLOAT" => st::incr_by_float(db, dbi, args),
        "GETSET" => st::getset(db, dbi, args),
        "GETDEL" => st::getdel(db, dbi, args),
        "GETEX" => st::getex(db, dbi, args),
        "GETRANGE" | "SUBSTR" => st::getrange(db, dbi, args),
        "SETRANGE" => st::setrange(db, dbi, args),
        "SETBIT" => st::setbit(db, dbi, args),
        "GETBIT" => st::getbit(db, dbi, args),
        "BITCOUNT" => st::bitcount(db, dbi, args),
        // ── keys ─────────────────────────────────────────────────────────────
        "DEL" | "UNLINK" => st::del(db, dbi, args),
        "EXISTS" => st::exists(db, dbi, args),
        "TYPE" => st::type_cmd(db, dbi, args),
        "TTL" => st::ttl(db, dbi, args, false),
        "PTTL" => st::ttl(db, dbi, args, true),
        "EXPIRETIME" => st::expiretime(db, dbi, args, false),
        "PEXPIRETIME" => st::expiretime(db, dbi, args, true),
        "EXPIRE" => st::expire(db, dbi, args, ExpireUnit::Sec, false),
        "PEXPIRE" => st::expire(db, dbi, args, ExpireUnit::Ms, false),
        "EXPIREAT" => st::expire(db, dbi, args, ExpireUnit::Sec, true),
        "PEXPIREAT" => st::expire(db, dbi, args, ExpireUnit::Ms, true),
        "PERSIST" => st::persist(db, dbi, args),
        "RENAME" => st::rename(db, dbi, args, false),
        "RENAMENX" => st::rename(db, dbi, args, true),
        "COPY" => st::copy(db, dbi, args),
        "TOUCH" => st::exists(db, dbi, args), // TOUCH == EXISTS count semantics
        "KEYS" => st::keys(db, dbi, args),
        "SCAN" => st::scan(db, dbi, args),
        "RANDOMKEY" => st::randomkey(db, dbi, args),
        "DBSIZE" => Resp::Int(db.dbsize(dbi) as i64),
        "FLUSHDB" => st::flushdb(db, dbi, args),
        "FLUSHALL" => st::flushall(db, args),
        "SWAPDB" => st::swapdb(db, args),
        "OBJECT" => st::object(db, dbi, args),
        // ── hashes ───────────────────────────────────────────────────────────
        "HSET" | "HMSET" => hl::hset(db, dbi, args, cmd == "HMSET"),
        "HSETNX" => hl::hsetnx(db, dbi, args),
        "HGET" => hl::hget(db, dbi, args),
        "HMGET" => hl::hmget(db, dbi, args),
        "HDEL" => hl::hdel(db, dbi, args),
        "HLEN" => hl::hlen(db, dbi, args),
        "HEXISTS" => hl::hexists(db, dbi, args),
        "HKEYS" => hl::hkeys(db, dbi, args, true),
        "HVALS" => hl::hkeys(db, dbi, args, false),
        "HGETALL" => hl::hgetall(db, dbi, args),
        "HSTRLEN" => hl::hstrlen(db, dbi, args),
        "HINCRBY" => hl::hincrby(db, dbi, args),
        "HINCRBYFLOAT" => hl::hincrbyfloat(db, dbi, args),
        "HRANDFIELD" => hl::hrandfield(db, dbi, args),
        "HSCAN" => hl::hscan(db, dbi, args),
        // ── lists ────────────────────────────────────────────────────────────
        "LPUSH" => hl::push(db, dbi, args, true, false),
        "RPUSH" => hl::push(db, dbi, args, false, false),
        "LPUSHX" => hl::push(db, dbi, args, true, true),
        "RPUSHX" => hl::push(db, dbi, args, false, true),
        "LPOP" => hl::pop(db, dbi, args, true),
        "RPOP" => hl::pop(db, dbi, args, false),
        "LLEN" => hl::llen(db, dbi, args),
        "LRANGE" => hl::lrange(db, dbi, args),
        "LINDEX" => hl::lindex(db, dbi, args),
        "LSET" => hl::lset(db, dbi, args),
        "LREM" => hl::lrem(db, dbi, args),
        "LTRIM" => hl::ltrim(db, dbi, args),
        "LINSERT" => hl::linsert(db, dbi, args),
        "RPOPLPUSH" => hl::lmove_compat(db, dbi, args),
        "LMOVE" => hl::lmove(db, dbi, args),
        "LPOS" => hl::lpos(db, dbi, args),
        // ── sets ─────────────────────────────────────────────────────────────
        "SADD" => sz::sadd(db, dbi, args),
        "SREM" => sz::srem(db, dbi, args),
        "SMEMBERS" => sz::smembers(db, dbi, args),
        "SISMEMBER" => sz::sismember(db, dbi, args),
        "SMISMEMBER" => sz::smismember(db, dbi, args),
        "SCARD" => sz::scard(db, dbi, args),
        "SPOP" => sz::spop(db, dbi, args),
        "SRANDMEMBER" => sz::srandmember(db, dbi, args),
        "SMOVE" => sz::smove(db, dbi, args),
        "SUNION" => sz::setop(db, dbi, args, SetOp::Union, false),
        "SINTER" => sz::setop(db, dbi, args, SetOp::Inter, false),
        "SDIFF" => sz::setop(db, dbi, args, SetOp::Diff, false),
        "SUNIONSTORE" => sz::setop(db, dbi, args, SetOp::Union, true),
        "SINTERSTORE" => sz::setop(db, dbi, args, SetOp::Inter, true),
        "SDIFFSTORE" => sz::setop(db, dbi, args, SetOp::Diff, true),
        "SINTERCARD" => sz::sintercard(db, dbi, args),
        "SSCAN" => sz::sscan(db, dbi, args),
        // ── sorted sets ──────────────────────────────────────────────────────
        "ZADD" => sz::zadd(db, dbi, args),
        "ZREM" => sz::zrem(db, dbi, args),
        "ZSCORE" => sz::zscore(db, dbi, args),
        "ZMSCORE" => sz::zmscore(db, dbi, args),
        "ZCARD" => sz::zcard(db, dbi, args),
        "ZCOUNT" => sz::zcount(db, dbi, args),
        "ZINCRBY" => sz::zincrby(db, dbi, args),
        "ZRANK" => sz::zrank(db, dbi, args, false),
        "ZREVRANK" => sz::zrank(db, dbi, args, true),
        "ZRANGE" => sz::zrange(db, dbi, args),
        "ZREVRANGE" => sz::zrevrange(db, dbi, args),
        "ZRANGEBYSCORE" => sz::zrangebyscore(db, dbi, args, false),
        "ZREVRANGEBYSCORE" => sz::zrangebyscore(db, dbi, args, true),
        "ZRANGEBYLEX" => sz::zrangebylex(db, dbi, args, false),
        "ZREVRANGEBYLEX" => sz::zrangebylex(db, dbi, args, true),
        "ZLEXCOUNT" => sz::zlexcount(db, dbi, args),
        "ZPOPMIN" => sz::zpop(db, dbi, args, true),
        "ZPOPMAX" => sz::zpop(db, dbi, args, false),
        "ZRANDMEMBER" => sz::zrandmember(db, dbi, args),
        "ZREMRANGEBYRANK" => sz::zremrangebyrank(db, dbi, args),
        "ZREMRANGEBYSCORE" => sz::zremrangebyscore(db, dbi, args),
        "ZREMRANGEBYLEX" => sz::zremrangebylex(db, dbi, args),
        "ZSCAN" => sz::zscan(db, dbi, args),
        "ZUNIONSTORE" => sz::zstore(db, dbi, args, SetOp::Union),
        "ZINTERSTORE" => sz::zstore(db, dbi, args, SetOp::Inter),
        "ZDIFFSTORE" => sz::zstore(db, dbi, args, SetOp::Diff),
        _ => Resp::err(format!("ERR unknown command '{}'", cmd.to_lowercase())),
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum ExpireUnit {
    Sec,
    Ms,
}

#[derive(Clone, Copy, PartialEq)]
pub enum SetOp {
    Union,
    Inter,
    Diff,
}

/// Generic SCAN over a sorted item list: returns (next_cursor, page).
pub fn scan_page<T: Clone>(items: &[T], cursor: u64, count: usize) -> (u64, Vec<T>) {
    let start = cursor as usize;
    if start >= items.len() {
        return (0, Vec::new());
    }
    let end = (start + count.max(1)).min(items.len());
    let next = if end >= items.len() { 0 } else { end as u64 };
    (next, items[start..end].to_vec())
}

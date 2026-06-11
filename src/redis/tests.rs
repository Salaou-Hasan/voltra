//! Redis command engine tests — run every family through `dispatch_data`
//! against an in-memory Db, plus end-to-end sequencer tests via MvccStore.

use super::engine::{dispatch_data, is_write, Db};
use super::resp::Resp;
use crate::mvcc::Datum;
use bytes::Bytes;
use std::collections::HashMap;

/// Plain in-memory Db for pure command-logic tests.
struct TestDb {
    map: HashMap<(u32, Bytes), (Datum, Option<u64>)>,
    now: u64,
}

impl TestDb {
    fn new() -> Self {
        Self { map: HashMap::new(), now: 1_000_000 }
    }
}

impl Db for TestDb {
    fn get(&mut self, ns: u32, key: &Bytes) -> Option<Datum> {
        let k = (ns, key.clone());
        if let Some((_, Some(exp))) = self.map.get(&k) {
            if *exp <= self.now {
                self.map.remove(&k);
                return None;
            }
        }
        self.map.get(&k).map(|(d, _)| d.clone())
    }
    fn expiry(&mut self, ns: u32, key: &Bytes) -> Option<u64> {
        self.map
            .get(&(ns, key.clone()))
            .and_then(|(_, e)| *e)
            .filter(|e| *e > self.now)
    }
    fn put(&mut self, ns: u32, key: Bytes, val: Datum, exp: Option<u64>) {
        self.map.insert((ns, key), (val, exp));
    }
    fn del(&mut self, ns: u32, key: Bytes) -> bool {
        self.map.remove(&(ns, key)).is_some()
    }
    fn keys(&mut self, ns: u32) -> Vec<Bytes> {
        let mut v: Vec<Bytes> = self
            .map
            .keys()
            .filter(|(n, _)| *n == ns)
            .map(|(_, k)| k.clone())
            .collect();
        v.sort();
        v
    }
    fn dbsize(&mut self, ns: u32) -> u64 {
        self.map.keys().filter(|(n, _)| *n == ns).count() as u64
    }
    fn now_ms(&self) -> u64 {
        self.now
    }
}

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn run(db: &mut TestDb, cmd: &str, args: &[&str]) -> Resp {
    let a: Vec<Bytes> = args.iter().map(|s| b(s)).collect();
    dispatch_data(db, 0, cmd, &a)
}

fn bulk(s: &str) -> Resp {
    Resp::bulk_str(s)
}

#[test]
fn string_set_get_roundtrip() {
    let mut db = TestDb::new();
    assert_eq!(run(&mut db, "SET", &["k", "v"]), Resp::ok());
    assert_eq!(run(&mut db, "GET", &["k"]), bulk("v"));
    assert_eq!(run(&mut db, "GET", &["missing"]), Resp::Null);
    assert_eq!(run(&mut db, "STRLEN", &["k"]), Resp::Int(1));
    assert_eq!(run(&mut db, "APPEND", &["k", "abc"]), Resp::Int(4));
    assert_eq!(run(&mut db, "GET", &["k"]), bulk("vabc"));
}

#[test]
fn set_nx_xx_get_options() {
    let mut db = TestDb::new();
    assert_eq!(run(&mut db, "SET", &["k", "v1", "NX"]), Resp::ok());
    assert_eq!(run(&mut db, "SET", &["k", "v2", "NX"]), Resp::Null); // exists
    assert_eq!(run(&mut db, "SET", &["k", "v2", "XX", "GET"]), bulk("v1"));
    assert_eq!(run(&mut db, "GET", &["k"]), bulk("v2"));
    assert_eq!(run(&mut db, "SET", &["nope", "x", "XX"]), Resp::Null);
}

#[test]
fn set_with_ttl_and_keepttl() {
    let mut db = TestDb::new();
    run(&mut db, "SET", &["k", "v", "EX", "100"]);
    let ttl = run(&mut db, "TTL", &["k"]);
    assert_eq!(ttl, Resp::Int(100));
    // Plain SET clears TTL.
    run(&mut db, "SET", &["k", "v2"]);
    assert_eq!(run(&mut db, "TTL", &["k"]), Resp::Int(-1));
    // KEEPTTL preserves it.
    run(&mut db, "SET", &["k", "v3", "EX", "50"]);
    run(&mut db, "SET", &["k", "v4", "KEEPTTL"]);
    assert_eq!(run(&mut db, "TTL", &["k"]), Resp::Int(50));
}

#[test]
fn incr_decr_family() {
    let mut db = TestDb::new();
    assert_eq!(run(&mut db, "INCR", &["n"]), Resp::Int(1));
    assert_eq!(run(&mut db, "INCRBY", &["n", "9"]), Resp::Int(10));
    assert_eq!(run(&mut db, "DECR", &["n"]), Resp::Int(9));
    assert_eq!(run(&mut db, "DECRBY", &["n", "4"]), Resp::Int(5));
    assert_eq!(run(&mut db, "INCRBYFLOAT", &["n", "0.5"]), bulk("5.5"));
    run(&mut db, "SET", &["s", "abc"]);
    assert_eq!(run(&mut db, "INCR", &["s"]), Resp::not_int());
}

#[test]
fn wrong_type_is_rejected() {
    let mut db = TestDb::new();
    run(&mut db, "LPUSH", &["l", "a"]);
    assert_eq!(run(&mut db, "GET", &["l"]), Resp::wrong_type());
    assert_eq!(run(&mut db, "INCR", &["l"]), Resp::wrong_type());
    assert_eq!(run(&mut db, "HGET", &["l", "f"]), Resp::wrong_type());
    assert_eq!(run(&mut db, "SADD", &["l", "m"]), Resp::wrong_type());
}

#[test]
fn bitmap_ops() {
    let mut db = TestDb::new();
    assert_eq!(run(&mut db, "SETBIT", &["b", "7", "1"]), Resp::Int(0));
    assert_eq!(run(&mut db, "GETBIT", &["b", "7"]), Resp::Int(1));
    assert_eq!(run(&mut db, "GETBIT", &["b", "6"]), Resp::Int(0));
    assert_eq!(run(&mut db, "BITCOUNT", &["b"]), Resp::Int(1));
    run(&mut db, "SET", &["s", "foobar"]);
    assert_eq!(run(&mut db, "BITCOUNT", &["s"]), Resp::Int(26));
    assert_eq!(run(&mut db, "BITCOUNT", &["s", "1", "1"]), Resp::Int(6));
}

#[test]
fn key_management() {
    let mut db = TestDb::new();
    run(&mut db, "MSET", &["a", "1", "b", "2", "c", "3"]);
    assert_eq!(run(&mut db, "EXISTS", &["a", "b", "nope"]), Resp::Int(2));
    assert_eq!(run(&mut db, "DEL", &["a", "nope"]), Resp::Int(1));
    assert_eq!(run(&mut db, "TYPE", &["b"]), Resp::Simple("string".into()));
    assert_eq!(run(&mut db, "RENAME", &["b", "bb"]), Resp::ok());
    assert_eq!(run(&mut db, "GET", &["bb"]), bulk("2"));
    assert_eq!(run(&mut db, "COPY", &["bb", "cc"]), Resp::Int(1));
    assert_eq!(run(&mut db, "GET", &["cc"]), bulk("2"));
    // KEYS with glob
    let r = run(&mut db, "KEYS", &["*c*"]);
    match r {
        Resp::Array(items) => assert_eq!(items.len(), 2), // c, cc
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn expire_and_persist() {
    let mut db = TestDb::new();
    run(&mut db, "SET", &["k", "v"]);
    assert_eq!(run(&mut db, "EXPIRE", &["k", "100"]), Resp::Int(1));
    assert_eq!(run(&mut db, "TTL", &["k"]), Resp::Int(100));
    assert_eq!(run(&mut db, "PERSIST", &["k"]), Resp::Int(1));
    assert_eq!(run(&mut db, "TTL", &["k"]), Resp::Int(-1));
    // EXPIRE NX/XX/GT/LT conditions
    assert_eq!(run(&mut db, "EXPIRE", &["k", "100", "XX"]), Resp::Int(0)); // no TTL set
    assert_eq!(run(&mut db, "EXPIRE", &["k", "100", "NX"]), Resp::Int(1));
    assert_eq!(run(&mut db, "EXPIRE", &["k", "50", "GT"]), Resp::Int(0)); // 50 < 100
    assert_eq!(run(&mut db, "EXPIRE", &["k", "200", "GT"]), Resp::Int(1));
    // Expiring in the past deletes.
    assert_eq!(run(&mut db, "EXPIRE", &["k", "-1"]), Resp::Int(1));
    assert_eq!(run(&mut db, "EXISTS", &["k"]), Resp::Int(0));
}

#[test]
fn hash_family() {
    let mut db = TestDb::new();
    assert_eq!(run(&mut db, "HSET", &["h", "f1", "v1", "f2", "v2"]), Resp::Int(2));
    assert_eq!(run(&mut db, "HGET", &["h", "f1"]), bulk("v1"));
    assert_eq!(run(&mut db, "HLEN", &["h"]), Resp::Int(2));
    assert_eq!(run(&mut db, "HEXISTS", &["h", "f2"]), Resp::Int(1));
    assert_eq!(run(&mut db, "HSETNX", &["h", "f1", "no"]), Resp::Int(0));
    assert_eq!(run(&mut db, "HINCRBY", &["h", "n", "5"]), Resp::Int(5));
    assert_eq!(run(&mut db, "HDEL", &["h", "f1", "f2", "n"]), Resp::Int(3));
    // Empty hash deletes the key.
    assert_eq!(run(&mut db, "EXISTS", &["h"]), Resp::Int(0));
}

#[test]
fn list_family() {
    let mut db = TestDb::new();
    assert_eq!(run(&mut db, "RPUSH", &["l", "a", "b", "c"]), Resp::Int(3));
    assert_eq!(run(&mut db, "LPUSH", &["l", "z"]), Resp::Int(4));
    assert_eq!(run(&mut db, "LLEN", &["l"]), Resp::Int(4));
    assert_eq!(run(&mut db, "LINDEX", &["l", "0"]), bulk("z"));
    assert_eq!(run(&mut db, "LINDEX", &["l", "-1"]), bulk("c"));
    assert_eq!(
        run(&mut db, "LRANGE", &["l", "0", "-1"]),
        Resp::Array(vec![bulk("z"), bulk("a"), bulk("b"), bulk("c")])
    );
    assert_eq!(run(&mut db, "LPOP", &["l"]), bulk("z"));
    assert_eq!(run(&mut db, "RPOP", &["l"]), bulk("c"));
    assert_eq!(run(&mut db, "LINSERT", &["l", "BEFORE", "b", "x"]), Resp::Int(3));
    assert_eq!(
        run(&mut db, "LRANGE", &["l", "0", "-1"]),
        Resp::Array(vec![bulk("a"), bulk("x"), bulk("b")])
    );
    assert_eq!(run(&mut db, "LSET", &["l", "1", "y"]), Resp::ok());
    assert_eq!(run(&mut db, "LPOS", &["l", "y"]), Resp::Int(1));
    run(&mut db, "RPUSH", &["l", "y", "y"]);
    assert_eq!(run(&mut db, "LREM", &["l", "2", "y"]), Resp::Int(2));
    assert_eq!(run(&mut db, "LTRIM", &["l", "0", "0"]), Resp::ok());
    assert_eq!(run(&mut db, "LLEN", &["l"]), Resp::Int(1));
}

#[test]
fn lmove_between_lists() {
    let mut db = TestDb::new();
    run(&mut db, "RPUSH", &["src", "1", "2", "3"]);
    assert_eq!(run(&mut db, "LMOVE", &["src", "dst", "RIGHT", "LEFT"]), bulk("3"));
    assert_eq!(run(&mut db, "LRANGE", &["dst", "0", "-1"]), Resp::Array(vec![bulk("3")]));
    assert_eq!(run(&mut db, "RPOPLPUSH", &["src", "dst"]), bulk("2"));
    assert_eq!(
        run(&mut db, "LRANGE", &["dst", "0", "-1"]),
        Resp::Array(vec![bulk("2"), bulk("3")])
    );
}

#[test]
fn set_family() {
    let mut db = TestDb::new();
    assert_eq!(run(&mut db, "SADD", &["s", "a", "b", "c", "a"]), Resp::Int(3));
    assert_eq!(run(&mut db, "SCARD", &["s"]), Resp::Int(3));
    assert_eq!(run(&mut db, "SISMEMBER", &["s", "a"]), Resp::Int(1));
    assert_eq!(
        run(&mut db, "SMISMEMBER", &["s", "a", "zz"]),
        Resp::Array(vec![Resp::Int(1), Resp::Int(0)])
    );
    assert_eq!(run(&mut db, "SREM", &["s", "a"]), Resp::Int(1));
    run(&mut db, "SADD", &["s2", "b", "d"]);
    // SINTER {b,c} ∩ {b,d} = {b}
    assert_eq!(
        run(&mut db, "SINTER", &["s", "s2"]),
        Resp::SetReply(vec![bulk("b")])
    );
    assert_eq!(run(&mut db, "SINTERCARD", &["2", "s", "s2"]), Resp::Int(1));
    // SUNIONSTORE
    assert_eq!(run(&mut db, "SUNIONSTORE", &["dst", "s", "s2"]), Resp::Int(3));
    assert_eq!(run(&mut db, "SMOVE", &["s2", "s", "d"]), Resp::Int(1));
    assert_eq!(run(&mut db, "SISMEMBER", &["s", "d"]), Resp::Int(1));
}

#[test]
fn zset_family() {
    let mut db = TestDb::new();
    assert_eq!(run(&mut db, "ZADD", &["z", "1", "a", "2", "b", "3", "c"]), Resp::Int(3));
    assert_eq!(run(&mut db, "ZCARD", &["z"]), Resp::Int(3));
    assert_eq!(run(&mut db, "ZSCORE", &["z", "b"]), bulk("2"));
    assert_eq!(run(&mut db, "ZRANK", &["z", "c"]), Resp::Int(2));
    assert_eq!(run(&mut db, "ZREVRANK", &["z", "c"]), Resp::Int(0));
    assert_eq!(
        run(&mut db, "ZRANGE", &["z", "0", "-1"]),
        Resp::Array(vec![bulk("a"), bulk("b"), bulk("c")])
    );
    assert_eq!(
        run(&mut db, "ZRANGE", &["z", "0", "-1", "REV"]),
        Resp::Array(vec![bulk("c"), bulk("b"), bulk("a")])
    );
    assert_eq!(
        run(&mut db, "ZRANGEBYSCORE", &["z", "2", "+inf"]),
        Resp::Array(vec![bulk("b"), bulk("c")])
    );
    assert_eq!(
        run(&mut db, "ZRANGEBYSCORE", &["z", "(2", "+inf"]),
        Resp::Array(vec![bulk("c")])
    );
    assert_eq!(run(&mut db, "ZCOUNT", &["z", "-inf", "2"]), Resp::Int(2));
    assert_eq!(run(&mut db, "ZINCRBY", &["z", "10", "a"]), bulk("11"));
    assert_eq!(run(&mut db, "ZREVRANK", &["z", "a"]), Resp::Int(0)); // now highest
}

#[test]
fn zadd_options() {
    let mut db = TestDb::new();
    run(&mut db, "ZADD", &["z", "5", "m"]);
    // NX: don't update existing
    assert_eq!(run(&mut db, "ZADD", &["z", "NX", "9", "m"]), Resp::Int(0));
    assert_eq!(run(&mut db, "ZSCORE", &["z", "m"]), bulk("5"));
    // GT: only if greater
    assert_eq!(run(&mut db, "ZADD", &["z", "GT", "CH", "3", "m"]), Resp::Int(0));
    assert_eq!(run(&mut db, "ZADD", &["z", "GT", "CH", "9", "m"]), Resp::Int(1));
    // XX: only existing
    assert_eq!(run(&mut db, "ZADD", &["z", "XX", "1", "newbie"]), Resp::Int(0));
    assert_eq!(run(&mut db, "ZCARD", &["z"]), Resp::Int(1));
    // INCR mode returns new score
    assert_eq!(run(&mut db, "ZADD", &["z", "INCR", "1", "m"]), bulk("10"));
}

#[test]
fn zpop_and_remrange() {
    let mut db = TestDb::new();
    run(&mut db, "ZADD", &["z", "1", "a", "2", "b", "3", "c", "4", "d"]);
    assert_eq!(
        run(&mut db, "ZPOPMIN", &["z"]),
        Resp::Array(vec![bulk("a"), bulk("1")])
    );
    assert_eq!(
        run(&mut db, "ZPOPMAX", &["z"]),
        Resp::Array(vec![bulk("d"), bulk("4")])
    );
    assert_eq!(run(&mut db, "ZREMRANGEBYSCORE", &["z", "-inf", "2"]), Resp::Int(1));
    assert_eq!(run(&mut db, "ZCARD", &["z"]), Resp::Int(1));
}

#[test]
fn zstore_union_inter() {
    let mut db = TestDb::new();
    run(&mut db, "ZADD", &["z1", "1", "a", "2", "b"]);
    run(&mut db, "ZADD", &["z2", "10", "b", "20", "c"]);
    assert_eq!(run(&mut db, "ZUNIONSTORE", &["dst", "2", "z1", "z2"]), Resp::Int(3));
    assert_eq!(run(&mut db, "ZSCORE", &["dst", "b"]), bulk("12")); // 2 + 10
    assert_eq!(run(&mut db, "ZINTERSTORE", &["dst2", "2", "z1", "z2"]), Resp::Int(1));
    assert_eq!(
        run(&mut db, "ZUNIONSTORE", &["dst3", "2", "z1", "z2", "WEIGHTS", "2", "1"]),
        Resp::Int(3)
    );
    assert_eq!(run(&mut db, "ZSCORE", &["dst3", "b"]), bulk("14")); // 2*2 + 10
}

#[test]
fn scan_pagination() {
    let mut db = TestDb::new();
    for i in 0..25 {
        run(&mut db, "SET", &[&format!("key:{i:02}"), "v"]);
    }
    let r = run(&mut db, "SCAN", &["0", "COUNT", "10"]);
    let (cursor, page1) = match r {
        Resp::Array(mut v) => {
            let items = v.pop().unwrap();
            let cur = v.pop().unwrap();
            (cur, items)
        }
        other => panic!("bad scan reply {other:?}"),
    };
    assert_eq!(cursor, bulk("10"));
    match page1 {
        Resp::Array(items) => assert_eq!(items.len(), 10),
        other => panic!("bad page {other:?}"),
    }
    // MATCH filter applies within the page
    let r = run(&mut db, "SCAN", &["0", "MATCH", "key:0*", "COUNT", "100"]);
    match r {
        Resp::Array(mut v) => match v.pop().unwrap() {
            Resp::Array(items) => assert_eq!(items.len(), 10), // key:00..key:09
            other => panic!("bad page {other:?}"),
        },
        other => panic!("bad scan reply {other:?}"),
    }
}

#[test]
fn write_classification_is_complete() {
    // Every write command must also be a data command (routing depends on it).
    for cmd in [
        "SET", "DEL", "EXPIRE", "HSET", "LPUSH", "SADD", "ZADD", "FLUSHDB", "RENAME", "COPY",
        "GETDEL", "GETEX", "SETBIT", "LMOVE", "SPOP", "ZINCRBY",
    ] {
        assert!(is_write(cmd), "{cmd} must be classified as a write");
        assert!(super::engine::is_data_command(cmd), "{cmd} must be a data command");
    }
    for cmd in ["GET", "MGET", "KEYS", "SCAN", "HGETALL", "LRANGE", "SMEMBERS", "ZRANGE", "TTL"] {
        assert!(!is_write(cmd), "{cmd} must NOT be a write");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// End-to-end through the MVCC sequencer
// ─────────────────────────────────────────────────────────────────────────────

/// Full-stack tests over a real TCP socket speaking raw RESP.
mod server {
    use super::*;
    use crate::mvcc::MvccStore;
    use crate::redis::{serve, RedisCtx};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn spawn_server(password: Option<String>) -> (u16, MvccStore) {
        let store = MvccStore::open_memory();
        let ctx = RedisCtx::new(store.clone(), password);
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = serve(listener, ctx).await;
        });
        (port, store)
    }

    async fn send_recv(sock: &mut TcpStream, cmd: &[u8]) -> Vec<u8> {
        sock.write_all(cmd).await.unwrap();
        let mut buf = vec![0u8; 64 * 1024];
        let n = tokio::time::timeout(Duration::from_secs(3), sock.read(&mut buf))
            .await
            .expect("read timeout")
            .unwrap();
        buf.truncate(n);
        buf
    }

    fn cmd(parts: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
        }
        out
    }

    #[tokio::test]
    async fn ping_set_get_over_tcp() {
        let (port, store) = spawn_server(None).await;
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        assert_eq!(send_recv(&mut sock, &cmd(&["PING"])).await, b"+PONG\r\n");
        assert_eq!(send_recv(&mut sock, &cmd(&["SET", "k", "hello"])).await, b"+OK\r\n");
        assert_eq!(send_recv(&mut sock, &cmd(&["GET", "k"])).await, b"$5\r\nhello\r\n");
        assert_eq!(send_recv(&mut sock, &cmd(&["DEL", "k"])).await, b":1\r\n");
        store.close();
    }

    #[tokio::test]
    async fn pipelined_commands_one_write() {
        let (port, store) = spawn_server(None).await;
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        let mut pipeline = Vec::new();
        pipeline.extend_from_slice(&cmd(&["INCR", "n"]));
        pipeline.extend_from_slice(&cmd(&["INCR", "n"]));
        pipeline.extend_from_slice(&cmd(&["INCR", "n"]));
        let reply = send_recv(&mut sock, &pipeline).await;
        assert_eq!(reply, b":1\r\n:2\r\n:3\r\n");
        store.close();
    }

    #[tokio::test]
    async fn multi_exec_transaction() {
        let (port, store) = spawn_server(None).await;
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        assert_eq!(send_recv(&mut sock, &cmd(&["MULTI"])).await, b"+OK\r\n");
        assert_eq!(send_recv(&mut sock, &cmd(&["SET", "a", "1"])).await, b"+QUEUED\r\n");
        assert_eq!(send_recv(&mut sock, &cmd(&["INCR", "a"])).await, b"+QUEUED\r\n");
        let reply = send_recv(&mut sock, &cmd(&["EXEC"])).await;
        assert_eq!(reply, b"*2\r\n+OK\r\n:2\r\n");
        store.close();
    }

    #[tokio::test]
    async fn watch_aborts_on_concurrent_write() {
        let (port, store) = spawn_server(None).await;
        let mut c1 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut c2 = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        send_recv(&mut c1, &cmd(&["SET", "balance", "100"])).await;
        assert_eq!(send_recv(&mut c1, &cmd(&["WATCH", "balance"])).await, b"+OK\r\n");
        // Intruder writes the watched key.
        assert_eq!(send_recv(&mut c2, &cmd(&["SET", "balance", "999"])).await, b"+OK\r\n");

        send_recv(&mut c1, &cmd(&["MULTI"])).await;
        send_recv(&mut c1, &cmd(&["SET", "balance", "50"])).await;
        // EXEC must abort: RESP2 null array.
        assert_eq!(send_recv(&mut c1, &cmd(&["EXEC"])).await, b"*-1\r\n");
        assert_eq!(send_recv(&mut c1, &cmd(&["GET", "balance"])).await, b"$3\r\n999\r\n");
        store.close();
    }

    #[tokio::test]
    async fn pubsub_delivery() {
        let (port, store) = spawn_server(None).await;
        let mut subscriber = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut publisher = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        let sub_reply = send_recv(&mut subscriber, &cmd(&["SUBSCRIBE", "news"])).await;
        assert!(sub_reply.starts_with(b"*3\r\n$9\r\nsubscribe\r\n"));

        let pub_reply = send_recv(&mut publisher, &cmd(&["PUBLISH", "news", "hi"])).await;
        assert_eq!(pub_reply, b":1\r\n");

        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(3), subscriber.read(&mut buf))
            .await
            .expect("push timeout")
            .unwrap();
        buf.truncate(n);
        assert_eq!(buf, b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n");
        store.close();
    }

    #[tokio::test]
    async fn auth_required_when_password_set() {
        let (port, store) = spawn_server(Some("s3cret".into())).await;
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        let denied = send_recv(&mut sock, &cmd(&["GET", "k"])).await;
        assert!(denied.starts_with(b"-NOAUTH"));
        let wrong = send_recv(&mut sock, &cmd(&["AUTH", "nope"])).await;
        assert!(wrong.starts_with(b"-WRONGPASS"));
        assert_eq!(send_recv(&mut sock, &cmd(&["AUTH", "s3cret"])).await, b"+OK\r\n");
        assert_eq!(send_recv(&mut sock, &cmd(&["PING"])).await, b"+PONG\r\n");
        store.close();
    }

    #[tokio::test]
    async fn hello_resp3_negotiation() {
        let (port, store) = spawn_server(None).await;
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

        let reply = send_recv(&mut sock, &cmd(&["HELLO", "3"])).await;
        assert!(reply.starts_with(b"%7\r\n"), "expected RESP3 map, got {:?}", String::from_utf8_lossy(&reply));
        // RESP3 null is `_`
        let nil = send_recv(&mut sock, &cmd(&["GET", "missing"])).await;
        assert_eq!(nil, b"_\r\n");
        store.close();
    }

    #[tokio::test]
    async fn inline_commands_work() {
        let (port, store) = spawn_server(None).await;
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        assert_eq!(send_recv(&mut sock, b"PING\r\n").await, b"+PONG\r\n");
        store.close();
    }
}

mod sequencer {
    use super::*;
    use crate::mvcc::{MvccStore, Writer};
    use crate::redis::engine::SnapDb;
    use tokio::sync::oneshot;

    async fn wcmd(store: &MvccStore, cmd: &'static str, args: &[&str]) -> Resp {
        let a: Vec<Bytes> = args.iter().map(|s| b(s)).collect();
        let (tx, rx) = oneshot::channel();
        store
            .apply(move |w: &mut Writer| {
                let r = dispatch_data(w, 0, cmd, &a);
                Box::new(move || {
                    let _ = tx.send(r);
                })
            })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    fn rcmd(store: &MvccStore, cmd: &str, args: &[&str]) -> Resp {
        let a: Vec<Bytes> = args.iter().map(|s| b(s)).collect();
        let mut snap = SnapDb { store, ts: store.current_ts() };
        dispatch_data(&mut snap, 0, cmd, &a)
    }

    #[tokio::test]
    async fn writes_visible_to_snapshot_reads() {
        let store = MvccStore::open_memory();
        assert_eq!(wcmd(&store, "SET", &["k", "v"]).await, Resp::ok());
        assert_eq!(rcmd(&store, "GET", &["k"]), bulk("v"));
        assert_eq!(wcmd(&store, "INCR", &["n"]).await, Resp::Int(1));
        assert_eq!(wcmd(&store, "INCR", &["n"]).await, Resp::Int(2));
        assert_eq!(rcmd(&store, "GET", &["n"]), bulk("2"));
        store.close();
    }

    #[tokio::test]
    async fn concurrent_incrs_are_linearizable() {
        let store = MvccStore::open_memory();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = store.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..50 {
                    wcmd(&s, "INCR", &["ctr"]).await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(rcmd(&store, "GET", &["ctr"]), bulk("400"));
        store.close();
    }

    #[tokio::test]
    async fn hash_partial_update_no_read_modify_write_race() {
        let store = MvccStore::open_memory();
        // Two "fields" updated concurrently on the same hash — both must land.
        let s1 = store.clone();
        let s2 = store.clone();
        let h1 = tokio::spawn(async move {
            for i in 0..50 {
                wcmd(&s1, "HSET", &["player", "x", &i.to_string()]).await;
            }
        });
        let h2 = tokio::spawn(async move {
            for i in 0..50 {
                wcmd(&s2, "HSET", &["player", "y", &i.to_string()]).await;
            }
        });
        h1.await.unwrap();
        h2.await.unwrap();
        assert_eq!(rcmd(&store, "HGET", &["player", "x"]), bulk("49"));
        assert_eq!(rcmd(&store, "HGET", &["player", "y"]), bulk("49"));
        store.close();
    }
}

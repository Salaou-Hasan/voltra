//! SQL engine + pgwire tests.

use super::executor::{ExecOut, PgEngine, Session};
use crate::mvcc::{MvccStore, Scalar};

fn engine() -> PgEngine {
    PgEngine::new(MvccStore::open_memory())
}

async fn exec(e: &PgEngine, s: &mut Session, sql: &str) -> Vec<ExecOut> {
    e.execute(s, sql, &[]).await.unwrap_or_else(|err| panic!("SQL failed: {sql}\n{err}"))
}

async fn exec_err(e: &PgEngine, s: &mut Session, sql: &str) -> String {
    e.execute(s, sql, &[]).await.expect_err(&format!("expected error: {sql}"))
}

fn rows_of(out: &[ExecOut]) -> &[Vec<Scalar>] {
    match out.last().unwrap() {
        ExecOut::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn tag_of(out: &[ExecOut]) -> &str {
    match out.last().unwrap() {
        ExecOut::Rows { tag, .. } => tag,
        ExecOut::Tag(t) => t,
    }
}

fn t(s: &str) -> Scalar {
    Scalar::Text(s.into())
}
fn i(v: i64) -> Scalar {
    Scalar::Int(v)
}

#[tokio::test]
async fn create_insert_select_roundtrip() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE players (id SERIAL PRIMARY KEY, name TEXT NOT NULL, hp BIGINT, score DOUBLE PRECISION)").await;
    let out = exec(
        &e,
        &mut s,
        "INSERT INTO players (name, hp, score) VALUES ('alice', 100, 9.5), ('bob', 80, 7.25)",
    )
    .await;
    assert_eq!(tag_of(&out), "INSERT 0 2");

    let out = exec(&e, &mut s, "SELECT name, hp FROM players ORDER BY hp DESC").await;
    let rows = rows_of(&out);
    assert_eq!(rows, &[vec![t("alice"), i(100)], vec![t("bob"), i(80)]]);
    e.store.close();
}

#[tokio::test]
async fn where_and_expressions() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE items (name TEXT, qty BIGINT, price DOUBLE PRECISION)").await;
    exec(
        &e,
        &mut s,
        "INSERT INTO items VALUES ('sword', 3, 100.0), ('shield', 1, 50.0), ('potion', 20, 5.0)",
    )
    .await;

    let out = exec(&e, &mut s, "SELECT name FROM items WHERE qty > 1 AND price < 60 ORDER BY name").await;
    assert_eq!(rows_of(&out), &[vec![t("potion")]]);

    let out = exec(&e, &mut s, "SELECT name FROM items WHERE name LIKE 's%' ORDER BY name").await;
    assert_eq!(rows_of(&out), &[vec![t("shield")], vec![t("sword")]]);

    let out = exec(&e, &mut s, "SELECT name, qty * price AS total FROM items WHERE qty BETWEEN 2 AND 25 ORDER BY total DESC").await;
    let rows = rows_of(&out);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], t("sword"));

    let out = exec(&e, &mut s, "SELECT name FROM items WHERE name IN ('sword', 'potion') ORDER BY name").await;
    assert_eq!(rows_of(&out).len(), 2);
    e.store.close();
}

#[tokio::test]
async fn update_delete_returning() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE p (id BIGINT, hp BIGINT)").await;
    exec(&e, &mut s, "INSERT INTO p VALUES (1, 100), (2, 50), (3, 10)").await;

    let out = exec(&e, &mut s, "UPDATE p SET hp = hp - 10 WHERE hp > 20 RETURNING id, hp").await;
    assert_eq!(tag_of(&out), "UPDATE 2");
    assert_eq!(rows_of(&out).len(), 2);

    let out = exec(&e, &mut s, "DELETE FROM p WHERE hp < 20 RETURNING id").await;
    assert_eq!(tag_of(&out), "DELETE 1");

    let out = exec(&e, &mut s, "SELECT COUNT(*) FROM p").await;
    assert_eq!(rows_of(&out), &[vec![i(2)]]);
    e.store.close();
}

#[tokio::test]
async fn aggregates_and_group_by() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE scores (player TEXT, zone TEXT, points BIGINT)").await;
    exec(
        &e,
        &mut s,
        "INSERT INTO scores VALUES ('a','north',10),('b','north',20),('c','south',5),('d','south',15),('e','south',25)",
    )
    .await;

    let out = exec(&e, &mut s, "SELECT COUNT(*), SUM(points), AVG(points), MIN(points), MAX(points) FROM scores").await;
    assert_eq!(
        rows_of(&out),
        &[vec![i(5), i(75), Scalar::Float(15.0), i(5), i(25)]]
    );

    let out = exec(
        &e,
        &mut s,
        "SELECT zone, COUNT(*) AS n, SUM(points) AS total FROM scores GROUP BY zone ORDER BY total DESC",
    )
    .await;
    assert_eq!(
        rows_of(&out),
        &[vec![t("south"), i(3), i(45)], vec![t("north"), i(2), i(30)]]
    );

    let out = exec(
        &e,
        &mut s,
        "SELECT zone FROM scores GROUP BY zone HAVING SUM(points) > 40",
    )
    .await;
    assert_eq!(rows_of(&out), &[vec![t("south")]]);
    e.store.close();
}

#[tokio::test]
async fn joins_inner_and_left() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE users (id BIGINT, name TEXT)").await;
    exec(&e, &mut s, "CREATE TABLE orders (user_id BIGINT, item TEXT)").await;
    exec(&e, &mut s, "INSERT INTO users VALUES (1,'alice'),(2,'bob'),(3,'carol')").await;
    exec(&e, &mut s, "INSERT INTO orders VALUES (1,'sword'),(1,'shield'),(2,'potion')").await;

    let out = exec(
        &e,
        &mut s,
        "SELECT u.name, o.item FROM users u JOIN orders o ON u.id = o.user_id ORDER BY u.name, o.item",
    )
    .await;
    assert_eq!(
        rows_of(&out),
        &[
            vec![t("alice"), t("shield")],
            vec![t("alice"), t("sword")],
            vec![t("bob"), t("potion")]
        ]
    );

    let out = exec(
        &e,
        &mut s,
        "SELECT u.name, o.item FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.item IS NULL",
    )
    .await;
    assert_eq!(rows_of(&out), &[vec![t("carol"), Scalar::Null]]);
    e.store.close();
}

#[tokio::test]
async fn transactions_snapshot_isolation() {
    let e = engine();
    let mut s1 = Session::default();
    let mut s2 = Session::default();
    exec(&e, &mut s1, "CREATE TABLE acct (id BIGINT, bal BIGINT)").await;
    exec(&e, &mut s1, "INSERT INTO acct VALUES (1, 100)").await;

    exec(&e, &mut s1, "BEGIN").await;
    // s1's snapshot is pinned; s2 commits a concurrent change.
    exec(&e, &mut s2, "UPDATE acct SET bal = 999 WHERE id = 1").await;
    // s1 still sees the old value (snapshot isolation).
    let out = exec(&e, &mut s1, "SELECT bal FROM acct WHERE id = 1").await;
    assert_eq!(rows_of(&out), &[vec![i(100)]]);
    // s1 writes the same row → first-committer-wins → conflict at COMMIT.
    exec(&e, &mut s1, "UPDATE acct SET bal = 50 WHERE id = 1").await;
    let err = exec_err(&e, &mut s1, "COMMIT").await;
    assert!(err.contains("could not serialize"), "got: {err}");
    // s2's value survived.
    let out = exec(&e, &mut s2, "SELECT bal FROM acct WHERE id = 1").await;
    assert_eq!(rows_of(&out), &[vec![i(999)]]);
    e.store.close();
}

#[tokio::test]
async fn transaction_rollback_discards() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE x (v BIGINT)").await;
    exec(&e, &mut s, "BEGIN").await;
    exec(&e, &mut s, "INSERT INTO x VALUES (1)").await;
    // Read-your-writes inside the txn.
    let out = exec(&e, &mut s, "SELECT COUNT(*) FROM x").await;
    assert_eq!(rows_of(&out), &[vec![i(1)]]);
    exec(&e, &mut s, "ROLLBACK").await;
    let out = exec(&e, &mut s, "SELECT COUNT(*) FROM x").await;
    assert_eq!(rows_of(&out), &[vec![i(0)]]);
    e.store.close();
}

#[tokio::test]
async fn aborted_txn_rejects_until_rollback() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE y (v BIGINT)").await;
    exec(&e, &mut s, "BEGIN").await;
    let _ = exec_err(&e, &mut s, "SELECT nope FROM y").await;
    let err = exec_err(&e, &mut s, "SELECT 1").await;
    assert!(err.contains("aborted"), "got: {err}");
    exec(&e, &mut s, "ROLLBACK").await;
    let out = exec(&e, &mut s, "SELECT 1").await;
    assert_eq!(rows_of(&out), &[vec![i(1)]]);
    e.store.close();
}

#[tokio::test]
async fn not_null_and_serial() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE u (id SERIAL, email TEXT NOT NULL)").await;
    let err = exec_err(&e, &mut s, "INSERT INTO u (email) VALUES (NULL)").await;
    assert!(err.contains("not-null"), "got: {err}");
    exec(&e, &mut s, "INSERT INTO u (email) VALUES ('a@b.c'), ('d@e.f')").await;
    let out = exec(&e, &mut s, "SELECT id FROM u ORDER BY id").await;
    assert_eq!(rows_of(&out), &[vec![i(1)], vec![i(2)]]);
    e.store.close();
}

#[tokio::test]
async fn scalar_functions_and_case() {
    let e = engine();
    let mut s = Session::default();
    let out = exec(&e, &mut s, "SELECT UPPER('abc'), LENGTH('hello'), COALESCE(NULL, 'x'), 2 + 3 * 4").await;
    assert_eq!(rows_of(&out), &[vec![t("ABC"), i(5), t("x"), i(14)]]);

    let out = exec(
        &e,
        &mut s,
        "SELECT CASE WHEN 1 > 2 THEN 'no' WHEN 2 > 1 THEN 'yes' ELSE 'never' END",
    )
    .await;
    assert_eq!(rows_of(&out), &[vec![t("yes")]]);

    let out = exec(&e, &mut s, "SELECT version()").await;
    match &rows_of(&out)[0][0] {
        Scalar::Text(v) => assert!(v.contains("PostgreSQL"), "got {v}"),
        other => panic!("bad version {other:?}"),
    }
    e.store.close();
}

#[tokio::test]
async fn subqueries() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE a (v BIGINT)").await;
    exec(&e, &mut s, "CREATE TABLE b (v BIGINT)").await;
    exec(&e, &mut s, "INSERT INTO a VALUES (1),(2),(3)").await;
    exec(&e, &mut s, "INSERT INTO b VALUES (2),(3),(4)").await;

    let out = exec(&e, &mut s, "SELECT v FROM a WHERE v IN (SELECT v FROM b) ORDER BY v").await;
    assert_eq!(rows_of(&out), &[vec![i(2)], vec![i(3)]]);

    let out = exec(&e, &mut s, "SELECT (SELECT MAX(v) FROM b)").await;
    assert_eq!(rows_of(&out), &[vec![i(4)]]);
    e.store.close();
}

#[tokio::test]
async fn limit_offset_distinct() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE n (v BIGINT)").await;
    exec(&e, &mut s, "INSERT INTO n VALUES (1),(2),(2),(3),(3),(3)").await;
    let out = exec(&e, &mut s, "SELECT DISTINCT v FROM n ORDER BY v").await;
    assert_eq!(rows_of(&out), &[vec![i(1)], vec![i(2)], vec![i(3)]]);
    let out = exec(&e, &mut s, "SELECT v FROM n ORDER BY v LIMIT 2 OFFSET 1").await;
    assert_eq!(rows_of(&out), &[vec![i(2)], vec![i(2)]]);
    e.store.close();
}

#[tokio::test]
async fn information_schema_shims() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE t1 (a BIGINT, b TEXT)").await;
    let out = exec(&e, &mut s, "SELECT table_name FROM information_schema.tables").await;
    assert_eq!(rows_of(&out), &[vec![t("t1")]]);
    let out = exec(
        &e,
        &mut s,
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 't1' ORDER BY ordinal_position",
    )
    .await;
    assert_eq!(
        rows_of(&out),
        &[vec![t("a"), t("bigint")], vec![t("b"), t("text")]]
    );
    e.store.close();
}

#[tokio::test]
async fn params_via_placeholders() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE pp (k TEXT, v BIGINT)").await;
    e.execute(&mut s, "INSERT INTO pp VALUES ($1, $2)", &[t("key1"), i(42)])
        .await
        .unwrap();
    let out = e
        .execute(&mut s, "SELECT v FROM pp WHERE k = $1", &[t("key1")])
        .await
        .unwrap();
    assert_eq!(rows_of(&out), &[vec![i(42)]]);
    e.store.close();
}

#[tokio::test]
async fn drop_and_truncate() {
    let e = engine();
    let mut s = Session::default();
    exec(&e, &mut s, "CREATE TABLE d (v BIGINT)").await;
    exec(&e, &mut s, "INSERT INTO d VALUES (1),(2)").await;
    exec(&e, &mut s, "TRUNCATE d").await;
    let out = exec(&e, &mut s, "SELECT COUNT(*) FROM d").await;
    assert_eq!(rows_of(&out), &[vec![i(0)]]);
    exec(&e, &mut s, "DROP TABLE d").await;
    let err = exec_err(&e, &mut s, "SELECT * FROM d").await;
    assert!(err.contains("does not exist"));
    e.store.close();
}

#[tokio::test]
async fn catalog_survives_restart() {
    let dir = std::env::temp_dir().join(format!("neondb_pg_persist_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    {
        let store = MvccStore::open(crate::mvcc::MvccConfig {
            data_dir: Some(dir.clone()),
            fsync: crate::mvcc::FsyncPolicy::Always,
        })
        .unwrap();
        let e = PgEngine::new(store);
        let mut s = Session::default();
        exec(&e, &mut s, "CREATE TABLE persisted (id SERIAL, name TEXT)").await;
        exec(&e, &mut s, "INSERT INTO persisted (name) VALUES ('x'), ('y')").await;
        e.store.barrier().await;
        e.store.close();
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    {
        let store = MvccStore::open(crate::mvcc::MvccConfig {
            data_dir: Some(dir.clone()),
            fsync: crate::mvcc::FsyncPolicy::Always,
        })
        .unwrap();
        let e = PgEngine::new(store);
        let mut s = Session::default();
        let out = exec(&e, &mut s, "SELECT name FROM persisted ORDER BY id").await;
        assert_eq!(rows_of(&out), &[vec![t("x")], vec![t("y")]]);
        // Rowid counter resumed: next insert gets id 3.
        exec(&e, &mut s, "INSERT INTO persisted (name) VALUES ('z')").await;
        let out = exec(&e, &mut s, "SELECT id FROM persisted ORDER BY id DESC LIMIT 1").await;
        assert_eq!(rows_of(&out), &[vec![i(3)]]);
        e.store.close();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ─────────────────────────────────────────────────────────────────────────────
// pgwire over a real socket
// ─────────────────────────────────────────────────────────────────────────────

mod wire {
    use super::*;
    use crate::pg::{serve, PgCtx};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn spawn_server() -> (u16, MvccStore) {
        let store = MvccStore::open_memory();
        let ctx = PgCtx::new(PgEngine::new(store.clone()), None);
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = serve(listener, ctx).await;
        });
        (port, store)
    }

    async fn startup(port: u16) -> TcpStream {
        let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        for (k, v) in [("user", "test"), ("database", "neondb")] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        msg.extend_from_slice(&body);
        sock.write_all(&msg).await.unwrap();
        // Drain until ReadyForQuery ('Z').
        read_until_ready(&mut sock).await;
        sock
    }

    async fn read_until_ready(sock: &mut TcpStream) -> Vec<u8> {
        let mut all = Vec::new();
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = tokio::time::timeout(Duration::from_secs(3), sock.read(&mut buf))
                .await
                .expect("timeout")
                .unwrap();
            assert!(n > 0, "connection closed early");
            all.extend_from_slice(&buf[..n]);
            // Scan frames for ReadyForQuery.
            let mut i = 0;
            while i + 5 <= all.len() {
                let len = i32::from_be_bytes([all[i + 1], all[i + 2], all[i + 3], all[i + 4]]) as usize;
                if i + 1 + len > all.len() {
                    break;
                }
                if all[i] == b'Z' {
                    return all;
                }
                i += 1 + len;
            }
        }
    }

    async fn simple_query(sock: &mut TcpStream, sql: &str) -> Vec<u8> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(&body);
        sock.write_all(&msg).await.unwrap();
        read_until_ready(sock).await
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[tokio::test]
    async fn full_session_over_wire() {
        let (port, store) = spawn_server().await;
        let mut sock = startup(port).await;

        let r = simple_query(&mut sock, "CREATE TABLE wt (id BIGINT, name TEXT)").await;
        assert!(contains(&r, b"CREATE TABLE"), "missing tag: {:?}", String::from_utf8_lossy(&r));

        let r = simple_query(&mut sock, "INSERT INTO wt VALUES (1, 'hello'), (2, 'world')").await;
        assert!(contains(&r, b"INSERT 0 2"));

        let r = simple_query(&mut sock, "SELECT name FROM wt ORDER BY id").await;
        assert!(contains(&r, b"hello") && contains(&r, b"world"));
        assert!(contains(&r, b"SELECT 2"));

        // Error path: bad SQL produces ErrorResponse then ReadyForQuery.
        let r = simple_query(&mut sock, "SELECT FROM FROM").await;
        assert_eq!(r[0], b'E');

        // Multi-statement.
        let r = simple_query(&mut sock, "BEGIN; UPDATE wt SET name = 'x' WHERE id = 1; COMMIT").await;
        assert!(contains(&r, b"BEGIN") && contains(&r, b"UPDATE 1") && contains(&r, b"COMMIT"));

        store.close();
    }

    #[tokio::test]
    async fn extended_protocol_with_params() {
        let (port, store) = spawn_server().await;
        let mut sock = startup(port).await;
        simple_query(&mut sock, "CREATE TABLE ep (k TEXT, v BIGINT)").await;

        // Parse + Bind + Describe(portal) + Execute + Sync
        let mut msg = Vec::new();
        // Parse: unnamed stmt
        let mut body = Vec::new();
        body.push(0); // empty stmt name
        body.extend_from_slice(b"INSERT INTO ep VALUES ($1, $2)\0");
        body.extend_from_slice(&0i16.to_be_bytes()); // no param oids
        msg.push(b'P');
        msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(&body);
        // Bind: text params "alpha", "7"
        let mut body = Vec::new();
        body.push(0); // portal
        body.push(0); // stmt
        body.extend_from_slice(&0i16.to_be_bytes()); // fmt codes
        body.extend_from_slice(&2i16.to_be_bytes()); // 2 params
        for p in ["alpha", "7"] {
            body.extend_from_slice(&(p.len() as i32).to_be_bytes());
            body.extend_from_slice(p.as_bytes());
        }
        body.extend_from_slice(&0i16.to_be_bytes()); // result fmts
        msg.push(b'B');
        msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(&body);
        // Execute
        let mut body = Vec::new();
        body.push(0); // portal
        body.extend_from_slice(&0i32.to_be_bytes());
        msg.push(b'E');
        msg.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(&body);
        // Sync
        msg.push(b'S');
        msg.extend_from_slice(&4i32.to_be_bytes());

        sock.write_all(&msg).await.unwrap();
        let r = read_until_ready(&mut sock).await;
        assert!(contains(&r, b"INSERT 0 1"), "got: {:?}", String::from_utf8_lossy(&r));

        let r = simple_query(&mut sock, "SELECT v FROM ep WHERE k = 'alpha'").await;
        assert!(contains(&r, b"7"));
        store.close();
    }
}

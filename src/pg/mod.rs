//! PostgreSQL wire protocol (v3) server for Voltra.
//!
//! Speaks the protocol every PostgreSQL client uses: psql, pgAdmin, DBeaver,
//! psycopg, node-postgres, JDBC, tokio-postgres, …
//!
//! * Startup + trust/cleartext-password auth (+ SSLRequest politely declined)
//! * Simple query protocol ('Q') — full multi-statement support
//! * Extended protocol: Parse/Bind/Describe/Execute/Close/Sync with $n params
//! * Transactions map to MVCC snapshot isolation (BEGIN/COMMIT/ROLLBACK)

pub mod catalog;
pub mod executor;
pub mod types;

#[cfg(test)]
mod tests;

use crate::mvcc::Scalar;
use bytes::{Buf, BytesMut};
use executor::{ExecOut, PgEngine, Session};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use types::{scalar_to_text, OID_BOOL, OID_FLOAT8, OID_INT4, OID_INT8, OID_TEXT};

pub struct PgCtx {
    pub engine: PgEngine,
    pub password: Option<String>,
}

impl PgCtx {
    pub fn new(engine: PgEngine, password: Option<String>) -> Arc<Self> {
        Arc::new(Self { engine, password })
    }
}

pub async fn start_pg_listener(host: String, port: u16, ctx: Arc<PgCtx>) -> std::io::Result<()> {
    let listener = TcpListener::bind((host.as_str(), port)).await?;
    log::info!("[pg] PostgreSQL wire listener on {host}:{port}");
    serve(listener, ctx).await
}

pub async fn serve(listener: TcpListener, ctx: Arc<PgCtx>) -> std::io::Result<()> {
    loop {
        let (sock, _) = listener.accept().await?;
        let _ = sock.set_nodelay(true);
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let _ = handle_conn(sock, ctx).await;
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire helpers
// ─────────────────────────────────────────────────────────────────────────────

struct Out {
    buf: Vec<u8>,
}

impl Out {
    fn new() -> Self {
        Self { buf: Vec::with_capacity(8 * 1024) }
    }
    /// Append a typed message: tag byte + i32 length (incl. itself) + body.
    fn msg(&mut self, tag: u8, body: &[u8]) {
        self.buf.push(tag);
        self.buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        self.buf.extend_from_slice(body);
    }
    fn auth_ok(&mut self) {
        self.msg(b'R', &0i32.to_be_bytes());
    }
    fn auth_cleartext(&mut self) {
        self.msg(b'R', &3i32.to_be_bytes());
    }
    fn parameter_status(&mut self, k: &str, v: &str) {
        let mut b = Vec::new();
        b.extend_from_slice(k.as_bytes());
        b.push(0);
        b.extend_from_slice(v.as_bytes());
        b.push(0);
        self.msg(b'S', &b);
    }
    fn backend_key_data(&mut self) {
        let mut b = Vec::new();
        b.extend_from_slice(&(std::process::id() as i32).to_be_bytes());
        b.extend_from_slice(&0x6e656f6ei32.to_be_bytes()); // "neon"
        self.msg(b'K', &b);
    }
    fn ready(&mut self, status: u8) {
        self.msg(b'Z', &[status]);
    }
    fn row_description(&mut self, cols: &[(String, u32)]) {
        let mut b = Vec::new();
        b.extend_from_slice(&(cols.len() as i16).to_be_bytes());
        for (name, oid) in cols {
            b.extend_from_slice(name.as_bytes());
            b.push(0);
            b.extend_from_slice(&0i32.to_be_bytes()); // table oid
            b.extend_from_slice(&0i16.to_be_bytes()); // attnum
            b.extend_from_slice(&(*oid as i32).to_be_bytes());
            b.extend_from_slice(&(-1i16).to_be_bytes()); // typlen
            b.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
            b.extend_from_slice(&0i16.to_be_bytes()); // text format
        }
        self.msg(b'T', &b);
    }
    fn data_row(&mut self, row: &[Scalar]) {
        let mut b = Vec::new();
        b.extend_from_slice(&(row.len() as i16).to_be_bytes());
        for v in row {
            match scalar_to_text(v) {
                Some(t) => {
                    b.extend_from_slice(&(t.len() as i32).to_be_bytes());
                    b.extend_from_slice(t.as_bytes());
                }
                None => b.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        self.msg(b'D', &b);
    }
    fn command_complete(&mut self, tag: &str) {
        let mut b = Vec::new();
        b.extend_from_slice(tag.as_bytes());
        b.push(0);
        self.msg(b'C', &b);
    }
    fn empty_query(&mut self) {
        self.msg(b'I', &[]);
    }
    fn error(&mut self, msg: &str) {
        let code = error_code(msg);
        let mut b = Vec::new();
        for (f, v) in [(b'S', "ERROR"), (b'V', "ERROR"), (b'C', code), (b'M', msg)] {
            b.push(f);
            b.extend_from_slice(v.as_bytes());
            b.push(0);
        }
        b.push(0);
        self.msg(b'E', &b);
    }
    fn parse_complete(&mut self) {
        self.msg(b'1', &[]);
    }
    fn bind_complete(&mut self) {
        self.msg(b'2', &[]);
    }
    fn close_complete(&mut self) {
        self.msg(b'3', &[]);
    }
    fn no_data(&mut self) {
        self.msg(b'n', &[]);
    }
    fn parameter_description(&mut self, oids: &[u32]) {
        let mut b = Vec::new();
        b.extend_from_slice(&(oids.len() as i16).to_be_bytes());
        for o in oids {
            b.extend_from_slice(&(*o as i32).to_be_bytes());
        }
        self.msg(b't', &b);
    }
    async fn flush(&mut self, sock: &mut TcpStream) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            sock.write_all(&self.buf).await?;
            self.buf.clear();
        }
        Ok(())
    }
}

fn error_code(msg: &str) -> &'static str {
    if msg.starts_with("syntax error") {
        "42601"
    } else if msg.contains("does not exist") {
        "42P01"
    } else if msg.contains("not-null constraint") {
        "23502"
    } else if msg.contains("could not serialize") {
        "40001"
    } else if msg.contains("transaction is aborted") {
        "25P02"
    } else if msg.contains("already exists") {
        "42P07"
    } else {
        "XX000"
    }
}

fn read_cstr(buf: &mut &[u8]) -> Option<String> {
    let pos = buf.iter().position(|&b| b == 0)?;
    let s = String::from_utf8_lossy(&buf[..pos]).into_owned();
    *buf = &buf[pos + 1..];
    Some(s)
}

// ─────────────────────────────────────────────────────────────────────────────
// Connection
// ─────────────────────────────────────────────────────────────────────────────

struct Prepared {
    sql: String,
    param_oids: Vec<u32>,
}

struct Portal {
    sql: String,
    params: Vec<Scalar>,
    /// RowDescription already sent by Describe(portal)?
    described: bool,
    /// Result cached by an eager Describe(portal) execution.
    cached: Option<Vec<ExecOut>>,
}

async fn handle_conn(mut sock: TcpStream, ctx: Arc<PgCtx>) -> std::io::Result<()> {
    // ── Startup phase ────────────────────────────────────────────────────────
    let startup_params: HashMap<String, String>;
    loop {
        let len = match sock.read_i32().await {
            Ok(l) if (8..=64 * 1024).contains(&l) => l as usize,
            _ => return Ok(()),
        };
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await?;
        let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        match code {
            80877103 | 80877104 => {
                // SSLRequest / GSSENCRequest → decline, client retries plaintext.
                sock.write_all(b"N").await?;
                continue;
            }
            80877102 => return Ok(()), // CancelRequest — nothing to cancel
            196608 => {
                // StartupMessage 3.0
                let mut rest = &body[4..];
                let mut params = HashMap::new();
                while let Some(k) = read_cstr(&mut rest) {
                    if k.is_empty() {
                        break;
                    }
                    let v = read_cstr(&mut rest).unwrap_or_default();
                    params.insert(k, v);
                }
                startup_params = params;
                break;
            }
            _ => return Ok(()), // unsupported protocol
        }
    }
    let _ = &startup_params;

    let mut out = Out::new();

    // ── Auth ─────────────────────────────────────────────────────────────────
    if let Some(expected) = &ctx.password {
        out.auth_cleartext();
        out.flush(&mut sock).await?;
        // Expect PasswordMessage 'p'.
        let mut tag = [0u8; 1];
        sock.read_exact(&mut tag).await?;
        let len = sock.read_i32().await? as usize;
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await?;
        let mut rest = &body[..];
        let supplied = read_cstr(&mut rest).unwrap_or_default();
        if tag[0] != b'p' || &supplied != expected {
            out.error("password authentication failed");
            out.flush(&mut sock).await?;
            return Ok(());
        }
    }
    out.auth_ok();
    out.parameter_status("server_version", "16.4");
    out.parameter_status("server_encoding", "UTF8");
    out.parameter_status("client_encoding", "UTF8");
    out.parameter_status("DateStyle", "ISO, MDY");
    out.parameter_status("integer_datetimes", "on");
    out.parameter_status("standard_conforming_strings", "on");
    out.parameter_status("TimeZone", "UTC");
    out.backend_key_data();
    out.ready(b'I');
    out.flush(&mut sock).await?;

    // ── Message loop ─────────────────────────────────────────────────────────
    let mut sess = Session::default();
    let mut prepared: HashMap<String, Prepared> = HashMap::new();
    let mut portals: HashMap<String, Portal> = HashMap::new();
    // Skip until Sync after an error in the extended protocol.
    let mut ext_failed = false;
    let mut buf = BytesMut::with_capacity(16 * 1024);

    loop {
        // Frame: 1 tag byte + i32 length.
        while buf.len() < 5 {
            if sock.read_buf(&mut buf).await? == 0 {
                return Ok(());
            }
        }
        let tag = buf[0];
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        if !(4..=256 * 1024 * 1024).contains(&len) {
            return Ok(());
        }
        while buf.len() < 1 + len {
            if sock.read_buf(&mut buf).await? == 0 {
                return Ok(());
            }
        }
        buf.advance(5);
        let body = buf.split_to(len - 4).to_vec();

        match tag {
            b'X' => return Ok(()), // Terminate
            b'Q' => {
                let mut rest = &body[..];
                let sql = read_cstr(&mut rest).unwrap_or_default();
                if sql.trim().is_empty() {
                    out.empty_query();
                } else {
                    match ctx.engine.execute(&mut sess, &sql, &[]).await {
                        Ok(results) => {
                            for r in results {
                                emit_result(&mut out, r, true);
                            }
                        }
                        Err(e) => out.error(&e),
                    }
                }
                out.ready(txn_status(&sess));
                out.flush(&mut sock).await?;
            }
            b'P' => {
                // Parse: name, query, param type oids.
                if ext_failed {
                    continue;
                }
                let mut rest = &body[..];
                let name = read_cstr(&mut rest).unwrap_or_default();
                let sql = read_cstr(&mut rest).unwrap_or_default();
                let n = if rest.len() >= 2 {
                    let n = i16::from_be_bytes([rest[0], rest[1]]) as usize;
                    rest = &rest[2..];
                    n
                } else {
                    0
                };
                let mut oids = Vec::with_capacity(n);
                for _ in 0..n {
                    if rest.len() >= 4 {
                        oids.push(u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]));
                        rest = &rest[4..];
                    }
                }
                prepared.insert(name, Prepared { sql, param_oids: oids });
                out.parse_complete();
            }
            b'B' => {
                // Bind: portal, statement, formats, params, result formats.
                if ext_failed {
                    continue;
                }
                let mut rest = &body[..];
                let portal_name = read_cstr(&mut rest).unwrap_or_default();
                let stmt_name = read_cstr(&mut rest).unwrap_or_default();
                let Some(stmt) = prepared.get(&stmt_name) else {
                    out.error(&format!("prepared statement \"{stmt_name}\" does not exist"));
                    ext_failed = true;
                    continue;
                };
                let take_i16 = |rest: &mut &[u8]| -> i16 {
                    if rest.len() >= 2 {
                        let v = i16::from_be_bytes([rest[0], rest[1]]);
                        *rest = &rest[2..];
                        v
                    } else {
                        0
                    }
                };
                let nfmt = take_i16(&mut rest) as usize;
                let mut fmts = Vec::with_capacity(nfmt);
                for _ in 0..nfmt {
                    fmts.push(take_i16(&mut rest));
                }
                let nparams = take_i16(&mut rest) as usize;
                let mut params = Vec::with_capacity(nparams);
                for i in 0..nparams {
                    if rest.len() < 4 {
                        break;
                    }
                    let plen = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
                    rest = &rest[4..];
                    if plen < 0 {
                        params.push(Scalar::Null);
                        continue;
                    }
                    let plen = plen as usize;
                    if rest.len() < plen {
                        break;
                    }
                    let raw = &rest[..plen];
                    rest = &rest[plen..];
                    let fmt = fmts.get(i).or(fmts.first()).copied().unwrap_or(0);
                    let oid = stmt.param_oids.get(i).copied().unwrap_or(0);
                    params.push(decode_param(raw, fmt, oid));
                }
                portals.insert(
                    portal_name,
                    Portal { sql: stmt.sql.clone(), params, described: false, cached: None },
                );
                out.bind_complete();
            }
            b'D' => {
                // Describe statement ('S') or portal ('P').
                if ext_failed {
                    continue;
                }
                let kind = body.first().copied().unwrap_or(b'S');
                let mut rest = &body[1..];
                let name = read_cstr(&mut rest).unwrap_or_default();
                if kind == b'S' {
                    match prepared.get(&name) {
                        Some(p) => {
                            let oids: Vec<u32> =
                                p.param_oids.iter().map(|o| if *o == 0 { OID_TEXT } else { *o }).collect();
                            out.parameter_description(&oids);
                            match describe_columns(&ctx.engine, &p.sql) {
                                Some(cols) if !cols.is_empty() => out.row_description(&cols),
                                _ => out.no_data(),
                            }
                        }
                        None => {
                            out.error(&format!("prepared statement \"{name}\" does not exist"));
                            ext_failed = true;
                        }
                    }
                } else {
                    // Portal: execute eagerly, cache, emit real RowDescription.
                    let Some(portal) = portals.get_mut(&name) else {
                        out.error(&format!("portal \"{name}\" does not exist"));
                        ext_failed = true;
                        continue;
                    };
                    let results = ctx.engine.execute(&mut sess, &portal.sql, &portal.params).await;
                    match results {
                        Ok(results) => {
                            let has_rows = results.iter().any(|r| matches!(r, ExecOut::Rows { .. }));
                            if has_rows {
                                for r in &results {
                                    if let ExecOut::Rows { cols, .. } = r {
                                        out.row_description(cols);
                                        break;
                                    }
                                }
                            } else {
                                out.no_data();
                            }
                            portal.described = true;
                            portal.cached = Some(results);
                        }
                        Err(e) => {
                            out.error(&e);
                            ext_failed = true;
                        }
                    }
                }
            }
            b'E' => {
                // Execute portal.
                if ext_failed {
                    continue;
                }
                let mut rest = &body[..];
                let name = read_cstr(&mut rest).unwrap_or_default();
                let Some(portal) = portals.get_mut(&name) else {
                    out.error(&format!("portal \"{name}\" does not exist"));
                    ext_failed = true;
                    continue;
                };
                let results = match portal.cached.take() {
                    Some(r) => Ok(r),
                    None => ctx.engine.execute(&mut sess, &portal.sql, &portal.params).await,
                };
                match results {
                    Ok(results) => {
                        let send_desc = !portal.described;
                        for r in results {
                            emit_result(&mut out, r, send_desc);
                        }
                    }
                    Err(e) => {
                        out.error(&e);
                        ext_failed = true;
                    }
                }
            }
            b'C' => {
                // Close statement/portal.
                let kind = body.first().copied().unwrap_or(b'S');
                let mut rest = &body[1..];
                let name = read_cstr(&mut rest).unwrap_or_default();
                if kind == b'S' {
                    prepared.remove(&name);
                } else {
                    portals.remove(&name);
                }
                out.close_complete();
            }
            b'H' => {
                out.flush(&mut sock).await?;
            }
            b'S' => {
                // Sync: end of extended-protocol batch.
                ext_failed = false;
                out.ready(txn_status(&sess));
                out.flush(&mut sock).await?;
            }
            b'p' => { /* unexpected PasswordMessage — ignore */ }
            b'F' => {
                out.error("function call protocol is not supported");
                out.ready(txn_status(&sess));
                out.flush(&mut sock).await?;
            }
            _ => {
                out.error(&format!("unsupported message type '{}'", tag as char));
                out.ready(txn_status(&sess));
                out.flush(&mut sock).await?;
            }
        }
    }
}

fn txn_status(sess: &Session) -> u8 {
    match &sess.txn {
        None => b'I',
        Some(t) if t.aborted => b'E',
        Some(_) => b'T',
    }
}

/// Send one statement result. `with_desc` controls RowDescription emission
/// (simple protocol: yes; extended after Describe(portal): no).
fn emit_result(out: &mut Out, r: ExecOut, with_desc: bool) {
    match r {
        ExecOut::Rows { cols, rows, tag } => {
            if with_desc {
                out.row_description(&cols);
            }
            for row in &rows {
                out.data_row(row);
            }
            out.command_complete(&tag);
        }
        ExecOut::Tag(tag) => out.command_complete(&tag),
    }
}

fn decode_param(raw: &[u8], fmt: i16, oid: u32) -> Scalar {
    if fmt == 1 {
        // Binary format for the common types.
        return match (oid, raw.len()) {
            (OID_INT4, 4) => Scalar::Int(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as i64),
            (OID_INT8, 8) => Scalar::Int(i64::from_be_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ])),
            (OID_FLOAT8, 8) => Scalar::Float(f64::from_be_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ])),
            (OID_BOOL, 1) => Scalar::Bool(raw[0] != 0),
            _ => Scalar::Text(String::from_utf8_lossy(raw).into_owned()),
        };
    }
    let text = String::from_utf8_lossy(raw).into_owned();
    match oid {
        OID_INT4 | OID_INT8 => text.trim().parse::<i64>().map(Scalar::Int).unwrap_or(Scalar::Text(text)),
        OID_FLOAT8 => text.trim().parse::<f64>().map(Scalar::Float).unwrap_or(Scalar::Text(text)),
        OID_BOOL => Scalar::Bool(matches!(text.as_str(), "t" | "true" | "1" | "on" | "yes")),
        _ => Scalar::Text(text),
    }
}

/// Best-effort column metadata for Describe(statement) — resolves simple
/// SELECT projections against the catalog without executing the query.
fn describe_columns(engine: &PgEngine, sql: &str) -> Option<Vec<(String, u32)>> {
    use sqlparser::ast::{SelectItem, SetExpr, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).ok()?;
    let q = match stmts.first()? {
        Statement::Query(q) => q,
        _ => return None,
    };
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    let def = match select.from.first() {
        Some(twj) => match &twj.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                let n = name.0.iter().map(|i| i.value.to_lowercase()).collect::<Vec<_>>().join(".");
                engine.catalog.get(&n)
            }
            _ => None,
        },
        None => None,
    };
    let mut cols = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                let d = def.as_ref()?;
                for c in &d.columns {
                    cols.push((c.name.clone(), c.ctype.oid()));
                }
            }
            SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(id)) => {
                let n = id.value.to_lowercase();
                let oid = def
                    .as_ref()
                    .and_then(|d| d.column(&n))
                    .map(|c| c.ctype.oid())
                    .unwrap_or(OID_TEXT);
                cols.push((n, oid));
            }
            SelectItem::UnnamedExpr(e) => {
                cols.push((format!("{e}").to_lowercase(), OID_TEXT));
            }
            SelectItem::ExprWithAlias { alias, expr } => {
                let oid = match expr {
                    sqlparser::ast::Expr::Identifier(id) => def
                        .as_ref()
                        .and_then(|d| d.column(&id.value.to_lowercase()))
                        .map(|c| c.ctype.oid())
                        .unwrap_or(OID_TEXT),
                    _ => OID_TEXT,
                };
                cols.push((alias.value.clone(), oid));
            }
            SelectItem::QualifiedWildcard(..) => return None,
        }
    }
    Some(cols)
}

//! SQL execution engine over the MVCC store.
//!
//! sqlparser-rs (PostgreSQL dialect) front end; this module walks the AST.
//! Reads run on MVCC snapshots; writes stage `WriteOp` effects that commit
//! through the sequencer with first-committer-wins conflict detection —
//! i.e. real snapshot isolation, the same model PostgreSQL ships.

use super::catalog::{rowid_key, Catalog, ColumnDef as CatColumn, TableDef};
use super::types::{scalar_oid, scalar_to_text, ColType};
use crate::mvcc::{
    Datum, MvccStore, NsKey, Scalar, SnapshotGuard, WriteOp, NS_PG_CATALOG,
};
use bytes::Bytes;
use sqlparser::ast::{
    self, BinaryOperator, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, JoinConstraint,
    JoinOperator, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    UnaryOperator, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;

pub type RowMap = im::HashMap<String, Scalar>;

pub const PG_VERSION: &str = "PostgreSQL 16.4 (Voltra)";

// ─────────────────────────────────────────────────────────────────────────────
// Session / transactions
// ─────────────────────────────────────────────────────────────────────────────

pub struct Txn {
    pub snap: SnapshotGuard,
    /// (ns, rowid-key) → staged row (None = deleted).
    pub overlay: HashMap<(u32, Bytes), Option<RowMap>>,
    pub writes: Vec<WriteOp>,
    pub conflict: Vec<NsKey>,
    pub aborted: bool,
}

#[derive(Default)]
pub struct Session {
    pub txn: Option<Txn>,
}

impl Session {
    pub fn in_txn(&self) -> bool {
        self.txn.is_some()
    }
}

/// One statement's output.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecOut {
    Rows {
        cols: Vec<(String, u32)>,
        rows: Vec<Vec<Scalar>>,
        /// Command tag, e.g. `SELECT 3`.
        tag: String,
    },
    Tag(String),
}

pub struct PgEngine {
    pub store: MvccStore,
    pub catalog: Catalog,
}

struct RowsOut {
    cols: Vec<(String, u32)>,
    rows: Vec<Vec<Scalar>>,
}

/// Ordered relation produced by FROM resolution.
struct RowSet {
    /// (qualified "alias.col", bare "col") in declaration order.
    col_order: Vec<(String, String)>,
    rows: Vec<RowCtx>,
}

type RowCtx = HashMap<String, Scalar>;

impl PgEngine {
    pub fn new(store: MvccStore) -> Self {
        let catalog = Catalog::load(&store);
        Self { store, catalog }
    }

    pub async fn execute(
        &self,
        sess: &mut Session,
        sql: &str,
        params: &[Scalar],
    ) -> Result<Vec<ExecOut>, String> {
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .map_err(|e| format!("syntax error: {e}"))?;
        let mut out = Vec::with_capacity(statements.len());
        for stmt in statements {
            // An aborted transaction rejects everything except COMMIT/ROLLBACK.
            if sess.txn.as_ref().map(|t| t.aborted).unwrap_or(false)
                && !matches!(stmt, Statement::Commit { .. } | Statement::Rollback { .. })
            {
                return Err(
                    "current transaction is aborted, commands ignored until end of transaction block"
                        .into(),
                );
            }
            match self.exec_stmt(sess, stmt, params).await {
                Ok(r) => out.push(r),
                Err(e) => {
                    if let Some(t) = sess.txn.as_mut() {
                        t.aborted = true;
                    }
                    return Err(e);
                }
            }
        }
        Ok(out)
    }

    async fn exec_stmt(
        &self,
        sess: &mut Session,
        stmt: Statement,
        params: &[Scalar],
    ) -> Result<ExecOut, String> {
        match stmt {
            Statement::Query(q) => {
                let rows = self.run_query(sess, &q, params)?;
                let n = rows.rows.len();
                Ok(ExecOut::Rows { cols: rows.cols, rows: rows.rows, tag: format!("SELECT {n}") })
            }
            Statement::Insert { table_name, columns, source, returning, .. } => {
                self.exec_insert(sess, table_name, columns, source, returning, params).await
            }
            Statement::Update { table, assignments, selection, returning, .. } => {
                self.exec_update(sess, table, assignments, selection, returning, params).await
            }
            Statement::Delete { from, selection, returning, .. } => {
                self.exec_delete(sess, from, selection, returning, params).await
            }
            Statement::CreateTable { name, columns, if_not_exists, .. } => {
                self.exec_create_table(sess, name, columns, if_not_exists).await
            }
            Statement::Drop { object_type, names, if_exists, .. } => {
                if object_type != ast::ObjectType::Table {
                    return Err(format!("DROP {object_type} is not supported"));
                }
                self.exec_drop(sess, names, if_exists).await
            }
            Statement::Truncate { table_name, .. } => {
                let mut writes = Vec::new();
                let mut keys = Vec::new();
                let def = self.resolve_def(&table_name)?;
                let ts = self.snapshot_ts(sess);
                self.store.for_each_visible(def.ns, ts, |key, _| {
                    writes.push(WriteOp::Del { ns: def.ns, key: Bytes::copy_from_slice(key) });
                    keys.push(NsKey::new(def.ns, Bytes::copy_from_slice(key)));
                });
                self.apply_writes(sess, writes, keys).await?;
                Ok(ExecOut::Tag("TRUNCATE TABLE".into()))
            }
            Statement::StartTransaction { .. } => {
                if sess.txn.is_some() {
                    // PG warns and keeps the existing transaction.
                    return Ok(ExecOut::Tag("BEGIN".into()));
                }
                sess.txn = Some(Txn {
                    snap: self.store.pin_snapshot(),
                    overlay: HashMap::new(),
                    writes: Vec::new(),
                    conflict: Vec::new(),
                    aborted: false,
                });
                Ok(ExecOut::Tag("BEGIN".into()))
            }
            Statement::Commit { .. } => {
                match sess.txn.take() {
                    None => Ok(ExecOut::Tag("COMMIT".into())),
                    Some(t) if t.aborted => Ok(ExecOut::Tag("ROLLBACK".into())),
                    Some(t) => {
                        if t.writes.is_empty() {
                            return Ok(ExecOut::Tag("COMMIT".into()));
                        }
                        let read_ts = t.snap.ts;
                        self.store
                            .commit(read_ts, t.writes, t.conflict)
                            .await
                            .map_err(|e| format!("could not serialize access due to concurrent update: {e}"))?;
                        Ok(ExecOut::Tag("COMMIT".into()))
                    }
                }
            }
            Statement::Rollback { .. } => {
                sess.txn = None;
                Ok(ExecOut::Tag("ROLLBACK".into()))
            }
            Statement::SetVariable { .. }
            | Statement::SetNames { .. }
            | Statement::SetTimeZone { .. } => Ok(ExecOut::Tag("SET".into())),
            Statement::ShowVariable { variable } => {
                let name = variable
                    .iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".")
                    .to_lowercase();
                let value = match name.as_str() {
                    "server_version" => "16.4".to_string(),
                    "server_encoding" | "client_encoding" => "UTF8".to_string(),
                    "transaction_isolation" | "transaction isolation level" => {
                        "repeatable read".to_string()
                    }
                    _ => String::new(),
                };
                Ok(ExecOut::Rows {
                    cols: vec![(name, super::types::OID_TEXT)],
                    rows: vec![vec![Scalar::Text(value)]],
                    tag: "SHOW".into(),
                })
            }
            Statement::CreateIndex { .. } => Ok(ExecOut::Tag("CREATE INDEX".into())),
            other => Err(format!("statement not supported: {other}")),
        }
    }

    // ── DDL ──────────────────────────────────────────────────────────────────

    async fn exec_create_table(
        &self,
        sess: &mut Session,
        name: ObjectName,
        columns: Vec<ast::ColumnDef>,
        if_not_exists: bool,
    ) -> Result<ExecOut, String> {
        let name = object_name_str(&name);
        if if_not_exists && self.catalog.get(&name).is_some() {
            return Ok(ExecOut::Tag("CREATE TABLE".into()));
        }
        let mut cols = Vec::with_capacity(columns.len());
        for c in &columns {
            let serial = matches!(
                c.data_type,
                ast::DataType::Custom(ref n, _) if object_name_str(n).eq_ignore_ascii_case("serial")
                    || object_name_str(n).eq_ignore_ascii_case("bigserial")
            );
            let ctype = if serial { ColType::Int } else { ColType::from_sql(&c.data_type) };
            let mut not_null = false;
            let mut primary_key = false;
            for opt in &c.options {
                match &opt.option {
                    ast::ColumnOption::NotNull => not_null = true,
                    ast::ColumnOption::Unique { is_primary, .. } if *is_primary => {
                        primary_key = true;
                        not_null = true;
                    }
                    _ => {}
                }
            }
            cols.push(CatColumn {
                name: c.name.value.to_lowercase(),
                ctype,
                not_null,
                primary_key,
                serial,
            });
        }
        let (_def, key, blob) = self.catalog.create(&name, cols)?;
        let writes = vec![WriteOp::Put {
            ns: NS_PG_CATALOG,
            key: key.clone(),
            value: Datum::Str(blob),
            expires_at_ms: None,
        }];
        let keys = vec![NsKey::new(NS_PG_CATALOG, key)];
        // DDL commits immediately, even inside a transaction (v1 behavior).
        self.store
            .commit(self.store.current_ts(), writes, keys)
            .await
            .map_err(|e| format!("catalog write failed: {e}"))?;
        let _ = sess;
        Ok(ExecOut::Tag("CREATE TABLE".into()))
    }

    async fn exec_drop(
        &self,
        sess: &mut Session,
        names: Vec<ObjectName>,
        if_exists: bool,
    ) -> Result<ExecOut, String> {
        for n in &names {
            let name = object_name_str(n);
            let Some(def) = self.catalog.drop_table(&name) else {
                if if_exists {
                    continue;
                }
                return Err(format!("table \"{name}\" does not exist"));
            };
            let mut writes = vec![WriteOp::Del {
                ns: NS_PG_CATALOG,
                key: Bytes::from(name.clone().into_bytes()),
            }];
            let ts = self.store.current_ts();
            self.store.for_each_visible(def.ns, ts, |key, _| {
                writes.push(WriteOp::Del { ns: def.ns, key: Bytes::copy_from_slice(key) });
            });
            self.store
                .commit(ts, writes, Vec::new())
                .await
                .map_err(|e| format!("drop failed: {e}"))?;
        }
        let _ = sess;
        Ok(ExecOut::Tag("DROP TABLE".into()))
    }

    // ── DML ──────────────────────────────────────────────────────────────────

    async fn exec_insert(
        &self,
        sess: &mut Session,
        table_name: ObjectName,
        columns: Vec<Ident>,
        source: Option<Box<Query>>,
        returning: Option<Vec<SelectItem>>,
        params: &[Scalar],
    ) -> Result<ExecOut, String> {
        let def = self.resolve_def(&table_name)?;
        let target_cols: Vec<String> = if columns.is_empty() {
            def.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            columns.iter().map(|c| c.value.to_lowercase()).collect()
        };
        for c in &target_cols {
            if def.column(c).is_none() {
                return Err(format!("column \"{c}\" of relation \"{}\" does not exist", def.name));
            }
        }
        let Some(source) = &source else {
            return Err("INSERT requires a VALUES clause or query".into());
        };
        // Evaluate the source rows (VALUES list or arbitrary SELECT).
        let value_rows: Vec<Vec<Scalar>> = match source.body.as_ref() {
            SetExpr::Values(values) => {
                let env = QueryEnv { engine: self, sess, params };
                let mut rows = Vec::with_capacity(values.rows.len());
                for row in &values.rows {
                    let mut out = Vec::with_capacity(row.len());
                    for e in row {
                        out.push(eval(&env, &HashMap::new(), None, e)?);
                    }
                    rows.push(out);
                }
                rows
            }
            _ => {
                let r = self.run_query(sess, source, params)?;
                r.rows
            }
        };

        let mut writes = Vec::new();
        let mut keys = Vec::new();
        let mut inserted: Vec<(u64, RowMap)> = Vec::new();
        for vr in &value_rows {
            if vr.len() != target_cols.len() {
                return Err(format!(
                    "INSERT has {} expressions but {} target columns",
                    vr.len(),
                    target_cols.len()
                ));
            }
            // Validate before consuming a rowid so a rejected insert
            // doesn't burn serial ids.
            let mut row: RowMap = im::HashMap::new();
            for col in &def.columns {
                let supplied = target_cols.iter().position(|c| c == &col.name);
                let val = match supplied {
                    Some(i) => col.ctype.coerce(vr[i].clone()),
                    None => Scalar::Null,
                };
                if val.is_null() && col.not_null && !col.serial {
                    return Err(format!(
                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                        col.name, def.name
                    ));
                }
                row.insert(col.name.clone(), val);
            }
            let rowid = def.alloc_rowid();
            for col in &def.columns {
                if col.serial && row.get(&col.name).map(|v| v.is_null()).unwrap_or(true) {
                    row.insert(col.name.clone(), Scalar::Int(rowid as i64));
                }
            }
            let key = rowid_key(rowid);
            writes.push(WriteOp::Put {
                ns: def.ns,
                key: key.clone(),
                value: Datum::Row(row.clone()),
                expires_at_ms: None,
            });
            keys.push(NsKey::new(def.ns, key));
            inserted.push((rowid, row));
        }

        let n = inserted.len();
        let returning = self.project_returning(sess, &def, &inserted, &returning, params)?;
        self.apply_writes(sess, writes, keys).await?;
        match returning {
            Some(rows) => {
                Ok(ExecOut::Rows { cols: rows.cols, rows: rows.rows, tag: format!("INSERT 0 {n}") })
            }
            None => Ok(ExecOut::Tag(format!("INSERT 0 {n}"))),
        }
    }

    async fn exec_update(
        &self,
        sess: &mut Session,
        table: ast::TableWithJoins,
        assignments: Vec<ast::Assignment>,
        selection: Option<Expr>,
        returning: Option<Vec<SelectItem>>,
        params: &[Scalar],
    ) -> Result<ExecOut, String> {
        let TableFactor::Table { name, .. } = &table.relation else {
            return Err("UPDATE target must be a plain table".into());
        };
        let def = self.resolve_def(name)?;
        let rows = self.scan(sess, &def);
        let mut writes = Vec::new();
        let mut keys = Vec::new();
        let mut updated: Vec<(u64, RowMap)> = Vec::new();
        {
            let env = QueryEnv { engine: self, sess, params };
            for (rowid, row) in rows {
                let ctx = row_ctx(&def.name, &row);
                if let Some(pred) = &selection {
                    if !truthy(&eval(&env, &ctx, None, pred)?) {
                        continue;
                    }
                }
                let mut new_row = row.clone();
                for a in &assignments {
                    let col_name = assignment_col(a)?;
                    let Some(col) = def.column(&col_name) else {
                        return Err(format!(
                            "column \"{col_name}\" of relation \"{}\" does not exist",
                            def.name
                        ));
                    };
                    let v = eval(&env, &ctx, None, &a.value)?;
                    let v = col.ctype.coerce(v);
                    if v.is_null() && col.not_null {
                        return Err(format!(
                            "null value in column \"{}\" violates not-null constraint",
                            col.name
                        ));
                    }
                    new_row.insert(col.name.clone(), v);
                }
                let key = rowid_key(rowid);
                writes.push(WriteOp::Put {
                    ns: def.ns,
                    key: key.clone(),
                    value: Datum::Row(new_row.clone()),
                    expires_at_ms: None,
                });
                keys.push(NsKey::new(def.ns, key));
                updated.push((rowid, new_row));
            }
        }
        let n = updated.len();
        let returning = self.project_returning(sess, &def, &updated, &returning, params)?;
        self.apply_writes(sess, writes, keys).await?;
        match returning {
            Some(rows) => {
                Ok(ExecOut::Rows { cols: rows.cols, rows: rows.rows, tag: format!("UPDATE {n}") })
            }
            None => Ok(ExecOut::Tag(format!("UPDATE {n}"))),
        }
    }

    async fn exec_delete(
        &self,
        sess: &mut Session,
        from: ast::FromTable,
        selection: Option<Expr>,
        returning: Option<Vec<SelectItem>>,
        params: &[Scalar],
    ) -> Result<ExecOut, String> {
        let from = match &from {
            ast::FromTable::WithFromKeyword(f) | ast::FromTable::WithoutKeyword(f) => f,
        };
        let Some(twj) = from.first() else {
            return Err("DELETE requires a table".into());
        };
        let TableFactor::Table { name, .. } = &twj.relation else {
            return Err("DELETE target must be a plain table".into());
        };
        let def = self.resolve_def(name)?;
        let rows = self.scan(sess, &def);
        let mut writes = Vec::new();
        let mut keys = Vec::new();
        let mut deleted: Vec<(u64, RowMap)> = Vec::new();
        {
            let env = QueryEnv { engine: self, sess, params };
            for (rowid, row) in rows {
                let ctx = row_ctx(&def.name, &row);
                if let Some(pred) = &selection {
                    if !truthy(&eval(&env, &ctx, None, pred)?) {
                        continue;
                    }
                }
                let key = rowid_key(rowid);
                writes.push(WriteOp::Del { ns: def.ns, key: key.clone() });
                keys.push(NsKey::new(def.ns, key));
                deleted.push((rowid, row));
            }
        }
        let n = deleted.len();
        let returning = self.project_returning(sess, &def, &deleted, &returning, params)?;
        self.apply_writes(sess, writes, keys).await?;
        match returning {
            Some(rows) => {
                Ok(ExecOut::Rows { cols: rows.cols, rows: rows.rows, tag: format!("DELETE {n}") })
            }
            None => Ok(ExecOut::Tag(format!("DELETE {n}"))),
        }
    }

    fn project_returning(
        &self,
        sess: &Session,
        def: &TableDef,
        rows: &[(u64, RowMap)],
        returning: &Option<Vec<SelectItem>>,
        params: &[Scalar],
    ) -> Result<Option<RowsOut>, String> {
        let Some(items) = returning else {
            return Ok(None);
        };
        let env = QueryEnv { engine: self, sess, params };
        let mut cols: Vec<(String, u32)> = Vec::new();
        let mut out_rows = Vec::with_capacity(rows.len());
        for (idx, (_, row)) in rows.iter().enumerate() {
            let ctx = row_ctx(&def.name, row);
            let mut out = Vec::new();
            for item in items {
                match item {
                    SelectItem::Wildcard(_) => {
                        for c in &def.columns {
                            if idx == 0 {
                                cols.push((c.name.clone(), c.ctype.oid()));
                            }
                            out.push(row.get(&c.name).cloned().unwrap_or(Scalar::Null));
                        }
                    }
                    SelectItem::UnnamedExpr(e) => {
                        let v = eval(&env, &ctx, None, e)?;
                        if idx == 0 {
                            cols.push((expr_label(e), scalar_oid(&v)));
                        }
                        out.push(v);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let v = eval(&env, &ctx, None, expr)?;
                        if idx == 0 {
                            cols.push((alias.value.clone(), scalar_oid(&v)));
                        }
                        out.push(v);
                    }
                    SelectItem::QualifiedWildcard(..) => {
                        return Err("qualified wildcard in RETURNING is not supported".into())
                    }
                }
            }
            out_rows.push(out);
        }
        if rows.is_empty() {
            // Still produce a column header.
            for item in items {
                match item {
                    SelectItem::Wildcard(_) => {
                        for c in &def.columns {
                            cols.push((c.name.clone(), c.ctype.oid()));
                        }
                    }
                    SelectItem::UnnamedExpr(e) => cols.push((expr_label(e), super::types::OID_TEXT)),
                    SelectItem::ExprWithAlias { alias, .. } => {
                        cols.push((alias.value.clone(), super::types::OID_TEXT))
                    }
                    _ => {}
                }
            }
        }
        Ok(Some(RowsOut { cols, rows: out_rows }))
    }

    // ── Write plumbing ───────────────────────────────────────────────────────

    /// Stage writes into the open transaction, or commit immediately.
    async fn apply_writes(
        &self,
        sess: &mut Session,
        writes: Vec<WriteOp>,
        keys: Vec<NsKey>,
    ) -> Result<(), String> {
        if writes.is_empty() {
            return Ok(());
        }
        match sess.txn.as_mut() {
            Some(t) => {
                for w in &writes {
                    match w {
                        WriteOp::Put { ns, key, value: Datum::Row(r), .. } => {
                            t.overlay.insert((*ns, key.clone()), Some(r.clone()));
                        }
                        WriteOp::Put { ns, key, .. } => {
                            t.overlay.insert((*ns, key.clone()), None);
                        }
                        WriteOp::Del { ns, key } => {
                            t.overlay.insert((*ns, key.clone()), None);
                        }
                    }
                }
                t.writes.extend(writes);
                t.conflict.extend(keys);
                Ok(())
            }
            None => self
                .store
                .commit(self.store.current_ts(), writes, keys)
                .await
                .map(|_| ())
                .map_err(|e| format!("could not serialize access due to concurrent update: {e}")),
        }
    }

    fn snapshot_ts(&self, sess: &Session) -> u64 {
        sess.txn.as_ref().map(|t| t.snap.ts).unwrap_or_else(|| self.store.current_ts())
    }

    /// All visible rows of a table at the session snapshot, with txn overlay.
    fn scan(&self, sess: &Session, def: &TableDef) -> Vec<(u64, RowMap)> {
        let ts = self.snapshot_ts(sess);
        let mut rows: Vec<(u64, RowMap)> = Vec::new();
        self.store.for_each_visible(def.ns, ts, |key, datum| {
            if let Datum::Row(r) = datum {
                if key.len() == 8 {
                    let mut be = [0u8; 8];
                    be.copy_from_slice(key);
                    rows.push((u64::from_be_bytes(be), r.clone()));
                }
            }
        });
        if let Some(t) = &sess.txn {
            // Remove rows the txn replaced or deleted, then add staged versions.
            rows.retain(|(rowid, _)| !t.overlay.contains_key(&(def.ns, rowid_key(*rowid))));
            for ((ns, key), staged) in &t.overlay {
                if *ns == def.ns && key.len() == 8 {
                    if let Some(r) = staged {
                        let mut be = [0u8; 8];
                        be.copy_from_slice(key);
                        rows.push((u64::from_be_bytes(be), r.clone()));
                    }
                }
            }
            rows.sort_by_key(|(id, _)| *id);
        }
        rows
    }

    fn resolve_def(&self, name: &ObjectName) -> Result<Arc<TableDef>, String> {
        let n = object_name_str(name);
        self.catalog
            .get(&n)
            .ok_or_else(|| format!("relation \"{n}\" does not exist"))
    }

    // ── SELECT ───────────────────────────────────────────────────────────────

    fn run_query(
        &self,
        sess: &Session,
        q: &Query,
        params: &[Scalar],
    ) -> Result<RowsOut, String> {
        let select = match q.body.as_ref() {
            SetExpr::Select(s) => s,
            SetExpr::Values(values) => {
                // bare VALUES (...) — used by some drivers.
                let env = QueryEnv { engine: self, sess, params };
                let mut rows = Vec::new();
                for r in &values.rows {
                    let mut out = Vec::new();
                    for e in r {
                        out.push(eval(&env, &HashMap::new(), None, e)?);
                    }
                    rows.push(out);
                }
                let ncols = rows.first().map(|r| r.len()).unwrap_or(0);
                let cols = (1..=ncols)
                    .map(|i| (format!("column{i}"), super::types::OID_TEXT))
                    .collect();
                return Ok(RowsOut { cols, rows });
            }
            other => return Err(format!("query form not supported: {other}")),
        };

        let env = QueryEnv { engine: self, sess, params };
        let base = self.resolve_from(sess, select, params)?;

        // WHERE
        let mut filtered: Vec<RowCtx> = Vec::with_capacity(base.rows.len());
        for ctx in base.rows {
            let keep = match &select.selection {
                Some(pred) => truthy(&eval(&env, &ctx, None, pred)?),
                None => true,
            };
            if keep {
                filtered.push(ctx);
            }
        }

        // GROUP BY / aggregates
        let group_exprs: Vec<Expr> = match &select.group_by {
            GroupByExpr::Expressions(exprs) => exprs.clone(),
            GroupByExpr::All => Vec::new(),
        };
        let has_aggs = select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                contains_aggregate(e)
            }
            _ => false,
        }) || select.having.is_some();

        let mut cols: Vec<(String, u32)> = Vec::new();
        let mut out_rows: Vec<Vec<Scalar>> = Vec::new();

        if has_aggs || !group_exprs.is_empty() {
            // Group rows.
            let mut groups: Vec<(Vec<Scalar>, Vec<RowCtx>)> = Vec::new();
            for ctx in filtered {
                let mut key = Vec::with_capacity(group_exprs.len());
                for ge in &group_exprs {
                    key.push(eval(&env, &ctx, None, ge)?);
                }
                match groups.iter_mut().find(|(k, _)| scalars_eq(k, &key)) {
                    Some((_, rows)) => rows.push(ctx),
                    None => groups.push((key, vec![ctx])),
                }
            }
            if groups.is_empty() && group_exprs.is_empty() {
                groups.push((Vec::new(), Vec::new())); // aggregates over empty input
            }
            for (gi, (_, grows)) in groups.iter().enumerate() {
                let rep = grows.first().cloned().unwrap_or_default();
                if let Some(having) = &select.having {
                    if !truthy(&eval(&env, &rep, Some(grows), having)?) {
                        continue;
                    }
                }
                let mut out = Vec::new();
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(e) => {
                            let v = eval(&env, &rep, Some(grows), e)?;
                            if (gi == 0 || cols.is_empty())
                                && out.len() >= cols.len() {
                                    cols.push((expr_label(e), scalar_oid(&v)));
                                }
                            out.push(v);
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            let v = eval(&env, &rep, Some(grows), expr)?;
                            if out.len() >= cols.len() {
                                cols.push((alias.value.clone(), scalar_oid(&v)));
                            }
                            out.push(v);
                        }
                        _ => return Err("wildcard with GROUP BY/aggregates is not supported".into()),
                    }
                }
                out_rows.push(out);
            }
        } else {
            // Plain projection: project every row, keeping its source context
            // so ORDER BY can reference both output aliases and table columns.
            let mut projected: Vec<(RowCtx, Vec<Scalar>)> = Vec::with_capacity(filtered.len());
            for (ri, ctx) in filtered.into_iter().enumerate() {
                let mut out = Vec::new();
                for item in &select.projection {
                    match item {
                        SelectItem::Wildcard(_) => {
                            for (qual, bare) in &base.col_order {
                                if ri == 0 {
                                    let v = ctx.get(qual).cloned().unwrap_or(Scalar::Null);
                                    cols.push((bare.clone(), scalar_oid(&v)));
                                }
                                out.push(ctx.get(qual).cloned().unwrap_or(Scalar::Null));
                            }
                        }
                        SelectItem::QualifiedWildcard(prefix, _) => {
                            let p = prefix.to_string().to_lowercase();
                            for (qual, bare) in &base.col_order {
                                if qual.starts_with(&format!("{p}.")) {
                                    if ri == 0 {
                                        let v = ctx.get(qual).cloned().unwrap_or(Scalar::Null);
                                        cols.push((bare.clone(), scalar_oid(&v)));
                                    }
                                    out.push(ctx.get(qual).cloned().unwrap_or(Scalar::Null));
                                }
                            }
                        }
                        SelectItem::UnnamedExpr(e) => {
                            let v = eval(&env, &ctx, None, e)?;
                            if ri == 0 {
                                cols.push((expr_label(e), scalar_oid(&v)));
                            }
                            out.push(v);
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            let v = eval(&env, &ctx, None, expr)?;
                            if ri == 0 {
                                cols.push((alias.value.clone(), scalar_oid(&v)));
                            }
                            out.push(v);
                        }
                    }
                }
                projected.push((ctx, out));
            }
            if projected.is_empty() {
                // No rows: still validate the projection and emit headers by
                // evaluating against an all-NULL row of the base relation.
                let mut null_ctx: RowCtx = HashMap::new();
                for (qual, bare) in &base.col_order {
                    null_ctx.insert(qual.clone(), Scalar::Null);
                    null_ctx.insert(bare.clone(), Scalar::Null);
                }
                for item in &select.projection {
                    match item {
                        SelectItem::Wildcard(_) => {
                            for (_, bare) in &base.col_order {
                                cols.push((bare.clone(), super::types::OID_TEXT));
                            }
                        }
                        SelectItem::UnnamedExpr(e) => {
                            let v = eval(&env, &null_ctx, None, e)?;
                            cols.push((expr_label(e), scalar_oid(&v)));
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            let v = eval(&env, &null_ctx, None, expr)?;
                            cols.push((alias.value.clone(), scalar_oid(&v)));
                        }
                        SelectItem::QualifiedWildcard(prefix, _) => {
                            let p = prefix.to_string().to_lowercase();
                            for (qual, bare) in &base.col_order {
                                if qual.starts_with(&format!("{p}.")) {
                                    cols.push((bare.clone(), super::types::OID_TEXT));
                                }
                            }
                        }
                    }
                }
            }
            // ORDER BY: output aliases win, then any expression over the row.
            if !q.order_by.is_empty() {
                let mut sort_keys: Vec<Vec<(Scalar, bool)>> = Vec::with_capacity(projected.len());
                for (ctx, out) in &projected {
                    let mut keys = Vec::with_capacity(q.order_by.len());
                    for ob in &q.order_by {
                        let asc = ob.asc.unwrap_or(true);
                        let v = match &ob.expr {
                            Expr::Identifier(id) => {
                                let n = id.value.to_lowercase();
                                match cols.iter().position(|(c, _)| c.eq_ignore_ascii_case(&n)) {
                                    Some(idx) => out[idx].clone(),
                                    None => eval(&env, ctx, None, &ob.expr)?,
                                }
                            }
                            Expr::Value(Value::Number(n, _)) => {
                                let idx: usize =
                                    n.parse().map_err(|_| "bad ORDER BY position")?;
                                if idx == 0 || idx > out.len() {
                                    return Err("ORDER BY position out of range".into());
                                }
                                out[idx - 1].clone()
                            }
                            e => eval(&env, ctx, None, e)?,
                        };
                        keys.push((v, asc));
                    }
                    sort_keys.push(keys);
                }
                let mut order: Vec<usize> = (0..projected.len()).collect();
                order.sort_by(|&a, &b| {
                    for ((va, asc), (vb, _)) in sort_keys[a].iter().zip(&sort_keys[b]) {
                        let ord = scalar_cmp(va, vb);
                        let ord = if *asc { ord } else { ord.reverse() };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                out_rows = order.into_iter().map(|i| projected[i].1.clone()).collect();
            } else {
                out_rows = projected.into_iter().map(|(_, out)| out).collect();
            }
        }

        // ORDER BY for grouped output: sort by matching output column names.
        if (has_aggs || !group_exprs.is_empty()) && !q.order_by.is_empty() {
            sort_output(&cols, &mut out_rows, &q.order_by)?;
        }

        // DISTINCT
        if select.distinct.is_some() {
            let mut seen: Vec<Vec<Scalar>> = Vec::new();
            out_rows.retain(|r| {
                if seen.iter().any(|s| scalars_eq(s, r)) {
                    false
                } else {
                    seen.push(r.clone());
                    true
                }
            });
        }

        // OFFSET / LIMIT
        if let Some(off) = &q.offset {
            let n = match eval(&env, &HashMap::new(), None, &off.value)? {
                Scalar::Int(i) => i.max(0) as usize,
                _ => 0,
            };
            if n < out_rows.len() {
                out_rows.drain(..n);
            } else {
                out_rows.clear();
            }
        }
        if let Some(lim) = &q.limit {
            if let Scalar::Int(n) = eval(&env, &HashMap::new(), None, lim)? {
                out_rows.truncate(n.max(0) as usize);
            }
        }

        Ok(RowsOut { cols, rows: out_rows })
    }

    /// Resolve FROM (0 or 1 relation + INNER/LEFT joins) into a row set.
    fn resolve_from(
        &self,
        sess: &Session,
        select: &Select,
        params: &[Scalar],
    ) -> Result<RowSet, String> {
        if select.from.is_empty() {
            return Ok(RowSet { col_order: Vec::new(), rows: vec![HashMap::new()] });
        }
        if select.from.len() > 1 {
            return Err("comma joins are not supported — use explicit JOIN".into());
        }
        let twj = &select.from[0];
        let mut current = self.relation_rows(sess, &twj.relation)?;

        let env = QueryEnv { engine: self, sess, params };
        for join in &twj.joins {
            let right = self.relation_rows(sess, &join.relation)?;
            let (constraint, left_outer) = match &join.join_operator {
                JoinOperator::Inner(c) => (c, false),
                JoinOperator::LeftOuter(c) => (c, true),
                other => return Err(format!("join type not supported: {other:?}")),
            };
            let on = match constraint {
                JoinConstraint::On(e) => Some(e),
                JoinConstraint::None => None,
                _ => return Err("only JOIN ... ON is supported".into()),
            };
            let mut joined_rows = Vec::new();
            for l in &current.rows {
                let mut matched = false;
                for r in &right.rows {
                    let mut merged = l.clone();
                    for (k, v) in r {
                        merged.insert(k.clone(), v.clone());
                    }
                    let ok = match on {
                        Some(pred) => truthy(&eval(&env, &merged, None, pred)?),
                        None => true,
                    };
                    if ok {
                        matched = true;
                        joined_rows.push(merged);
                    }
                }
                if left_outer && !matched {
                    let mut merged = l.clone();
                    for (qual, bare) in &right.col_order {
                        merged.insert(qual.clone(), Scalar::Null);
                        merged.entry(bare.clone()).or_insert(Scalar::Null);
                    }
                    joined_rows.push(merged);
                }
            }
            let mut col_order = current.col_order;
            col_order.extend(right.col_order);
            current = RowSet { col_order, rows: joined_rows };
        }
        Ok(current)
    }

    fn relation_rows(&self, sess: &Session, factor: &TableFactor) -> Result<RowSet, String> {
        let TableFactor::Table { name, alias, .. } = factor else {
            return Err("subquery FROM items are not supported".into());
        };
        let full = object_name_str(name);
        let alias_name = alias
            .as_ref()
            .map(|a| a.name.value.to_lowercase())
            .unwrap_or_else(|| full.rsplit('.').next().unwrap_or(&full).to_string());

        // Virtual catalog tables.
        if let Some(rs) = self.virtual_table(&full, &alias_name) {
            return Ok(rs);
        }

        let def = self.resolve_def(name)?;
        let rows = self.scan(sess, &def);
        let col_order: Vec<(String, String)> = def
            .columns
            .iter()
            .map(|c| (format!("{alias_name}.{}", c.name), c.name.clone()))
            .collect();
        let mut out = Vec::with_capacity(rows.len());
        for (_, r) in rows {
            let mut ctx: RowCtx = HashMap::with_capacity(r.len() * 2);
            for c in &def.columns {
                let v = r.get(&c.name).cloned().unwrap_or(Scalar::Null);
                ctx.insert(format!("{alias_name}.{}", c.name), v.clone());
                ctx.insert(c.name.clone(), v);
            }
            out.push(ctx);
        }
        Ok(RowSet { col_order, rows: out })
    }

    /// information_schema / pg_catalog shims so SQL tools can introspect.
    fn virtual_table(&self, full: &str, alias: &str) -> Option<RowSet> {
        let make = |cols: Vec<(&str, Vec<Scalar>)>| -> RowSet {
            // cols: (name, per-row values) — all the same length.
            let nrows = cols.first().map(|(_, v)| v.len()).unwrap_or(0);
            let col_order: Vec<(String, String)> = cols
                .iter()
                .map(|(n, _)| (format!("{alias}.{n}"), n.to_string()))
                .collect();
            let mut rows = Vec::with_capacity(nrows);
            for i in 0..nrows {
                let mut ctx: RowCtx = HashMap::new();
                for (n, vals) in &cols {
                    ctx.insert(format!("{alias}.{n}"), vals[i].clone());
                    ctx.insert(n.to_string(), vals[i].clone());
                }
                rows.push(ctx);
            }
            RowSet { col_order, rows }
        };
        let t = |s: &str| Scalar::Text(s.to_string());

        match full {
            "information_schema.tables" => {
                let tables = self.catalog.list();
                Some(make(vec![
                    ("table_catalog", tables.iter().map(|_| t("voltra")).collect()),
                    ("table_schema", tables.iter().map(|_| t("public")).collect()),
                    ("table_name", tables.iter().map(|d| t(&d.name)).collect()),
                    ("table_type", tables.iter().map(|_| t("BASE TABLE")).collect()),
                ]))
            }
            "information_schema.columns" => {
                let mut tn = Vec::new();
                let mut cn = Vec::new();
                let mut dt = Vec::new();
                let mut pos = Vec::new();
                let mut nullable = Vec::new();
                for d in self.catalog.list() {
                    for (i, c) in d.columns.iter().enumerate() {
                        tn.push(t(&d.name));
                        cn.push(t(&c.name));
                        dt.push(t(match c.ctype {
                            ColType::Bool => "boolean",
                            ColType::Int => "bigint",
                            ColType::Float => "double precision",
                            ColType::Text => "text",
                        }));
                        pos.push(Scalar::Int(i as i64 + 1));
                        nullable.push(t(if c.not_null { "NO" } else { "YES" }));
                    }
                }
                Some(make(vec![
                    ("table_name", tn),
                    ("column_name", cn),
                    ("data_type", dt),
                    ("ordinal_position", pos),
                    ("is_nullable", nullable),
                ]))
            }
            "pg_catalog.pg_tables" | "pg_tables" => {
                let tables = self.catalog.list();
                Some(make(vec![
                    ("schemaname", tables.iter().map(|_| t("public")).collect()),
                    ("tablename", tables.iter().map(|d| t(&d.name)).collect()),
                    ("tableowner", tables.iter().map(|_| t("voltra")).collect()),
                ]))
            }
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Expression evaluation
// ─────────────────────────────────────────────────────────────────────────────

struct QueryEnv<'a> {
    engine: &'a PgEngine,
    sess: &'a Session,
    params: &'a [Scalar],
}

fn row_ctx(table: &str, row: &RowMap) -> RowCtx {
    let mut ctx = HashMap::with_capacity(row.len() * 2);
    for (k, v) in row {
        ctx.insert(format!("{table}.{k}"), v.clone());
        ctx.insert(k.clone(), v.clone());
    }
    ctx
}

fn truthy(v: &Scalar) -> bool {
    match v {
        Scalar::Bool(b) => *b,
        Scalar::Int(i) => *i != 0,
        Scalar::Float(f) => *f != 0.0,
        Scalar::Null => false,
        Scalar::Text(t) => !t.is_empty(),
    }
}

fn scalars_eq(a: &[Scalar], b: &[Scalar]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| scalar_cmp(x, y) == std::cmp::Ordering::Equal)
}

/// Total order for sorting/equality: NULLs sort last, numerics unify.
pub fn scalar_cmp(a: &Scalar, b: &Scalar) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (Scalar::Null, Scalar::Null) => Equal,
        (Scalar::Null, _) => Greater,
        (_, Scalar::Null) => Less,
        (Scalar::Bool(x), Scalar::Bool(y)) => x.cmp(y),
        (Scalar::Text(x), Scalar::Text(y)) => x.cmp(y),
        _ => {
            let xf = scalar_num(a);
            let yf = scalar_num(b);
            match (xf, yf) {
                (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(Equal),
                _ => scalar_to_text(a)
                    .unwrap_or_default()
                    .cmp(&scalar_to_text(b).unwrap_or_default()),
            }
        }
    }
}

fn scalar_num(v: &Scalar) -> Option<f64> {
    match v {
        Scalar::Int(i) => Some(*i as f64),
        Scalar::Float(f) => Some(*f),
        Scalar::Bool(b) => Some(*b as i64 as f64),
        Scalar::Text(t) => t.trim().parse().ok(),
        Scalar::Null => None,
    }
}

fn eval(
    env: &QueryEnv,
    ctx: &RowCtx,
    group: Option<&[RowCtx]>,
    expr: &Expr,
) -> Result<Scalar, String> {
    match expr {
        Expr::Identifier(id) => {
            let key = id.value.to_lowercase();
            ctx.get(&key)
                .cloned()
                .ok_or_else(|| format!("column \"{key}\" does not exist"))
        }
        Expr::CompoundIdentifier(parts) => {
            let key = parts
                .iter()
                .map(|p| p.value.to_lowercase())
                .collect::<Vec<_>>()
                .join(".");
            ctx.get(&key)
                .cloned()
                .ok_or_else(|| format!("column \"{key}\" does not exist"))
        }
        Expr::Value(v) => literal(env, v),
        Expr::Nested(e) => eval(env, ctx, group, e),
        Expr::UnaryOp { op, expr } => {
            let v = eval(env, ctx, group, expr)?;
            match op {
                UnaryOperator::Not => Ok(Scalar::Bool(!truthy(&v))),
                UnaryOperator::Minus => match v {
                    Scalar::Int(i) => Ok(Scalar::Int(-i)),
                    Scalar::Float(f) => Ok(Scalar::Float(-f)),
                    Scalar::Null => Ok(Scalar::Null),
                    other => Err(format!("cannot negate {other:?}")),
                },
                UnaryOperator::Plus => Ok(v),
                other => Err(format!("unary operator not supported: {other}")),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            // Short-circuit logic ops.
            match op {
                BinaryOperator::And => {
                    let l = eval(env, ctx, group, left)?;
                    if !truthy(&l) {
                        return Ok(Scalar::Bool(false));
                    }
                    let r = eval(env, ctx, group, right)?;
                    return Ok(Scalar::Bool(truthy(&r)));
                }
                BinaryOperator::Or => {
                    let l = eval(env, ctx, group, left)?;
                    if truthy(&l) {
                        return Ok(Scalar::Bool(true));
                    }
                    let r = eval(env, ctx, group, right)?;
                    return Ok(Scalar::Bool(truthy(&r)));
                }
                _ => {}
            }
            let l = eval(env, ctx, group, left)?;
            let r = eval(env, ctx, group, right)?;
            binop(op, l, r)
        }
        Expr::IsNull(e) => Ok(Scalar::Bool(eval(env, ctx, group, e)?.is_null())),
        Expr::IsNotNull(e) => Ok(Scalar::Bool(!eval(env, ctx, group, e)?.is_null())),
        Expr::IsTrue(e) => Ok(Scalar::Bool(truthy(&eval(env, ctx, group, e)?))),
        Expr::IsFalse(e) => Ok(Scalar::Bool(!truthy(&eval(env, ctx, group, e)?))),
        Expr::IsNotTrue(e) => Ok(Scalar::Bool(!truthy(&eval(env, ctx, group, e)?))),
        Expr::IsNotFalse(e) => Ok(Scalar::Bool(truthy(&eval(env, ctx, group, e)?))),
        Expr::InList { expr, list, negated } => {
            let v = eval(env, ctx, group, expr)?;
            let mut found = false;
            for item in list {
                let iv = eval(env, ctx, group, item)?;
                if scalar_cmp(&v, &iv) == std::cmp::Ordering::Equal && !v.is_null() {
                    found = true;
                    break;
                }
            }
            Ok(Scalar::Bool(found != *negated))
        }
        Expr::InSubquery { expr, subquery, negated } => {
            let v = eval(env, ctx, group, expr)?;
            let rs = env.engine.run_query(env.sess, subquery, env.params)?;
            let found = rs.rows.iter().any(|r| {
                r.first()
                    .map(|x| scalar_cmp(&v, x) == std::cmp::Ordering::Equal && !v.is_null())
                    .unwrap_or(false)
            });
            Ok(Scalar::Bool(found != *negated))
        }
        Expr::Subquery(q) => {
            let rs = env.engine.run_query(env.sess, q, env.params)?;
            Ok(rs
                .rows
                .first()
                .and_then(|r| r.first().cloned())
                .unwrap_or(Scalar::Null))
        }
        Expr::Exists { subquery, negated } => {
            let rs = env.engine.run_query(env.sess, subquery, env.params)?;
            Ok(Scalar::Bool(rs.rows.is_empty() == *negated))
        }
        Expr::Between { expr, negated, low, high } => {
            let v = eval(env, ctx, group, expr)?;
            let lo = eval(env, ctx, group, low)?;
            let hi = eval(env, ctx, group, high)?;
            let inside = scalar_cmp(&v, &lo) != std::cmp::Ordering::Less
                && scalar_cmp(&v, &hi) != std::cmp::Ordering::Greater
                && !v.is_null();
            Ok(Scalar::Bool(inside != *negated))
        }
        Expr::Like { negated, expr, pattern, .. } => {
            let v = eval(env, ctx, group, expr)?;
            let p = eval(env, ctx, group, pattern)?;
            let m = like_match(
                &scalar_to_text(&v).unwrap_or_default(),
                &scalar_to_text(&p).unwrap_or_default(),
                false,
            );
            Ok(Scalar::Bool(m != *negated))
        }
        Expr::ILike { negated, expr, pattern, .. } => {
            let v = eval(env, ctx, group, expr)?;
            let p = eval(env, ctx, group, pattern)?;
            let m = like_match(
                &scalar_to_text(&v).unwrap_or_default(),
                &scalar_to_text(&p).unwrap_or_default(),
                true,
            );
            Ok(Scalar::Bool(m != *negated))
        }
        Expr::Cast { expr, data_type, .. } => {
            let v = eval(env, ctx, group, expr)?;
            Ok(ColType::from_sql(data_type).coerce(v))
        }
        Expr::Case { operand, conditions, results, else_result } => {
            let base = match operand {
                Some(o) => Some(eval(env, ctx, group, o)?),
                None => None,
            };
            for (cond, res) in conditions.iter().zip(results) {
                let matched = match &base {
                    Some(b) => {
                        let cv = eval(env, ctx, group, cond)?;
                        scalar_cmp(b, &cv) == std::cmp::Ordering::Equal
                    }
                    None => truthy(&eval(env, ctx, group, cond)?),
                };
                if matched {
                    return eval(env, ctx, group, res);
                }
            }
            match else_result {
                Some(e) => eval(env, ctx, group, e),
                None => Ok(Scalar::Null),
            }
        }
        Expr::Substring { expr, substring_from, substring_for, .. } => {
            let s = scalar_to_text(&eval(env, ctx, group, expr)?).unwrap_or_default();
            let from = match substring_from {
                Some(f) => match eval(env, ctx, group, f)? {
                    Scalar::Int(i) => (i.max(1) - 1) as usize,
                    _ => 0,
                },
                None => 0,
            };
            let len = match substring_for {
                Some(f) => match eval(env, ctx, group, f)? {
                    Scalar::Int(i) => i.max(0) as usize,
                    _ => usize::MAX,
                },
                None => usize::MAX,
            };
            let chars: Vec<char> = s.chars().collect();
            let out: String = chars.iter().skip(from).take(len).collect();
            Ok(Scalar::Text(out))
        }
        Expr::Trim { expr, .. } => {
            let s = scalar_to_text(&eval(env, ctx, group, expr)?).unwrap_or_default();
            Ok(Scalar::Text(s.trim().to_string()))
        }
        Expr::Function(f) => eval_function(env, ctx, group, f),
        Expr::Tuple(items) if items.len() == 1 => eval(env, ctx, group, &items[0]),
        other => Err(format!("expression not supported: {other}")),
    }
}

fn literal(env: &QueryEnv, v: &Value) -> Result<Scalar, String> {
    match v {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Scalar::Int(i))
            } else {
                n.parse::<f64>()
                    .map(Scalar::Float)
                    .map_err(|_| format!("invalid number literal: {n}"))
            }
        }
        Value::SingleQuotedString(s) | Value::DollarQuotedString(ast::DollarQuotedString { value: s, .. }) => {
            Ok(Scalar::Text(s.clone()))
        }
        Value::EscapedStringLiteral(s) => Ok(Scalar::Text(s.clone())),
        Value::Boolean(b) => Ok(Scalar::Bool(*b)),
        Value::Null => Ok(Scalar::Null),
        Value::Placeholder(p) => {
            let idx: usize = p
                .trim_start_matches('$')
                .parse()
                .map_err(|_| format!("invalid parameter reference: {p}"))?;
            env.params
                .get(idx.saturating_sub(1))
                .cloned()
                .ok_or_else(|| format!("parameter {p} not bound"))
        }
        other => Err(format!("literal not supported: {other}")),
    }
}

fn binop(op: &BinaryOperator, l: Scalar, r: Scalar) -> Result<Scalar, String> {
    use std::cmp::Ordering::*;
    match op {
        BinaryOperator::Eq => Ok(Scalar::Bool(
            !l.is_null() && !r.is_null() && scalar_cmp(&l, &r) == Equal,
        )),
        BinaryOperator::NotEq => Ok(Scalar::Bool(
            !l.is_null() && !r.is_null() && scalar_cmp(&l, &r) != Equal,
        )),
        BinaryOperator::Lt => Ok(Scalar::Bool(!l.is_null() && !r.is_null() && scalar_cmp(&l, &r) == Less)),
        BinaryOperator::LtEq => {
            Ok(Scalar::Bool(!l.is_null() && !r.is_null() && scalar_cmp(&l, &r) != Greater))
        }
        BinaryOperator::Gt => {
            Ok(Scalar::Bool(!l.is_null() && !r.is_null() && scalar_cmp(&l, &r) == Greater))
        }
        BinaryOperator::GtEq => {
            Ok(Scalar::Bool(!l.is_null() && !r.is_null() && scalar_cmp(&l, &r) != Less))
        }
        BinaryOperator::StringConcat => {
            if l.is_null() || r.is_null() {
                return Ok(Scalar::Null);
            }
            Ok(Scalar::Text(format!(
                "{}{}",
                scalar_to_text(&l).unwrap_or_default(),
                scalar_to_text(&r).unwrap_or_default()
            )))
        }
        BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply
        | BinaryOperator::Divide | BinaryOperator::Modulo => {
            if l.is_null() || r.is_null() {
                return Ok(Scalar::Null);
            }
            // Integer arithmetic stays integral (except /0 guards).
            if let (Scalar::Int(a), Scalar::Int(b)) = (&l, &r) {
                return match op {
                    BinaryOperator::Plus => Ok(Scalar::Int(a.wrapping_add(*b))),
                    BinaryOperator::Minus => Ok(Scalar::Int(a.wrapping_sub(*b))),
                    BinaryOperator::Multiply => Ok(Scalar::Int(a.wrapping_mul(*b))),
                    BinaryOperator::Divide => {
                        if *b == 0 {
                            Err("division by zero".into())
                        } else {
                            Ok(Scalar::Int(a / b))
                        }
                    }
                    _ => {
                        if *b == 0 {
                            Err("division by zero".into())
                        } else {
                            Ok(Scalar::Int(a % b))
                        }
                    }
                };
            }
            let (Some(a), Some(b)) = (scalar_num(&l), scalar_num(&r)) else {
                return Err("operator requires numeric operands".into());
            };
            match op {
                BinaryOperator::Plus => Ok(Scalar::Float(a + b)),
                BinaryOperator::Minus => Ok(Scalar::Float(a - b)),
                BinaryOperator::Multiply => Ok(Scalar::Float(a * b)),
                BinaryOperator::Divide => {
                    if b == 0.0 {
                        Err("division by zero".into())
                    } else {
                        Ok(Scalar::Float(a / b))
                    }
                }
                _ => Ok(Scalar::Float(a % b)),
            }
        }
        other => Err(format!("operator not supported: {other}")),
    }
}

fn contains_aggregate(e: &Expr) -> bool {
    match e {
        Expr::Function(f) => {
            let name = object_name_str(&f.name).to_uppercase();
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
                || f.args.iter().any(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => contains_aggregate(e),
                    _ => false,
                })
        }
        Expr::BinaryOp { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Cast { expr, .. } => {
            contains_aggregate(expr)
        }
        _ => false,
    }
}

fn eval_function(
    env: &QueryEnv,
    ctx: &RowCtx,
    group: Option<&[RowCtx]>,
    f: &ast::Function,
) -> Result<Scalar, String> {
    let name = object_name_str(&f.name).to_uppercase();

    // Collect plain expression args.
    let mut args: Vec<&Expr> = Vec::new();
    let mut has_wildcard = false;
    for a in &f.args {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => args.push(e),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => has_wildcard = true,
            _ => {}
        }
    }

    // Aggregates need the group context.
    if matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
        let rows: &[RowCtx] = match group {
            Some(g) => g,
            None => std::slice::from_ref(ctx),
        };
        return match name.as_str() {
            "COUNT" => {
                if has_wildcard || args.is_empty() {
                    Ok(Scalar::Int(rows.len() as i64))
                } else {
                    let mut n = 0i64;
                    for r in rows {
                        if !eval(env, r, None, args[0])?.is_null() {
                            n += 1;
                        }
                    }
                    Ok(Scalar::Int(n))
                }
            }
            "SUM" | "AVG" => {
                let mut sum = 0.0f64;
                let mut count = 0u64;
                let mut all_int = true;
                for r in rows {
                    let v = eval(env, r, None, args.first().ok_or("aggregate needs an argument")?)?;
                    if v.is_null() {
                        continue;
                    }
                    if !matches!(v, Scalar::Int(_)) {
                        all_int = false;
                    }
                    sum += scalar_num(&v).ok_or("aggregate over non-numeric value")?;
                    count += 1;
                }
                if count == 0 {
                    return Ok(Scalar::Null);
                }
                if name == "AVG" {
                    Ok(Scalar::Float(sum / count as f64))
                } else if all_int {
                    Ok(Scalar::Int(sum as i64))
                } else {
                    Ok(Scalar::Float(sum))
                }
            }
            _ => {
                let mut best: Option<Scalar> = None;
                for r in rows {
                    let v = eval(env, r, None, args.first().ok_or("aggregate needs an argument")?)?;
                    if v.is_null() {
                        continue;
                    }
                    best = Some(match best {
                        None => v,
                        Some(b) => {
                            let keep_new = if name == "MIN" {
                                scalar_cmp(&v, &b) == std::cmp::Ordering::Less
                            } else {
                                scalar_cmp(&v, &b) == std::cmp::Ordering::Greater
                            };
                            if keep_new {
                                v
                            } else {
                                b
                            }
                        }
                    });
                }
                Ok(best.unwrap_or(Scalar::Null))
            }
        };
    }

    // Scalar functions.
    let mut vals = Vec::with_capacity(args.len());
    for a in &args {
        vals.push(eval(env, ctx, group, a)?);
    }
    let text0 = || scalar_to_text(vals.first().unwrap_or(&Scalar::Null)).unwrap_or_default();
    match name.as_str() {
        "LOWER" => Ok(Scalar::Text(text0().to_lowercase())),
        "UPPER" => Ok(Scalar::Text(text0().to_uppercase())),
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            match vals.first() {
                Some(Scalar::Null) | None => Ok(Scalar::Null),
                _ => Ok(Scalar::Int(text0().chars().count() as i64)),
            }
        }
        "CONCAT" => {
            let mut s = String::new();
            for v in &vals {
                if let Some(t) = scalar_to_text(v) {
                    s.push_str(&t);
                }
            }
            Ok(Scalar::Text(s))
        }
        "COALESCE" => Ok(vals.into_iter().find(|v| !v.is_null()).unwrap_or(Scalar::Null)),
        "NULLIF" => {
            if vals.len() == 2 && scalar_cmp(&vals[0], &vals[1]) == std::cmp::Ordering::Equal {
                Ok(Scalar::Null)
            } else {
                Ok(vals.into_iter().next().unwrap_or(Scalar::Null))
            }
        }
        "GREATEST" => Ok(vals
            .into_iter()
            .filter(|v| !v.is_null())
            .max_by(scalar_cmp)
            .unwrap_or(Scalar::Null)),
        "LEAST" => Ok(vals
            .into_iter()
            .filter(|v| !v.is_null())
            .min_by(scalar_cmp)
            .unwrap_or(Scalar::Null)),
        "ABS" => match vals.first() {
            Some(Scalar::Int(i)) => Ok(Scalar::Int(i.abs())),
            Some(Scalar::Float(f)) => Ok(Scalar::Float(f.abs())),
            Some(Scalar::Null) | None => Ok(Scalar::Null),
            other => Err(format!("abs() requires a number, got {other:?}")),
        },
        "ROUND" => {
            let n = scalar_num(vals.first().unwrap_or(&Scalar::Null)).unwrap_or(0.0);
            let digits = vals.get(1).and_then(scalar_num).unwrap_or(0.0) as i32;
            let mul = 10f64.powi(digits);
            let rounded = (n * mul).round() / mul;
            if digits <= 0 {
                Ok(Scalar::Int(rounded as i64))
            } else {
                Ok(Scalar::Float(rounded))
            }
        }
        "FLOOR" => Ok(Scalar::Int(
            scalar_num(vals.first().unwrap_or(&Scalar::Null)).unwrap_or(0.0).floor() as i64,
        )),
        "CEIL" | "CEILING" => Ok(Scalar::Int(
            scalar_num(vals.first().unwrap_or(&Scalar::Null)).unwrap_or(0.0).ceil() as i64,
        )),
        "RANDOM" => {
            let seed = crate::mvcc::now_ms();
            let mut x = seed | 1;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            Ok(Scalar::Float((x % 1_000_000) as f64 / 1_000_000.0))
        }
        "NOW" | "CURRENT_TIMESTAMP" => {
            let ms = crate::mvcc::now_ms();
            let secs = ms / 1000;
            Ok(Scalar::Text(format_epoch(secs)))
        }
        "VERSION" => Ok(Scalar::Text(PG_VERSION.to_string())),
        "CURRENT_DATABASE" | "CURRENT_CATALOG" => Ok(Scalar::Text("voltra".into())),
        "CURRENT_SCHEMA" => Ok(Scalar::Text("public".into())),
        "CURRENT_USER" | "SESSION_USER" | "USER" => Ok(Scalar::Text("voltra".into())),
        "PG_BACKEND_PID" => Ok(Scalar::Int(std::process::id() as i64)),
        other => Err(format!("function {other}() is not supported")),
    }
}

/// Minimal civil-time conversion for now() — good enough for display.
fn format_epoch(secs: u64) -> String {
    let days = secs / 86400;
    let rem = secs % 86400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // days→date (proleptic Gregorian), algorithm from Howard Hinnant.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{m:02}:{s:02}+00")
}

fn like_match(text: &str, pattern: &str, ci: bool) -> bool {
    let (t, p) = if ci {
        (text.to_lowercase(), pattern.to_lowercase())
    } else {
        (text.to_string(), pattern.to_string())
    };
    let tb: Vec<char> = t.chars().collect();
    let pb: Vec<char> = p.chars().collect();
    like_inner(&tb, &pb)
}

fn like_inner(t: &[char], p: &[char]) -> bool {
    let (mut ti, mut pi) = (0usize, 0usize);
    let (mut star_t, mut star_p) = (0usize, usize::MAX);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star_p = pi;
            star_t = ti;
            pi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

fn sort_output(
    cols: &[(String, u32)],
    rows: &mut [Vec<Scalar>],
    order_by: &[ast::OrderByExpr],
) -> Result<(), String> {
    let mut specs: Vec<(usize, bool)> = Vec::new();
    for ob in order_by {
        let asc = ob.asc.unwrap_or(true);
        match &ob.expr {
            Expr::Identifier(id) => {
                let name = id.value.to_lowercase();
                let Some(idx) = cols.iter().position(|(c, _)| c.eq_ignore_ascii_case(&name)) else {
                    return Err(format!("ORDER BY column \"{name}\" is not in the output"));
                };
                specs.push((idx, asc));
            }
            Expr::Value(Value::Number(n, _)) => {
                let idx: usize = n.parse().map_err(|_| "bad ORDER BY position")?;
                if idx == 0 || idx > cols.len() {
                    return Err("ORDER BY position out of range".into());
                }
                specs.push((idx - 1, asc));
            }
            other => return Err(format!("ORDER BY expression not supported here: {other}")),
        }
    }
    rows.sort_by(|a, b| {
        for (idx, asc) in &specs {
            let ord = scalar_cmp(&a[*idx], &b[*idx]);
            let ord = if *asc { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

fn assignment_col(a: &ast::Assignment) -> Result<String, String> {
    let parts: Vec<String> = a.id.iter().map(|i| i.value.to_lowercase()).collect();
    parts
        .last()
        .cloned()
        .ok_or_else(|| "empty assignment target".to_string())
}

fn object_name_str(n: &ObjectName) -> String {
    n.0.iter()
        .map(|i| i.value.to_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

fn expr_label(e: &Expr) -> String {
    match e {
        Expr::Identifier(id) => id.value.to_lowercase(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.to_lowercase())
            .unwrap_or_else(|| "?column?".into()),
        Expr::Function(f) => object_name_str(&f.name).to_lowercase(),
        _ => "?column?".into(),
    }
}

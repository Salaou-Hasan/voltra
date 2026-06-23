// ============================================================================
// VQL — Query Executor
//
// Executes VQL AST statements against TableStore.
// Supports: SELECT (JOINs, GROUP BY, HAVING, ORDER BY, LIMIT, UNION),
//           INSERT (ON CONFLICT, RETURNING), UPDATE, DELETE,
//           SUBSCRIBE (reactive), LEADERBOARD, UPSERT,
//           BEGIN/COMMIT/ROLLBACK.
// ============================================================================

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use serde_json::{Value, Map, json};

use super::ast::*;
use super::error::VqlError;
use crate::table::TableStore;

// ── Result types ──────────────────────────────────────────────────────────────

pub type Row = Map<String, Value>;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub columns: Vec<String>,
    pub rows_affected: usize,
}

// ── Executor ──────────────────────────────────────────────────────────────────

pub struct Executor {
    tables: Arc<TableStore>,
}

impl Executor {
    pub fn new(tables: Arc<TableStore>) -> Self {
        Executor { tables }
    }

    pub fn execute(&self, stmt: &Statement) -> Result<QueryResult, VqlError> {
        match stmt {
            Statement::Select(sel) => self.execute_select(sel),
            Statement::Insert(ins) => self.execute_insert(ins),
            Statement::Update(upd) => self.execute_update(upd),
            Statement::Delete(del) => self.execute_delete(del),
            Statement::Subscribe(sub) => self.execute_subscribe(sub),
            Statement::Leaderboard(lb) => self.execute_leaderboard(lb),
            Statement::Upsert(up) => self.execute_upsert(up),
            Statement::Begin { .. } | Statement::Commit { .. } | Statement::Rollback { .. } => {
                Ok(QueryResult { rows: vec![], columns: vec![], rows_affected: 0 })
            }
        }
    }

    // ── Helper: load table rows as Vec<Row> ───────────────────────────────

    /// Load table rows — zero extra clone: extracts Map directly from owned Value.
    fn load_table(&self, name: &str) -> Result<Vec<(String, Row)>, VqlError> {
        let raw = self.tables.list_rows_with_keys(name)
            .map_err(|e| VqlError::new(0, format!("table scan error: {}", e)))?;
        let mut rows = Vec::with_capacity(raw.len());
        for (k, v) in raw {
            if let Value::Object(m) = v {
                rows.push((k, m));
            }
        }
        Ok(rows)
    }

    /// Lazy-load table: decode only WHERE fields first for filtering,
    /// then decode full rows only for matching entries.
    fn load_table_lazy(&self, name: &str, where_: Option<&Expr>, _projection: &[Expr]) -> Result<Vec<(String, Row)>, VqlError> {
        let filter_fields = where_.map(|w| extract_field_names(w)).unwrap_or_default();

        // If no WHERE fields to filter on, use indexed path
        if filter_fields.is_empty() {
            return self.load_table_indexed(name, where_);
        }

        // Get raw bytes for lazy decode
        let raw = self.tables.list_rows_with_keys_raw(name)
            .map_err(|e| VqlError::new(0, format!("table scan error: {}", e)))?;

        let filter_refs: Vec<&str> = filter_fields.iter().map(|s| s.as_str()).collect();
        let mut result = Vec::new();

        for (key, data) in raw {
            // Lazy decode: only decode WHERE fields
            let filter_row = match crate::table::decode_fields_from_bytes(&data, &filter_refs) {
                Ok(Some(r)) => r,
                Ok(None) => Row::new(), // Fields not found — still check predicate
                _ => continue,
            };

            // Check WHERE predicate against the partially-decoded row
            if let Some(where_) = where_ {
                let truthy = eval_expr(where_, &filter_row, &self.tables)
                    .map(|v| is_truthy(&v)).unwrap_or(false);
                if !truthy { continue; }
            }

            // Row matched — decode the full row via get_row
            match self.tables.get_row(name, &key) {
                Ok(Some(val)) => {
                    if let Some(obj) = val.as_object() {
                        result.push((key, obj.clone()));
                    }
                }
                _ => continue,
            }
        }

        Ok(result)
    }

    /// Load table rows using index-accelerated lookup when possible.
    fn load_table_indexed(&self, name: &str, where_: Option<&Expr>) -> Result<Vec<(String, Row)>, VqlError> {
        if let Some(filter) = where_ {
            let eq_conditions = extract_eq_conditions(filter);
            if !eq_conditions.is_empty() {
                let mut candidate_keys: Option<Vec<String>> = None;

                for (field, value) in &eq_conditions {
                    let _ = self.tables.create_index(name, field);
                    if let Some(keys) = self.tables.index_lookup(name, field, value) {
                        candidate_keys = Some(match candidate_keys {
                            Some(existing) => existing.into_iter().filter(|k| keys.contains(k)).collect(),
                            None => keys,
                        });
                    }
                }

                if let Some(keys) = candidate_keys {
                    let mut rows = Vec::with_capacity(keys.len());
                    for key in &keys {
                        if let Ok(Some(val)) = self.tables.get_row(name, key) {
                            if let Value::Object(m) = val {
                                rows.push((key.clone(), m));
                            }
                        }
                    }
                    return Ok(rows);
                }
            }
        }
        self.load_table(name)
    }

    // ── SELECT ────────────────────────────────────────────────────────────

    fn execute_select(&self, sel: &SelectStmt) -> Result<QueryResult, VqlError> {
        let mut table_rows: Vec<(String, Vec<(String, Row)>)> = Vec::new();
        for table_ref in &sel.from {
            match table_ref {
                TableRef::Named { name, alias } => {
                    // Use lazy loading: decode only WHERE fields first
                    let rows = self.load_table_lazy(name, sel.where_.as_ref(), &sel.columns)?;
                    table_rows.push((alias.clone().unwrap_or_else(|| name.clone()), rows));
                }
                TableRef::Subquery { query, alias } => {
                    let result = self.execute_select(query)?;
                    let named_rows: Vec<(String, Row)> = result.rows.into_iter().enumerate()
                        .map(|(i, r)| (i.to_string(), r)).collect();
                    table_rows.push((alias.clone(), named_rows));
                }
            }
        }

        let mut combined: Vec<Row> = if table_rows.is_empty() {
            vec![Row::new()]
        } else {
            self.cross_join_tables(&table_rows)?
        };

        for join in &sel.joins {
            combined = self.apply_join(join, &combined)?;
        }

        if let Some(where_) = &sel.where_ {
            combined.retain(|row| {
                eval_expr(where_, row, &self.tables).map(|v| is_truthy(&v)).unwrap_or(false)
            });
        }

        if !sel.group_by.is_empty() {
            combined = self.apply_group_by(&sel.group_by, sel.having.as_ref(), &combined)?;
        }

        let mut projected: Vec<Row> = Vec::new();
        let mut columns: Vec<String> = Vec::new();

        for row in &combined {
            let mut out_row = Row::new();
            for col_expr in &sel.columns {
                match col_expr {
                    Expr::Wildcard { table: None } => {
                        for (k, v) in row {
                            out_row.insert(k.clone(), v.clone());
                        }
                    }
                    Expr::Wildcard { table: Some(tbl) } => {
                        let prefix = format!("{}.", tbl);
                        for (k, v) in row {
                            if k.starts_with(&prefix) {
                                let field = &k[prefix.len()..];
                                out_row.insert(field.to_string(), v.clone());
                            }
                        }
                    }
                    Expr::Alias { expr, alias } => {
                        let val = eval_expr(expr, row, &self.tables).unwrap_or(Value::Null);
                        out_row.insert(alias.clone(), val);
                    }
                    other => {
                        let val = eval_expr(other, row, &self.tables).unwrap_or(Value::Null);
                        let key = expr_to_col_name(other);
                        out_row.insert(key, val);
                    }
                }
            }
            projected.push(out_row);
        }

        if !projected.is_empty() {
            columns = projected[0].keys().cloned().collect();
        }

        if !sel.order_by.is_empty() {
            sort_rows(&mut projected, &sel.order_by);
        }

        let offset = sel.offset.unwrap_or(0);
        if offset > 0 {
            projected = projected.into_iter().skip(offset).collect();
        }
        if let Some(limit) = sel.limit {
            projected.truncate(limit);
        }

        if let Some((all, rhs)) = &sel.union {
            let rhs_result = self.execute_select(rhs)?;
            if *all {
                projected.extend(rhs_result.rows);
            } else {
                let mut seen = std::collections::HashSet::new();
                for row in rhs_result.rows {
                    let key = serde_json::to_string(&row).unwrap_or_default();
                    if seen.insert(key) {
                        projected.push(row);
                    }
                }
            }
        }

        Ok(QueryResult { rows: projected, columns, rows_affected: 0 })
    }

    fn cross_join_tables(&self, tables: &[(String, Vec<(String, Row)>)]) -> Result<Vec<Row>, VqlError> {
        let mut result: Vec<Row> = vec![Row::new()];
        for (table_name, rows) in tables {
            let prefix = format!("{}.", table_name);
            let row_key_name = format!("{}_row_key", table_name);
            let mut new_result = Vec::with_capacity(result.len() * rows.len());
            for existing_row in &result {
                for (key, row) in rows {
                    let mut combined = existing_row.clone();
                    for (field, value) in row {
                        // Qualified name: table.field
                        let mut qualified = String::with_capacity(prefix.len() + field.len());
                        qualified.push_str(&prefix);
                        qualified.push_str(field);
                        combined.insert(qualified, value.clone());
                        // Unqualified name (last table wins)
                        combined.insert(field.clone(), value.clone());
                    }
                    combined.insert(row_key_name.clone(), Value::String(key.clone()));
                    new_result.push(combined);
                }
            }
            result = new_result;
        }
        Ok(result)
    }

    fn apply_join(&self, join: &Join, left_rows: &[Row]) -> Result<Vec<Row>, VqlError> {
        match &join.table {
            TableRef::Named { name, alias } => {
                let right_name = alias.clone().unwrap_or_else(|| name.clone());
                let right_rows = self.load_table(name)?;
                self.merge_join(join, left_rows, &right_name, &right_rows)
            }
            TableRef::Subquery { query, alias } => {
                let result = self.execute_select(query)?;
                let named_rows: Vec<(String, Row)> = result.rows.into_iter().enumerate()
                    .map(|(i, r)| (i.to_string(), r)).collect();
                self.merge_join(join, left_rows, alias, &named_rows)
            }
        }
    }

    fn merge_join(&self, join: &Join, left_rows: &[Row], right_name: &str, right_rows: &[(String, Row)]) -> Result<Vec<Row>, VqlError> {
        let mut result = Vec::new();
        for left in left_rows {
            let mut matched = false;
            for (right_key, right) in right_rows {
                let mut combined = left.clone();
                for (field, value) in right {
                    combined.insert(format!("{}.{}", right_name, field), value.clone());
                    combined.insert(field.clone(), value.clone());
                }
                combined.insert(format!("{}_row_key", right_name), Value::String(right_key.clone()));

                if let Some(on) = &join.on {
                    let truthy = eval_expr(on, &combined, &self.tables)
                        .map(|v| is_truthy(&v)).unwrap_or(false);
                    if truthy {
                        result.push(combined);
                        matched = true;
                    }
                } else {
                    result.push(combined);
                    matched = true;
                }
            }
            if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
                let mut combined = left.clone();
                combined.insert(format!("{}_row_key", right_name), Value::Null);
                result.push(combined);
            }
        }
        Ok(result)
    }

    fn apply_group_by(&self, group_by: &[Expr], having: Option<&Expr>, rows: &[Row]) -> Result<Vec<Row>, VqlError> {
        // Use Rc<Row> to share rows across groups without cloning
        let mut groups: BTreeMap<String, Vec<Rc<Row>>> = BTreeMap::new();
        let dummy_tables = Arc::new(TableStore::new());

        for row in rows {
            let mut key_parts = Vec::with_capacity(group_by.len());
            for expr in group_by {
                let val = eval_expr(expr, row, &dummy_tables).unwrap_or(Value::Null);
                // Fast key: format directly without full JSON serialization
                let part = match &val {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => if *b { "1".into() } else { "0".into() },
                    Value::Null => String::new(),
                    other => other.to_string(),
                };
                key_parts.push(part);
            }
            let key = key_parts.join("\x00");
            groups.entry(key).or_default().push(Rc::new(row.clone()));
        }

        let mut result = Vec::with_capacity(groups.len());
        for (_, group_rows) in groups {
            let count = group_rows.len();
            let mut representative = (*group_rows[0]).clone();
            representative.insert("__group_rows__".to_string(), json!(count));

            if let Some(having_expr) = having {
                let truthy = eval_expr(having_expr, &representative, &dummy_tables)
                    .map(|v| is_truthy(&v)).unwrap_or(false);
                if !truthy { continue; }
            }
            result.push(representative);
        }
        Ok(result)
    }

    // ── INSERT ────────────────────────────────────────────────────────────

    fn execute_insert(&self, ins: &InsertStmt) -> Result<QueryResult, VqlError> {
        let mut rows_affected = 0;
        let mut returning_rows = Vec::new();

        for value_row in &ins.values {
            let mut row_data = Row::new();
            if ins.columns.len() == value_row.len() {
                for (col, expr) in ins.columns.iter().zip(value_row) {
                    let val = eval_expr(expr, &Row::new(), &self.tables).unwrap_or(Value::Null);
                    row_data.insert(col.clone(), val);
                }
            } else {
                for (i, expr) in value_row.iter().enumerate() {
                    let val = eval_expr(expr, &Row::new(), &self.tables).unwrap_or(Value::Null);
                    row_data.insert(format!("col_{}", i), val);
                }
            }

            let key = row_data.get("id").or_else(|| row_data.get("key"))
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| format!("auto_{}", uuid_simple()));
            row_data.remove("id");
            row_data.remove("key");

            if let Some(upsert) = &ins.upsert {
                if let Ok(Some(existing)) = self.tables.get_row(&ins.table, &key) {
                    let mut merged = existing.as_object().cloned().unwrap_or_default();
                    for (col, expr) in &upsert.do_update {
                        let val = eval_expr(expr, &Row::new(), &self.tables).unwrap_or(Value::Null);
                        merged.insert(col.clone(), val);
                    }
                    self.tables.set_row(ins.table.clone(), key.clone(), Value::Object(merged.clone()))
                        .map_err(|e| VqlError::new(0, e.to_string()))?;
                    rows_affected += 1;
                    if ins.returning.is_some() { returning_rows.push(merged); }
                    continue;
                }
            }

            self.tables.set_row(ins.table.clone(), key.clone(), Value::Object(row_data.clone()))
                .map_err(|e| VqlError::new(0, e.to_string()))?;
            rows_affected += 1;
            if ins.returning.is_some() { returning_rows.push(row_data); }
        }

        let columns = if !returning_rows.is_empty() {
            returning_rows[0].keys().cloned().collect()
        } else { vec![] };

        Ok(QueryResult { rows: returning_rows, columns, rows_affected })
    }

    // ── UPDATE ────────────────────────────────────────────────────────────

    fn execute_update(&self, upd: &UpdateStmt) -> Result<QueryResult, VqlError> {
        let rows = self.load_table(&upd.table)?;
        let mut rows_affected = 0;
        let mut returning_rows = Vec::new();

        for (key, row) in &rows {
            if let Some(where_) = &upd.where_ {
                let truthy = eval_expr(where_, row, &self.tables)
                    .map(|v| is_truthy(&v)).unwrap_or(false);
                if !truthy { continue; }
            }
            let mut new_row = row.clone();
            for (col, expr) in &upd.sets {
                let val = eval_expr(expr, row, &self.tables).unwrap_or(Value::Null);
                new_row.insert(col.clone(), val);
            }
            self.tables.set_row(upd.table.clone(), key.clone(), Value::Object(new_row.clone()))
                .map_err(|e| VqlError::new(0, e.to_string()))?;
            rows_affected += 1;
            if upd.returning.is_some() { returning_rows.push(new_row); }
        }

        let columns = if !returning_rows.is_empty() {
            returning_rows[0].keys().cloned().collect()
        } else { vec![] };

        Ok(QueryResult { rows: returning_rows, columns, rows_affected })
    }

    // ── DELETE ────────────────────────────────────────────────────────────

    fn execute_delete(&self, del: &DeleteStmt) -> Result<QueryResult, VqlError> {
        let rows = self.load_table(&del.table)?;
        let mut rows_affected = 0;
        let mut returning_rows = Vec::new();

        for (key, row) in &rows {
            if let Some(where_) = &del.where_ {
                let truthy = eval_expr(where_, row, &self.tables)
                    .map(|v| is_truthy(&v)).unwrap_or(false);
                if !truthy { continue; }
            }
            if del.returning.is_some() { returning_rows.push(row.clone()); }
            self.tables.delete_row(&del.table, key)
                .map_err(|e| VqlError::new(0, e.to_string()))?;
            rows_affected += 1;
        }

        let columns = if !returning_rows.is_empty() {
            returning_rows[0].keys().cloned().collect()
        } else { vec![] };

        Ok(QueryResult { rows: returning_rows, columns, rows_affected })
    }

    // ── SUBSCRIBE (reactive — returns initial snapshot as SELECT) ────────

    fn execute_subscribe(&self, sub: &SubscribeStmt) -> Result<QueryResult, VqlError> {
        let sel = SelectStmt {
            distinct: false,
            columns: vec![Expr::Wildcard { table: None }],
            from: vec![TableRef::Named { name: sub.table.clone(), alias: sub.alias.clone() }],
            joins: vec![],
            where_: sub.where_.clone(),
            group_by: vec![],
            having: None,
            order_by: sub.order_by.clone(),
            limit: sub.limit,
            offset: None,
            union: None,
        };
        self.execute_select(&sel)
    }

    // ── LEADERBOARD (game primitive) ──────────────────────────────────────

    fn execute_leaderboard(&self, lb: &LeaderboardStmt) -> Result<QueryResult, VqlError> {
        let mut rows = self.load_table(&lb.table)?;

        if let Some(where_) = &lb.where_ {
            rows.retain(|(_, row)| {
                eval_expr(where_, row, &self.tables).map(|v| is_truthy(&v)).unwrap_or(false)
            });
        }

        let by = lb.by.clone();
        rows.sort_by(|a, b| {
            let val_a = a.1.get(&by).cloned().unwrap_or(Value::Null);
            let val_b = b.1.get(&by).cloned().unwrap_or(Value::Null);
            let ord = compare_values(&val_a, &val_b);
            if lb.asc { ord } else { ord.reverse() }
        });

        if let Some(limit) = lb.limit {
            rows.truncate(limit);
        }

        let result: Vec<Row> = rows.into_iter().enumerate().map(|(i, (key, mut row))| {
            row.insert("rank".to_string(), json!(i + 1));
            row.insert("_key".to_string(), Value::String(key));
            row
        }).collect();

        let columns = if !result.is_empty() {
            let mut cols: Vec<String> = result[0].keys().cloned().collect();
            cols.insert(0, "rank".to_string());
            cols
        } else {
            vec!["rank".to_string(), "_key".to_string()]
        };

        Ok(QueryResult { rows: result, columns, rows_affected: 0 })
    }

    // ── UPSERT (game primitive) ──────────────────────────────────────────

    fn execute_upsert(&self, up: &UpsertStmt) -> Result<QueryResult, VqlError> {
        let key_val = eval_expr(&up.key, &Row::new(), &self.tables)
            .map_err(|e| VqlError::new(0, e.to_string()))?;
        let key = key_val.as_str().unwrap_or("").to_string();
        let key = if key.is_empty() { serde_json::to_string(&key_val).unwrap_or_default() } else { key };

        let existing = self.tables.get_row(&up.table, &key)
            .map_err(|e| VqlError::new(0, e.to_string()))?
            .unwrap_or_else(|| Value::Object(Row::new()));

        let mut new_row = existing.as_object().cloned().unwrap_or_default();
        for (col, expr) in &up.sets {
            let val = eval_expr(expr, &Row::new(), &self.tables).unwrap_or(Value::Null);
            new_row.insert(col.clone(), val);
        }

        self.tables.set_row(up.table.clone(), key.clone(), Value::Object(new_row.clone()))
            .map_err(|e| VqlError::new(0, e.to_string()))?;

        new_row.insert("_key".to_string(), Value::String(key));

        Ok(QueryResult {
            rows: vec![new_row.clone()],
            columns: new_row.keys().cloned().collect(),
            rows_affected: 1,
        })
    }
}

// ── Expression evaluator ──────────────────────────────────────────────────────

pub fn eval_expr(expr: &Expr, row: &Row, tables: &Arc<TableStore>) -> Result<Value, VqlError> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),

        Expr::Column { table: Some(tbl), name } => {
            let qualified = format!("{}.{}", tbl, name);
            Ok(row.get(&qualified).or_else(|| row.get(name)).cloned().unwrap_or(Value::Null))
        }
        Expr::Column { table: None, name } => {
            Ok(row.get(name).cloned().unwrap_or(Value::Null))
        }

        Expr::Wildcard { .. } => Ok(Value::Null),

        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, row, tables)?;
            let r = eval_expr(right, row, tables)?;
            eval_binary_op(op, &l, &r)
        }

        Expr::UnaryOp { op, expr } => {
            let val = eval_expr(expr, row, tables)?;
            match op {
                UnaryOp::Not => Ok(Value::Bool(!is_truthy(&val))),
                UnaryOp::Neg => match &val {
                    Value::Number(n) => Ok(json!(-n.as_f64().unwrap_or(0.0))),
                    _ => Ok(Value::Null),
                },
                UnaryOp::Pos => Ok(val),
            }
        }

        Expr::IsNull { expr, negated } => {
            let val = eval_expr(expr, row, tables)?;
            let is_null = val.is_null();
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }

        Expr::InList { expr, list, negated } => {
            let val = eval_expr(expr, row, tables)?;
            let found = list.iter().any(|item| {
                eval_expr(item, row, tables).map(|v| values_equal(&v, &val)).unwrap_or(false)
            });
            Ok(Value::Bool(if *negated { !found } else { found }))
        }

        Expr::InSubquery { expr, query, negated } => {
            let val = eval_expr(expr, row, tables)?;
            let executor = Executor::new(tables.clone());
            let result = executor.execute_select(query)?;
            let found = result.rows.iter().any(|r| {
                r.values().any(|v| values_equal(v, &val))
            });
            Ok(Value::Bool(if *negated { !found } else { found }))
        }

        Expr::Between { expr, low, high, negated } => {
            let val = eval_expr(expr, row, tables)?;
            let lo = eval_expr(low, row, tables)?;
            let hi = eval_expr(high, row, tables)?;
            let in_range = compare_values(&val, &lo) != std::cmp::Ordering::Less
                && compare_values(&val, &hi) != std::cmp::Ordering::Greater;
            Ok(Value::Bool(if *negated { !in_range } else { in_range }))
        }

        Expr::Like { expr, pattern, negated } => {
            let val = eval_expr(expr, row, tables)?;
            let pat = eval_expr(pattern, row, tables)?;
            let m = match (&val, &pat) {
                (Value::String(s), Value::String(p)) => like_match(s, p),
                _ => false,
            };
            Ok(Value::Bool(if *negated { !m } else { m }))
        }

        Expr::ILike { expr, pattern, negated } => {
            let val = eval_expr(expr, row, tables)?;
            let pat = eval_expr(pattern, row, tables)?;
            let m = match (&val, &pat) {
                (Value::String(s), Value::String(p)) => like_match(&s.to_lowercase(), &p.to_lowercase()),
                _ => false,
            };
            Ok(Value::Bool(if *negated { !m } else { m }))
        }

        Expr::Aggregate { func, arg, .. } => {
            match func {
                AggFunc::Count if arg.is_none() => {
                    let count = row.get("__group_rows__").and_then(|v| v.as_u64()).unwrap_or(1);
                    Ok(json!(count))
                }
                AggFunc::Count => {
                    let v = eval_expr(arg.as_ref().unwrap(), row, tables)?;
                    if v.is_null() { Ok(json!(0)) } else { Ok(json!(1)) }
                }
                _ => {
                    let v = eval_expr(arg.as_ref().unwrap(), row, tables)?;
                    Ok(v)
                }
            }
        }

        Expr::Function { name, args } => {
            let vals: Result<Vec<Value>, _> = args.iter().map(|a| eval_expr(a, row, tables)).collect();
            eval_function(name, &vals?)
        }

        Expr::Case { operand, branches, else_ } => {
            for (cond, result) in branches {
                let cond_val = if let Some(op) = operand {
                    let op_val = eval_expr(op, row, tables)?;
                    let cond_val = eval_expr(cond, row, tables)?;
                    values_equal(&op_val, &cond_val)
                } else {
                    eval_expr(cond, row, tables).map(|v| is_truthy(&v)).unwrap_or(false)
                };
                if cond_val { return eval_expr(result, row, tables); }
            }
            if let Some(default) = else_ { eval_expr(default, row, tables) } else { Ok(Value::Null) }
        }

        Expr::Subquery(sel) => {
            let executor = Executor::new(tables.clone());
            let result = executor.execute_select(sel)?;
            Ok(result.rows.first().and_then(|r| r.values().next()).cloned().unwrap_or(Value::Null))
        }

        Expr::Exists { query, negated } => {
            let executor = Executor::new(tables.clone());
            let result = executor.execute_select(query)?;
            let exists = !result.rows.is_empty();
            Ok(Value::Bool(if *negated { !exists } else { exists }))
        }

        Expr::Alias { expr, .. } => eval_expr(expr, row, tables),

        Expr::RowAccess { table, key } => {
            let key_val = eval_expr(key, row, tables)?;
            let key_str = key_val.as_str().unwrap_or("").to_string();
            let key_str = if key_str.is_empty() { key_val.to_string() } else { key_str };
            let val = tables.get_row(table, &key_str)
                .map_err(|e| VqlError::new(0, e.to_string()))?;
            Ok(val.unwrap_or(Value::Null))
        }

        Expr::FieldAccess { object, field } => {
            let obj = eval_expr(object, row, tables)?;
            match &obj {
                Value::Object(map) => Ok(map.get(field.as_str()).cloned().unwrap_or(Value::Null)),
                _ => Ok(Value::Null),
            }
        }

        Expr::RowLiteral { fields } => {
            let mut map = Row::new();
            for (name, expr) in fields {
                let val = eval_expr(expr, row, tables)?;
                map.insert(name.clone(), val);
            }
            Ok(Value::Object(map))
        }
    }
}

fn eval_binary_op(op: &BinOp, left: &Value, right: &Value) -> Result<Value, VqlError> {
    match op {
        BinOp::Add => match (left, right) {
            (Value::Number(a), Value::Number(b)) => Ok(json!(a.as_f64().unwrap_or(0.0) + b.as_f64().unwrap_or(0.0))),
            (Value::String(a), Value::String(b)) => Ok(json!(format!("{}{}", a, b))),
            _ => Ok(Value::Null),
        },
        BinOp::Sub => match (left, right) {
            (Value::Number(a), Value::Number(b)) => Ok(json!(a.as_f64().unwrap_or(0.0) - b.as_f64().unwrap_or(0.0))),
            _ => Ok(Value::Null),
        },
        BinOp::Mul => match (left, right) {
            (Value::Number(a), Value::Number(b)) => Ok(json!(a.as_f64().unwrap_or(0.0) * b.as_f64().unwrap_or(0.0))),
            _ => Ok(Value::Null),
        },
        BinOp::Div => match (left, right) {
            (Value::Number(a), Value::Number(b)) => {
                let d = b.as_f64().unwrap_or(1.0);
                if d == 0.0 { Ok(Value::Null) } else { Ok(json!(a.as_f64().unwrap_or(0.0) / d)) }
            }
            _ => Ok(Value::Null),
        },
        BinOp::Mod => match (left, right) {
            (Value::Number(a), Value::Number(b)) => {
                let d = b.as_f64().unwrap_or(1.0);
                if d == 0.0 { Ok(Value::Null) } else { Ok(json!(a.as_f64().unwrap_or(0.0) % d)) }
            }
            _ => Ok(Value::Null),
        },
        BinOp::Eq => Ok(Value::Bool(values_equal(left, right))),
        BinOp::Ne => Ok(Value::Bool(!values_equal(left, right))),
        BinOp::Lt => Ok(Value::Bool(compare_values(left, right) == std::cmp::Ordering::Less)),
        BinOp::Le => Ok(Value::Bool(matches!(compare_values(left, right), std::cmp::Ordering::Less | std::cmp::Ordering::Equal))),
        BinOp::Gt => Ok(Value::Bool(compare_values(left, right) == std::cmp::Ordering::Greater)),
        BinOp::Ge => Ok(Value::Bool(matches!(compare_values(left, right), std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))),
        BinOp::And => Ok(Value::Bool(is_truthy(left) && is_truthy(right))),
        BinOp::Or => Ok(Value::Bool(is_truthy(left) || is_truthy(right))),
        BinOp::Concat => {
            let l = left.as_str().unwrap_or("");
            let r = right.as_str().unwrap_or("");
            Ok(json!(format!("{}{}", l, r)))
        }
    }
}

fn eval_function(name: &str, args: &[Value]) -> Result<Value, VqlError> {
    match name {
        "upper" => Ok(args.first().and_then(|v| v.as_str()).map(|s| json!(s.to_uppercase())).unwrap_or(Value::Null)),
        "lower" => Ok(args.first().and_then(|v| v.as_str()).map(|s| json!(s.to_lowercase())).unwrap_or(Value::Null)),
        "length" => Ok(args.first().and_then(|v| v.as_str()).map(|s| json!(s.len() as i64)).unwrap_or(json!(0))),
        "trim" => Ok(args.first().and_then(|v| v.as_str()).map(|s| json!(s.trim())).unwrap_or(Value::Null)),
        "ltrim" => Ok(args.first().and_then(|v| v.as_str()).map(|s| json!(s.trim_start())).unwrap_or(Value::Null)),
        "rtrim" => Ok(args.first().and_then(|v| v.as_str()).map(|s| json!(s.trim_end())).unwrap_or(Value::Null)),
        "round" => Ok(args.first().and_then(|v| v.as_f64()).map(|f| json!(f.round())).unwrap_or(Value::Null)),
        "floor" => Ok(args.first().and_then(|v| v.as_f64()).map(|f| json!(f.floor())).unwrap_or(Value::Null)),
        "ceil" => Ok(args.first().and_then(|v| v.as_f64()).map(|f| json!(f.ceil())).unwrap_or(Value::Null)),
        "abs" => Ok(args.first().and_then(|v| v.as_f64()).map(|f| json!(f.abs())).unwrap_or(Value::Null)),
        "coalesce" => Ok(args.iter().find(|v| !v.is_null()).cloned().unwrap_or(Value::Null)),
        "now" => {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64;
            Ok(json!(ts))
        }
        "random" => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
            Ok(json!((ts % 10000) as f64 / 10000.0))
        }
        "concat" => {
            let parts: Vec<String> = args.iter().map(|v| v.to_string()).collect();
            Ok(json!(parts.join("")))
        }
        "replace" => {
            if args.len() >= 3 {
                let s = args[0].as_str().unwrap_or("");
                let from = args[1].as_str().unwrap_or("");
                let to = args[2].as_str().unwrap_or("");
                Ok(json!(s.replace(from, to)))
            } else { Ok(Value::Null) }
        }
        "substr" => {
            if args.len() >= 2 {
                let s = args[0].as_str().unwrap_or("");
                let start = args[1].as_i64().unwrap_or(0).max(0) as usize;
                let len = args.get(2).and_then(|v| v.as_i64()).unwrap_or(s.len() as i64).max(0) as usize;
                let end = s.len().min(start + len);
                if start >= s.len() { Ok(json!("")) } else { Ok(json!(&s[start..end])) }
            } else { Ok(Value::Null) }
        }
        name if name.starts_with("cast::") => {
            let target = &name[6..];
            let val = args.first().cloned().unwrap_or(Value::Null);
            match target {
                "int" => Ok(json!(val.as_f64().unwrap_or(0.0) as i64)),
                "float" => Ok(json!(val.as_f64().unwrap_or(0.0))),
                "str" => Ok(json!(val.to_string())),
                "bool" => Ok(json!(is_truthy(&val))),
                _ => Ok(val),
            }
        }
        _ => Ok(Value::Null),
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Extract all field names referenced in an expression tree.
fn extract_field_names(expr: &Expr) -> Vec<String> {
    match expr {
        Expr::Column { table: Some(tbl), name } => vec![name.clone(), format!("{}.{}", tbl, name)],
        Expr::Column { table: None, name } => vec![name.clone()],
        Expr::BinaryOp { left, right, .. } => {
            let mut names = extract_field_names(left);
            names.extend(extract_field_names(right));
            names
        }
        Expr::UnaryOp { expr, .. } => extract_field_names(expr),
        Expr::IsNull { expr, .. } => extract_field_names(expr),
        Expr::InList { expr, list, .. } => {
            let mut names = extract_field_names(expr);
            for item in list { names.extend(extract_field_names(item)); }
            names
        }
        Expr::Between { expr, low, high, .. } => {
            let mut names = extract_field_names(expr);
            names.extend(extract_field_names(low));
            names.extend(extract_field_names(high));
            names
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            let mut names = extract_field_names(expr);
            names.extend(extract_field_names(pattern));
            names
        }
        Expr::Alias { expr, .. } => extract_field_names(expr),
        Expr::FieldAccess { object, .. } => extract_field_names(object),
        _ => vec![],
    }
}

/// Extract equality conditions from a WHERE expression tree.
/// Returns `(field_name, value)` pairs for conditions like `field = literal`.
/// Supports AND-connected conditions (all must be equality for index use).
fn extract_eq_conditions(expr: &Expr) -> Vec<(String, Value)> {
    match expr {
        Expr::BinaryOp { left, op: BinOp::Eq, right } => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column { table: None, name }, Expr::Literal(val)) => {
                    vec![(name.clone(), val.clone())]
                }
                (Expr::Literal(val), Expr::Column { table: None, name }) => {
                    vec![(name.clone(), val.clone())]
                }
                (Expr::Column { table: Some(_), name }, Expr::Literal(val)) => {
                    vec![(name.clone(), val.clone())]
                }
                (Expr::Literal(val), Expr::Column { table: Some(_), name }) => {
                    vec![(name.clone(), val.clone())]
                }
                _ => vec![],
            }
        }
        Expr::BinaryOp { left, op: BinOp::And, right } => {
            let mut left_conds = extract_eq_conditions(left);
            let right_conds = extract_eq_conditions(right);
            left_conds.extend(right_conds);
            left_conds
        }
        _ => vec![],
    }
}

pub fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(_) => true,
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Number(x), Value::Number(y)) => {
            x.as_f64().unwrap_or(0.0).partial_cmp(&y.as_f64().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => format!("{}", a).cmp(&format!("{}", b)),
    }
}

fn like_match(s: &str, pattern: &str) -> bool {
    let chars: Vec<char> = pattern.chars().collect();
    let s_chars: Vec<char> = s.chars().collect();
    like_recursive(&s_chars, &chars, 0, 0)
}

fn like_recursive(s: &[char], p: &[char], si: usize, pi: usize) -> bool {
    if pi == p.len() { return si == s.len(); }
    match p[pi] {
        '%' => {
            // Try matching zero or more characters
            let mut i = si;
            while i <= s.len() {
                if like_recursive(s, p, i, pi + 1) { return true; }
                i += 1;
            }
            false
        }
        '_' => {
            if si < s.len() { like_recursive(s, p, si + 1, pi + 1) } else { false }
        }
        c => {
            if si < s.len() && s[si] == c { like_recursive(s, p, si + 1, pi + 1) } else { false }
        }
    }
}

fn sort_rows(rows: &mut Vec<Row>, order_by: &[OrderByItem]) {
    let dummy_tables = Arc::new(TableStore::new());
    rows.sort_by(|a, b| {
        for item in order_by {
            let val_a = eval_expr(&item.expr, a, &dummy_tables).unwrap_or(Value::Null);
            let val_b = eval_expr(&item.expr, b, &dummy_tables).unwrap_or(Value::Null);
            let ord = compare_values(&val_a, &val_b);
            let ord = if item.asc { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal { return ord; }
        }
        std::cmp::Ordering::Equal
    });
}

fn expr_to_col_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { table: Some(tbl), name } => format!("{}.{}", tbl, name),
        Expr::Column { table: None, name } => name.clone(),
        Expr::Function { name, args } => {
            let arg_names: Vec<String> = args.iter().map(|a| expr_to_col_name(a)).collect();
            format!("{}({})", name, arg_names.join(", "))
        }
        Expr::Alias { alias, .. } => alias.clone(),
        Expr::Literal(Value::Number(n)) => n.to_string(),
        Expr::Literal(Value::String(s)) => s.clone(),
        _ => "expr".to_string(),
    }
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    format!("{:x}", ts)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vql::lexer::tokenize;
    use crate::vql::parser;

    fn exec_src(src: &str) -> QueryResult {
        let tokens = tokenize(src).expect("lex failed");
        let program = parser::parse(tokens).expect("parse failed");
        let tables = Arc::new(TableStore::new());
        let executor = Executor::new(tables);
        executor.execute(&program.statements[0]).expect("exec failed")
    }

    fn exec_with_tables(src: &str, tables: Arc<TableStore>) -> QueryResult {
        let tokens = tokenize(src).expect("lex failed");
        let program = parser::parse(tokens).expect("parse failed");
        let executor = Executor::new(tables);
        executor.execute(&program.statements[0]).expect("exec failed")
    }

    #[test]
    fn select_literal() {
        let r = exec_src("SELECT 1 + 1 AS result");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0]["result"].as_f64(), Some(2.0));
    }

    #[test]
    fn insert_and_select() {
        let tables = Arc::new(TableStore::new());
        let r = exec_with_tables("INSERT INTO players (id, score) VALUES ('p1', 100), ('p2', 200)", tables.clone());
        assert_eq!(r.rows_affected, 2);
        let r = exec_with_tables("SELECT * FROM players", tables);
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn update_with_where() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO players (id, score) VALUES ('p1', 100), ('p2', 200)", tables.clone());
        let r = exec_with_tables("UPDATE players SET score = 999 WHERE score > 150", tables.clone());
        assert_eq!(r.rows_affected, 1);
        let r = exec_with_tables("SELECT * FROM players WHERE score = 999", tables);
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn delete_with_where() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO players (id, score) VALUES ('p1', 100), ('p2', 200)", tables.clone());
        let r = exec_with_tables("DELETE FROM players WHERE score < 150", tables.clone());
        assert_eq!(r.rows_affected, 1);
        let r = exec_with_tables("SELECT * FROM players", tables);
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn order_by_and_limit() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO scores (id, value) VALUES ('a', 30), ('b', 10), ('c', 20)", tables.clone());
        let r = exec_with_tables("SELECT * FROM scores ORDER BY value DESC LIMIT 2", tables);
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0]["value"], json!(30));
        assert_eq!(r.rows[1]["value"], json!(20));
    }

    #[test]
    fn leaderboard() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO scores (id, score) VALUES ('a', 100), ('b', 300), ('c', 200)", tables.clone());
        let r = exec_with_tables("LEADERBOARD scores BY score DESC LIMIT 2", tables);
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0]["rank"], json!(1));
        assert_eq!(r.rows[0]["score"], json!(300));
        assert_eq!(r.rows[1]["rank"], json!(2));
        assert_eq!(r.rows[1]["score"], json!(200));
    }

    #[test]
    fn upsert_existing() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO players (id, hp) VALUES ('p1', 100)", tables.clone());
        let r = exec_with_tables("UPSERT players['p1'] SET hp = 50", tables);
        assert_eq!(r.rows_affected, 1);
        assert_eq!(r.rows[0]["hp"], json!(50));
    }

    #[test]
    fn upsert_new() {
        let tables = Arc::new(TableStore::new());
        let r = exec_with_tables("UPSERT players['p2'] SET hp = 200", tables);
        assert_eq!(r.rows_affected, 1);
        assert_eq!(r.rows[0]["hp"], json!(200));
    }

    #[test]
    fn select_with_where() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO players (id, level, name) VALUES ('p1', 5, 'alice'), ('p2', 10, 'bob'), ('p3', 3, 'carol')", tables.clone());
        let r = exec_with_tables("SELECT * FROM players WHERE level > 4", tables);
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn insert_returning() {
        let tables = Arc::new(TableStore::new());
        let r = exec_with_tables("INSERT INTO t (id, x) VALUES ('k1', 42) RETURNING *", tables);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0]["x"], json!(42));
    }

    #[test]
    fn subscribe_returns_matching_rows() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO players (id, zone) VALUES ('p1', 'z1'), ('p2', 'z2'), ('p3', 'z1')", tables.clone());
        let r = exec_with_tables("SUBSCRIBE players WHERE zone = 'z1'", tables);
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn scalar_functions() {
        let r = exec_src("SELECT upper('hello') AS u, length('world') AS l, round(3.7) AS r");
        assert_eq!(r.rows[0]["u"], json!("HELLO"));
        assert_eq!(r.rows[0]["l"].as_i64(), Some(5));
        assert_eq!(r.rows[0]["r"].as_f64(), Some(4.0));
    }

    #[test]
    fn case_expression() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO t (id, score) VALUES ('a', 150)", tables.clone());
        let r = exec_with_tables("SELECT CASE WHEN score > 100 THEN 'high' ELSE 'low' END AS tier FROM t", tables);
        assert_eq!(r.rows[0]["tier"], json!("high"));
    }

    #[test]
    fn in_list_filter() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO players (id, class) VALUES ('p1', 'warrior'), ('p2', 'mage'), ('p3', 'warrior')", tables.clone());
        let r = exec_with_tables("SELECT * FROM players WHERE class IN ('warrior', 'mage')", tables);
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    fn is_null_filter() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO t (id, val) VALUES ('a', 'x')", tables.clone());
        exec_with_tables("INSERT INTO t (id) VALUES ('b')", tables.clone());
        let r = exec_with_tables("SELECT * FROM t WHERE val IS NULL", tables.clone());
        assert_eq!(r.rows.len(), 1);
        let r = exec_with_tables("SELECT * FROM t WHERE val IS NOT NULL", tables);
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn between_filter() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO t (id, v) VALUES ('a', 5), ('b', 15), ('c', 25)", tables.clone());
        let r = exec_with_tables("SELECT * FROM t WHERE v BETWEEN 10 AND 20", tables);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0]["v"], json!(15));
    }

    #[test]
    fn like_filter() {
        let tables = Arc::new(TableStore::new());
        exec_with_tables("INSERT INTO t (id, name) VALUES ('a', 'alice'), ('b', 'bob'), ('c', 'alicia')", tables.clone());
        let r = exec_with_tables("SELECT * FROM t WHERE name LIKE 'al%'", tables);
        assert_eq!(r.rows.len(), 2);
    }
}

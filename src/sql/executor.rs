// ============================================================================
// NeonDB SQL Executor
//
// Evaluates a parsed SQL AST against the live TableStore.
//
// Supported:
//   SELECT  — full projection, WHERE, INNER/LEFT/RIGHT/FULL JOIN, GROUP BY,
//             HAVING, ORDER BY, LIMIT, OFFSET, DISTINCT, UNION / UNION ALL,
//             scalar subqueries, IN-subquery, EXISTS, CASE, scalar functions,
//             aggregate functions (COUNT/SUM/AVG/MIN/MAX), column aliases,
//             table aliases, cross-table qualified column references.
//   INSERT  — single or multi-row; respects column list.
//   UPDATE  — SET with WHERE.
//   DELETE  — with optional WHERE.
//
// Result type: Vec<serde_json::Map<String, Value>> — each row is a JSON object
// with column names as keys.  Callers serialise to their preferred wire format.
// ============================================================================

use super::ast::*;
use crate::error::{NeonDBError, Result};
use crate::table::TableStore;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

// ── Row type ──────────────────────────────────────────────────────────────────

pub type Row = Map<String, Value>;

// ── Query result ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct QueryResult {
    pub rows:    Vec<Row>,
    pub columns: Vec<String>,
    /// Number of rows affected (INSERT/UPDATE/DELETE)
    pub rows_affected: usize,
}

impl QueryResult {
    fn select(rows: Vec<Row>, columns: Vec<String>) -> Self {
        QueryResult { rows_affected: rows.len(), columns, rows }
    }

    fn dml(rows_affected: usize) -> Self {
        QueryResult { rows: vec![], columns: vec![], rows_affected }
    }
}

// ── Executor ──────────────────────────────────────────────────────────────────

pub struct Executor {
    tables: Arc<TableStore>,
}

impl Executor {
    pub fn new(tables: Arc<TableStore>) -> Self {
        Executor { tables }
    }

    pub fn execute_statement(&self, stmt: &Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Select(s) => self.execute_select(s),
            Statement::Insert(s) => self.execute_insert(s),
            Statement::Update(s) => self.execute_update(s),
            Statement::Delete(s) => self.execute_delete(s),
        }
    }

    // ── SELECT ────────────────────────────────────────────────────────────────

    fn execute_select(&self, stmt: &SelectStmt) -> Result<QueryResult> {
        let rows = self.eval_select_core(stmt)?;

        // UNION / UNION ALL
        let mut all_rows = rows;
        if let Some((all, rhs)) = &stmt.union {
            let rhs_result = self.execute_select(rhs)?;
            if *all {
                all_rows.extend(rhs_result.rows);
            } else {
                // UNION deduplicates — use string rep as key
                let mut seen: std::collections::HashSet<String> = all_rows
                    .iter().map(|r| value_fingerprint(r)).collect();
                for row in rhs_result.rows {
                    let fp = value_fingerprint(&row);
                    if seen.insert(fp) { all_rows.push(row); }
                }
            }
        }

        let col_names: Vec<String> = if all_rows.is_empty() {
            vec![]
        } else {
            all_rows[0].keys().cloned().collect()
        };

        Ok(QueryResult::select(all_rows, col_names))
    }

    fn eval_select_core(&self, stmt: &SelectStmt) -> Result<Vec<Row>> {
        // 1. Build working set from FROM + JOINs
        let mut rows: Vec<Row> = if stmt.from.is_empty() {
            vec![Map::new()] // valueless SELECT: single empty row
        } else {
            self.eval_from(&stmt.from, &stmt.joins)?
        };

        // 2. WHERE
        if let Some(cond) = &stmt.where_ {
            rows.retain(|row| eval_expr(cond, row, &self.tables).map(|v| is_truthy(&v)).unwrap_or(false));
        }

        // 3. GROUP BY / aggregation
        if !stmt.group_by.is_empty() || has_aggregate(&stmt.columns) {
            rows = self.eval_group_by(stmt, rows)?;
        } else {
            // 3b. No grouping — project each row individually
            rows = rows.iter()
                .map(|row| self.project_row(&stmt.columns, row, None))
                .collect::<Result<Vec<_>>>()?;
        }

        // 4. HAVING (only when not using grouping/aggregation execution path)
        // Aggregation-aware HAVING is handled inside `eval_group_by`.
        if let Some(having) = &stmt.having {
            let has_aggs_in_select = has_aggregate(&stmt.columns);
            let has_aggs_in_having = expr_has_aggregate(having);
            if stmt.group_by.is_empty() && !has_aggs_in_select && !has_aggs_in_having {
                rows.retain(|row| eval_expr(having, row, &self.tables).map(|v| is_truthy(&v)).unwrap_or(false));
            }
        }

        // 5. DISTINCT
        if stmt.distinct {
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| seen.insert(value_fingerprint(row)));
        }

        // 6. ORDER BY
        if !stmt.order_by.is_empty() {
            let tables = self.tables.clone();
            rows.sort_by(|a, b| {
                for item in &stmt.order_by {
                    let av = eval_expr(&item.expr, a, &tables).ok().unwrap_or(Value::Null);
                    let bv = eval_expr(&item.expr, b, &tables).ok().unwrap_or(Value::Null);
                    let nulls_first = item.nulls_first.unwrap_or(!item.asc);
                    let ord = compare_values_ord(&av, &bv, nulls_first);
                    let ord = if item.asc { ord } else { ord.reverse() };
                    if ord != std::cmp::Ordering::Equal { return ord; }
                }
                std::cmp::Ordering::Equal
            });
        }

        // 7. OFFSET then LIMIT
        let rows = if let Some(off) = stmt.offset { rows.into_iter().skip(off).collect() } else { rows };
        let rows = if let Some(lim) = stmt.limit  { rows.into_iter().take(lim).collect() } else { rows };

        Ok(rows)
    }

    // ── FROM + JOIN ───────────────────────────────────────────────────────────

    fn eval_from(&self, from: &[TableRef], joins: &[Join]) -> Result<Vec<Row>> {
        // Start with the first table (Cartesian product of all comma-separated tables)
        let mut result = self.eval_table_ref(&from[0])?;

        // Additional comma-separated tables (implicit cross join)
        for tref in &from[1..] {
            let right = self.eval_table_ref(tref)?;
            result = cross_product(result, right);
        }

        // Explicit JOIN clauses
        for join in joins {
            let right_rows = self.eval_table_ref(&join.table)?;
            result = match join.kind {
                JoinKind::Inner => self.nested_loop_join(result, right_rows, &join.on, false, false)?,
                JoinKind::Left  => self.nested_loop_join(result, right_rows, &join.on, true, false)?,
                JoinKind::Right => self.nested_loop_join(result, right_rows, &join.on, false, true)?,
                JoinKind::Full  => self.full_outer_join(result, right_rows, &join.on)?,
                JoinKind::Cross => cross_product(result, right_rows),
            };
        }
        Ok(result)
    }

    fn eval_table_ref(&self, tref: &TableRef) -> Result<Vec<Row>> {
        match tref {
            TableRef::Named { name, alias } => {
                let raw = self.tables.list_rows_with_keys(name)?;
                let prefix = alias.as_deref().unwrap_or(name.as_str());
                Ok(raw.into_iter().map(|(key, mut val)| {
                    // Inject row_key if not present
                    if let Some(obj) = val.as_object_mut() {
                        obj.entry("row_key".to_string()).or_insert_with(|| Value::String(key.clone()));
                        // Strip internal fields injected by the table engine
                        obj.remove("shard_id");
                    }
                    qualify_row(val, prefix)
                }).collect())
            }
            TableRef::Subquery { query, alias } => {
                let result = self.execute_select(query)?;
                Ok(result.rows.into_iter().map(|row| qualify_row(Value::Object(row), alias)).collect())
            }
        }
    }

    fn nested_loop_join(
        &self,
        left: Vec<Row>,
        right: Vec<Row>,
        on: &Option<Expr>,
        preserve_left: bool,
        preserve_right: bool,
    ) -> Result<Vec<Row>> {
        let mut result = Vec::new();
        let mut right_matched = vec![false; right.len()];

        for l in &left {
            let mut any_match = false;
            for (ri, r) in right.iter().enumerate() {
                let combined = merge_rows(l.clone(), r.clone());
                let passes = match on {
                    None => true,
                    Some(cond) => eval_expr(cond, &combined, &self.tables)
                        .map(|v| is_truthy(&v))
                        .unwrap_or(false),
                };
                if passes {
                    result.push(combined);
                    any_match = true;
                    right_matched[ri] = true;
                }
            }
            if !any_match && preserve_left {
                // LEFT JOIN: emit left row with NULLs for right columns
                let null_right = null_row_like(right.first());
                result.push(merge_rows(l.clone(), null_right));
            }
        }

        if preserve_right {
            for (ri, r) in right.iter().enumerate() {
                if !right_matched[ri] {
                    let null_left = null_row_like(left.first());
                    result.push(merge_rows(null_left, r.clone()));
                }
            }
        }

        Ok(result)
    }

    fn full_outer_join(&self, left: Vec<Row>, right: Vec<Row>, on: &Option<Expr>) -> Result<Vec<Row>> {
        // Full = LEFT JOIN ∪ RIGHT-only rows
        let mut result = self.nested_loop_join(left.clone(), right.clone(), on, true, false)?;
        let mut right_matched = vec![false; right.len()];
        for l in &left {
            for (ri, r) in right.iter().enumerate() {
                let combined = merge_rows(l.clone(), r.clone());
                let passes = match on {
                    None => true,
                    Some(cond) => eval_expr(cond, &combined, &self.tables).map(|v| is_truthy(&v)).unwrap_or(false),
                };
                if passes { right_matched[ri] = true; }
            }
        }
        for (ri, r) in right.iter().enumerate() {
            if !right_matched[ri] {
                let null_left = null_row_like(left.first());
                result.push(merge_rows(null_left, r.clone()));
            }
        }
        Ok(result)
    }

    // ── Projection ────────────────────────────────────────────────────────────

    fn project_row(&self, cols: &[Expr], row: &Row, agg_vals: Option<&Row>) -> Result<Row> {
        let mut out = Map::new();
        for col_expr in cols {
            self.project_expr(col_expr, row, agg_vals, &mut out)?;
        }
        Ok(out)
    }

    fn project_expr(
        &self,
        expr: &Expr,
        row: &Row,
        agg_vals: Option<&Row>,
        out: &mut Row,
    ) -> Result<()> {
        match expr {
            Expr::Wildcard { table: None } => {
                // Emit all columns, stripping table qualifier and internal fields
                for (k, v) in row {
                    let bare = bare_col_name(k);
                    if bare == "row_key" || bare == "shard_id" { continue; }
                    out.entry(bare).or_insert_with(|| v.clone());
                }
            }
            Expr::Wildcard { table: Some(tbl) } => {
                let prefix = format!("{}.", tbl);
                for (k, v) in row {
                    if k.starts_with(&prefix) {
                        out.insert(k[prefix.len()..].to_string(), v.clone());
                    }
                }
            }
            Expr::Alias { expr: inner, alias } => {
                let val = if let Some(agg) = agg_vals {
                    agg.get(alias).cloned()
                        .or_else(|| eval_expr(inner, row, &self.tables).ok())
                        .unwrap_or(Value::Null)
                } else {
                    eval_expr(inner, row, &self.tables).unwrap_or(Value::Null)
                };
                out.insert(alias.clone(), val);
            }
            Expr::Aggregate { .. } => {
                // When grouping, aggregates are pre-computed into `agg_vals`.
                let val = agg_vals
                    .and_then(|agg| agg.get(&expr_output_name(expr)).cloned())
                    .unwrap_or(Value::Null);
                let name = expr_output_name(expr);
                out.insert(name, val);
            }
            other => {
                let val = eval_expr(other, row, &self.tables).unwrap_or(Value::Null);
                let name = expr_output_name(other);
                out.insert(name, val);
            }
        }
        Ok(())
    }

    // ── GROUP BY + aggregation ────────────────────────────────────────────────

    fn eval_group_by(&self, stmt: &SelectStmt, rows: Vec<Row>) -> Result<Vec<Row>> {
        fn eval_expr_with_aggs(
            expr: &Expr,
            row: &Row,
            tables: &Arc<TableStore>,
            agg_vals: &Row,
        ) -> Result<Value> {
            match expr {
                Expr::Aggregate { .. } => {
                    Ok(agg_vals
                        .get(&expr_output_name(expr))
                        .cloned()
                        .unwrap_or(Value::Null))
                }
                Expr::Alias { expr: inner, .. } => eval_expr_with_aggs(inner, row, tables, agg_vals),
                Expr::BinaryOp { left, op, right } => {
                    let lv = eval_expr_with_aggs(left, row, tables, agg_vals)?;
                    let rv = eval_expr_with_aggs(right, row, tables, agg_vals)?;
                    // Reuse existing binary operator implementation by evaluating with a fake row.
                    // This keeps type/coercion behavior consistent.
                    // (We temporarily evaluate by calling eval_binary on already-evaluated values.)
                    Ok(match op {
                        BinOp::Eq => Value::Bool(values_equal(&lv, &rv)),
                        BinOp::Ne => Value::Bool(!values_equal(&lv, &rv)),
                        BinOp::Lt => Value::Bool(compare_values_ord(&lv, &rv, false) == std::cmp::Ordering::Less),
                        BinOp::Le => Value::Bool(compare_values_ord(&lv, &rv, false) != std::cmp::Ordering::Greater),
                        BinOp::Gt => Value::Bool(compare_values_ord(&lv, &rv, false) == std::cmp::Ordering::Greater),
                        BinOp::Ge => Value::Bool(compare_values_ord(&lv, &rv, false) != std::cmp::Ordering::Less),
                        BinOp::Add => numeric_op(&lv, &rv, |a, b| a + b, |a, b| a + b),
                        BinOp::Sub => numeric_op(&lv, &rv, |a, b| a - b, |a, b| a - b),
                        BinOp::Mul => numeric_op(&lv, &rv, |a, b| a * b, |a, b| a * b),
                        BinOp::Div => {
                            if let (Some(a), Some(b)) = (as_f64(&lv), as_f64(&rv)) {
                                if b == 0.0 { Value::Null } else { json_f64(a / b) }
                            } else { Value::Null }
                        }
                        BinOp::Mod => {
                            if let (Some(a), Some(b)) = (as_i64(&lv), as_i64(&rv)) {
                                if b == 0 { Value::Null } else { Value::from(a % b) }
                            } else { Value::Null }
                        }
                        BinOp::Concat => {
                            let a = value_to_string(&lv);
                            let b = value_to_string(&rv);
                            Value::String(a + &b)
                        }
                        BinOp::And | BinOp::Or => unreachable!(),
                    })
                }
                other => eval_expr(other, row, tables),
            }
        }

        // Group rows by the GROUP BY key
        let mut groups: Vec<(Vec<Value>, Vec<Row>)> = Vec::new();
        let mut key_index: HashMap<String, usize> = HashMap::new();

        for row in rows {
            let key: Vec<Value> = stmt.group_by.iter()
                .map(|e| eval_expr(e, &row, &self.tables).unwrap_or(Value::Null))
                .collect();
            let key_str = serde_json::to_string(&key).unwrap_or_default();
            let idx = key_index.entry(key_str.clone()).or_insert_with(|| {
                groups.push((key.clone(), Vec::new()));
                groups.len() - 1
            });
            groups[*idx].1.push(row);
        }

        // If there are no input rows but we have aggregates (e.g. COUNT(*)),
        // SQL still produces a single row.
        if groups.is_empty() {
            let representative = Map::new();
            let group_rows: Vec<Row> = Vec::new();

            let mut agg_vals: Row = Map::new();
            for col_expr in &stmt.columns {
                self.compute_agg_expr(col_expr, &group_rows, &mut agg_vals, &representative)?;
            }
            if let Some(having) = &stmt.having {
                self.compute_agg_expr(having, &group_rows, &mut agg_vals, &representative)?;
                let ok = eval_expr_with_aggs(having, &representative, &self.tables, &agg_vals)
                    .map(|v| is_truthy(&v))
                    .unwrap_or(false);
                if !ok { return Ok(vec![]); }
            }

            let projected = self.project_row(&stmt.columns, &representative, Some(&agg_vals))?;
            return Ok(vec![projected]);
        }

        // For each group, compute aggregates and project
        let mut result = Vec::new();
        for (_group_key, group_rows) in &groups {
            let representative = group_rows.first().cloned().unwrap_or_default();

            let mut agg_vals: Row = Map::new();
            for col_expr in &stmt.columns {
                self.compute_agg_expr(col_expr, group_rows, &mut agg_vals, &representative)?;
            }
            if let Some(having) = &stmt.having {
                self.compute_agg_expr(having, group_rows, &mut agg_vals, &representative)?;
                let ok = eval_expr_with_aggs(having, &representative, &self.tables, &agg_vals)
                    .map(|v| is_truthy(&v))
                    .unwrap_or(false);
                if !ok { continue; }
            }

            let projected = self.project_row(&stmt.columns, &representative, Some(&agg_vals))?;
            result.push(projected);
        }

        Ok(result)
    }

    /// Walk an expression tree and evaluate any aggregate nodes, storing their
    /// results in `agg_vals` keyed by the expression's output name.
    fn compute_agg_expr(
        &self,
        expr: &Expr,
        group: &[Row],
        agg_vals: &mut Row,
        representative: &Row,
    ) -> Result<()> {
        match expr {
            Expr::Aggregate { func, distinct, arg } => {
                let name = expr_output_name(expr);
                if !agg_vals.contains_key(&name) {
                    let val = eval_aggregate(func, *distinct, arg.as_deref(), group, &self.tables)?;
                    agg_vals.insert(name, val);
                }
            }
            Expr::Alias { expr: inner, alias } => {
                self.compute_agg_expr(inner, group, agg_vals, representative)?;
                // Move the inner agg result to the alias name
                let inner_name = expr_output_name(inner);
                if let Some(v) = agg_vals.remove(&inner_name) {
                    agg_vals.insert(alias.clone(), v);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.compute_agg_expr(left, group, agg_vals, representative)?;
                self.compute_agg_expr(right, group, agg_vals, representative)?;
            }
            Expr::UnaryOp { expr: inner, .. } => {
                self.compute_agg_expr(inner, group, agg_vals, representative)?;
            }
            Expr::Function { args, .. } => {
                for a in args { self.compute_agg_expr(a, group, agg_vals, representative)?; }
            }
            _ => {} // literal, column ref — no aggregates inside
        }
        Ok(())
    }

    // ── INSERT ────────────────────────────────────────────────────────────────

    fn execute_insert(&self, stmt: &InsertStmt) -> Result<QueryResult> {
        let mut count = 0;
        for row_exprs in &stmt.values {
            let mut obj = Map::new();
            for (i, expr) in row_exprs.iter().enumerate() {
                let val = eval_expr(expr, &Map::new(), &self.tables)?;
                let col_name = if i < stmt.columns.len() {
                    stmt.columns[i].clone()
                } else {
                    format!("col{}", i)
                };
                obj.insert(col_name, val);
            }
            // Use 'id' or 'row_key' as the DashMap key; fall back to auto-generated
            let key = obj.get("id")
                .or_else(|| obj.get("row_key"))
                .and_then(|v| v.as_str())
                .map(String::from)
                .unwrap_or_else(|| {
                    use std::time::{SystemTime, UNIX_EPOCH};
                    format!("row_{}", SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0))
                });
            self.tables.set_row(stmt.table.clone(), key, Value::Object(obj))?;
            count += 1;
        }
        Ok(QueryResult::dml(count))
    }

    // ── UPDATE ────────────────────────────────────────────────────────────────

    fn execute_update(&self, stmt: &UpdateStmt) -> Result<QueryResult> {
        let raw = self.tables.list_rows_with_keys(&stmt.table)?;
        let mut count = 0;
        for (key, val) in raw {
            let row = val.as_object().cloned().unwrap_or_default();
            let passes = match &stmt.where_ {
                None => true,
                Some(cond) => eval_expr(cond, &row, &self.tables).map(|v| is_truthy(&v)).unwrap_or(false),
            };
            if passes {
                let mut updated = row;
                for (col, expr) in &stmt.sets {
                    let new_val = eval_expr(expr, &updated, &self.tables)?;
                    updated.insert(col.clone(), new_val);
                }
                self.tables.set_row(stmt.table.clone(), key, Value::Object(updated))?;
                count += 1;
            }
        }
        Ok(QueryResult::dml(count))
    }

    // ── DELETE ────────────────────────────────────────────────────────────────

    fn execute_delete(&self, stmt: &DeleteStmt) -> Result<QueryResult> {
        let raw = self.tables.list_rows_with_keys(&stmt.table)?;
        let mut count = 0;
        for (key, val) in raw {
            let row = val.as_object().cloned().unwrap_or_default();
            let passes = match &stmt.where_ {
                None => true,
                Some(cond) => eval_expr(cond, &row, &self.tables).map(|v| is_truthy(&v)).unwrap_or(false),
            };
            if passes {
                self.tables.delete_row(&stmt.table, &key)?;
                count += 1;
            }
        }
        Ok(QueryResult::dml(count))
    }
}

// ── Expression evaluator ──────────────────────────────────────────────────────

pub fn eval_expr(expr: &Expr, row: &Row, tables: &Arc<TableStore>) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),

        Expr::Column { table, name } => {
            // Try qualified key first, then bare name
            if let Some(tbl) = table {
                let qualified = format!("{}.{}", tbl, name);
                if let Some(v) = row.get(&qualified) { return Ok(v.clone()); }
            }
            // Try bare name
            if let Some(v) = row.get(name) { return Ok(v.clone()); }
            // Try any qualified key that ends with ".{name}"
            let suffix = format!(".{}", name);
            for (k, v) in row {
                if k.ends_with(&suffix) { return Ok(v.clone()); }
            }
            Ok(Value::Null)
        }

        Expr::Wildcard { .. } => Ok(Value::Object(row.clone())),

        Expr::BinaryOp { left, op, right } => {
            eval_binary(op, left, right, row, tables)
        }

        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, row, tables)?;
            match op {
                UnaryOp::Neg => Ok(negate_value(v)),
                UnaryOp::Pos => Ok(v),
                UnaryOp::Not => Ok(Value::Bool(!is_truthy(&v))),
            }
        }

        Expr::IsNull { expr, negated } => {
            let v = eval_expr(expr, row, tables)?;
            let is_null = v.is_null();
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }

        Expr::InList { expr, list, negated } => {
            let needle = eval_expr(expr, row, tables)?;
            let found = list.iter().any(|item| {
                eval_expr(item, row, tables).map(|v| values_equal(&needle, &v)).unwrap_or(false)
            });
            Ok(Value::Bool(if *negated { !found } else { found }))
        }

        Expr::InSubquery { expr, query, negated } => {
            let needle = eval_expr(expr, row, tables)?;
            let exec = Executor::new(tables.clone());
            let result = exec.execute_select(query)?;
            let found = result.rows.iter().any(|r| {
                r.values().any(|v| values_equal(&needle, v))
            });
            Ok(Value::Bool(if *negated { !found } else { found }))
        }

        Expr::Between { expr, low, high, negated } => {
            let v = eval_expr(expr, row, tables)?;
            let lo = eval_expr(low, row, tables)?;
            let hi = eval_expr(high, row, tables)?;
            let in_range = compare_values_ord(&v, &lo, false) != std::cmp::Ordering::Less
                        && compare_values_ord(&v, &hi, false) != std::cmp::Ordering::Greater;
            Ok(Value::Bool(if *negated { !in_range } else { in_range }))
        }

        Expr::Like { expr, pattern, negated } => {
            let val = eval_expr(expr, row, tables)?;
            let pat = eval_expr(pattern, row, tables)?;
            let result = match (&val, &pat) {
                (Value::String(s), Value::String(p)) => like_match(s, p),
                _ => false,
            };
            Ok(Value::Bool(if *negated { !result } else { result }))
        }

        Expr::Aggregate { .. } => {
            // In non-grouped contexts aggregates should have been resolved earlier.
            Ok(Value::Null)
        }

        Expr::Function { name, args } => {
            eval_function(name, args, row, tables)
        }

        Expr::Case { operand, branches, else_ } => {
            let base = operand.as_ref().map(|e| eval_expr(e, row, tables)).transpose()?;
            for (cond, then) in branches {
                let matches = if let Some(ref b) = base {
                    // Simple CASE: compare operand to WHEN value
                    let w = eval_expr(cond, row, tables)?;
                    values_equal(b, &w)
                } else {
                    // Searched CASE: evaluate WHEN as boolean
                    eval_expr(cond, row, tables).map(|v| is_truthy(&v)).unwrap_or(false)
                };
                if matches { return eval_expr(then, row, tables); }
            }
            else_.as_ref().map(|e| eval_expr(e, row, tables)).unwrap_or(Ok(Value::Null))
        }

        Expr::Subquery(query) => {
            let exec = Executor::new(tables.clone());
            let result = exec.execute_select(query)?;
            // Scalar subquery: return first column of first row
            Ok(result.rows.into_iter().next()
                .and_then(|r| r.into_values().next())
                .unwrap_or(Value::Null))
        }

        Expr::Exists { query, negated } => {
            let exec = Executor::new(tables.clone());
            let result = exec.execute_select(query)?;
            let exists = !result.rows.is_empty();
            Ok(Value::Bool(if *negated { !exists } else { exists }))
        }

        Expr::Alias { expr, .. } => eval_expr(expr, row, tables),
    }
}

    fn eval_binary(op: &BinOp, left: &Expr, right: &Expr, row: &Row, tables: &Arc<TableStore>) -> Result<Value> {
        // Short-circuit AND / OR using boolean truthiness.
        match op {
            BinOp::And => {
                let lv = eval_expr(left, row, tables)?;
                if !is_truthy(&lv) { return Ok(Value::Bool(false)); }
                return Ok(Value::Bool(is_truthy(&eval_expr(right, row, tables)?)));
            }
            BinOp::Or => {
                let lv = eval_expr(left, row, tables)?;
                if is_truthy(&lv) { return Ok(Value::Bool(true)); }
                return Ok(Value::Bool(is_truthy(&eval_expr(right, row, tables)?)));
            }
            _ => {}
        }

    let lv = eval_expr(left, row, tables)?;
    let rv = eval_expr(right, row, tables)?;

    Ok(match op {
        BinOp::Eq  => Value::Bool(values_equal(&lv, &rv)),
        BinOp::Ne  => Value::Bool(!values_equal(&lv, &rv)),

        BinOp::Lt => {
            if lv.is_null() || rv.is_null() { Value::Bool(false) }
            else { Value::Bool(compare_values_ord(&lv, &rv, false) == std::cmp::Ordering::Less) }
        }
        BinOp::Le => {
            if lv.is_null() || rv.is_null() { Value::Bool(false) }
            else { Value::Bool(compare_values_ord(&lv, &rv, false) != std::cmp::Ordering::Greater) }
        }
        BinOp::Gt => {
            if lv.is_null() || rv.is_null() { Value::Bool(false) }
            else { Value::Bool(compare_values_ord(&lv, &rv, false) == std::cmp::Ordering::Greater) }
        }
        BinOp::Ge => {
            if lv.is_null() || rv.is_null() { Value::Bool(false) }
            else { Value::Bool(compare_values_ord(&lv, &rv, false) != std::cmp::Ordering::Less) }
        }

        BinOp::Add => numeric_op(&lv, &rv, |a, b| a + b, |a, b| a + b),
        BinOp::Sub => numeric_op(&lv, &rv, |a, b| a - b, |a, b| a - b),
        BinOp::Mul => numeric_op(&lv, &rv, |a, b| a * b, |a, b| a * b),
        BinOp::Div => {
            if let (Some(a), Some(b)) = (as_f64(&lv), as_f64(&rv)) {
                if b == 0.0 { Value::Null } else { json_f64(a / b) }
            } else { Value::Null }
        }
        BinOp::Mod => {
            if let (Some(a), Some(b)) = (as_i64(&lv), as_i64(&rv)) {
                if b == 0 { Value::Null } else { Value::from(a % b) }
            } else { Value::Null }
        }
        BinOp::Concat => {
            let a = value_to_string(&lv);
            let b = value_to_string(&rv);
            Value::String(a + &b)
        }
        BinOp::And | BinOp::Or => unreachable!(),
    })
}

// ── Aggregate evaluator ───────────────────────────────────────────────────────

fn eval_aggregate(
    func: &AggFunc,
    distinct: bool,
    arg: Option<&Expr>,
    rows: &[Row],
    tables: &Arc<TableStore>,
) -> Result<Value> {
    match func {
        AggFunc::Count => {
            if arg.is_none() {
                return Ok(Value::from(rows.len() as i64));
            }
            let arg = arg.unwrap();
            let mut vals: Vec<Value> = rows.iter()
                .filter_map(|row| eval_expr(arg, row, tables).ok())
                .filter(|v| !v.is_null())
                .collect();
            if distinct { dedup_values(&mut vals); }
            Ok(Value::from(vals.len() as i64))
        }
        AggFunc::Sum => {
            let arg = arg.ok_or_else(|| NeonDBError::invalid_argument("SUM requires an argument"))?;
            let mut total = 0.0f64;
            let mut any = false;
            let mut is_int = true;
            for row in rows {
                if let Ok(v) = eval_expr(arg, row, tables) {
                    if let Some(n) = as_f64(&v) {
                        if !v.is_i64() && !v.is_u64() { is_int = false; }
                        total += n;
                        any = true;
                    }
                }
            }
            if !any { return Ok(Value::Null); }
            if is_int { Ok(Value::from(total as i64)) } else { Ok(json_f64(total)) }
        }
        AggFunc::Avg => {
            let arg = arg.ok_or_else(|| NeonDBError::invalid_argument("AVG requires an argument"))?;
            let mut total = 0.0f64;
            let mut count = 0usize;
            for row in rows {
                if let Ok(v) = eval_expr(arg, row, tables) {
                    if let Some(n) = as_f64(&v) { total += n; count += 1; }
                }
            }
            if count == 0 { Ok(Value::Null) } else { Ok(json_f64(total / count as f64)) }
        }
        AggFunc::Min => {
            let arg = arg.ok_or_else(|| NeonDBError::invalid_argument("MIN requires an argument"))?;
            let vals: Vec<Value> = rows.iter().filter_map(|r| eval_expr(arg, r, tables).ok()).filter(|v| !v.is_null()).collect();
            Ok(vals.into_iter().min_by(|a, b| compare_values_ord(a, b, false)).unwrap_or(Value::Null))
        }
        AggFunc::Max => {
            let arg = arg.ok_or_else(|| NeonDBError::invalid_argument("MAX requires an argument"))?;
            let vals: Vec<Value> = rows.iter().filter_map(|r| eval_expr(arg, r, tables).ok()).filter(|v| !v.is_null()).collect();
            Ok(vals.into_iter().max_by(|a, b| compare_values_ord(a, b, false)).unwrap_or(Value::Null))
        }
    }
}

// ── Scalar function evaluator ─────────────────────────────────────────────────

fn eval_function(name: &str, args: &[Expr], row: &Row, tables: &Arc<TableStore>) -> Result<Value> {
    let eval = |i: usize| -> Value {
        args.get(i).and_then(|e| eval_expr(e, row, tables).ok()).unwrap_or(Value::Null)
    };

    Ok(match name {
        "upper"  => Value::String(value_to_string(&eval(0)).to_uppercase()),
        "lower"  => Value::String(value_to_string(&eval(0)).to_lowercase()),
        "length" => {
            let s = value_to_string(&eval(0));
            Value::from(s.chars().count() as i64)
        }
        "trim"   => Value::String(value_to_string(&eval(0)).trim().to_string()),
        "ltrim"  => Value::String(value_to_string(&eval(0)).trim_start().to_string()),
        "rtrim"  => Value::String(value_to_string(&eval(0)).trim_end().to_string()),
        "replace" => {
            let s = value_to_string(&eval(0));
            let from = value_to_string(&eval(1));
            let to = value_to_string(&eval(2));
            Value::String(s.replace(&from as &str, &to as &str))
        }
        "substr" | "substring" => {
            let s = value_to_string(&eval(0));
            let chars: Vec<char> = s.chars().collect();
            let start = as_i64(&eval(1)).unwrap_or(1).max(1) as usize - 1;
            let result: String = if args.len() >= 3 {
                let len = as_i64(&eval(2)).unwrap_or(0).max(0) as usize;
                chars.iter().skip(start).take(len).collect()
            } else {
                chars.iter().skip(start).collect()
            };
            Value::String(result)
        }
        "round" => {
            let n = as_f64(&eval(0)).unwrap_or(0.0);
            let decimals = as_i64(&eval(1)).unwrap_or(0).max(0) as u32;
            let factor = 10f64.powi(decimals as i32);
            json_f64((n * factor).round() / factor)
        }
        "floor" => as_f64(&eval(0)).map(|n| json_f64(n.floor())).unwrap_or(Value::Null),
        "ceil"  => as_f64(&eval(0)).map(|n| json_f64(n.ceil())).unwrap_or(Value::Null),
        "abs"   => {
            let v = eval(0);
            if let Some(i) = as_i64(&v) { Value::from(i.abs()) }
            else if let Some(f) = as_f64(&v) { json_f64(f.abs()) }
            else { Value::Null }
        }
        "coalesce" => {
            for expr in args {
                let v = eval_expr(expr, row, tables)?;
                if !v.is_null() { return Ok(v); }
            }
            Value::Null
        }
        "nullif" => {
            let a = eval(0);
            let b = eval(1);
            if values_equal(&a, &b) { Value::Null } else { a }
        }
        "concat" => {
            let parts: Vec<String> = args.iter()
                .filter_map(|e| eval_expr(e, row, tables).ok())
                .map(|v| value_to_string(&v))
                .collect();
            Value::String(parts.join(""))
        }
        "now" => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
            Value::from(ts)
        }
        s if s.starts_with("cast::") => {
            let type_name = &s["cast::".len()..];
            let v = eval(0);
            cast_value(v, type_name)
        }
        unknown => {
            return Err(NeonDBError::invalid_argument(format!("Unknown function: {}", unknown)));
        }
    })
}

fn cast_value(v: Value, type_name: &str) -> Value {
    match type_name {
        "integer" | "int" | "bigint" => as_i64(&v).map(Value::from).unwrap_or(Value::Null),
        "float" | "real" | "double"  => as_f64(&v).map(json_f64).unwrap_or(Value::Null),
        "text" | "varchar" | "string" => Value::String(value_to_string(&v)),
        "boolean" | "bool" => Value::Bool(is_truthy(&v)),
        _ => v,
    }
}

// ── Helper functions ──────────────────────────────────────────────────────────

fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b)   => *b,
        Value::Null      => false,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a)  => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true, // NULL = NULL in our engine (SpacetimeDB style)
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Number(an), Value::Number(bn)) => {
            if let (Some(ai), Some(bi)) = (an.as_i64(), bn.as_i64()) { ai == bi }
            else if let (Some(af), Some(bf)) = (an.as_f64(), bn.as_f64()) { af == bf }
            else { false }
        }
        _ => a == b,
    }
}

fn compare_values_ord(a: &Value, b: &Value, nulls_first: bool) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _)  => if nulls_first { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater },
        (_, Value::Null)  => if nulls_first { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less },
        (Value::Number(an), Value::Number(bn)) => {
            let af = an.as_f64().unwrap_or(0.0);
            let bf = bn.as_f64().unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(as_), Value::String(bs)) => as_.cmp(bs),
        (Value::Bool(ab), Value::Bool(bb)) => ab.cmp(bb),
        _ => a.to_string().cmp(&b.to_string()),
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        Value::Bool(b)   => Some(if *b { 1.0 } else { 0.0 }),
        _                => None,
    }
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.parse().ok(),
        Value::Bool(b)   => Some(if *b { 1 } else { 0 }),
        _                => None,
    }
}

fn negate_value(v: Value) -> Value {
    match &v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() { Value::from(-i) }
            else if let Some(f) = n.as_f64() { json_f64(-f) }
            else { Value::Null }
        }
        _ => Value::Null,
    }
}

fn numeric_op(l: &Value, r: &Value, int_op: fn(i64, i64) -> i64, float_op: fn(f64, f64) -> f64) -> Value {
    if let (Some(a), Some(b)) = (as_i64(l), as_i64(r)) {
        if l.is_i64() && r.is_i64() { return Value::from(int_op(a, b)); }
    }
    if let (Some(a), Some(b)) = (as_f64(l), as_f64(r)) {
        return json_f64(float_op(a, b));
    }
    Value::Null
}

fn json_f64(f: f64) -> Value {
    serde_json::Number::from_f64(f)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null      => String::new(),
        other            => other.to_string(),
    }
}

/// Simple SQL LIKE pattern matching.
/// `%` matches any sequence of characters; `_` matches a single character.
fn like_match(s: &str, pattern: &str) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_dp(&s, &p, 0, 0)
}

fn like_dp(s: &[char], p: &[char], si: usize, pi: usize) -> bool {
    if pi == p.len() { return si == s.len(); }
    if p[pi] == '%' {
        // Match zero or more characters
        for k in si..=s.len() {
            if like_dp(s, p, k, pi + 1) { return true; }
        }
        false
    } else if pi < p.len() && si < s.len() && (p[pi] == '_' || p[pi] == s[si]) {
        like_dp(s, p, si + 1, pi + 1)
    } else {
        false
    }
}

fn has_aggregate(cols: &[Expr]) -> bool {
    cols.iter().any(|e| expr_has_aggregate(e))
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Alias { expr, .. } => expr_has_aggregate(expr),
        Expr::BinaryOp { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::UnaryOp { expr, .. } => expr_has_aggregate(expr),
        Expr::Function { args, .. } => args.iter().any(expr_has_aggregate),
        _ => false,
    }
}

fn expr_output_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::Alias { alias, .. } => alias.clone(),
        Expr::Aggregate { func, .. } => format!("{:?}", func).to_lowercase(),
        Expr::Literal(v) => v.to_string(),
        Expr::BinaryOp { left, op, right } => {
            format!("{}_{}_{}", expr_output_name(left), format!("{:?}", op).to_lowercase(), expr_output_name(right))
        }
        Expr::Function { name, .. } => name.clone(),
        _ => "expr".to_string(),
    }
}

fn bare_col_name(qualified: &str) -> String {
    if let Some(pos) = qualified.rfind('.') {
        qualified[pos + 1..].to_string()
    } else {
        qualified.to_string()
    }
}

/// Prefix all keys in a row value with `table_alias.`
fn qualify_row(val: Value, alias: &str) -> Row {
    let mut out = Map::new();
    if let Value::Object(obj) = val {
        for (k, v) in obj {
            out.insert(format!("{}.{}", alias, k), v);
        }
    }
    out
}

fn cross_product(left: Vec<Row>, right: Vec<Row>) -> Vec<Row> {
    let mut result = Vec::with_capacity(left.len() * right.len().max(1));
    for l in &left {
        for r in &right {
            result.push(merge_rows(l.clone(), r.clone()));
        }
    }
    result
}

fn merge_rows(mut left: Row, right: Row) -> Row {
    left.extend(right);
    left
}

fn null_row_like(template: Option<&Row>) -> Row {
    match template {
        None => Map::new(),
        Some(t) => t.keys().map(|k| (k.clone(), Value::Null)).collect(),
    }
}

fn dedup_values(vals: &mut Vec<Value>) {
    let mut seen = std::collections::HashSet::new();
    vals.retain(|v| seen.insert(serde_json::to_string(v).unwrap_or_default()));
}

fn value_fingerprint(row: &Row) -> String {
    serde_json::to_string(row).unwrap_or_default()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::parse;

    fn make_store() -> Arc<TableStore> {
        Arc::new(TableStore::new())
    }

    fn exec(tables: &Arc<TableStore>, sql: &str) -> QueryResult {
        let stmt = parse(sql).expect("parse failed");
        Executor::new(tables.clone()).execute_statement(&stmt).expect("exec failed")
    }

    fn populate_players(ts: &Arc<TableStore>) {
        for (id, score, zone, active) in [
            ("alice", 200, "north", true),
            ("bob", 50, "south", true),
            ("carol", 150, "north", false),
            ("dave", 80, "south", true),
            ("eve", 300, "north", true),
        ] {
            ts.set_row("players".into(), id.into(),
                serde_json::json!({ "id": id, "score": score, "zone": zone, "active": active }))
                .unwrap();
        }
    }

    // ── Basic SELECT ─────────────────────────────────────────────────────────

    #[test]
    fn select_star_all_rows() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players");
        assert_eq!(r.rows.len(), 5);
    }

    #[test]
    fn select_with_where_eq() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players WHERE zone = 'north'");
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    fn select_with_where_gt() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players WHERE score > 100");
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    fn select_column_projection() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT id, score FROM players WHERE zone = 'north'");
        for row in &r.rows {
            assert!(row.contains_key("id"));
            assert!(row.contains_key("score"));
        }
    }

    #[test]
    fn select_with_alias() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT score AS pts FROM players WHERE id = 'alice'");
        assert_eq!(r.rows.len(), 1);
        assert!(r.rows[0].contains_key("pts"));
    }

    // ── WHERE operators ──────────────────────────────────────────────────────

    #[test]
    fn where_and() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players WHERE zone = 'north' AND score > 100");
        assert_eq!(r.rows.len(), 3); // alice(200), carol(150), eve(300)
    }

    #[test]
    fn where_or() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players WHERE zone = 'north' OR score > 200");
        // north: alice, carol, eve; score>200: eve(300). Union = alice, carol, eve
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    fn where_not() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players WHERE NOT active");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0]["id"], serde_json::json!("carol"));
    }

    #[test]
    fn where_in_list() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players WHERE zone IN ('north', 'east')");
        assert_eq!(r.rows.len(), 3); // alice, carol, eve
    }

    #[test]
    fn where_not_in() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players WHERE zone NOT IN ('north')");
        assert_eq!(r.rows.len(), 2); // bob, dave
    }

    #[test]
    fn where_between() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players WHERE score BETWEEN 80 AND 200");
        assert_eq!(r.rows.len(), 3); // alice(200), carol(150), dave(80)
    }

    #[test]
    fn where_like() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players WHERE id LIKE 'a%'");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0]["id"], serde_json::json!("alice"));
    }

    #[test]
    fn where_is_null() {
        let ts = make_store();
        ts.set_row("items".into(), "i1".into(), serde_json::json!({"name": "sword", "owner": null})).unwrap();
        ts.set_row("items".into(), "i2".into(), serde_json::json!({"name": "shield", "owner": "alice"})).unwrap();
        let r = exec(&ts, "SELECT * FROM items WHERE owner IS NULL");
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn where_is_not_null() {
        let ts = make_store();
        ts.set_row("items".into(), "i1".into(), serde_json::json!({"name": "sword", "owner": null})).unwrap();
        ts.set_row("items".into(), "i2".into(), serde_json::json!({"name": "shield", "owner": "alice"})).unwrap();
        let r = exec(&ts, "SELECT * FROM items WHERE owner IS NOT NULL");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0]["name"], serde_json::json!("shield"));
    }

    // ── ORDER BY, LIMIT, OFFSET ──────────────────────────────────────────────

    #[test]
    fn order_by_desc() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT id, score FROM players ORDER BY score DESC");
        assert_eq!(r.rows[0]["id"], serde_json::json!("eve"));   // 300
        assert_eq!(r.rows[4]["id"], serde_json::json!("bob"));   // 50
    }

    #[test]
    fn order_by_asc() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT id, score FROM players ORDER BY score ASC");
        assert_eq!(r.rows[0]["id"], serde_json::json!("bob"));   // 50
    }

    #[test]
    fn limit_caps_results() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players ORDER BY score DESC LIMIT 3");
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    fn offset_skips_rows() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players ORDER BY score ASC LIMIT 2 OFFSET 2");
        assert_eq!(r.rows.len(), 2);
    }

    // ── DISTINCT ─────────────────────────────────────────────────────────────

    #[test]
    fn distinct_zones() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT DISTINCT zone FROM players");
        assert_eq!(r.rows.len(), 2);
    }

    // ── Aggregates ───────────────────────────────────────────────────────────

    #[test]
    fn count_star() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT COUNT(*) FROM players");
        assert_eq!(r.rows[0]["count"], serde_json::json!(5));
    }

    #[test]
    fn count_with_where() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT COUNT(*) FROM players WHERE zone = 'north'");
        assert_eq!(r.rows[0]["count"], serde_json::json!(3));
    }

    #[test]
    fn sum_aggregate() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT SUM(score) FROM players");
        // 200+50+150+80+300 = 780
        assert_eq!(r.rows[0]["sum"], serde_json::json!(780));
    }

    #[test]
    fn avg_aggregate() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT AVG(score) FROM players");
        let avg = r.rows[0]["avg"].as_f64().unwrap();
        assert!((avg - 156.0).abs() < 0.01);
    }

    #[test]
    fn min_max_aggregate() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT MIN(score), MAX(score) FROM players");
        assert_eq!(r.rows[0]["min"], serde_json::json!(50));
        assert_eq!(r.rows[0]["max"], serde_json::json!(300));
    }

    // ── GROUP BY + HAVING ─────────────────────────────────────────────────────

    #[test]
    fn group_by_zone() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT zone, COUNT(*) AS n FROM players GROUP BY zone");
        assert_eq!(r.rows.len(), 2);
        let north = r.rows.iter().find(|row| row["zone"] == serde_json::json!("north")).unwrap();
        assert_eq!(north["n"], serde_json::json!(3));
    }

    #[test]
    fn group_by_having() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT zone, COUNT(*) AS n FROM players GROUP BY zone HAVING COUNT(*) > 1");
        // Both zones have > 1 player (north=3, south=2)
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn group_by_sum_having() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT zone, SUM(score) AS total FROM players GROUP BY zone HAVING SUM(score) > 400");
        // north: 200+150+300=650, south: 50+80=130. Only north > 400.
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0]["zone"], serde_json::json!("north"));
    }

    // ── JOIN ─────────────────────────────────────────────────────────────────

    #[test]
    fn inner_join() {
        let ts = make_store();
        ts.set_row("players".into(), "alice".into(), serde_json::json!({"id": "alice", "item_id": "sword"})).unwrap();
        ts.set_row("players".into(), "bob".into(),   serde_json::json!({"id": "bob",   "item_id": "axe"})).unwrap();
        ts.set_row("items".into(),   "sword".into(), serde_json::json!({"id": "sword", "damage": 30})).unwrap();
        ts.set_row("items".into(),   "shield".into(), serde_json::json!({"id": "shield", "damage": 0})).unwrap();

        let r = exec(&ts, "SELECT * FROM players p JOIN items i ON p.item_id = i.id");
        // Only alice+sword matches (bob has axe which doesn't exist in items)
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn left_join_preserves_unmatched_left() {
        let ts = make_store();
        ts.set_row("players".into(), "alice".into(), serde_json::json!({"id": "alice", "item_id": "sword"})).unwrap();
        ts.set_row("players".into(), "bob".into(),   serde_json::json!({"id": "bob",   "item_id": "axe"})).unwrap();
        ts.set_row("items".into(),   "sword".into(), serde_json::json!({"id": "sword", "damage": 30})).unwrap();

        let r = exec(&ts, "SELECT * FROM players p LEFT JOIN items i ON p.item_id = i.id");
        // alice+sword + bob+NULL = 2 rows
        assert_eq!(r.rows.len(), 2);
    }

    // ── Subqueries ────────────────────────────────────────────────────────────

    #[test]
    fn in_subquery() {
        let ts = make_store();
        populate_players(&ts);
        ts.set_row("vip_list".into(), "1".into(), serde_json::json!({"player_id": "alice"})).unwrap();
        ts.set_row("vip_list".into(), "2".into(), serde_json::json!({"player_id": "eve"})).unwrap();

        let r = exec(&ts, "SELECT * FROM players WHERE id IN (SELECT player_id FROM vip_list)");
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    fn scalar_subquery() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT * FROM players WHERE score = (SELECT MAX(score) FROM players)");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0]["id"], serde_json::json!("eve"));
    }

    // ── Scalar functions ──────────────────────────────────────────────────────

    #[test]
    fn upper_lower_functions() {
        let ts = make_store();
        ts.set_row("words".into(), "w1".into(), serde_json::json!({"word": "Hello"})).unwrap();
        let r = exec(&ts, "SELECT UPPER(word) AS up, LOWER(word) AS lo FROM words");
        assert_eq!(r.rows[0]["up"], serde_json::json!("HELLO"));
        assert_eq!(r.rows[0]["lo"], serde_json::json!("hello"));
    }

    #[test]
    fn length_function() {
        let ts = make_store();
        ts.set_row("words".into(), "w1".into(), serde_json::json!({"word": "hello"})).unwrap();
        let r = exec(&ts, "SELECT LENGTH(word) AS len FROM words");
        assert_eq!(r.rows[0]["len"], serde_json::json!(5));
    }

    #[test]
    fn coalesce_function() {
        let ts = make_store();
        ts.set_row("t".into(), "r1".into(), serde_json::json!({"a": null, "b": "fallback"})).unwrap();
        let r = exec(&ts, "SELECT COALESCE(a, b, 'default') AS v FROM t");
        assert_eq!(r.rows[0]["v"], serde_json::json!("fallback"));
    }

    #[test]
    fn round_function() {
        let ts = make_store();
        ts.set_row("t".into(), "r1".into(), serde_json::json!({"n": 3.14159})).unwrap();
        let r = exec(&ts, "SELECT ROUND(n, 2) AS r FROM t");
        let rounded = r.rows[0]["r"].as_f64().unwrap();
        assert!((rounded - 3.14).abs() < 0.001);
    }

    // ── Arithmetic ───────────────────────────────────────────────────────────

    #[test]
    fn arithmetic_in_select() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT id, score * 2 AS double_score FROM players WHERE id = 'alice'");
        assert_eq!(r.rows[0]["double_score"], serde_json::json!(400));
    }

    // ── CASE expression ───────────────────────────────────────────────────────

    #[test]
    fn case_expression() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT id, CASE WHEN score > 200 THEN 'high' WHEN score > 100 THEN 'mid' ELSE 'low' END AS tier FROM players");
        assert_eq!(r.rows.len(), 5);
        let alice = r.rows.iter().find(|row| row.get("id") == Some(&serde_json::json!("alice"))).unwrap();
        assert_eq!(alice["tier"], serde_json::json!("mid")); // 200 is not > 200
    }

    // ── UNION ─────────────────────────────────────────────────────────────────

    #[test]
    fn union_all() {
        let ts = make_store();
        populate_players(&ts);
        let r = exec(&ts, "SELECT id FROM players WHERE zone = 'north' UNION ALL SELECT id FROM players WHERE zone = 'south'");
        assert_eq!(r.rows.len(), 5);
    }

    #[test]
    fn union_distinct() {
        let ts = make_store();
        populate_players(&ts);
        // alice appears in both (north AND score>100), but UNION deduplicates
        let r = exec(&ts, "SELECT id FROM players WHERE zone = 'north' UNION SELECT id FROM players WHERE score > 100");
        assert_eq!(r.rows.len(), 3); // alice, carol, eve (no duplicates)
    }

    // ── INSERT ────────────────────────────────────────────────────────────────

    #[test]
    fn insert_single_row() {
        let ts = make_store();
        exec(&ts, "INSERT INTO players (id, score, zone) VALUES ('frank', 120, 'east')");
        let r = exec(&ts, "SELECT * FROM players WHERE id = 'frank'");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0]["score"], serde_json::json!(120));
    }

    #[test]
    fn insert_multi_row() {
        let ts = make_store();
        exec(&ts, "INSERT INTO players (id, score) VALUES ('a', 1), ('b', 2), ('c', 3)");
        let r = exec(&ts, "SELECT COUNT(*) FROM players");
        assert_eq!(r.rows[0]["count"], serde_json::json!(3));
    }

    // ── UPDATE ────────────────────────────────────────────────────────────────

    #[test]
    fn update_with_where() {
        let ts = make_store();
        populate_players(&ts);
        exec(&ts, "UPDATE players SET score = 999 WHERE id = 'alice'");
        let r = exec(&ts, "SELECT score FROM players WHERE id = 'alice'");
        assert_eq!(r.rows[0]["score"], serde_json::json!(999));
    }

    #[test]
    fn update_rows_affected() {
        let ts = make_store();
        populate_players(&ts);
        let r = Executor::new(ts.clone()).execute_statement(
            &parse("UPDATE players SET active = false WHERE zone = 'north'").unwrap()
        ).unwrap();
        assert_eq!(r.rows_affected, 3);
    }

    // ── DELETE ────────────────────────────────────────────────────────────────

    #[test]
    fn delete_with_where() {
        let ts = make_store();
        populate_players(&ts);
        exec(&ts, "DELETE FROM players WHERE zone = 'south'");
        let r = exec(&ts, "SELECT COUNT(*) FROM players");
        assert_eq!(r.rows[0]["count"], serde_json::json!(3));
    }

    #[test]
    fn delete_all() {
        let ts = make_store();
        populate_players(&ts);
        exec(&ts, "DELETE FROM players");
        let r = exec(&ts, "SELECT COUNT(*) FROM players");
        assert_eq!(r.rows[0]["count"], serde_json::json!(0));
    }

    // ── Valueless SELECT ─────────────────────────────────────────────────────

    #[test]
    fn select_arithmetic_no_table() {
        let ts = make_store();
        let r = exec(&ts, "SELECT 1 + 1");
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    fn select_string_literal() {
        let ts = make_store();
        let r = exec(&ts, "SELECT 'hello' AS greeting");
        assert_eq!(r.rows[0]["greeting"], serde_json::json!("hello"));
    }
}

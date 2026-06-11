//! PostgreSQL type system glue: column types, wire OIDs, text encoding.

use crate::mvcc::Scalar;
use serde::{Deserialize, Serialize};

// PostgreSQL built-in type OIDs (pg_type.oid).
pub const OID_BOOL: u32 = 16;
pub const OID_INT8: u32 = 20;
pub const OID_INT4: u32 = 23;
pub const OID_TEXT: u32 = 25;
pub const OID_FLOAT8: u32 = 701;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColType {
    Bool,
    Int,
    Float,
    Text,
}

impl ColType {
    pub fn oid(self) -> u32 {
        match self {
            ColType::Bool => OID_BOOL,
            ColType::Int => OID_INT8,
            ColType::Float => OID_FLOAT8,
            ColType::Text => OID_TEXT,
        }
    }

    pub fn from_sql(dt: &sqlparser::ast::DataType) -> ColType {
        use sqlparser::ast::DataType as DT;
        match dt {
            DT::Boolean | DT::Bool => ColType::Bool,
            DT::TinyInt(_) | DT::SmallInt(_) | DT::Int(_) | DT::Integer(_) | DT::BigInt(_)
            | DT::Int2(_) | DT::Int4(_) | DT::Int8(_) | DT::UnsignedInt(_)
            | DT::UnsignedBigInt(_) | DT::UnsignedSmallInt(_) => ColType::Int,
            DT::Real | DT::Float(_) | DT::Double | DT::DoublePrecision | DT::Float4
            | DT::Float8 | DT::Numeric(_) | DT::Decimal(_) | DT::Dec(_) => ColType::Float,
            _ => ColType::Text,
        }
    }

    /// Best-effort coercion of a scalar into this column type.
    pub fn coerce(self, v: Scalar) -> Scalar {
        match (self, v) {
            (_, Scalar::Null) => Scalar::Null,
            (ColType::Bool, Scalar::Bool(b)) => Scalar::Bool(b),
            (ColType::Bool, Scalar::Int(i)) => Scalar::Bool(i != 0),
            (ColType::Bool, Scalar::Text(t)) => {
                let l = t.to_ascii_lowercase();
                Scalar::Bool(l == "t" || l == "true" || l == "1" || l == "yes" || l == "on")
            }
            (ColType::Int, Scalar::Int(i)) => Scalar::Int(i),
            (ColType::Int, Scalar::Float(f)) => Scalar::Int(f as i64),
            (ColType::Int, Scalar::Bool(b)) => Scalar::Int(b as i64),
            (ColType::Int, Scalar::Text(t)) => {
                t.trim().parse::<i64>().map(Scalar::Int).unwrap_or(Scalar::Text(t))
            }
            (ColType::Float, Scalar::Float(f)) => Scalar::Float(f),
            (ColType::Float, Scalar::Int(i)) => Scalar::Float(i as f64),
            (ColType::Float, Scalar::Text(t)) => {
                t.trim().parse::<f64>().map(Scalar::Float).unwrap_or(Scalar::Text(t))
            }
            (ColType::Text, Scalar::Text(t)) => Scalar::Text(t),
            (ColType::Text, other) => Scalar::Text(scalar_to_text(&other).unwrap_or_default()),
            (_, other) => other,
        }
    }
}

/// PostgreSQL text-format encoding. None = SQL NULL.
pub fn scalar_to_text(v: &Scalar) -> Option<String> {
    match v {
        Scalar::Null => None,
        Scalar::Bool(b) => Some(if *b { "t".into() } else { "f".into() }),
        Scalar::Int(i) => Some(i.to_string()),
        Scalar::Float(f) => {
            if f.is_nan() {
                Some("NaN".into())
            } else if f.is_infinite() {
                Some(if *f > 0.0 { "Infinity".into() } else { "-Infinity".into() })
            } else {
                Some(format!("{f}"))
            }
        }
        Scalar::Text(t) => Some(t.clone()),
    }
}

/// OID a scalar would report when the column type is unknown (expressions).
pub fn scalar_oid(v: &Scalar) -> u32 {
    match v {
        Scalar::Bool(_) => OID_BOOL,
        Scalar::Int(_) => OID_INT8,
        Scalar::Float(_) => OID_FLOAT8,
        Scalar::Null | Scalar::Text(_) => OID_TEXT,
    }
}

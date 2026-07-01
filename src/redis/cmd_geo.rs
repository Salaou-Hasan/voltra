//! Geospatial commands (GEOADD/GEOPOS/GEODIST/GEOHASH/GEOSEARCH/GEORADIUS*).
//!
//! Redis stores geo data as a plain ZSET: each member's score is a 52-bit
//! interleaved (Morton-order) geohash of its (longitude, latitude), computed
//! over the fixed range lon in [-180, 180], lat in [-85.05112878, 85.05112878]
//! (the square that keeps the Mercator-ish projection bijective). No new
//! `Datum` variant is needed — GEOADD is ZADD with a computed score, and the
//! geo commands here decode that score back to a (lon, lat) pair and run a
//! haversine distance against it. This mirrors real Redis's implementation.

use super::engine::{read_zset, store_coll, Db};
use super::resp::Resp;
use super::util::{parse_f64, upper};
use crate::mvcc::Datum;
use bytes::Bytes;

const GEO_STEP: u32 = 26; // bits per dimension -> 52-bit interleaved score
const LON_MIN: f64 = -180.0;
const LON_MAX: f64 = 180.0;
const LAT_MIN: f64 = -85.05112878;
const LAT_MAX: f64 = 85.05112878;
const EARTH_RADIUS_M: f64 = 6372797.560856;

// ─────────────────────────────────────────────────────────────────────────────
// Geohash encode / decode (interleaved 26+26 bit Morton code, Redis-compatible)
// ─────────────────────────────────────────────────────────────────────────────

fn interleave64(xlo: u32, ylo: u32) -> u64 {
    fn spread(mut v: u64) -> u64 {
        v &= 0xFFFFFFFF;
        v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
        v = (v | (v << 8)) & 0x00FF00FF00FF00FF;
        v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F;
        v = (v | (v << 2)) & 0x3333333333333333;
        v = (v | (v << 1)) & 0x5555555555555555;
        v
    }
    spread(xlo as u64) | (spread(ylo as u64) << 1)
}

fn deinterleave64(interleaved: u64) -> (u32, u32) {
    fn squash(mut v: u64) -> u32 {
        v &= 0x5555555555555555;
        v = (v | (v >> 1)) & 0x3333333333333333;
        v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
        v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
        v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
        v = (v | (v >> 16)) & 0x00000000FFFFFFFF;
        v as u32
    }
    (squash(interleaved), squash(interleaved >> 1))
}

/// Encode (lon, lat) into a 52-bit geohash score, Redis-style.
fn geohash_encode(lon: f64, lat: f64) -> u64 {
    let lat_off = (lat - LAT_MIN) / (LAT_MAX - LAT_MIN);
    let lon_off = (lon - LON_MIN) / (LON_MAX - LON_MIN);
    let ilat = (lat_off * (1u64 << GEO_STEP) as f64) as u32;
    let ilon = (lon_off * (1u64 << GEO_STEP) as f64) as u32;
    interleave64(ilat, ilon)
}

/// Decode a 52-bit geohash score back to the (lon, lat) center of its cell.
fn geohash_decode(bits: u64) -> (f64, f64) {
    let (ilat, ilon) = deinterleave64(bits);
    let scale = (1u64 << GEO_STEP) as f64;
    let lat_min = LAT_MIN + (ilat as f64 / scale) * (LAT_MAX - LAT_MIN);
    let lat_max = LAT_MIN + ((ilat + 1) as f64 / scale) * (LAT_MAX - LAT_MIN);
    let lon_min = LON_MIN + (ilon as f64 / scale) * (LON_MAX - LON_MIN);
    let lon_max = LON_MIN + ((ilon + 1) as f64 / scale) * (LON_MAX - LON_MIN);
    ((lon_min + lon_max) / 2.0, (lat_min + lat_max) / 2.0)
}

fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (lat1r, lat2r) = (lat1.to_radians(), lat2.to_radians());
    let u = ((lat2r - lat1r) / 2.0).sin();
    let v = ((lon2 - lon1).to_radians() / 2.0).sin();
    2.0 * EARTH_RADIUS_M * (u * u + lat1r.cos() * lat2r.cos() * v * v).sqrt().asin()
}

fn unit_to_meters(unit: &str) -> Option<f64> {
    match unit.to_ascii_lowercase().as_str() {
        "m" => Some(1.0),
        "km" => Some(1000.0),
        "mi" => Some(1609.34),
        "ft" => Some(0.3048),
        _ => None,
    }
}

/// Standard 11-character base32 geohash string, matching Redis's GEOHASH command
/// (which re-encodes at full 26-bit precision on a [-180,180]x[-90,90] grid —
/// distinct from the internal score range — then formats as base32).
fn geohash_string(lon: f64, lat: f64) -> String {
    const ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let mut lat_range = (-90.0f64, 90.0f64);
    let mut lon_range = (-180.0f64, 180.0f64);
    let mut bits: Vec<bool> = Vec::with_capacity(55);
    let mut even = true;
    while bits.len() < 55 {
        if even {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon >= mid {
                bits.push(true);
                lon_range.0 = mid;
            } else {
                bits.push(false);
                lon_range.1 = mid;
            }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat >= mid {
                bits.push(true);
                lat_range.0 = mid;
            } else {
                bits.push(false);
                lat_range.1 = mid;
            }
        }
        even = !even;
    }
    let mut out = String::with_capacity(11);
    for chunk in bits.chunks(5) {
        let mut idx = 0usize;
        for (i, b) in chunk.iter().enumerate() {
            if *b {
                idx |= 1 << (4 - i);
            }
        }
        out.push(ALPHABET[idx] as char);
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// GEOADD (wraps ZADD)
// ─────────────────────────────────────────────────────────────────────────────

pub fn geoadd(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 4 {
        return Resp::arity("geoadd");
    }
    let mut i = 1;
    let (mut nx, mut xx, mut ch) = (false, false, false);
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "CH" => {
                ch = true;
                i += 1;
            }
            _ => break,
        }
    }
    if nx && xx {
        return Resp::err("ERR XX and NX options at the same time are not compatible");
    }
    let rest = &args[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(3) {
        return Resp::syntax();
    }
    let (mut z, exp) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut added = 0i64;
    let mut changed = 0i64;
    for triple in rest.chunks(3) {
        let (Some(lon), Some(lat)) = (parse_f64(&triple[0]), parse_f64(&triple[1])) else {
            return Resp::err("ERR value is not a valid float");
        };
        if !(LON_MIN..=LON_MAX).contains(&lon) || !(LAT_MIN..=LAT_MAX).contains(&lat) {
            return Resp::err(format!(
                "ERR invalid longitude,latitude pair {lon:.6},{lat:.6}"
            ));
        }
        let member = triple[2].clone();
        let exists = z.score(&member).is_some();
        if (nx && exists) || (xx && !exists) {
            continue;
        }
        let score = geohash_encode(lon, lat) as f64;
        let prev = z.insert(member, score);
        if prev.is_none() {
            added += 1;
        } else if prev != Some(score) {
            changed += 1;
        }
    }
    store_coll(db, ns, args[0].clone(), Datum::ZSet(z), exp);
    Resp::Int(if ch { added + changed } else { added })
}

// ─────────────────────────────────────────────────────────────────────────────
// GEOPOS / GEODIST / GEOHASH
// ─────────────────────────────────────────────────────────────────────────────

pub fn geopos(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() {
        return Resp::arity("geopos");
    }
    let (z, _) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    Resp::Array(
        args[1..]
            .iter()
            .map(|m| match z.score(m) {
                Some(score) => {
                    let (lon, lat) = geohash_decode(score as u64);
                    Resp::Array(vec![
                        Resp::bulk_str(format!("{lon:.17}")),
                        Resp::bulk_str(format!("{lat:.17}")),
                    ])
                }
                None => Resp::NullArray,
            })
            .collect(),
    )
}

pub fn geodist(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.len() < 3 || args.len() > 4 {
        return Resp::arity("geodist");
    }
    let unit = args
        .get(3)
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_else(|| "m".to_string());
    let Some(factor) = unit_to_meters(&unit) else {
        return Resp::err("ERR unsupported unit provided. please use M, KM, FT, MI");
    };
    let (z, _) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (Some(s1), Some(s2)) = (z.score(&args[1]), z.score(&args[2])) else {
        return Resp::Null;
    };
    let (lon1, lat1) = geohash_decode(s1 as u64);
    let (lon2, lat2) = geohash_decode(s2 as u64);
    let meters = haversine_m(lon1, lat1, lon2, lat2);
    Resp::bulk_str(format!("{:.4}", meters / factor))
}

pub fn geohash_cmd(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() {
        return Resp::arity("geohash");
    }
    let (z, _) = match read_zset(db, ns, &args[0]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    Resp::Array(
        args[1..]
            .iter()
            .map(|m| match z.score(m) {
                Some(score) => {
                    let (lon, lat) = geohash_decode(score as u64);
                    Resp::bulk_str(geohash_string(lon, lat))
                }
                None => Resp::Null,
            })
            .collect(),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// GEOSEARCH / GEORADIUS(BYMEMBER) — brute-force scan + haversine filter
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct SearchOpts {
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    any: bool,
    asc: Option<bool>,
    store_key: Option<Bytes>,
    storedist_key: Option<Bytes>,
}

enum Shape {
    Radius(f64),
    Box(f64, f64), // width, height (meters)
}

fn build_reply(
    member: Bytes,
    dist_m: f64,
    lon: f64,
    lat: f64,
    hash: u64,
    factor: f64,
    o: &SearchOpts,
) -> Resp {
    if !o.with_coord && !o.with_dist && !o.with_hash {
        return Resp::Bulk(member);
    }
    let mut parts = vec![Resp::Bulk(member)];
    if o.with_dist {
        parts.push(Resp::bulk_str(format!("{:.4}", dist_m / factor)));
    }
    if o.with_hash {
        parts.push(Resp::Int(hash as i64));
    }
    if o.with_coord {
        parts.push(Resp::Array(vec![
            Resp::bulk_str(format!("{lon:.17}")),
            Resp::bulk_str(format!("{lat:.17}")),
        ]));
    }
    Resp::Array(parts)
}

#[allow(clippy::too_many_arguments)]
fn run_search(
    db: &mut dyn Db,
    ns: u32,
    key: &Bytes,
    center_lon: f64,
    center_lat: f64,
    shape: Shape,
    factor: f64,
    o: SearchOpts,
) -> Resp {
    let (z, exp) = match read_zset(db, ns, key) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut hits: Vec<(Bytes, f64, f64, f64, u64)> = Vec::new(); // member, dist_m, lon, lat, hash
    for (member, score) in z.by_member.iter() {
        let hash = *score as u64;
        let (lon, lat) = geohash_decode(hash);
        let dist = haversine_m(center_lon, center_lat, lon, lat);
        let within = match shape {
            Shape::Radius(r) => dist <= r,
            Shape::Box(w, h) => {
                // Approximate box membership: convert lon/lat deltas to meters
                // using local scale factors (matches Redis's own approximation
                // for GEOSEARCH BYBOX, which is not a perfect geodesic box either).
                let dy = haversine_m(center_lon, center_lat, center_lon, lat);
                let dx = haversine_m(center_lon, center_lat, lon, center_lat);
                dx <= w / 2.0 && dy <= h / 2.0
            }
        };
        if within {
            hits.push((member.clone(), dist, lon, lat, hash));
        }
        if o.any {
            if let Some(c) = o.count {
                if hits.len() >= c {
                    break;
                }
            }
        }
    }
    if let Some(asc) = o.asc {
        hits.sort_by(|a, b| {
            if asc {
                a.1.partial_cmp(&b.1).unwrap()
            } else {
                b.1.partial_cmp(&a.1).unwrap()
            }
        });
    } else if o.store_key.is_some() || o.storedist_key.is_some() {
        // STORE without explicit ASC/DESC still needs a stable order — Redis
        // sorts ascending by distance in this case too.
        hits.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    }
    if let Some(c) = o.count {
        hits.truncate(c);
    }

    if let Some(store_key) = &o.store_key {
        let mut out = crate::mvcc::ZSet::default();
        for (member, _, _, _, hash) in &hits {
            out.insert(member.clone(), *hash as f64);
        }
        let n = out.len();
        store_coll(db, ns, store_key.clone(), Datum::ZSet(out), exp);
        return Resp::Int(n as i64);
    }
    if let Some(store_key) = &o.storedist_key {
        let mut out = crate::mvcc::ZSet::default();
        for (member, dist, _, _, _) in &hits {
            out.insert(member.clone(), *dist);
        }
        let n = out.len();
        store_coll(db, ns, store_key.clone(), Datum::ZSet(out), exp);
        return Resp::Int(n as i64);
    }

    Resp::Array(
        hits.into_iter()
            .map(|(m, d, lon, lat, h)| build_reply(m, d, lon, lat, h, factor, &o))
            .collect(),
    )
}

/// GEOSEARCH key <FROMMEMBER member | FROMLONLAT lon lat>
///           <BYRADIUS r unit | BYBOX w h unit>
///           [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
pub fn geosearch(db: &mut dyn Db, ns: u32, args: &[Bytes]) -> Resp {
    if args.is_empty() {
        return Resp::arity("geosearch");
    }
    let key = args[0].clone();
    let mut i = 1;
    let mut center: Option<(f64, f64)> = None;
    let mut from_member: Option<Bytes> = None;
    let mut shape: Option<(Shape, f64)> = None; // shape, unit factor
    let mut o = SearchOpts::default();

    while i < args.len() {
        match upper(&args[i]).as_str() {
            "FROMMEMBER" => {
                let Some(m) = args.get(i + 1) else {
                    return Resp::syntax();
                };
                from_member = Some(m.clone());
                i += 2;
            }
            "FROMLONLAT" => {
                let (Some(lon), Some(lat)) = (
                    args.get(i + 1).and_then(parse_f64),
                    args.get(i + 2).and_then(parse_f64),
                ) else {
                    return Resp::syntax();
                };
                center = Some((lon, lat));
                i += 3;
            }
            "BYRADIUS" => {
                let (Some(r), Some(unit)) = (args.get(i + 1).and_then(parse_f64), args.get(i + 2))
                else {
                    return Resp::syntax();
                };
                let Some(factor) = unit_to_meters(&String::from_utf8_lossy(unit)) else {
                    return Resp::err("ERR unsupported unit provided. please use M, KM, FT, MI");
                };
                shape = Some((Shape::Radius(r * factor), factor));
                i += 3;
            }
            "BYBOX" => {
                let (Some(w), Some(h), Some(unit)) = (
                    args.get(i + 1).and_then(parse_f64),
                    args.get(i + 2).and_then(parse_f64),
                    args.get(i + 3),
                ) else {
                    return Resp::syntax();
                };
                let Some(factor) = unit_to_meters(&String::from_utf8_lossy(unit)) else {
                    return Resp::err("ERR unsupported unit provided. please use M, KM, FT, MI");
                };
                shape = Some((Shape::Box(w * factor, h * factor), factor));
                i += 4;
            }
            "ASC" => {
                o.asc = Some(true);
                i += 1;
            }
            "DESC" => {
                o.asc = Some(false);
                i += 1;
            }
            "COUNT" => {
                let Some(n) = args
                    .get(i + 1)
                    .and_then(super::util::parse_i64)
                    .filter(|n| *n > 0)
                else {
                    return Resp::err("ERR COUNT must be > 0");
                };
                o.count = Some(n as usize);
                i += 2;
                if args.get(i).map(upper).as_deref() == Some("ANY") {
                    o.any = true;
                    i += 1;
                }
            }
            "WITHCOORD" => {
                o.with_coord = true;
                i += 1;
            }
            "WITHDIST" => {
                o.with_dist = true;
                i += 1;
            }
            "WITHHASH" => {
                o.with_hash = true;
                i += 1;
            }
            _ => return Resp::syntax(),
        }
    }
    let Some((shape, factor)) = shape else {
        return Resp::err("ERR exactly one of FROMMEMBER, FROMLONLAT is required");
    };

    let (z, _) = match read_zset(db, ns, &key) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (center_lon, center_lat) = if let Some(m) = from_member {
        match z.score(&m) {
            Some(s) => geohash_decode(s as u64),
            None => return Resp::err("ERR could not decode requested zset member"),
        }
    } else if let Some(c) = center {
        c
    } else {
        return Resp::err("ERR exactly one of FROMMEMBER, FROMLONLAT is required");
    };

    run_search(db, ns, &key, center_lon, center_lat, shape, factor, o)
}

/// Legacy GEORADIUS[_RO] key lon lat radius unit [options...]
/// and GEORADIUSBYMEMBER[_RO] key member radius unit [options...].
pub fn georadius(db: &mut dyn Db, ns: u32, args: &[Bytes], by_member: bool) -> Resp {
    let min_args = if by_member { 4 } else { 5 };
    if args.len() < min_args {
        return Resp::arity(if by_member {
            "georadiusbymember"
        } else {
            "georadius"
        });
    }
    let key = args[0].clone();
    let mut i = 1;
    let center_member;
    let mut center: Option<(f64, f64)> = None;
    if by_member {
        center_member = Some(args[i].clone());
        i += 1;
    } else {
        let (Some(lon), Some(lat)) = (
            args.get(i).and_then(parse_f64),
            args.get(i + 1).and_then(parse_f64),
        ) else {
            return Resp::err("ERR value is not a valid float");
        };
        center = Some((lon, lat));
        center_member = None;
        i += 2;
    }
    let Some(radius) = args.get(i).and_then(parse_f64) else {
        return Resp::err("ERR value is not a valid float");
    };
    let Some(unit) = args.get(i + 1) else {
        return Resp::syntax();
    };
    let Some(factor) = unit_to_meters(&String::from_utf8_lossy(unit)) else {
        return Resp::err("ERR unsupported unit provided. please use M, KM, FT, MI");
    };
    i += 2;

    let mut o = SearchOpts::default();
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "ASC" => {
                o.asc = Some(true);
                i += 1;
            }
            "DESC" => {
                o.asc = Some(false);
                i += 1;
            }
            "COUNT" => {
                let Some(n) = args
                    .get(i + 1)
                    .and_then(super::util::parse_i64)
                    .filter(|n| *n > 0)
                else {
                    return Resp::err("ERR COUNT must be > 0");
                };
                o.count = Some(n as usize);
                i += 2;
                if args.get(i).map(upper).as_deref() == Some("ANY") {
                    o.any = true;
                    i += 1;
                }
            }
            "WITHCOORD" => {
                o.with_coord = true;
                i += 1;
            }
            "WITHDIST" => {
                o.with_dist = true;
                i += 1;
            }
            "WITHHASH" => {
                o.with_hash = true;
                i += 1;
            }
            "STORE" => {
                let Some(k) = args.get(i + 1) else {
                    return Resp::syntax();
                };
                o.store_key = Some(k.clone());
                i += 2;
            }
            "STOREDIST" => {
                let Some(k) = args.get(i + 1) else {
                    return Resp::syntax();
                };
                o.storedist_key = Some(k.clone());
                i += 2;
            }
            _ => return Resp::syntax(),
        }
    }

    let (z, _) = match read_zset(db, ns, &key) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (center_lon, center_lat) = if let Some(m) = center_member {
        match z.score(&m) {
            Some(s) => geohash_decode(s as u64),
            None => return Resp::err("ERR could not decode requested zset member"),
        }
    } else {
        center.unwrap()
    };

    run_search(
        db,
        ns,
        &key,
        center_lon,
        center_lat,
        Shape::Radius(radius * factor),
        factor,
        o,
    )
}

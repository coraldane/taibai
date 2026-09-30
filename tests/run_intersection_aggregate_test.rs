//! Run-intersection aggregation correctness: the fast GROUP BY loop folds
//! dictionary-keyed, run-length-encoded arguments by intersecting id runs
//! with value runs. These tests pin it against a plain per-row reference
//! over synthetic clustered data, including boundary and null shapes.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn collect_rows(result: ExecResult) -> Vec<Vec<ScalarValue>> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|col_idx| batch.scalar(row_idx, col_idx).expect("scalar"))
                    .collect(),
            );
        }
    }
    rows
}

/// Clustered tick shape: each symbol holds one constant price per day, and
/// day boundaries cut value runs mid-symbol — the run intersection must
/// stitch (symbol × price-band) chunks exactly like per-row accumulation.
#[test]
fn run_intersection_group_by_matches_per_row_reference() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (sym VARCHAR, day INT, px DECIMAL(9,2), vol INT)",
        )
        .expect("create");

    // 4 symbols x 3 days; per (symbol, day) a constant px over a run of
    // rows; vols constant per run too (BitPacked on disk, plain in memory).
    let mut parts = Vec::new();
    let mut expected: std::collections::HashMap<String, (i64, i64, i64)> =
        std::collections::HashMap::new();
    for (sym, base) in [("aa", 10i64), ("bb", 20), ("cc", 30), ("dd", 40)] {
        for day in 0..3 {
            let px = base * 100 + day * 50; // raw decimal (scale 2)
            let vol = base + day;
            for i in 0..500 {
                parts.push(format!("('{}', {}, {}, {})", sym, day, px, vol + (i % 2)));
                let e = expected.entry(sym.to_string()).or_insert((0, 0i64, 0));
                // px literals bind to the column's scale (x100 raw).
                e.1 += px * 100;
                e.2 += 1;
                e.0 += vol + (i % 2);
            }
        }
    }
    let sql = format!("INSERT INTO ticks VALUES {}", parts.join(", "));
    engine.execute(&mut session, &sql).expect("insert");
    engine
        .execute(&mut session, "FLUSH TABLE ticks")
        .expect("flush");
    engine
        .execute(&mut session, "OPTIMIZE TABLE ticks")
        .expect("optimize");

    let grouped = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT sym, count(*), sum(vol), avg(px) FROM ticks GROUP BY sym ORDER BY sym",
            )
            .expect("group by"),
    );
    assert_eq!(grouped.len(), 4);
    for row in &grouped {
        let ScalarValue::VarChar(Some(sym)) = &row[0] else {
            panic!("symbol column");
        };
        let (vol_sum, px_sum, n) = expected[sym.as_str()];
        assert_eq!(row[1], ScalarValue::Int64(Some(n)), "{sym} count");
        assert_eq!(row[2], ScalarValue::Int64(Some(vol_sum)), "{sym} sum(vol)");
        match &row[3] {
            ScalarValue::Float64(Some(v)) => {
                // px_sum is raw-scaled (x100); AVG must divide the DECIMAL
                // scale back out at finalize.
                let want = px_sum as f64 / n as f64 / 100.0;
                assert!((v - want).abs() < 1e-9, "{sym} avg {v} vs {want}");
            }
            other => panic!("{sym} avg scalar {other:?}"),
        }
    }
    engine.segment_mgr().shutdown();
}

/// Zero-length and single-row edge cases plus a single run covering the
/// whole row group.
#[test]
fn run_intersection_handles_edge_shapes() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (sym VARCHAR, px INT)")
        .expect("create");
    engine
        .execute(
            &mut session,
            "INSERT INTO t VALUES ('a', 7), ('a', 7), ('b', 9)",
        )
        .expect("insert small");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush");
    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT sym, count(*), sum(px), avg(px) FROM t GROUP BY sym ORDER BY sym",
            )
            .expect("edge group by"),
    );
    assert_eq!(rows[0][0], ScalarValue::VarChar(Some("a".into())));
    assert_eq!(rows[0][1], ScalarValue::Int64(Some(2)));
    assert_eq!(rows[0][2], ScalarValue::Int64(Some(14)));
    match &rows[0][3] {
        ScalarValue::Float64(Some(v)) => assert!((v - 7.0).abs() < 1e-9),
        other => panic!("avg {other:?}"),
    }
    assert_eq!(rows[1][1], ScalarValue::Int64(Some(1)));
    assert_eq!(rows[1][2], ScalarValue::Int64(Some(9)));
    engine.segment_mgr().shutdown();
}

/// NULL group keys and NULL arguments bypass the run loop; the generic
/// per-row path must produce identical answers on the same table shape.
#[test]
fn run_intersection_nulls_fall_back_correctly() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (sym VARCHAR, px INT)")
        .expect("create");
    let mut parts = Vec::new();
    for i in 0..400 {
        let sym = if i % 100 == 99 { "NULL" } else { "'s'" };
        let px = if i % 50 == 49 { "NULL" } else { "5" };
        parts.push(format!("({}, {})", sym, px));
    }
    let sql = format!("INSERT INTO t VALUES {}", parts.join(", "));
    engine.execute(&mut session, &sql).expect("insert");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush");

    let grouped = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT sym, count(*), count(px), sum(px), avg(px) FROM t GROUP BY sym ORDER BY sym",
            )
            .expect("null group by"),
    );
    // One NULL-key row group and one 's' group; verify against arithmetic.
    let null_row = grouped
        .iter()
        .find(|r| r[0].is_null())
        .unwrap_or_else(|| panic!("null group missing: {grouped:?}"));
    let s_row = grouped
        .iter()
        .find(|r| matches!(&r[0], ScalarValue::VarChar(Some(v)) if v == "s"))
        .unwrap();
    // NULL keys: 4 rows (i=99,199,299,399); their px values are NULL only
    // when i%50==49 -> i=99,199,299,399 all NULL px -> count(px)=0.
    assert_eq!(null_row[1], ScalarValue::Int64(Some(4)));
    assert_eq!(null_row[2], ScalarValue::Int64(Some(0)));
    assert!(null_row[3].is_null());
    assert!(null_row[4].is_null());
    // 's': 396 rows; px NULL for i%50==49 and i%100!=99 -> i=49,149,249,349.
    assert_eq!(s_row[1], ScalarValue::Int64(Some(396)));
    assert_eq!(s_row[2], ScalarValue::Int64(Some(392)));
    assert_eq!(s_row[3], ScalarValue::Int64(Some(392 * 5)));
    match &s_row[4] {
        ScalarValue::Float64(Some(v)) => assert!((v - 5.0).abs() < 1e-9),
        other => panic!("avg {other:?}"),
    }
    engine.segment_mgr().shutdown();
}

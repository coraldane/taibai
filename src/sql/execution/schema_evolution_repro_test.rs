//! S8：完整还原 2026-11-07 事故序列。
//! 时间线：8 天数据落盘 → 22:22 回补删除部分日期（留下删除位图）→
//! 22:25 再删一些 → ALTER 加列 → 22:43 全部重插 → FLUSH →
//! OPTIMIZE（模拟后台 auto-compact）→ 重启引擎 → 读。
//! 以及并发变体：ALTER 期间写线程持续 DML。

use super::*;
use crate::net::session::Session;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use tempfile::TempDir;

fn exec(engine: &Engine, session: &mut Session, sql: &str) -> std::result::Result<(), String> {
    engine
        .execute(session, sql)
        .map(|_| ())
        .map_err(|e| format!("{e}"))
}

fn q_rows(engine: &Engine, session: &mut Session, sql: &str) -> std::result::Result<usize, String> {
    match engine.execute(session, sql) {
        Ok(ExecResult::Query { batches, .. }) => {
            let mut n = 0usize;
            for b in batches {
                n += b.num_rows();
            }
            Ok(n)
        }
        Ok(_) => Ok(0),
        Err(e) => Err(format!("{e}")),
    }
}

const DDL: &str = "CREATE TABLE s8 (
        deal_date INT32 NOT NULL PRIMARY KEY,
        sector_code VARCHAR NOT NULL PRIMARY KEY,
        sector_name VARCHAR NOT NULL,
        score FLOAT64,
        change_ratio FLOAT64,
        limit_count INT32,
        max_streak INT32,
        value_pct FLOAT64,
        leaders_json VARCHAR
    )";

/// 每天 200 行 × 8 天；sector_code 含日期后缀避免主键冲突。
fn seed_day(
    engine: &Engine,
    session: &mut Session,
    day: i64,
    rows: i64,
) -> std::result::Result<(), String> {
    for i in 0..rows {
        let sql = format!(
            "INSERT INTO s8 VALUES ({}, 'BK{:04}', '板块{}', 1.5, -0.25, {}, 2, 12.3, '[]')",
            day,
            (day % 10000) * 1000 + i,
            i % 50,
            i % 7
        );
        exec(engine, session, &sql)?;
    }
    Ok(())
}

#[test]
fn s8_full_incident_sequence() {
    println!("== S8 full incident sequence ==");
    let temp_dir = TempDir::new().expect("temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        exec(&engine, &mut session, DDL).expect("create");

        // 11 月 8 个交易日 × 200 行，落盘成旧 schema 段
        for day in [
            20261028i64,
            20261029,
            20261030,
            20261102,
            20261103,
            20261104,
            20261105,
            20261106,
        ] {
            seed_day(&engine, &mut session, day, 200).expect("seed");
        }
        exec(&engine, &mut session, "FLUSH TABLE s8").expect("flush");
        println!("  seeded 8 days x 200 rows, flushed");

        // 22:22 / 22:25 两轮回补：每轮先删全部 8 天、INSERT 因缺列失败 →
        // 到 22:43 前表里旧数据已全部删除（只剩删除位图挂在旧段上）
        for day in [
            20261028i64,
            20261029,
            20261030,
            20261102,
            20261103,
            20261104,
            20261105,
            20261106,
        ] {
            exec(
                &engine,
                &mut session,
                &format!("DELETE FROM s8 WHERE deal_date = {day}"),
            )
            .unwrap_or_else(|e| println!("  delete {day} err: {e}"));
        }
        println!("  deleted all 8 days (22:22+22:25 backfill rounds)");

        // ALTER（22:25~22:43 之间发生）
        println!(
            "  alter: {:?}",
            exec(
                &engine,
                &mut session,
                "ALTER TABLE s8 ADD COLUMN bottom_chg FLOAT64 DEFAULT 0"
            )
        );

        // 22:43 回补：重插全部 8 天（新 schema）
        let ins = |session: &mut Session| -> std::result::Result<(), String> {
            for day in [
                20261028i64,
                20261029,
                20261030,
                20261102,
                20261103,
                20261104,
                20261105,
                20261106,
            ] {
                for i in 0..200i64 {
                    let sql = format!(
                        "INSERT INTO s8 VALUES ({}, 'BK{:04}', '板块{}', 1.5, -0.25, {}, 2, 12.3, '[]', 0.4)",
                        day,
                        (day % 10000) * 1000 + i,
                        i % 50,
                        i % 7
                    );
                    exec(&engine, session, &sql)?;
                }
            }
            Ok(())
        };
        ins(&mut session).expect("reinsert all");
        exec(&engine, &mut session, "FLUSH TABLE s8").expect("flush new");
        println!("  reinserted 8 days x 200 rows (new schema), flushed");

        // 读验证（此时应当正常）
        match q_rows(
            &engine,
            &mut session,
            "SELECT deal_date FROM s8 WHERE deal_date = 20261106",
        ) {
            Ok(n) => println!("  read before optimize: OK rows={n}"),
            Err(e) => println!("  read before optimize: ERR {e}"),
        }

        // 模拟后台 auto-compact
        println!(
            "  optimize: {:?}",
            exec(&engine, &mut session, "OPTIMIZE TABLE s8")
        );
        match q_rows(
            &engine,
            &mut session,
            "SELECT deal_date FROM s8 WHERE deal_date = 20261106",
        ) {
            Ok(n) => println!("  read after optimize: OK rows={n}"),
            Err(e) => println!("  read after optimize: ERR {e}"),
        }
        engine.segment_mgr().shutdown();
    }

    // 重启后读（对应第二天才有人访问 /market）
    let engine = Engine::new(temp_dir.path(), "main").expect("engine restart");
    let mut session = Session::new("root".to_string(), "main".to_string());
    match q_rows(
        &engine,
        &mut session,
        "SELECT deal_date FROM s8 WHERE deal_date = 20261106",
    ) {
        Ok(n) => println!("  read after restart: OK rows={n}"),
        Err(e) => println!("  read after restart: ERR {e}"),
    }
    match q_rows(
        &engine,
        &mut session,
        "SELECT deal_date, bottom_chg FROM s8",
    ) {
        Ok(n) => println!("  full scan after restart: OK rows={n}"),
        Err(e) => println!("  full scan after restart: ERR {e}"),
    }
    engine.segment_mgr().shutdown();
}

/// S9：ALTER 收敛窗口内的并发 DML（多连接同时 INSERT/DELETE + 反复 ALTER）。
#[test]
fn s9_concurrent_dml_during_alter() {
    println!("== S9 concurrent DML during ALTER ==");
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("engine"));
    {
        let mut session = Session::new("root".to_string(), "main".to_string());
        exec(&engine, &mut session, DDL).expect("create");
        for day in [20261101i64, 20261102, 20261103] {
            seed_day(&engine, &mut session, day, 50).expect("seed");
        }
        exec(&engine, &mut session, "FLUSH TABLE s8").expect("flush");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let stop_w = stop.clone();
    let engine_w = engine.clone();
    let writer = std::thread::spawn(move || {
        let mut session = Session::new("root".to_string(), "main".to_string());
        let mut i = 0i64;
        while !stop_w.load(AtomicOrdering::SeqCst) {
            let sql = format!(
                "INSERT INTO s8 VALUES ({}, 'BKX{:04}', '并发{}', 1.5, -0.25, 3, 2, 12.3, '[]', 0.4)",
                20261300 + i % 5,
                i % 1000,
                i % 10
            );
            let _ = engine_w.execute(&mut session, &sql);
            let _ = engine_w.execute(
                &mut session,
                "DELETE FROM s8 WHERE deal_date = 20261304 AND sector_code LIKE 'BKX99%'",
            );
            i += 1;
        }
        i
    });

    // 交替做宽度 ALTER + 落盘 + 读取；每轮留出窗口让写线程真正并发
    for round in 0..3i64 {
        let mut session = Session::new("root".to_string(), "main".to_string());
        let _ = exec(&engine, &mut session, "FLUSH TABLE s8");
        std::thread::sleep(std::time::Duration::from_millis(200));
        let r = exec(
            &engine,
            &mut session,
            &format!("ALTER TABLE s8 ADD COLUMN c{round} FLOAT64 DEFAULT 1"),
        );
        println!("  round {round} alter: {r:?}");
        std::thread::sleep(std::time::Duration::from_millis(200));
        match q_rows(&engine, &mut session, "SELECT deal_date FROM s8") {
            Ok(n) => println!("  round {round} read: OK rows={n}"),
            Err(e) => println!("  round {round} read: ERR {e}"),
        }
    }

    stop.store(true, AtomicOrdering::SeqCst);
    let writes = writer.join().expect("writer join");
    println!("  writer did {writes} ops");

    let mut session = Session::new("root".to_string(), "main".to_string());
    match q_rows(
        &engine,
        &mut session,
        "SELECT deal_date, c0, c1, c2 FROM s8",
    ) {
        Ok(n) => println!("  final read: OK rows={n}"),
        Err(e) => println!("  final read: ERR {e}"),
    }
    let _ = exec(&engine, &mut session, "FLUSH TABLE s8");
    let _ = exec(&engine, &mut session, "OPTIMIZE TABLE s8");
    match q_rows(&engine, &mut session, "SELECT deal_date FROM s8") {
        Ok(n) => println!("  post-optimize read: OK rows={n}"),
        Err(e) => println!("  post-optimize read: ERR {e}"),
    }
    engine.segment_mgr().shutdown();
}

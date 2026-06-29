use std::sync::Arc;
use std::thread;

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::storage::core::memtable::MemTableState;

fn session() -> Session {
    Session::new("root".to_string(), "main".to_string())
}

fn count_star(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches
        .first()
        .and_then(|batch| batch.scalar(0, 0))
        .and_then(|value| match value {
            taibai::types::scalar::ScalarValue::Int64(Some(v)) => Some(v),
            _ => None,
        })
        .expect("count(*) value")
}

fn rows_affected(result: ExecResult) -> u64 {
    let ExecResult::Ok { rows_affected } = result else {
        panic!("expected ok result");
    };
    rows_affected
}

#[test]
fn concurrent_insert_and_select_on_active_segment_is_stable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(&mut setup, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");

    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let mut s = session();
        for i in 0..80 {
            writer_engine
                .execute(&mut s, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .expect("insert row");
        }
    });

    let reader_engine = Arc::clone(&engine);
    let reader = thread::spawn(move || {
        let mut s = session();
        for _ in 0..120 {
            reader_engine
                .execute(&mut s, "SELECT id, value FROM t")
                .expect("select rows while inserting");
        }
    });

    writer.join().expect("writer thread panicked");
    reader.join().expect("reader thread panicked");
    engine.segment_mgr().shutdown();
}

#[test]
fn concurrent_insert_and_delete_footer_rewrite_is_stable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(&mut setup, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");

    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let mut s = session();
        for i in 0..60 {
            writer_engine
                .execute(&mut s, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .expect("insert row");
        }
    });

    let deleter_engine = Arc::clone(&engine);
    let deleter = thread::spawn(move || {
        let mut s = session();
        for i in 0..30 {
            deleter_engine
                .execute(&mut s, &format!("DELETE FROM t WHERE id = {}", i))
                .expect("delete row while inserting");
        }
    });

    writer.join().expect("writer thread panicked");
    deleter.join().expect("deleter thread panicked");
    engine.segment_mgr().shutdown();
}

#[test]
fn concurrent_insert_and_update_table_rewrite_is_stable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(&mut setup, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    engine
        .execute(&mut setup, "INSERT INTO t VALUES (0, 0)")
        .expect("seed table");

    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let mut s = session();
        for i in 1..50 {
            writer_engine
                .execute(&mut s, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .expect("insert row");
        }
    });

    let updater_engine = Arc::clone(&engine);
    let updater = thread::spawn(move || {
        let mut s = session();
        for _ in 0..10 {
            updater_engine
                .execute(&mut s, "UPDATE t SET value = 1")
                .expect("update table while inserting");
        }
    });

    writer.join().expect("writer thread panicked");
    updater.join().expect("updater thread panicked");
    engine.segment_mgr().shutdown();
}

#[test]
fn flushing_memtable_rows_remain_countable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut setup = session();
    engine
        .execute(
            &mut setup,
            "CREATE TABLE t_stock_daily (
                id INT64 AUTO_INCREMENT PRIMARY KEY,
                security_id VARCHAR NOT NULL,
                deal_date INT32 NOT NULL,
                close_px DECIMAL(12, 4)
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (security_id, deal_date)",
        )
        .expect("create table");
    std::fs::create_dir_all(temp_dir.path().join("main").join("t_stock_daily"))
        .expect("create table data dir");
    engine
        .execute(
            &mut setup,
            "INSERT INTO t_stock_daily (security_id, deal_date, close_px) VALUES
                ('new_1', 20260629, 1.0000),
                ('new_2', 20260629, 2.0000),
                ('other', 20260630, 3.0000)",
        )
        .expect("insert rows");

    for memtable in engine
        .memtable_manager()
        .get_by_table("main", "t_stock_daily")
    {
        memtable.set_state(MemTableState::Flushing);
    }

    let mut verifier = session();
    let count = engine
        .execute(
            &mut verifier,
            "SELECT COUNT(*) FROM t_stock_daily WHERE deal_date = 20260629",
        )
        .expect("count flushing rows");
    assert_eq!(count_star(count), 2);
    engine.segment_mgr().shutdown();
}

#[test]
fn concurrent_sql_insert_after_delete_same_date_is_immediately_countable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(
            &mut setup,
            "CREATE TABLE t_stock_daily (
                id INT64 AUTO_INCREMENT PRIMARY KEY,
                gmt_create TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                security_id VARCHAR NOT NULL,
                deal_date INT32 NOT NULL,
                close_px DECIMAL(12, 4)
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (security_id, deal_date)",
        )
        .expect("create table");
    engine
        .execute(
            &mut setup,
            "INSERT INTO t_stock_daily (security_id, deal_date, close_px) VALUES
                ('old_1', 20260629, 1.0000),
                ('old_2', 20260629, 2.0000),
                ('other', 20260630, 3.0000)",
        )
        .expect("insert old rows");
    engine
        .execute(&mut setup, "FLUSH TABLE t_stock_daily")
        .expect("flush old rows");
    engine
        .execute(
            &mut setup,
            "DELETE FROM t_stock_daily WHERE deal_date = 20260629",
        )
        .expect("delete old date rows");

    let mut writers = Vec::new();
    for worker in 0..4 {
        let writer_engine = Arc::clone(&engine);
        writers.push(thread::spawn(move || {
            let mut s = session();
            for row in 0..50 {
                let security_id = format!("W{}_{:03}.SZ", worker, row);
                writer_engine
                    .execute(
                        &mut s,
                        &format!(
                            "INSERT INTO t_stock_daily (security_id, deal_date, close_px) \
                             VALUES ('{}', 20260629, {}.0000)",
                            security_id,
                            worker * 100 + row
                        ),
                    )
                    .expect("insert replacement row");
            }
        }));
    }
    for writer in writers {
        writer.join().expect("writer thread panicked");
    }

    let mut verifier = session();
    let count = engine
        .execute(
            &mut verifier,
            "SELECT COUNT(*) FROM t_stock_daily WHERE deal_date = 20260629",
        )
        .expect("count replacement rows");
    assert_eq!(count_star(count), 200);
    engine.segment_mgr().shutdown();
}

#[test]
fn multi_value_sql_insert_after_delete_same_date_is_immediately_countable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut setup = session();
    engine
        .execute(
            &mut setup,
            "CREATE TABLE t_stock_daily (
                id INT64 AUTO_INCREMENT PRIMARY KEY,
                gmt_create TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                security_id VARCHAR NOT NULL,
                trade_date INT32,
                deal_date INT32 NOT NULL,
                close_px DECIMAL(12, 4)
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (security_id, deal_date)",
        )
        .expect("create table");
    engine
        .execute(
            &mut setup,
            "INSERT INTO t_stock_daily (security_id, trade_date, deal_date, close_px) VALUES
                ('old_1', 20260629, 20260629, 1.0000),
                ('old_2', 20260629, 20260629, 2.0000),
                ('other', 20260630, 20260630, 3.0000)",
        )
        .expect("insert old rows");
    engine
        .execute(&mut setup, "FLUSH TABLE t_stock_daily")
        .expect("flush old rows");
    assert_eq!(
        rows_affected(
            engine
                .execute(
                    &mut setup,
                    "DELETE FROM t_stock_daily WHERE deal_date = 20260629"
                )
                .expect("delete old date rows")
        ),
        2
    );

    let mut sql = String::from(
        "INSERT INTO t_stock_daily (security_id, trade_date, deal_date, close_px) VALUES ",
    );
    for row in 0..5189 {
        if row > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "('S{:06}.SZ',20260629,20260629,{}.0000)",
            row, row
        ));
    }

    assert_eq!(
        rows_affected(
            engine
                .execute(&mut setup, &sql)
                .expect("insert replacement rows")
        ),
        5189
    );

    let mut verifier = session();
    let count = engine
        .execute(
            &mut verifier,
            "SELECT COUNT(*) FROM t_stock_daily WHERE deal_date = 20260629",
        )
        .expect("count replacement rows");
    assert_eq!(count_star(count), 5189);
    engine.segment_mgr().shutdown();
}

#[test]
fn recovered_segment_insert_after_delete_same_date_is_immediately_countable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut setup = session();
        engine
            .execute(
                &mut setup,
                "CREATE TABLE t_stock_daily (
                    id INT64 AUTO_INCREMENT PRIMARY KEY,
                    gmt_create TIMESTAMP,
                    gmt_modified TIMESTAMP,
                    security_id VARCHAR NOT NULL,
                    deal_date INT32 NOT NULL,
                    open_px DECIMAL(12, 4),
                    high_px DECIMAL(12, 4),
                    low_px DECIMAL(12, 4),
                    high_limit DECIMAL(12, 4),
                    low_limit DECIMAL(12, 4),
                    close_px DECIMAL(12, 4),
                    trade_volume INT64,
                    trade_value DECIMAL(20, 4),
                    adj DECIMAL(18, 6),
                    mean VARCHAR,
                    pre_close DECIMAL(12, 4),
                    change_ratio DECIMAL(12, 2)
                ) PARTITION BY YEAR(deal_date) CLUSTER BY (security_id, deal_date)",
            )
            .expect("create table");

        let mut seed = String::from(
            "INSERT INTO t_stock_daily (
                gmt_create, gmt_modified, security_id, deal_date, open_px, high_px, low_px,
                high_limit, low_limit, close_px, trade_volume, trade_value, adj, mean,
                pre_close, change_ratio
            ) VALUES ",
        );
        for row in 0..5189 {
            if row > 0 {
                seed.push(',');
            }
            seed.push_str(&format!(
                "(1782718238000000,1782718759000000,'S{:06}.SZ',20260629,10.0000,11.0000,9.0000,12.0000,8.0000,10.5000,{},1000.0000,1.000000,'10.50',10.0000,0.50)",
                row, row
            ));
        }
        engine.execute(&mut setup, &seed).expect("insert seed rows");
        engine
            .execute(&mut setup, "FLUSH TABLE t_stock_daily")
            .expect("flush seed rows");
        engine.segment_mgr().shutdown();
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("reopen engine");
    let mut delete_session = session();
    let before = engine
        .execute(
            &mut delete_session,
            "SELECT COUNT(*) FROM t_stock_daily WHERE deal_date = 20260629",
        )
        .expect("count seed rows");
    assert_eq!(count_star(before), 5189);
    assert_eq!(
        rows_affected(
            engine
                .execute(
                    &mut delete_session,
                    "DELETE FROM t_stock_daily WHERE deal_date = 20260629"
                )
                .expect("delete seed rows")
        ),
        5189
    );
    let after_delete = engine
        .execute(
            &mut delete_session,
            "SELECT COUNT(*) FROM t_stock_daily WHERE deal_date = 20260629",
        )
        .expect("count after delete");
    assert_eq!(count_star(after_delete), 0);

    let mut insert = String::from(
        "INSERT INTO t_stock_daily (
            gmt_create, gmt_modified, security_id, deal_date, open_px, high_px, low_px,
            high_limit, low_limit, close_px, trade_volume, trade_value, adj, mean,
            pre_close, change_ratio
        ) VALUES ",
    );
    for row in 0..5189 {
        if row > 0 {
            insert.push(',');
        }
        insert.push_str(&format!(
            "(1782718238000000,1782718759000000,'N{:06}.SZ',20260629,10.0000,11.0000,9.0000,12.0000,8.0000,10.5000,{},1000.0000,1.000000,'10.50',10.0000,0.50)",
            row, row
        ));
    }

    let mut insert_session = session();
    assert_eq!(
        rows_affected(
            engine
                .execute(&mut insert_session, &insert)
                .expect("insert replacement rows")
        ),
        5189
    );

    engine
        .flush_all_buffers()
        .expect("simulate writer disconnect flush");

    let mut verifier = session();
    let count = engine
        .execute(
            &mut verifier,
            "SELECT COUNT(*) FROM t_stock_daily WHERE deal_date = 20260629",
        )
        .expect("count replacement rows");
    assert_eq!(count_star(count), 5189);
    engine.segment_mgr().shutdown();
}

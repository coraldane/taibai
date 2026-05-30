use std::time::Instant;

use monax::net::client::Client;
use monax::types::scalar::ScalarValue;

macro_rules! log_msg {
    ($($arg:tt)*) => {
        {
            let now = chrono::Local::now();
            print!("[{}] ", now.format("%Y-%m-%d %H:%M:%S"));
            println!($($arg)*);
        }
    };
}

fn print_usage() {
    eprintln!("Usage: prepare_test <host> [--database=<db>]");
    eprintln!();
    eprintln!("Tests server-side prepared statement with binary parameter binding.");
    eprintln!("  host              Server address (e.g. 127.0.0.1:5151)");
    eprintln!("  --database=<db>   Database name (default: main)");
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    let host = &args[1];
    let database = args
        .iter()
        .find(|a| a.starts_with("--database="))
        .and_then(|a| a.split('=').nth(1))
        .map(|s| s.to_string())
        .unwrap_or_else(|| "main".to_string());

    log_msg!("Connecting to {} ...", host);
    let mut client = Client::connect(host).await.expect("connect failed");

    client
        .login("root", "taibai", &database)
        .await
        .expect("login failed");
    log_msg!("Logged in to database '{}'.", database);

    let mut passed = 0u32;
    let mut failed = 0u32;

    test_basic_insert_select(&mut client, &mut passed, &mut failed).await;
    test_null_params(&mut client, &mut passed, &mut failed).await;
    test_multiple_param_types(&mut client, &mut passed, &mut failed).await;
    test_update_with_params(&mut client, &mut passed, &mut failed).await;
    test_delete_with_params(&mut client, &mut passed, &mut failed).await;
    test_repeated_execute(&mut client, &mut passed, &mut failed).await;
    test_select_no_params(&mut client, &mut passed, &mut failed).await;

    log_msg!("========================================");
    log_msg!("Results: {} passed, {} failed", passed, failed);

    let _ = client.logout().await;
    let _ = client.close().await;

    if failed > 0 {
        std::process::exit(1);
    }
}

async fn test_basic_insert_select(client: &mut Client, passed: &mut u32, failed: &mut u32) {
    log_msg!("--- Test: basic INSERT + SELECT with Int64 param ---");

    client
        .execute("DROP TABLE IF EXISTS test_prep_1")
        .await
        .ok();
    client
        .execute("CREATE TABLE test_prep_1 (id INT64, name VARCHAR, score FLOAT64)")
        .await
        .expect("create table failed");
    client
        .execute("INSERT INTO test_prep_1 VALUES (1, 'alice', 95.5)")
        .await
        .expect("insert failed");
    client
        .execute("INSERT INTO test_prep_1 VALUES (2, 'bob', 87.3)")
        .await
        .expect("insert failed");

    let stmt = client
        .prepare("SELECT * FROM test_prep_1 WHERE id = ?")
        .await
        .expect("prepare failed");
    log_msg!(
        "  Prepared stmt_id={}, param_count={}",
        stmt.id,
        stmt.param_count
    );
    assert_eq!(stmt.param_count, 1, "expected 1 param");

    let result = client
        .execute_prepared(&stmt, &[ScalarValue::Int64(Some(1))])
        .await
        .expect("execute_prepared failed");
    match &result {
        monax::net::client::QueryResult::Rows { batches, .. } => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            log_msg!("  Query id=1: {} rows", row_count);
            assert_eq!(row_count, 1, "expected 1 row for id=1");
        }
        other => {
            log_msg!("  FAIL: unexpected result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    let result = client
        .execute_prepared(&stmt, &[ScalarValue::Int64(Some(999))])
        .await
        .expect("execute_prepared failed");
    match &result {
        monax::net::client::QueryResult::Rows { batches, .. } => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            log_msg!("  Query id=999: {} rows", row_count);
            assert_eq!(row_count, 0, "expected 0 rows for id=999");
        }
        other => {
            log_msg!("  FAIL: unexpected result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    client.close_stmt(&stmt).await.expect("close_stmt failed");
    log_msg!("  PASS");
    *passed += 1;
}

async fn test_null_params(client: &mut Client, passed: &mut u32, failed: &mut u32) {
    log_msg!("--- Test: INSERT with NULL param ---");

    client
        .execute("DROP TABLE IF EXISTS test_prep_2")
        .await
        .ok();
    client
        .execute("CREATE TABLE test_prep_2 (id INT64, val VARCHAR)")
        .await
        .expect("create table failed");

    let stmt = client
        .prepare("INSERT INTO test_prep_2 VALUES (?, ?)")
        .await
        .expect("prepare failed");
    log_msg!(
        "  Prepared stmt_id={}, param_count={}",
        stmt.id,
        stmt.param_count
    );
    assert_eq!(stmt.param_count, 2, "expected 2 params");

    let result = client
        .execute_prepared(
            &stmt,
            &[
                ScalarValue::Int64(Some(42)),
                ScalarValue::VarChar(None),
            ],
        )
        .await
        .expect("execute_prepared failed");
    match result {
        monax::net::client::QueryResult::Ok { rows_affected, .. } => {
            log_msg!("  Insert with NULL: rows_affected={}", rows_affected);
        }
        other => {
            log_msg!("  FAIL: unexpected result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    let result = client
        .execute_prepared(
            &stmt,
            &[
                ScalarValue::Int64(Some(43)),
                ScalarValue::VarChar(Some("hello".to_string())),
            ],
        )
        .await
        .expect("execute_prepared failed");
    match result {
        monax::net::client::QueryResult::Ok { rows_affected, .. } => {
            log_msg!("  Insert with string: rows_affected={}", rows_affected);
        }
        other => {
            log_msg!("  FAIL: unexpected result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    let verify = client
        .query("SELECT * FROM test_prep_2 ORDER BY id")
        .await
        .expect("verify query failed");
    match &verify {
        monax::net::client::QueryResult::Rows { batches, .. } => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            log_msg!("  Verify: {} rows", row_count);
            assert_eq!(row_count, 2, "expected 2 rows");
        }
        other => {
            log_msg!("  FAIL: unexpected verify result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    client.close_stmt(&stmt).await.expect("close_stmt failed");
    log_msg!("  PASS");
    *passed += 1;
}

async fn test_multiple_param_types(client: &mut Client, passed: &mut u32, failed: &mut u32) {
    log_msg!("--- Test: multiple data types as params ---");

    client
        .execute("DROP TABLE IF EXISTS test_prep_3")
        .await
        .ok();
    client
        .execute("CREATE TABLE test_prep_3 (a INT32, b FLOAT64, c VARCHAR, d DECIMAL(9,2))")
        .await
        .expect("create table failed");

    let stmt = client
        .prepare("INSERT INTO test_prep_3 VALUES (?, ?, ?, ?)")
        .await
        .expect("prepare failed");
    log_msg!(
        "  Prepared stmt_id={}, param_count={}",
        stmt.id,
        stmt.param_count
    );
    assert_eq!(stmt.param_count, 4);

    let result = client
        .execute_prepared(
            &stmt,
            &[
                ScalarValue::Int32(Some(100)),
                ScalarValue::Float64(Some(3.14)),
                ScalarValue::VarChar(Some("test".to_string())),
                ScalarValue::Decimal(Some(9999)),
            ],
        )
        .await
        .expect("execute_prepared failed");

    match result {
        monax::net::client::QueryResult::Ok { rows_affected, .. } => {
            log_msg!("  Insert multi-type: rows_affected={}", rows_affected);
        }
        other => {
            log_msg!("  FAIL: unexpected result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    let verify = client
        .query("SELECT * FROM test_prep_3")
        .await
        .expect("verify query failed");
    match &verify {
        monax::net::client::QueryResult::Rows { batches, .. } => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            log_msg!("  Verify: {} rows", row_count);
            assert_eq!(row_count, 1, "expected 1 row");
        }
        other => {
            log_msg!("  FAIL: unexpected verify result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    client.close_stmt(&stmt).await.expect("close_stmt failed");
    log_msg!("  PASS");
    *passed += 1;
}

async fn test_update_with_params(client: &mut Client, passed: &mut u32, failed: &mut u32) {
    log_msg!("--- Test: UPDATE with param ---");

    client
        .execute("DROP TABLE IF EXISTS test_prep_4")
        .await
        .ok();
    client
        .execute("CREATE TABLE test_prep_4 (id INT64, val VARCHAR)")
        .await
        .expect("create table failed");
    client
        .execute("INSERT INTO test_prep_4 VALUES (1, 'old')")
        .await
        .expect("insert failed");

    let stmt = client
        .prepare("UPDATE test_prep_4 SET val = ? WHERE id = ?")
        .await
        .expect("prepare failed");
    log_msg!(
        "  Prepared stmt_id={}, param_count={}",
        stmt.id,
        stmt.param_count
    );
    assert_eq!(stmt.param_count, 2);

    let result = client
        .execute_prepared(
            &stmt,
            &[
                ScalarValue::VarChar(Some("new".to_string())),
                ScalarValue::Int64(Some(1)),
            ],
        )
        .await
        .expect("execute_prepared failed");

    match result {
        monax::net::client::QueryResult::Ok { rows_affected, .. } => {
            log_msg!("  Update: rows_affected={}", rows_affected);
        }
        other => {
            log_msg!("  FAIL: unexpected result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    let verify = client
        .query("SELECT val FROM test_prep_4 WHERE id = 1")
        .await
        .expect("verify query failed");
    match &verify {
        monax::net::client::QueryResult::Rows { batches, schema: _, .. } => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            log_msg!("  Verify: {} rows", row_count);
            assert_eq!(row_count, 1);
        }
        other => {
            log_msg!("  FAIL: unexpected verify result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    client.close_stmt(&stmt).await.expect("close_stmt failed");
    log_msg!("  PASS");
    *passed += 1;
}

async fn test_delete_with_params(client: &mut Client, passed: &mut u32, failed: &mut u32) {
    log_msg!("--- Test: DELETE with param ---");

    client
        .execute("DROP TABLE IF EXISTS test_prep_5")
        .await
        .ok();
    client
        .execute("CREATE TABLE test_prep_5 (id INT64, name VARCHAR)")
        .await
        .expect("create table failed");
    client
        .execute("INSERT INTO test_prep_5 VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .expect("insert failed");

    let stmt = client
        .prepare("DELETE FROM test_prep_5 WHERE id = ?")
        .await
        .expect("prepare failed");
    log_msg!(
        "  Prepared stmt_id={}, param_count={}",
        stmt.id,
        stmt.param_count
    );
    assert_eq!(stmt.param_count, 1);

    let result = client
        .execute_prepared(&stmt, &[ScalarValue::Int64(Some(2))])
        .await
        .expect("execute_prepared failed");

    match result {
        monax::net::client::QueryResult::Ok { rows_affected, .. } => {
            log_msg!("  Delete: rows_affected={}", rows_affected);
        }
        other => {
            log_msg!("  FAIL: unexpected result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    let verify = client
        .query("SELECT COUNT(*) as cnt FROM test_prep_5")
        .await
        .expect("verify query failed");
    match &verify {
        monax::net::client::QueryResult::Rows { batches, .. } => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            log_msg!("  Verify: {} summary rows", row_count);
        }
        other => {
            log_msg!("  FAIL: unexpected verify result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    client.close_stmt(&stmt).await.expect("close_stmt failed");
    log_msg!("  PASS");
    *passed += 1;
}

async fn test_repeated_execute(client: &mut Client, passed: &mut u32, failed: &mut u32) {
    log_msg!("--- Test: repeated execute (perf) ---");

    client
        .execute("DROP TABLE IF EXISTS test_prep_6")
        .await
        .ok();
    client
        .execute("CREATE TABLE test_prep_6 (id INT64, val INT32)")
        .await
        .expect("create table failed");

    let insert_stmt = client
        .prepare("INSERT INTO test_prep_6 VALUES (?, ?)")
        .await
        .expect("prepare insert failed");
    log_msg!(
        "  Prepared insert stmt_id={}, param_count={}",
        insert_stmt.id,
        insert_stmt.param_count
    );

    let n = 1000u64;
    let start = Instant::now();
    for i in 0u64..n {
        let result = client
            .execute_prepared(
                &insert_stmt,
                &[
                    ScalarValue::Int64(Some(i as i64)),
                    ScalarValue::Int32(Some((i % 100) as i32)),
                ],
            )
            .await
            .expect("execute_prepared failed");
        match result {
            monax::net::client::QueryResult::Ok { rows_affected, .. } => {}
            other => {
                log_msg!("  FAIL at row {}: {:?}", i, other);
                *failed += 1;
                return;
            }
        }
    }
    let elapsed = start.elapsed();
    log_msg!(
        "  Inserted {} rows in {:.3}s ({:.0} rows/sec)",
        n,
        elapsed.as_secs_f64(),
        n as f64 / elapsed.as_secs_f64()
    );

    let verify = client
        .query("SELECT COUNT(*) as cnt FROM test_prep_6")
        .await
        .expect("verify count failed");
    match &verify {
        monax::net::client::QueryResult::Rows { batches, .. } => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            log_msg!("  Verify: {} summary rows returned", row_count);
        }
        other => {
            log_msg!("  FAIL: unexpected verify result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    let select_stmt = client
        .prepare("SELECT * FROM test_prep_6 WHERE id = ?")
        .await
        .expect("prepare select failed");

    let start = Instant::now();
    for i in 0u64..n {
        let result = client
            .execute_prepared(&select_stmt, &[ScalarValue::Int64(Some(i as i64))])
            .await
            .expect("execute_prepared select failed");
        match result {
            monax::net::client::QueryResult::Rows { batches, .. } => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                if rows != 1 {
                    log_msg!("  FAIL at select id={}: {} rows", i, rows);
                    *failed += 1;
                    return;
                }
            }
            other => {
                log_msg!("  FAIL at select id={}: {:?}", i, other);
                *failed += 1;
                return;
            }
        }
    }
    let elapsed = start.elapsed();
    log_msg!(
        "  Selected {} times in {:.3}s ({:.0} queries/sec)",
        n,
        elapsed.as_secs_f64(),
        n as f64 / elapsed.as_secs_f64()
    );

    client
        .close_stmt(&insert_stmt)
        .await
        .expect("close insert stmt failed");
    client
        .close_stmt(&select_stmt)
        .await
        .expect("close select stmt failed");
    log_msg!("  PASS");
    *passed += 1;
}

async fn test_select_no_params(client: &mut Client, passed: &mut u32, failed: &mut u32) {
    log_msg!("--- Test: SELECT with no params ---");

    let stmt = client
        .prepare("SELECT 1 as one, 'hello' as greeting")
        .await
        .expect("prepare failed");
    log_msg!(
        "  Prepared stmt_id={}, param_count={}",
        stmt.id,
        stmt.param_count
    );
    assert_eq!(stmt.param_count, 0, "expected 0 params");

    let result = client
        .execute_prepared(&stmt, &[])
        .await
        .expect("execute_prepared failed");
    match &result {
        monax::net::client::QueryResult::Rows { batches, .. } => {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            log_msg!("  Result: {} rows", row_count);
            assert_eq!(row_count, 1, "expected 1 row");
        }
        other => {
            log_msg!("  FAIL: unexpected result: {:?}", other);
            *failed += 1;
            return;
        }
    }

    client.close_stmt(&stmt).await.expect("close_stmt failed");
    log_msg!("  PASS");
    *passed += 1;
}

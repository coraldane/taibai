//! Diagnostic counterexamples for the 2026-10-05 CRUD audit.
//! Every write, including the deliberately broken segment, uses a temporary database.
//! Printed successful operations can represent bugs; this is not a correctness test.

use taibai::net::session::Session;
use taibai::sql::execution::engine::Engine;
fn main() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::new(dir.path(), "main").unwrap();
    let mut s = Session::new("root".into(), "main".into());
    for sql in [
        "CREATE TABLE u (id INT64, uk INT64, v INT64 NOT NULL, UNIQUE KEY uk_val (uk))",
        "INSERT INTO u VALUES (1, 100, 10)",
        "UPDATE u SET uk = 200 WHERE id = 1",
        "INSERT INTO u VALUES (2, 200, 20)",
        "SELECT id, uk FROM u ORDER BY id",
        "INSERT INTO u VALUES (3, 100, 30)",
        "UPDATE u SET v = NULL WHERE id = 1",
        "SELECT id, v FROM u ORDER BY id",
        "CREATE TABLE decs (id INT64, v DECIMAL(10,2))",
        "INSERT INTO decs VALUES (1, 1.23)",
        "UPDATE decs SET v = 4.56 WHERE id = 1",
        "SELECT v FROM decs",
        "FLUSH TABLE decs",
        "UPDATE decs SET v = 7.89 WHERE id = 1",
        "SELECT v FROM decs",
        "CREATE TABLE w (id INT64, v INT64)",
        "INSERT INTO w VALUES (1, 10), (2, 20), (3, 30)",
        "SELECT id, AVG(v) OVER (ORDER BY id) AS av FROM w ORDER BY id",
        "CREATE TABLE x (id INT64, v INT64)",
        "BEGIN",
        "INSERT INTO x VALUES (1, 10)",
        "UPDATE x SET v = 99, nosuch = 5 WHERE id = 1",
        "SELECT * FROM x",
        "COMMIT",
        "SELECT * FROM x",
    ] {
        let r = e.execute(&mut s, sql);
        match r {
            Ok(taibai::sql::execution::engine::ExecResult::Query { batches, .. }) => println!(
                "{sql} => {:?}",
                batches
                    .iter()
                    .flat_map(|b| (0..b.num_rows()).map(move |row| (0..b.num_columns())
                        .map(|c| b.scalar(row, c))
                        .collect::<Vec<_>>()))
                    .collect::<Vec<_>>()
            ),
            other => println!("{sql} => {other:?}"),
        };
    }

    e.execute(&mut s, "CREATE TABLE d (id INT64, UNIQUE KEY uq (id))")
        .unwrap();
    e.execute(&mut s, "INSERT INTO d VALUES (1)").unwrap();
    let broken = dir.path().join("main/d/seg_99999.dat");
    std::fs::write(&broken, b"broken").unwrap();
    println!(
        "DELETE with unreadable segment => {:?}",
        e.execute(&mut s, "DELETE FROM d")
    );
    std::fs::remove_file(&broken).unwrap();
    println!(
        "remaining physical memtable rows => {}",
        e.memtable_manager()
            .get_by_table("main", "d")
            .iter()
            .map(|m| m.row_count())
            .sum::<u64>()
    );
    println!(
        "INSERT of deleted key after failed DELETE => {:?}",
        e.execute(&mut s, "INSERT INTO d VALUES (1)")
    );
    println!(
        "COUNT after failed DELETE => {:?}",
        e.execute(&mut s, "SELECT COUNT(*) FROM d")
    );
    e.segment_mgr().shutdown();
}

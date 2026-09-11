use super::*;

#[test]
fn fast_insert_sql_materializes_generic_multi_values() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, amount DECIMAL(9,2), active BOOL)",
    )?;
    engine.execute(
            &mut session,
            "INSERT INTO t (id, name, amount, active) VALUES (1, 'alpha', 12.34, true), (2, 'it''s', -5.60, 0)",
        )?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
            0,
            0
        ),
        1
    );
    assert_eq!(
        first_text(
            engine.execute(&mut session, "SELECT name FROM t WHERE id = 2")?,
            0,
            0,
        ),
        "it's"
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn fast_insert_sql_materializes_missing_nullable_and_literal_default_columns() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, tag VARCHAR DEFAULT 'new', note VARCHAR)",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, name) VALUES (1, 'alpha'), (2, 'beta')",
    )?;

    assert_eq!(
        first_text(
            engine.execute(&mut session, "SELECT tag FROM t WHERE id = 1")?,
            0,
            0,
        ),
        "new"
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE note IS NULL")?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn fast_insert_sql_materializes_missing_auto_increment_and_current_timestamp() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64 AUTO_INCREMENT PRIMARY KEY, name VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (name) VALUES ('alpha'), ('beta')",
    )?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
            0,
            0
        ),
        1
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
            1,
            0
        ),
        2
    );
    assert_eq!(
        first_i64(
            engine.execute(
                &mut session,
                "SELECT COUNT(*) FROM t WHERE created_at IS NOT NULL"
            )?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn delete_removes_memtable_rows_without_flushing() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, PRIMARY KEY (id))",
        )?;
        engine.execute(
            &mut session,
            "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        )?;
        let epoch_before = engine.segment_mgr().memtable_flush_epoch("main", "t");

        engine.execute(&mut session, "DELETE FROM t WHERE id = 1")?;
        let count = first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0,
        );
        assert_eq!(count, 2);
        // No forced flush: the rows were removed from the memtable in place.
        assert_eq!(
            engine.segment_mgr().memtable_flush_epoch("main", "t"),
            epoch_before
        );
    }

    // The filtered durable tail survives a restart; the deleted row must not
    // resurrect, and its key must be insertable again.
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 2);
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 'uno')")?;
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(name, "uno");
    Ok(())
}

#[test]
fn bulk_multi_value_insert_routes_every_value_partition() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE partitioned (id INT64, value INT64) PARTITION BY id",
    )?;
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine.execute(
        &mut session,
        "INSERT INTO partitioned VALUES (1, 10), (2, 20)",
    )?;

    let mut active = engine
        .segment_mgr()
        .active_partition_keys("main", "partitioned");
    active.sort();
    assert_eq!(active, vec!["1".to_string(), "2".to_string()]);

    session.bulk_import_mode = false;
    engine.end_bulk_import();
    Ok(())
}

#[test]
fn like_predicate_filters_rows_in_scan() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, name VARCHAR)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, name) VALUES (1, 'apple'), (2, 'apply'), (3, 'banana'), (4, 'Apple'), (5, NULL)",
    )?;

    let contains = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE '%appl%'",
        )?,
        0,
        0,
    );
    assert_eq!(contains, 2);
    let prefix = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE 'app%'",
        )?,
        0,
        0,
    );
    assert_eq!(prefix, 2);
    let underscore = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE 'banan_'",
        )?,
        0,
        0,
    );
    assert_eq!(underscore, 1);
    let underscore_two = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE 'appl_'",
        )?,
        0,
        0,
    );
    assert_eq!(underscore_two, 2);
    let exact = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE 'banana'",
        )?,
        0,
        0,
    );
    assert_eq!(exact, 1);
    let negated = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name NOT LIKE '%an%'",
        )?,
        0,
        0,
    );
    assert_eq!(negated, 3);
    let no_match = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE '%zz%'",
        )?,
        0,
        0,
    );
    assert_eq!(no_match, 0);

    // Same predicate after a flush must filter on segment data as well.
    engine.execute(&mut session, "FLUSH TABLE t")?;
    let contains = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE '%appl%'",
        )?,
        0,
        0,
    );
    assert_eq!(contains, 2);
    Ok(())
}

#[test]
fn delete_with_like_predicate_only_removes_matching_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, name VARCHAR)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, name) VALUES (1, 'apple'), (2, 'apply'), (3, 'banana'), (4, 'Apple')",
    )?;
    engine.execute(&mut session, "DELETE FROM t WHERE name LIKE '%appl%'")?;
    let total = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(total, 2);
    let remaining = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 3")?,
        0,
        0,
    );
    assert_eq!(remaining, "banana");
    Ok(())
}

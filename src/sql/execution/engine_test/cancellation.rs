use super::*;

/// A cancelled SELECT aborts at the first safe point instead of running to
/// completion, while an INSERT already past materialization runs to the end
/// (commit phases are not cancellable) and the row becomes visible.
#[test]
fn select_cancels_but_inflight_insert_commits() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE cancel_t (id INT64)")?;
    engine.execute(&mut session, "INSERT INTO cancel_t VALUES (1)")?;

    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let context = Arc::new(crate::common::query_cancel::QueryContext::new(
        cancelled.clone(),
    ));
    let _guard = crate::common::query_cancel::QueryCancellationGuard::enter(context);
    cancelled.store(true, std::sync::atomic::Ordering::Release);

    let select_error = engine
        .execute(&mut session, "SELECT id FROM cancel_t WHERE id >= 0")
        .unwrap_err();
    assert!(
        select_error.to_string().contains("cancelled"),
        "unexpected error: {select_error}"
    );

    engine.execute(&mut session, "INSERT INTO cancel_t VALUES (2)")?;
    cancelled.store(false, std::sync::atomic::Ordering::Release);
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM cancel_t")?,
            0,
            0
        ),
        2
    );
    Ok(())
}

/// A statement deadline aborts reads with a timeout error but never abandons
/// an in-flight insert.
#[test]
fn statement_deadline_cancels_reads_and_protects_writes() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE deadline_t (id INT64)")?;

    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let context = Arc::new(
        crate::common::query_cancel::QueryContext::new(cancelled)
            .with_deadline(Some(std::time::Instant::now())),
    );
    let _guard = crate::common::query_cancel::QueryCancellationGuard::enter(context);

    let select_error = engine
        .execute(&mut session, "SELECT id FROM deadline_t WHERE id >= 0")
        .unwrap_err();
    assert!(
        select_error.to_string().contains("statement timeout"),
        "unexpected error: {select_error}"
    );

    engine.execute(&mut session, "INSERT INTO deadline_t VALUES (7)")?;
    drop(_guard);
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM deadline_t")?,
            0,
            0
        ),
        1
    );
    Ok(())
}

/// `SHOW PROCESSLIST` lists registered queries and `KILL QUERY` cancels the
/// target's cancellation context with the killer recorded in the message.
#[test]
fn kill_query_and_processlist_roundtrip() -> Result<()> {
    use crate::sql::parser::parser::Parser;

    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE kill_t (id INT64)")?;

    let parsed = Parser::parse("KILL QUERY 42").unwrap();
    assert_eq!(
        parsed.first(),
        Some(&crate::sql::parser::ast::Statement::KillQuery { query_id: 42 })
    );
    assert!(Parser::parse("KILL 42").is_err());
    assert!(Parser::parse("KILL QUERY -1").is_err());
    assert!(Parser::parse("KILL QUERY nope").is_err());

    let registry = engine.query_registry();
    let context = Arc::new(crate::common::query_cancel::QueryContext::new(Arc::new(
        std::sync::atomic::AtomicBool::new(false),
    )));
    let guard = registry.register(
        Arc::clone(&context),
        "session-1".to_string(),
        "alice".to_string(),
        "main".to_string(),
        "SELECT id FROM kill_t WHERE id = 1",
    );
    // The request handler moves Planning→Executing on entry; mirror that for
    // this manually registered query.
    context.enter_phase(crate::common::query_cancel::QueryPhase::Executing);

    let rows = engine.execute(&mut session, "SHOW PROCESSLIST")?;
    let ExecResult::Query { batches, .. } = &rows else {
        panic!("expected query result");
    };
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0].scalar(0, 1).unwrap(),
        ScalarValue::VarChar(Some("alice".to_string()))
    );
    assert_eq!(
        batches[0].scalar(0, 5).unwrap(),
        ScalarValue::VarChar(Some("Executing".to_string()))
    );

    let query_id = registry.list()[0].query_id;
    engine.execute(&mut session, &format!("KILL QUERY {}", query_id))?;
    let kill_error = context.check().unwrap_err().to_string();
    assert!(
        kill_error.contains("killed by user 'root'"),
        "unexpected error: {kill_error}"
    );
    let rows = engine.execute(&mut session, "SHOW PROCESSLIST")?;
    let ExecResult::Query { batches, .. } = &rows else {
        panic!("expected query result");
    };
    assert_eq!(
        batches[0].scalar(0, 5).unwrap(),
        ScalarValue::VarChar(Some("Killed".to_string()))
    );

    let unknown = engine
        .execute(&mut session, "KILL QUERY 987654")
        .unwrap_err();
    assert!(
        unknown.to_string().contains("not running"),
        "unexpected error: {unknown}"
    );

    drop(guard);
    let rows = engine.execute(&mut session, "SHOW PROCESSLIST")?;
    let ExecResult::Query { batches, .. } = &rows else {
        panic!("expected query result");
    };
    assert_eq!(batches[0].num_rows(), 0, "finished queries must not linger");
    Ok(())
}

/// `KILL QUERY` is restricted to global admins.
#[test]
fn kill_query_requires_global_admin() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = Session::new("bob".to_string(), "main".to_string());
    let error = engine.execute(&mut session, "KILL QUERY 1").unwrap_err();
    assert!(
        error.to_string().contains("access denied"),
        "unexpected error: {error}"
    );
    Ok(())
}

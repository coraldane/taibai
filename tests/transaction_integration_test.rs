use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::storage::txn::{IsolationLevel, TxnState};

fn create_test_engine() -> (TempDir, Engine) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    (temp_dir, engine)
}

fn create_test_session() -> Session {
    Session::new("root".to_string(), "main".to_string())
}

fn setup_test_table(engine: &Engine, session: &mut Session) {
    let create_sql = "CREATE TABLE test_txn (id INT32, name VARCHAR, value INT64)";
    engine
        .execute(session, create_sql)
        .expect("Failed to create table");
}

mod begin_tests {
    use super::*;

    #[test]
    fn test_begin_enters_transaction_mode() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        assert!(
            !session.in_txn(),
            "Session should not be in transaction initially"
        );
        assert_eq!(session.active_txn_id(), 0, "txn_id should be 0 initially");

        let result = engine.execute(&mut session, "BEGIN");
        match result {
            Ok(ExecResult::Ok {
                rows_affected: txn_id,
            }) => {
                assert!(txn_id > 0, "BEGIN should return positive txn_id");
            }
            _ => panic!("BEGIN failed: {:?}", result),
        }

        assert!(
            session.in_txn(),
            "Session should be in transaction after BEGIN"
        );
        assert!(
            session.active_txn_id() > 0,
            "txn_id should be positive after BEGIN"
        );

        if let Some(ref txn) = session.active_txn {
            assert_eq!(
                txn.state,
                TxnState::Active,
                "Transaction state should be Active"
            );
        } else {
            panic!("No active transaction found");
        }
    }

    #[test]
    fn test_begin_returns_unique_txn_ids() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session1 = create_test_session();
        let mut session2 = create_test_session();

        let txn_id1 = match engine.execute(&mut session1, "BEGIN") {
            Ok(ExecResult::Ok { rows_affected }) => rows_affected,
            _ => panic!("BEGIN failed"),
        };
        let txn_id2 = match engine.execute(&mut session2, "BEGIN") {
            Ok(ExecResult::Ok { rows_affected }) => rows_affected,
            _ => panic!("BEGIN failed"),
        };

        assert_ne!(
            txn_id1, txn_id2,
            "Different sessions should get different txn_ids"
        );
    }

    #[test]
    fn test_double_begin_fails() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        engine
            .execute(&mut session, "BEGIN")
            .expect("First BEGIN should succeed");

        let result = engine.execute(&mut session, "BEGIN");
        assert!(result.is_err(), "Double BEGIN should fail");
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                msg.contains("already in progress"),
                "Error should mention already in progress: {}",
                msg
            );
        }
    }
}

mod commit_tests {
    use super::*;

    #[test]
    fn test_commit_persists_data() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();
        setup_test_table(&engine, &mut session);

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");
        let txn_id = session.active_txn_id();
        assert!(txn_id > 0);

        engine
            .execute(
                &mut session,
                "INSERT INTO test_txn (id, name, value) VALUES (1, 'test1', 100)",
            )
            .expect("INSERT should succeed");

        let result = engine.execute(&mut session, "COMMIT");
        assert!(result.is_ok(), "COMMIT should succeed: {:?}", result);

        assert!(
            !session.in_txn(),
            "Session should not be in transaction after COMMIT"
        );
        assert_eq!(
            session.active_txn_id(),
            0,
            "txn_id should be 0 after COMMIT"
        );
    }

    #[test]
    fn test_commit_without_begin_fails() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        let result = engine.execute(&mut session, "COMMIT");
        assert!(result.is_err(), "COMMIT without BEGIN should fail");
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                msg.contains("no active transaction"),
                "Error should mention no active transaction: {}",
                msg
            );
        }
    }

    #[test]
    fn test_commit_with_no_writes_succeeds() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");

        let result = engine.execute(&mut session, "COMMIT");
        assert!(
            result.is_ok(),
            "COMMIT with no writes should succeed: {:?}",
            result
        );

        assert!(
            !session.in_txn(),
            "Session should not be in transaction after COMMIT"
        );
    }

    #[test]
    fn test_commit_multiple_inserts() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();
        setup_test_table(&engine, &mut session);

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");

        for i in 0..10 {
            engine
                .execute(
                    &mut session,
                    &format!(
                        "INSERT INTO test_txn (id, name, value) VALUES ({}, 'name{}', {})",
                        i,
                        i,
                        i * 10
                    ),
                )
                .expect("INSERT should succeed");
        }

        let result = engine.execute(&mut session, "COMMIT");
        assert!(result.is_ok(), "COMMIT should succeed: {:?}", result);
        assert!(!session.in_txn());
    }
}

mod rollback_tests {
    use super::*;

    #[test]
    fn test_rollback_discards_data() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();
        setup_test_table(&engine, &mut session);

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");
        let txn_id = session.active_txn_id();
        assert!(txn_id > 0);

        engine
            .execute(
                &mut session,
                "INSERT INTO test_txn (id, name, value) VALUES (1, 'test1', 100)",
            )
            .expect("INSERT should succeed");

        let result = engine.execute(&mut session, "ROLLBACK");
        assert!(result.is_ok(), "ROLLBACK should succeed: {:?}", result);

        assert!(
            !session.in_txn(),
            "Session should not be in transaction after ROLLBACK"
        );
        assert_eq!(
            session.active_txn_id(),
            0,
            "txn_id should be 0 after ROLLBACK"
        );
    }

    #[test]
    fn test_rollback_without_begin_fails() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        let result = engine.execute(&mut session, "ROLLBACK");
        assert!(result.is_err(), "ROLLBACK without BEGIN should fail");
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                msg.contains("no active transaction"),
                "Error should mention no active transaction: {}",
                msg
            );
        }
    }

    #[test]
    fn test_rollback_returns_rows_discarded() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();
        setup_test_table(&engine, &mut session);

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");

        let mut total_rows = 0u64;
        for i in 0..5 {
            let result = engine.execute(
                &mut session,
                &format!(
                    "INSERT INTO test_txn (id, name, value) VALUES ({}, 'name{}', {})",
                    i,
                    i,
                    i * 10
                ),
            );
            if let Ok(ExecResult::Ok { rows_affected }) = result {
                total_rows += rows_affected;
            }
        }

        let result = engine.execute(&mut session, "ROLLBACK");
        match result {
            Ok(ExecResult::Ok {
                rows_affected: rolled_back,
            }) => {
                assert_eq!(
                    rolled_back, total_rows,
                    "ROLLBACK should return total rows discarded"
                );
            }
            _ => panic!("ROLLBACK failed: {:?}", result),
        }
    }

    #[test]
    fn test_rollback_after_commit_fails() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();
        setup_test_table(&engine, &mut session);

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");
        engine
            .execute(
                &mut session,
                "INSERT INTO test_txn (id, name, value) VALUES (1, 'test1', 100)",
            )
            .expect("INSERT should succeed");
        engine
            .execute(&mut session, "COMMIT")
            .expect("COMMIT should succeed");

        let result = engine.execute(&mut session, "ROLLBACK");
        assert!(result.is_err(), "ROLLBACK after COMMIT should fail");
    }
}

mod state_transition_tests {
    use super::*;

    #[test]
    fn test_transaction_state_begin_to_commit() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        assert!(!session.in_txn());
        assert!(session.active_txn.is_none());

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");
        assert!(session.in_txn());
        if let Some(ref txn) = session.active_txn {
            assert_eq!(txn.state, TxnState::Active);
        }

        engine
            .execute(&mut session, "COMMIT")
            .expect("COMMIT should succeed");
        assert!(!session.in_txn());
        assert!(session.active_txn.is_none());
    }

    #[test]
    fn test_transaction_state_begin_to_rollback() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");
        assert!(session.in_txn());

        engine
            .execute(&mut session, "ROLLBACK")
            .expect("ROLLBACK should succeed");
        assert!(!session.in_txn());
        assert!(session.active_txn.is_none());
    }

    #[test]
    fn test_sequential_transactions() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();
        setup_test_table(&engine, &mut session);

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN 1 should succeed");
        engine
            .execute(
                &mut session,
                "INSERT INTO test_txn (id, name, value) VALUES (1, 'first', 100)",
            )
            .expect("INSERT 1 should succeed");
        engine
            .execute(&mut session, "COMMIT")
            .expect("COMMIT 1 should succeed");

        assert!(
            !session.in_txn(),
            "Should not be in transaction after first COMMIT"
        );

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN 2 should succeed");
        engine
            .execute(
                &mut session,
                "INSERT INTO test_txn (id, name, value) VALUES (2, 'second', 200)",
            )
            .expect("INSERT 2 should succeed");
        engine
            .execute(&mut session, "ROLLBACK")
            .expect("ROLLBACK 2 should succeed");

        assert!(
            !session.in_txn(),
            "Should not be in transaction after second ROLLBACK"
        );
    }

    #[test]
    fn test_transaction_isolation_level_default() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");

        if let Some(ref txn) = session.active_txn {
            assert_eq!(txn.isolation_level, IsolationLevel::ReadCommitted);
        } else {
            panic!("No active transaction");
        }
    }
}

mod error_scenarios {
    use super::*;

    #[test]
    fn test_commit_without_begin_error() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        let result = engine.execute(&mut session, "COMMIT");
        assert!(result.is_err());
    }

    #[test]
    fn test_rollback_without_begin_error() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        let result = engine.execute(&mut session, "ROLLBACK");
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_in_non_transactional_mode_succeeds() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();
        setup_test_table(&engine, &mut session);

        assert!(!session.in_txn(), "Should not be in transaction");
        assert_eq!(session.active_txn_id(), 0, "txn_id should be 0");

        let result = engine.execute(
            &mut session,
            "INSERT INTO test_txn (id, name, value) VALUES (1, 'test', 100)",
        );
        assert!(
            result.is_ok(),
            "INSERT without transaction should succeed: {:?}",
            result
        );
    }

    #[test]
    fn test_concurrent_transactions_different_sessions() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session1 = create_test_session();
        let mut session2 = create_test_session();
        setup_test_table(&engine, &mut session1);

        engine
            .execute(&mut session1, "BEGIN")
            .expect("Session1 BEGIN should succeed");
        engine
            .execute(&mut session2, "BEGIN")
            .expect("Session2 BEGIN should succeed");

        let txn1 = session1.active_txn_id();
        let txn2 = session2.active_txn_id();

        assert_ne!(
            txn1, txn2,
            "Different sessions should have different txn_ids"
        );

        engine
            .execute(
                &mut session1,
                "INSERT INTO test_txn (id, name, value) VALUES (1, 's1', 100)",
            )
            .expect("Session1 INSERT should succeed");
        engine
            .execute(
                &mut session2,
                "INSERT INTO test_txn (id, name, value) VALUES (2, 's2', 200)",
            )
            .expect("Session2 INSERT should succeed");

        engine
            .execute(&mut session1, "COMMIT")
            .expect("Session1 COMMIT should succeed");
        engine
            .execute(&mut session2, "ROLLBACK")
            .expect("Session2 ROLLBACK should succeed");

        assert!(!session1.in_txn());
        assert!(!session2.in_txn());
    }
}

mod txn_id_tests {
    use super::*;

    #[test]
    fn test_txn_id_is_positive() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");

        let txn_id = session.active_txn_id();
        assert!(txn_id > 0, "Transaction ID should be positive");
    }

    #[test]
    fn test_sequential_transactions_different_txn_ids() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN 1 should succeed");
        let txn1 = session.active_txn_id();
        engine
            .execute(&mut session, "COMMIT")
            .expect("COMMIT 1 should succeed");

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN 2 should succeed");
        let txn2 = session.active_txn_id();
        engine
            .execute(&mut session, "COMMIT")
            .expect("COMMIT 2 should succeed");

        assert_ne!(
            txn1, txn2,
            "Sequential transactions should have different txn_ids"
        );
    }
}

mod had_writes_tests {
    use super::*;

    #[test]
    fn test_had_writes_flag_unset_initially() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");

        if let Some(ref txn) = session.active_txn {
            assert!(
                !txn.had_writes,
                "Transaction should not have writes flag initially"
            );
        }
    }

    #[test]
    fn test_had_writes_flag_set_on_insert() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();
        setup_test_table(&engine, &mut session);

        engine
            .execute(&mut session, "BEGIN")
            .expect("BEGIN should succeed");

        if let Some(ref txn) = session.active_txn {
            assert!(!txn.had_writes, "had_writes should be false initially");
        }

        engine
            .execute(
                &mut session,
                "INSERT INTO test_txn (id, name, value) VALUES (1, 'test', 100)",
            )
            .expect("INSERT should succeed");

        if let Some(ref txn) = session.active_txn {
            assert!(txn.had_writes, "had_writes should be true after INSERT");
        }
    }

    #[test]
    fn test_had_writes_not_set_without_transaction() {
        let (_temp_dir, engine) = create_test_engine();
        let mut session = create_test_session();
        setup_test_table(&engine, &mut session);

        assert!(!session.in_txn());

        engine
            .execute(
                &mut session,
                "INSERT INTO test_txn (id, name, value) VALUES (1, 'test', 100)",
            )
            .expect("INSERT should succeed");

        assert!(!session.in_txn());
        assert!(session.active_txn.is_none());
    }
}

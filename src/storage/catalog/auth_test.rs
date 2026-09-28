use super::*;

#[test]
fn database_permission_matches_tables_in_that_database() {
    let mut user = UserInfo::new("alice", "pw");
    user.permissions.push(Permission {
        permission_type: PermissionType::Select,
        object_type: PermissionObjectType::Database,
        object_name: "db1".to_string(),
    });

    assert!(user.has_permission(
        PermissionType::Select,
        PermissionObjectType::Table,
        "db1.orders"
    ));
    assert!(!user.has_permission(
        PermissionType::Select,
        PermissionObjectType::Table,
        "db2.orders"
    ));
}

#[test]
fn table_permission_supports_database_qualified_wildcard() {
    let mut user = UserInfo::new("alice", "pw");
    user.permissions.push(Permission {
        permission_type: PermissionType::Insert,
        object_type: PermissionObjectType::Table,
        object_name: "db1.*".to_string(),
    });

    assert!(user.has_permission(
        PermissionType::Insert,
        PermissionObjectType::Table,
        "db1.orders"
    ));
    assert!(!user.has_permission(
        PermissionType::Insert,
        PermissionObjectType::Table,
        "db2.orders"
    ));
}

#[test]
fn revoke_only_removes_matching_type_and_object() {
    let mut store = AuthStore::new();
    store.create_user("alice", "pw").unwrap();
    store
        .grant_permission(
            "alice",
            vec![
                Permission {
                    permission_type: PermissionType::Select,
                    object_type: PermissionObjectType::Table,
                    object_name: "db1.t".to_string(),
                },
                Permission {
                    permission_type: PermissionType::Insert,
                    object_type: PermissionObjectType::Table,
                    object_name: "db1.t".to_string(),
                },
                Permission {
                    permission_type: PermissionType::Select,
                    object_type: PermissionObjectType::Table,
                    object_name: "db2.t".to_string(),
                },
            ],
        )
        .unwrap();

    store
        .revoke_permission("alice", &[PermissionType::Select], Some("db1.t"))
        .unwrap();
    let user = store.get_user("alice").unwrap();

    assert!(!user.has_permission(PermissionType::Select, PermissionObjectType::Table, "db1.t"));
    assert!(user.has_permission(PermissionType::Insert, PermissionObjectType::Table, "db1.t"));
    assert!(user.has_permission(PermissionType::Select, PermissionObjectType::Table, "db2.t"));
}

#[test]
fn new_credentials_use_argon2id_and_verify() {
    let user = UserInfo::new("bob", "s3cret");
    assert!(user.password_hash.starts_with("$argon2"), "hash: {}", user.password_hash);
    assert!(!user.password_hash_is_legacy());
    assert!(user.verify_password("s3cret"));
    assert!(!user.verify_password("wrong"));
    // Salts are random: two users with the same password store different hashes.
    let other = UserInfo::new("carol", "s3cret");
    assert_ne!(user.password_hash, other.password_hash);
}

#[test]
fn legacy_siphash_credentials_verify_and_report_legacy() {
    let mut user = UserInfo::new("bob", "old-pw");
    user.password_hash = legacy_hash_password("old-pw");
    assert!(user.password_hash_is_legacy());
    assert!(user.verify_password("old-pw"));
    assert!(!user.verify_password("wrong"));
}

#[test]
fn upgrade_password_hash_moves_to_argon2id() {
    let mut user = UserInfo::new("bob", "old-pw");
    user.password_hash = legacy_hash_password("old-pw");
    user.upgrade_password_hash("old-pw");
    assert!(user.password_hash.starts_with("$argon2"));
    assert!(!user.password_hash_is_legacy());
    assert!(user.verify_password("old-pw"));
    assert!(!user.verify_password("old-pw-wrong"));
}

#[test]
fn authenticate_migrates_legacy_hash_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let manager = AuthManager::new(dir.path()).unwrap();

    // Simulate a pre-upgrade auth.json entry.
    manager.create_user("legacy", "pw").unwrap();
    {
        let mut store = manager.store.write().unwrap();
        let user = store.get_user_mut("legacy").unwrap();
        user.password_hash = legacy_hash_password("pw");
        store.save(&manager.path).unwrap();
    }

    // Wrong password must not migrate.
    assert!(!manager.authenticate("legacy", "nope").unwrap());
    assert!(
        manager
            .store
            .read()
            .unwrap()
            .get_user("legacy")
            .unwrap()
            .password_hash_is_legacy()
    );

    // Successful login upgrades the stored hash and persists it.
    assert!(manager.authenticate("legacy", "pw").unwrap());
    {
        let store = manager.store.read().unwrap();
        let user = store.get_user("legacy").unwrap();
        assert!(!user.password_hash_is_legacy());
    }
    // Reload from disk: the upgrade survived the restart.
    let reloaded = AuthManager::new(dir.path()).unwrap();
    assert!(reloaded.authenticate("legacy", "pw").unwrap());
    assert!(!reloaded.authenticate("legacy", "nope").unwrap());
}

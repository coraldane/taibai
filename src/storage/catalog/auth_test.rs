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

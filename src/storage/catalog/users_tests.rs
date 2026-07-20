use super::*;
use tempfile::TempDir;

#[test]
fn test_create_user() {
    let mut catalog = UserCatalog::new();
    catalog.create_user("alice", "secret").unwrap();
    assert!(catalog.get_user("alice").is_some());
    assert!(catalog.create_user("alice", "secret2").is_err());
}

#[test]
fn test_authenticate() {
    let mut catalog = UserCatalog::new();
    catalog.create_user("bob", "password123").unwrap();
    let user = catalog.get_user("bob").unwrap();
    assert!(user.verify_password("password123"));
    assert!(!user.verify_password("wrong"));
}

#[test]
fn test_create_role() {
    let mut catalog = UserCatalog::new();
    catalog.create_role("admin").unwrap();
    assert!(catalog.get_role("admin").is_some());
}

#[test]
fn test_grant_role() {
    let mut catalog = UserCatalog::new();
    catalog.create_role("reader").unwrap();
    catalog
        .grant_to_role("reader", PermissionScope::Global, vec![Permission::Select])
        .unwrap();
    let role = catalog.get_role("reader").unwrap();
    assert!(role
        .privileges
        .get("*")
        .unwrap()
        .contains(&Permission::Select));
}

#[test]
fn test_grant_user_role() {
    let mut catalog = UserCatalog::new();
    catalog.create_user("charlie", "pw").unwrap();
    catalog.create_role("writer").unwrap();
    catalog.grant_to_user("charlie", "writer").unwrap();
    let user = catalog.get_user("charlie").unwrap();
    assert!(user.roles.contains(&"writer".to_string()));
}

#[test]
fn test_save_load() {
    let dir = TempDir::new().unwrap();
    let meta_dir = dir.path().join("meta");

    let mut catalog = UserCatalog::new();
    catalog.create_user("dave", "pass").unwrap();
    catalog.create_role("editor").unwrap();
    catalog.save(&meta_dir).unwrap();

    let loaded = UserCatalog::load_or_create(&meta_dir).unwrap();
    assert!(loaded.get_user("dave").is_some());
    assert!(loaded.get_role("editor").is_some());
}

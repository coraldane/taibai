use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::common::error::{MonaxError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub name: String,
    pub password_hash: String,
    pub roles: Vec<String>,
    pub created_at: i64,
}

impl User {
    pub fn new(name: &str, password: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self {
            name: name.to_string(),
            password_hash: hash_password(password),
            roles: Vec::new(),
            created_at: now,
        }
    }

    pub fn verify_password(&self, password: &str) -> bool {
        verify_password_hash(&self.password_hash, password)
    }
}

fn hash_password(password: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    password.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

fn verify_password_hash(hash: &str, password: &str) -> bool {
    hash_password(password) == hash
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub privileges: HashMap<String, Vec<Permission>>,
    pub created_at: i64,
}

impl Role {
    pub fn new(name: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self {
            name: name.to_string(),
            privileges: HashMap::new(),
            created_at: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Permission {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Truncate,
    All,
}

impl Permission {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "SELECT" => Ok(Permission::Select),
            "INSERT" => Ok(Permission::Insert),
            "UPDATE" => Ok(Permission::Update),
            "DELETE" => Ok(Permission::Delete),
            "CREATE" => Ok(Permission::Create),
            "DROP" => Ok(Permission::Drop),
            "ALTER" => Ok(Permission::Alter),
            "TRUNCATE" => Ok(Permission::Truncate),
            "ALL" | "ALL_PRIVILEGES" => Ok(Permission::All),
            _ => Err(MonaxError::Sql(format!("unknown permission: {}", s))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PermissionScope {
    Global,
    Database(String),
    Table(String, String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCatalog {
    pub users: HashMap<String, User>,
    pub roles: HashMap<String, Role>,
    pub created_at: i64,
}

impl UserCatalog {
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self {
            users: HashMap::new(),
            roles: HashMap::new(),
            created_at: now,
        }
    }

    pub fn load_or_create(meta_dir: &Path) -> Result<Self> {
        let path = meta_dir.join("users.json");
        if path.exists() {
            let data = fs::read_to_string(&path)
                .map_err(|e| MonaxError::Storage(format!("failed to read users.json: {}", e)))?;
            let catalog: UserCatalog = serde_json::from_str(&data)
                .map_err(|e| MonaxError::Storage(format!("failed to parse users.json: {}", e)))?;
            Ok(catalog)
        } else {
            let catalog = UserCatalog::new();
            fs::create_dir_all(meta_dir)
                .map_err(|e| MonaxError::Storage(format!("failed to create meta dir: {}", e)))?;
            catalog.save(meta_dir)?;
            Ok(catalog)
        }
    }

    pub fn save(&self, meta_dir: &Path) -> Result<()> {
        fs::create_dir_all(meta_dir)
            .map_err(|e| MonaxError::Storage(format!("failed to create meta dir: {}", e)))?;
        let path = meta_dir.join("users.json");
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| MonaxError::Storage(format!("failed to serialize user catalog: {}", e)))?;
        let tmp_path = path.with_extension("json.tmp");
        fs::write(&tmp_path, &data)
            .map_err(|e| MonaxError::Storage(format!("failed to write users.json.tmp: {}", e)))?;
        fs::rename(&tmp_path, &path)
            .map_err(|e| MonaxError::Storage(format!("failed to rename users.json: {}", e)))?;
        Ok(())
    }

    pub fn create_user(&mut self, name: &str, password: &str) -> Result<()> {
        if self.users.contains_key(name) {
            return Err(MonaxError::Sql(format!("user '{}' already exists", name)));
        }
        self.users.insert(name.to_string(), User::new(name, password));
        Ok(())
    }

    pub fn drop_user(&mut self, name: &str) -> Result<()> {
        if self.users.remove(name).is_none() {
            return Err(MonaxError::Sql(format!("user '{}' does not exist", name)));
        }
        Ok(())
    }

    pub fn get_user(&self, name: &str) -> Option<&User> {
        self.users.get(name)
    }

    pub fn create_role(&mut self, name: &str) -> Result<()> {
        if self.roles.contains_key(name) {
            return Err(MonaxError::Sql(format!("role '{}' already exists", name)));
        }
        self.roles.insert(name.to_string(), Role::new(name));
        Ok(())
    }

    pub fn drop_role(&mut self, name: &str) -> Result<()> {
        if self.roles.remove(name).is_none() {
            return Err(MonaxError::Sql(format!("role '{}' does not exist", name)));
        }
        Ok(())
    }

    pub fn get_role(&self, name: &str) -> Option<&Role> {
        self.roles.get(name)
    }

    pub fn grant_to_user(&mut self, user_name: &str, role_name: &str) -> Result<()> {
        let user = self.users.get_mut(user_name)
            .ok_or_else(|| MonaxError::Sql(format!("user '{}' does not exist", user_name)))?;
        if !self.roles.contains_key(role_name) {
            return Err(MonaxError::Sql(format!("role '{}' does not exist", role_name)));
        }
        if !user.roles.contains(&role_name.to_string()) {
            user.roles.push(role_name.to_string());
        }
        Ok(())
    }

    pub fn revoke_from_user(&mut self, user_name: &str, role_name: &str) -> Result<()> {
        let user = self.users.get_mut(user_name)
            .ok_or_else(|| MonaxError::Sql(format!("user '{}' does not exist", user_name)))?;
        user.roles.retain(|r| r != role_name);
        Ok(())
    }

    pub fn grant_to_role(&mut self, role_name: &str, scope: PermissionScope, permissions: Vec<Permission>) -> Result<()> {
        let role = self.roles.get_mut(role_name)
            .ok_or_else(|| MonaxError::Sql(format!("role '{}' does not exist", role_name)))?;
        let key = scope_to_key(&scope);
        let existing = role.privileges.entry(key).or_insert_with(Vec::new);
        for perm in permissions {
            if !existing.contains(&perm) {
                existing.push(perm);
            }
        }
        Ok(())
    }

    pub fn revoke_from_role(&mut self, role_name: &str, scope: PermissionScope, permissions: Vec<Permission>) -> Result<()> {
        let role = self.roles.get_mut(role_name)
            .ok_or_else(|| MonaxError::Sql(format!("role '{}' does not exist", role_name)))?;
        let key = scope_to_key(&scope);
        if let Some(existing) = role.privileges.get_mut(&key) {
            for perm in permissions {
                existing.retain(|p| p != &perm);
            }
        }
        Ok(())
    }

    pub fn check_permission(&self, user_name: &str, scope: &PermissionScope, permission: &Permission) -> bool {
        let user = self.users.get(user_name);
        if user.is_none() {
            return false;
        }
        let user = user.unwrap();

        for role_name in &user.roles {
            if let Some(role) = self.roles.get(role_name) {
                if role_has_permission(role, scope, permission) {
                    return true;
                }
            }
        }
        false
    }
}

fn scope_to_key(scope: &PermissionScope) -> String {
    match scope {
        PermissionScope::Global => "*".to_string(),
        PermissionScope::Database(db) => format!("db:{}", db),
        PermissionScope::Table(db, table) => format!("table:{}:{}", db, table),
    }
}

fn role_has_permission(role: &Role, scope: &PermissionScope, permission: &Permission) -> bool {
    if role.privileges.get("*").map(|p| p.contains(&Permission::All)).unwrap_or(false) {
        return true;
    }
    
    let key = scope_to_key(scope);
    if let Some(perms) = role.privileges.get(&key) {
        if perms.contains(&Permission::All) {
            return true;
        }
        if perms.contains(permission) {
            return true;
        }
    }

    match scope {
        PermissionScope::Table(db, _) => {
            let db_key = format!("db:{}", db);
            if let Some(perms) = role.privileges.get(&db_key) {
                if perms.contains(&Permission::All) || perms.contains(permission) {
                    return true;
                }
            }
        }
        _ => {}
    }

    false
}

pub struct UserManager {
    catalog: RwLock<UserCatalog>,
    meta_dir: std::path::PathBuf,
}

impl UserManager {
    pub fn new(meta_dir: &Path) -> Result<Self> {
        let catalog = UserCatalog::load_or_create(meta_dir)?;
        Ok(Self {
            catalog: RwLock::new(catalog),
            meta_dir: meta_dir.to_path_buf(),
        })
    }

    pub fn create_user(&self, name: &str, password: &str) -> Result<()> {
        {
            let mut catalog = self.catalog.write().unwrap();
            catalog.create_user(name, password)?;
            catalog.save(&self.meta_dir)?;
        }
        Ok(())
    }

    pub fn drop_user(&self, name: &str, if_exists: bool) -> Result<()> {
        {
            let mut catalog = self.catalog.write().unwrap();
            match catalog.drop_user(name) {
                Ok(_) => {
                    catalog.save(&self.meta_dir)?;
                    Ok(())
                }
                Err(e) => {
                    if if_exists {
                        Ok(())
                    } else {
                        Err(e)
                    }
                }
            }
        }
    }

    pub fn create_role(&self, name: &str) -> Result<()> {
        {
            let mut catalog = self.catalog.write().unwrap();
            catalog.create_role(name)?;
            catalog.save(&self.meta_dir)?;
        }
        Ok(())
    }

    pub fn drop_role(&self, name: &str, if_exists: bool) -> Result<()> {
        {
            let mut catalog = self.catalog.write().unwrap();
            match catalog.drop_role(name) {
                Ok(_) => {
                    catalog.save(&self.meta_dir)?;
                    Ok(())
                }
                Err(e) => {
                    if if_exists {
                        Ok(())
                    } else {
                        Err(e)
                    }
                }
            }
        }
    }

    pub fn grant(&self, permissions: Vec<Permission>, scope: Option<PermissionScope>, target: &str) -> Result<()> {
        {
            let mut catalog = self.catalog.write().unwrap();
            let scope = scope.unwrap_or(PermissionScope::Global);
            
            if catalog.roles.contains_key(target) {
                catalog.grant_to_role(target, scope, permissions)?;
            } else if catalog.users.contains_key(target) {
                for perm in permissions {
                    let role_name = format!("{}_granted", target);
                    if !catalog.roles.contains_key(&role_name) {
                        catalog.create_role(&role_name)?;
                    }
                    catalog.grant_to_role(&role_name, scope.clone(), vec![perm])?;
                    catalog.grant_to_user(target, &role_name)?;
                }
            } else {
                return Err(MonaxError::Sql(format!("'{}' is not a valid user or role", target)));
            }
            catalog.save(&self.meta_dir)?;
        }
        Ok(())
    }

    pub fn revoke(&self, permissions: Vec<Permission>, scope: Option<PermissionScope>, target: &str) -> Result<()> {
        {
            let mut catalog = self.catalog.write().unwrap();
            let scope = scope.unwrap_or(PermissionScope::Global);
            
            if catalog.roles.contains_key(target) {
                catalog.revoke_from_role(target, scope, permissions)?;
            } else if catalog.users.contains_key(target) {
                let role_name = format!("{}_granted", target);
                if catalog.roles.contains_key(&role_name) {
                    catalog.revoke_from_role(&role_name, scope, permissions)?;
                }
            } else {
                return Err(MonaxError::Sql(format!("'{}' is not a valid user or role", target)));
            }
            catalog.save(&self.meta_dir)?;
        }
        Ok(())
    }

    pub fn authenticate(&self, user_name: &str, password: &str) -> Result<bool> {
        let catalog = self.catalog.read().unwrap();
        match catalog.get_user(user_name) {
            Some(user) => Ok(user.verify_password(password)),
            None => Err(MonaxError::Sql(format!("user '{}' does not exist", user_name))),
        }
    }

    pub fn check_permission(&self, user_name: &str, scope: &PermissionScope, permission: &Permission) -> bool {
        let catalog = self.catalog.read().unwrap();
        catalog.check_permission(user_name, scope, permission)
    }

    pub fn catalog(&self) -> &RwLock<UserCatalog> {
        &self.catalog
    }
}

#[cfg(test)]
mod tests {
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
        catalog.grant_to_role("reader", PermissionScope::Global, vec![Permission::Select]).unwrap();
        let role = catalog.get_role("reader").unwrap();
        assert!(role.privileges.get("*").unwrap().contains(&Permission::Select));
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
}
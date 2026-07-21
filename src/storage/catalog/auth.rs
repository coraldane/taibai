#![allow(dead_code)]
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::common::error::{Result, TaibaiError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PermissionType {
    All,
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Truncate,
    Subscribe,
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;

impl PermissionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PermissionType::All => "ALL",
            PermissionType::Select => "SELECT",
            PermissionType::Insert => "INSERT",
            PermissionType::Update => "UPDATE",
            PermissionType::Delete => "DELETE",
            PermissionType::Create => "CREATE",
            PermissionType::Drop => "DROP",
            PermissionType::Alter => "ALTER",
            PermissionType::Truncate => "TRUNCATE",
            PermissionType::Subscribe => "SUBSCRIBE",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permission {
    pub permission_type: PermissionType,
    pub object_type: PermissionObjectType,
    pub object_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PermissionObjectType {
    Global,
    Database,
    Table,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    pub name: String,
    pub password_hash: String,
    pub roles: Vec<String>,
    pub permissions: Vec<Permission>,
    pub created_at: i64,
    #[serde(default)]
    pub account_locked: bool,
}

impl UserInfo {
    pub fn new(name: &str, password: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let password_hash = hash_password(password);
        Self {
            name: name.to_string(),
            password_hash,
            roles: Vec::new(),
            permissions: Vec::new(),
            created_at: now,
            account_locked: false,
        }
    }

    pub fn verify_password(&self, password: &str) -> bool {
        let hash = hash_password(password);
        self.password_hash == hash
    }

    pub fn has_permission(
        &self,
        perm_type: PermissionType,
        object_type: PermissionObjectType,
        object_name: &str,
    ) -> bool {
        self.permissions.iter().any(|p| {
            permission_type_matches(&p.permission_type, &perm_type)
                && permission_object_matches(p, &object_type, object_name)
        }) || self.roles.iter().any(|_role_name| false)
    }
}

fn is_admin_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "root" | "admin" | "administrator"
    )
}

fn permission_type_matches(granted: &PermissionType, requested: &PermissionType) -> bool {
    *granted == PermissionType::All || granted == requested
}

fn permission_object_matches(
    permission: &Permission,
    requested_type: &PermissionObjectType,
    requested_name: &str,
) -> bool {
    if permission.object_type == PermissionObjectType::Global {
        return true;
    }

    match (&permission.object_type, requested_type) {
        (PermissionObjectType::Database, PermissionObjectType::Database) => {
            permission.object_name == requested_name || permission.object_name == "*"
        }
        (PermissionObjectType::Database, PermissionObjectType::Table) => {
            permission.object_name == "*"
                || requested_name
                    .strip_prefix(&format!("{}.", permission.object_name))
                    .is_some()
        }
        (PermissionObjectType::Table, PermissionObjectType::Table) => {
            permission.object_name == requested_name
                || permission.object_name == "*"
                || permission
                    .object_name
                    .strip_suffix(".*")
                    .map(|db| requested_name.starts_with(&format!("{}.", db)))
                    .unwrap_or(false)
        }
        (PermissionObjectType::Table, PermissionObjectType::Database) => {
            permission.object_name == format!("{}.*", requested_name)
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleInfo {
    pub name: String,
    pub permissions: Vec<Permission>,
    pub created_at: i64,
}

impl RoleInfo {
    pub fn new(name: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self {
            name: name.to_string(),
            permissions: Vec::new(),
            created_at: now,
        }
    }
}

fn hash_password(password: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    password.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthStore {
    pub users: HashMap<String, UserInfo>,
    pub roles: HashMap<String, RoleInfo>,
}

impl Default for AuthStore {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthStore {
    pub fn new() -> Self {
        let mut store = Self {
            users: HashMap::new(),
            roles: HashMap::new(),
        };
        let root = UserInfo::new("root", "taibai");
        store.users.insert("root".to_string(), root);
        store
    }

    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            let data = fs::read_to_string(path)
                .map_err(|e| TaibaiError::Storage(format!("failed to read auth.json: {}", e)))?;
            let store: AuthStore = serde_json::from_str(&data)
                .map_err(|e| TaibaiError::Storage(format!("failed to parse auth.json: {}", e)))?;
            Ok(store)
        } else {
            let store = AuthStore::new();
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    TaibaiError::Storage(format!("failed to create auth dir: {}", e))
                })?;
            }
            store.save(path)?;
            Ok(store)
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| TaibaiError::Storage(format!("failed to create auth dir: {}", e)))?;
        }
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| TaibaiError::Storage(format!("failed to serialize auth: {}", e)))?;
        let tmp_path = path.with_extension("json.tmp");
        fs::write(&tmp_path, &data)
            .map_err(|e| TaibaiError::Storage(format!("failed to write auth.json.tmp: {}", e)))?;
        fs::rename(&tmp_path, path)
            .map_err(|e| TaibaiError::Storage(format!("failed to rename auth.json: {}", e)))?;
        Ok(())
    }

    pub fn create_user(&mut self, name: &str, password: &str) -> Result<()> {
        if self.users.contains_key(name) {
            return Err(TaibaiError::Sql(format!("user '{}' already exists", name)));
        }
        let user = UserInfo::new(name, password);
        self.users.insert(name.to_string(), user);
        Ok(())
    }

    pub fn drop_user(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if self.users.remove(name).is_none() && !if_exists {
            return Err(TaibaiError::Sql(format!("user '{}' does not exist", name)));
        }
        Ok(())
    }

    pub fn create_role(&mut self, name: &str) -> Result<()> {
        if self.roles.contains_key(name) {
            return Err(TaibaiError::Sql(format!("role '{}' already exists", name)));
        }
        let role = RoleInfo::new(name);
        self.roles.insert(name.to_string(), role);
        Ok(())
    }

    pub fn drop_role(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if self.roles.remove(name).is_none() && !if_exists {
            return Err(TaibaiError::Sql(format!("role '{}' does not exist", name)));
        }
        Ok(())
    }

    pub fn grant_permission(&mut self, target: &str, permissions: Vec<Permission>) -> Result<()> {
        if let Some(user) = self.users.get_mut(target) {
            for perm in permissions {
                user.permissions.push(perm);
            }
        } else if let Some(role) = self.roles.get_mut(target) {
            for perm in permissions {
                role.permissions.push(perm);
            }
        } else {
            return Err(TaibaiError::Sql(format!(
                "user or role '{}' does not exist",
                target
            )));
        }
        Ok(())
    }

    pub fn revoke_permission(
        &mut self,
        target: &str,
        permissions: &[PermissionType],
        object: Option<&str>,
    ) -> Result<()> {
        let is_user = self.users.contains_key(target);
        let is_role = self.roles.contains_key(target);

        if !is_user && !is_role {
            return Err(TaibaiError::Sql(format!(
                "user or role '{}' does not exist",
                target
            )));
        }

        if is_user {
            let user = self.users.get_mut(target).unwrap();
            user.permissions.retain(|p| {
                let type_match = permissions.contains(&p.permission_type);
                let object_match = object.is_none_or(|obj| p.object_name == obj);
                !(type_match && object_match)
            });
        }
        if is_role {
            let role = self.roles.get_mut(target).unwrap();
            role.permissions.retain(|p| {
                let type_match = permissions.contains(&p.permission_type);
                let object_match = object.is_none_or(|obj| p.object_name == obj);
                !(type_match && object_match)
            });
        }
        Ok(())
    }

    pub fn get_user(&self, name: &str) -> Option<&UserInfo> {
        self.users.get(name)
    }

    pub fn get_role(&self, name: &str) -> Option<&RoleInfo> {
        self.roles.get(name)
    }

    pub fn is_admin_user(&self, name: &str) -> bool {
        if is_admin_name(name) {
            return true;
        }
        let Some(user) = self.users.get(name) else {
            return false;
        };
        user.roles.iter().any(|role| is_admin_name(role))
            || user.permissions.iter().any(|p| {
                p.permission_type == PermissionType::All
                    && p.object_type == PermissionObjectType::Global
            })
    }

    pub fn has_permission(
        &self,
        username: &str,
        perm_type: PermissionType,
        object_type: PermissionObjectType,
        object_name: &str,
    ) -> bool {
        if self.is_admin_user(username) {
            return true;
        }
        let Some(user) = self.users.get(username) else {
            return false;
        };

        if user.permissions.iter().any(|p| {
            permission_type_matches(&p.permission_type, &perm_type)
                && permission_object_matches(p, &object_type, object_name)
        }) {
            return true;
        }

        user.roles.iter().any(|role_name| {
            self.roles
                .get(role_name)
                .map(|role| {
                    role.permissions.iter().any(|p| {
                        permission_type_matches(&p.permission_type, &perm_type)
                            && permission_object_matches(p, &object_type, object_name)
                    })
                })
                .unwrap_or(false)
        })
    }

    pub fn alter_user_password(&mut self, name: &str, password: &str) -> Result<()> {
        if let Some(user) = self.users.get_mut(name) {
            user.password_hash = hash_password(password);
            Ok(())
        } else {
            Err(TaibaiError::Sql(format!("user '{}' does not exist", name)))
        }
    }

    pub fn lock_user(&mut self, name: &str) -> Result<()> {
        if let Some(user) = self.users.get_mut(name) {
            user.account_locked = true;
            Ok(())
        } else {
            Err(TaibaiError::Sql(format!("user '{}' does not exist", name)))
        }
    }

    pub fn unlock_user(&mut self, name: &str) -> Result<()> {
        if let Some(user) = self.users.get_mut(name) {
            user.account_locked = false;
            Ok(())
        } else {
            Err(TaibaiError::Sql(format!("user '{}' does not exist", name)))
        }
    }

    pub fn list_users(&self) -> Vec<String> {
        self.users.keys().cloned().collect()
    }

    pub fn list_roles(&self) -> Vec<String> {
        self.roles.keys().cloned().collect()
    }

    pub fn show_grants(&self, name: &str) -> Vec<String> {
        if let Some(user) = self.users.get(name) {
            user.permissions
                .iter()
                .map(|p| {
                    format!(
                        "GRANT {} ON {} TO {}",
                        p.permission_type.as_str(),
                        p.object_name,
                        name
                    )
                })
                .collect()
        } else {
            Vec::new()
        }
    }
}

pub struct AuthManager {
    store: RwLock<AuthStore>,
    path: std::path::PathBuf,
}

impl AuthManager {
    pub fn new(data_dir: &Path) -> Result<Self> {
        let auth_path = data_dir.join("meta").join("auth.json");
        let store = AuthStore::load_or_create(&auth_path)?;
        Ok(Self {
            store: RwLock::new(store),
            path: auth_path,
        })
    }

    pub fn create_user(&self, name: &str, password: &str) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.create_user(name, password)?;
        store.save(&self.path)?;
        Ok(())
    }

    pub fn drop_user(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.drop_user(name, if_exists)?;
        store.save(&self.path)?;
        Ok(())
    }

    pub fn create_role(&self, name: &str) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.create_role(name)?;
        store.save(&self.path)?;
        Ok(())
    }

    pub fn drop_role(&self, name: &str, if_exists: bool) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.drop_role(name, if_exists)?;
        store.save(&self.path)?;
        Ok(())
    }

    pub fn grant(&self, target: &str, permissions: Vec<Permission>) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.grant_permission(target, permissions)?;
        store.save(&self.path)?;
        Ok(())
    }

    pub fn revoke(
        &self,
        target: &str,
        permissions: &[PermissionType],
        object: Option<&str>,
    ) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.revoke_permission(target, permissions, object)?;
        store.save(&self.path)?;
        Ok(())
    }

    pub fn authenticate(&self, username: &str, password: &str) -> Result<bool> {
        let store = self.store.read().unwrap();
        if store.users.is_empty() {
            return Ok(true);
        }
        match store.get_user(username) {
            Some(user) => {
                if user.account_locked {
                    return Ok(false);
                }
                Ok(user.verify_password(password))
            }
            None => Ok(false),
        }
    }

    pub fn check_permission(
        &self,
        username: &str,
        perm_type: PermissionType,
        object_type: PermissionObjectType,
        object_name: &str,
    ) -> bool {
        let store = self.store.read().unwrap();
        store.has_permission(username, perm_type, object_type, object_name)
    }

    pub fn alter_user_password(&self, name: &str, password: &str) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.alter_user_password(name, password)?;
        store.save(&self.path)?;
        Ok(())
    }

    pub fn lock_user(&self, name: &str) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.lock_user(name)?;
        store.save(&self.path)?;
        Ok(())
    }

    pub fn unlock_user(&self, name: &str) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.unlock_user(name)?;
        store.save(&self.path)?;
        Ok(())
    }

    pub fn list_users(&self) -> Vec<String> {
        let store = self.store.read().unwrap();
        store.list_users()
    }

    pub fn list_roles(&self) -> Vec<String> {
        let store = self.store.read().unwrap();
        store.list_roles()
    }

    pub fn show_grants(&self, name: &str) -> Vec<String> {
        let store = self.store.read().unwrap();
        store.show_grants(name)
    }

    pub fn is_locked(&self, name: &str) -> bool {
        let store = self.store.read().unwrap();
        store
            .get_user(name)
            .map(|u| u.account_locked)
            .unwrap_or(false)
    }
}

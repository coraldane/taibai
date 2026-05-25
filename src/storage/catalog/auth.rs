use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::common::error::{MonaxError, Result};

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
}

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
        if self
            .permissions
            .iter()
            .any(|p| p.permission_type == PermissionType::All)
        {
            return true;
        }
        self.permissions.iter().any(|p| {
            p.permission_type == perm_type
                && (p.object_type == PermissionObjectType::Global
                    || (p.object_type == object_type && p.object_name == object_name))
        }) || self.roles.iter().any(|role_name| false)
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

impl AuthStore {
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
            roles: HashMap::new(),
        }
    }

    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            let data = fs::read_to_string(path)
                .map_err(|e| MonaxError::Storage(format!("failed to read auth.json: {}", e)))?;
            let store: AuthStore = serde_json::from_str(&data)
                .map_err(|e| MonaxError::Storage(format!("failed to parse auth.json: {}", e)))?;
            Ok(store)
        } else {
            let store = AuthStore::new();
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    MonaxError::Storage(format!("failed to create auth dir: {}", e))
                })?;
            }
            store.save(path)?;
            Ok(store)
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| MonaxError::Storage(format!("failed to create auth dir: {}", e)))?;
        }
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| MonaxError::Storage(format!("failed to serialize auth: {}", e)))?;
        let tmp_path = path.with_extension("json.tmp");
        fs::write(&tmp_path, &data)
            .map_err(|e| MonaxError::Storage(format!("failed to write auth.json.tmp: {}", e)))?;
        fs::rename(&tmp_path, path)
            .map_err(|e| MonaxError::Storage(format!("failed to rename auth.json: {}", e)))?;
        Ok(())
    }

    pub fn create_user(&mut self, name: &str, password: &str) -> Result<()> {
        if self.users.contains_key(name) {
            return Err(MonaxError::Sql(format!("user '{}' already exists", name)));
        }
        let user = UserInfo::new(name, password);
        self.users.insert(name.to_string(), user);
        Ok(())
    }

    pub fn drop_user(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if self.users.remove(name).is_none() && !if_exists {
            return Err(MonaxError::Sql(format!("user '{}' does not exist", name)));
        }
        Ok(())
    }

    pub fn create_role(&mut self, name: &str) -> Result<()> {
        if self.roles.contains_key(name) {
            return Err(MonaxError::Sql(format!("role '{}' already exists", name)));
        }
        let role = RoleInfo::new(name);
        self.roles.insert(name.to_string(), role);
        Ok(())
    }

    pub fn drop_role(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if self.roles.remove(name).is_none() && !if_exists {
            return Err(MonaxError::Sql(format!("role '{}' does not exist", name)));
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
            return Err(MonaxError::Sql(format!(
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
            return Err(MonaxError::Sql(format!(
                "user or role '{}' does not exist",
                target
            )));
        }

        if is_user {
            let user = self.users.get_mut(target).unwrap();
            user.permissions.retain(|p| {
                !permissions.contains(&p.permission_type)
                    && object.map_or(true, |obj| p.object_name != obj)
            });
        }
        if is_role {
            let role = self.roles.get_mut(target).unwrap();
            role.permissions.retain(|p| {
                !permissions.contains(&p.permission_type)
                    && object.map_or(true, |obj| p.object_name != obj)
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

    pub fn alter_user_password(&mut self, name: &str, password: &str) -> Result<()> {
        if let Some(user) = self.users.get_mut(name) {
            user.password_hash = hash_password(password);
            Ok(())
        } else {
            Err(MonaxError::Sql(format!("user '{}' does not exist", name)))
        }
    }

    pub fn lock_user(&mut self, name: &str) -> Result<()> {
        if let Some(user) = self.users.get_mut(name) {
            user.account_locked = true;
            Ok(())
        } else {
            Err(MonaxError::Sql(format!("user '{}' does not exist", name)))
        }
    }

    pub fn unlock_user(&mut self, name: &str) -> Result<()> {
        if let Some(user) = self.users.get_mut(name) {
            user.account_locked = false;
            Ok(())
        } else {
            Err(MonaxError::Sql(format!("user '{}' does not exist", name)))
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
        match store.get_user(username) {
            Some(user) => Ok(user.verify_password(password)),
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
        match store.get_user(username) {
            Some(user) => user.has_permission(perm_type, object_type, object_name),
            None => false,
        }
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

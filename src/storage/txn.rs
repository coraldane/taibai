use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Idle,
    Active,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    ReadUncommitted,
    #[default]
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccessMode {
    #[default]
    ReadWrite,
    ReadOnly,
}

#[derive(Debug)]
pub struct Transaction {
    pub id: u64,
    pub state: TxnState,
    pub isolation_level: IsolationLevel,
    pub access_mode: AccessMode,
    pub started_at: Instant,
    pub had_writes: bool,
}

impl Transaction {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            state: TxnState::Active,
            isolation_level: IsolationLevel::default(),
            access_mode: AccessMode::default(),
            started_at: Instant::now(),
            had_writes: false,
        }
    }

    pub fn with_isolation(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    pub fn with_access_mode(mut self, mode: AccessMode) -> Self {
        self.access_mode = mode;
        self
    }

    pub fn is_active(&self) -> bool {
        self.state == TxnState::Active
    }

    pub fn is_idle(&self) -> bool {
        self.state == TxnState::Idle
    }

    pub fn is_read_only(&self) -> bool {
        self.access_mode == AccessMode::ReadOnly
    }

    pub fn commit(&mut self) {
        if self.state == TxnState::Active {
            self.state = TxnState::Committed;
        }
    }

    pub fn rollback(&mut self) {
        if self.state == TxnState::Active {
            self.state = TxnState::RolledBack;
        }
    }

    pub fn mark_write(&mut self) {
        self.had_writes = true;
    }

    pub fn elapsed_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyControl {
    Mvcc,
    OptimisticConcurrencyControl,
    TwoPhaseLocking,
}

impl ConcurrencyControl {
    pub fn supports_isolation(&self, level: IsolationLevel) -> bool {
        match self {
            ConcurrencyControl::Mvcc => matches!(
                level,
                IsolationLevel::ReadCommitted
                    | IsolationLevel::RepeatableRead
                    | IsolationLevel::Serializable
            ),
            ConcurrencyControl::OptimisticConcurrencyControl => {
                matches!(level, IsolationLevel::ReadCommitted | IsolationLevel::Serializable)
            }
            ConcurrencyControl::TwoPhaseLocking => true,
        }
    }
}

pub struct TransactionConfig {
    pub default_isolation_level: IsolationLevel,
    pub default_access_mode: AccessMode,
    pub concurrency_control: ConcurrencyControl,
    pub timeout_secs: u64,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            default_isolation_level: IsolationLevel::ReadCommitted,
            default_access_mode: AccessMode::ReadWrite,
            concurrency_control: ConcurrencyControl::Mvcc,
            timeout_secs: 300,
        }
    }
}
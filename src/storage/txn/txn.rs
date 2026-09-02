#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committed,
    RolledBack,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    #[default]
    ReadCommitted,
}

#[derive(Debug)]
pub struct Transaction {
    pub id: u64,
    pub state: TxnState,
    pub isolation_level: IsolationLevel,
    pub had_writes: bool,
}

impl Transaction {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            state: TxnState::Active,
            isolation_level: IsolationLevel::default(),
            had_writes: false,
        }
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
}

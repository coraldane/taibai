# Transaction State Model and Session Management

## Transaction State Model

### TxnState Enum

The `TxnState` enum tracks the lifecycle of a transaction:

| State | Description |
|-------|-------------|
| `Idle` | Initial state, no transaction active (session-level default) |
| `Active` | Transaction is currently executing |
| `Committed` | Transaction has successfully committed |
| `RolledBack` | Transaction was rolled back |

### State Transitions

```
         BEGIN
Idle ─────────────► Active
                        │
           ┌────────────┴────────────┐
           │                         │
        COMMIT                   ROLLBACK
           │                         │
           ▼                         ▼
      Committed                RolledBack
```

## Isolation Levels

The `IsolationLevel` enum supports standard SQL isolation levels:

| Level | Description | Current Support |
|-------|-------------|-----------------|
| `ReadUncommitted` | Can read uncommitted changes from other transactions | Planned |
| `ReadCommitted` | Default - only see committed changes | Implemented |
| `RepeatableRead` | Guarantees consistent reads within a transaction | Planned |
| `Serializable` | Full isolation, transactions appear sequential | Planned |

**Default**: `ReadCommitted`

## Access Modes

The `AccessMode` enum defines transaction behavior:

| Mode | Description |
|------|-------------|
| `ReadWrite` | Default - transaction can read and write |
| `ReadOnly` | Transaction can only read, enables optimizations |

## Session Management

### Session Structure

```rust
pub struct Session {
    pub id: Uuid,
    pub database: String,
    pub username: String,
    pub created_at: Instant,
    pub last_activity: Instant,
    pub prepared_statements: PreparedStatementCache,
    pub active_txn: Option<Transaction>,
    pub default_isolation_level: IsolationLevel,
    pub default_access_mode: AccessMode,
    // ... other fields
}
```

### Key Methods

| Method | Purpose |
|--------|---------|
| `begin_txn(txn_id)` | Start transaction with default settings |
| `begin_txn_with_options(txn_id, isolation, access)` | Start with explicit settings |
| `commit_txn()` | Commit active transaction |
| `rollback_txn()` | Rollback active transaction |
| `txn_isolation_level()` | Get current transaction's isolation |
| `txn_access_mode()` | Get current transaction's access mode |
| `txn_is_read_only()` | Check if transaction is read-only |

## Concurrency Control Strategy

### Primary Strategy: MVCC (Multi-Version Concurrency Control)

Taibai uses MVCC as its primary concurrency control mechanism:

1. **Snapshot Isolation**: Each transaction sees a consistent snapshot of data at transaction start time
2. **Version Chains**: Multiple versions of rows are maintained for concurrent readers
3. **No Read Locks**: Readers don't block writers, writers don't block readers
4. **Write Set Tracking**: Conflicts detected at commit time

### Concurrency Control Types

```rust
pub enum ConcurrencyControl {
    Mvcc,                       // Primary - best for read-heavy workloads
    OptimisticConcurrencyControl, // Good for low-conflict scenarios
    TwoPhaseLocking,            // Fallback for strict consistency
}
```

### Isolation Level Support Matrix

| Control Strategy | ReadUncommitted | ReadCommitted | RepeatableRead | Serializable |
|-----------------|----------------|---------------|----------------|---------------|
| MVCC | ✗ | ✓ | ✓ | ✓ |
| OCC | ✗ | ✓ | ✗ | ✓ |
| 2PL | ✓ | ✓ | ✓ | ✓ |

## Transaction Configuration

```rust
pub struct TransactionConfig {
    pub default_isolation_level: IsolationLevel,
    pub default_access_mode: AccessMode,
    pub concurrency_control: ConcurrencyControl,
    pub timeout_secs: u64,  // Default: 300
}
```

## Design Decisions

### Why MVCC as Primary Strategy?

1. **Non-blocking reads**: Critical for analytical workloads
2. **Natural fit for columnar storage**: Version management aligns with segment-based storage
3. **Snapshot isolation**: Provides good balance between consistency and performance
4. **Well-understood**: Proven in systems like PostgreSQL, MySQL, CockroachDB

### Why `Idle` State?

- Tracks session without active transaction
- Enables proper resource cleanup
- Supports connection pooling semantics
- Clear distinction between "no transaction" and "rolled back"

### Session-Level Defaults

- Isolation level and access mode can be set per-session
- Applied to all subsequent BEGIN statements
- Allows per-transaction overrides via `begin_txn_with_options`
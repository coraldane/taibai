# SQL Transaction Support Analysis: BEGIN / COMMIT / ROLLBACK

## 1. Current SQL Parsing Flow

### Entry Point
`Engine::execute()` in `src/sql/execution/engine.rs` is the main entry point for all SQL execution.

**Flow:**
```
Engine::execute(session, sql)
  ├─ Fast-path: try_fast_parse_insert(sql)   // INSERT-only shortcut
  └─ Normal-path: Parser::parse(sql) → execute_stmt(session, &stmts[0])
```

### Lexer (`src/sql/parser/lexer.rs`)
- Hand-written tokenizer producing `Token` enum
- **No transaction keywords exist**: `BEGIN`, `COMMIT`, `ROLLBACK`, `TRANSACTION`, `WORK` are missing
- Keywords map in `read_ident_or_keyword()` — these would be added here
- Unknown keywords fall through to `Token::Ident(word)` (so `BEGIN` is currently treated as a bare identifier)

### Parser (`src/sql/parser/parser.rs`)
- Recursive descent parser
- `parse_statement()` dispatches on the first token via a `match` on `self.peek()`
- Currently supported: `Create`, `Drop`, `Insert`, `Update`, `Delete`, `Select`, `Show`, `Use`, `Explain`
- **No transaction statement parsing** — `BEGIN`/`COMMIT`/`ROLLBACK` tokens are not matched and would produce `unexpected token` error

### AST (`src/sql/parser/ast.rs`)
- `Statement` enum defines all AST node types
- **No transaction variants**: `Begin`, `Commit`, `Rollback` do not exist
- These need to be added as new `Statement` variants

---

## 2. Current Execution Engine Flow

### Engine (`src/sql/execution/engine.rs`)
```
Engine::execute_stmt(session, stmt)
  ├─ CreateTable → ddl::exec_create_table()
  ├─ DropTable   → ddl::exec_drop_table()
  ├─ Insert      → dml_insert::exec_insert()
  ├─ Update      → dml_update::exec_update()
  ├─ Delete      → dml_delete::exec_delete()
  ├─ Select      → query::exec_select()
  ├─ UseDatabase → session.database = ...
  ├─ ShowTables  → ddl::exec_show_tables()
  ├─ ShowDatabases → ddl::exec_show_databases()
  └─ Explain     → explain::exec_explain()
```

**Key observation**: `execute_stmt()` does a full `match` on all `Statement` variants. Adding new variants requires adding new match arms.

### Session (`src/net/session.rs`)
- Has `active_txn_id: u64` field (default 0)
- No actual transaction state tracking beyond a simple counter
- No reference to the `Transaction` struct from `src/storage/txn.rs`

---

## 3. Existing Transaction Infrastructure (Protocol Level)

### Protocol Messages (`src/protocol/message.rs`)
Already defined:
| Constant | Value | Purpose |
|---|---|---|
| `MSG_TYPE_BEGIN_TXN` | 0x30 | Begin transaction |
| `MSG_TYPE_COMMIT_TXN` | 0x31 | Commit transaction |
| `MSG_TYPE_ROLLBACK_TXN` | 0x32 | Rollback transaction |

### Handler (`src/net/handler.rs`)
Transaction message handlers already exist but are **stub implementations**:
```rust
fn handle_begin_txn()    → session.active_txn_id += 1; build_ok(session.active_txn_id)
fn handle_commit_txn()   → session.active_txn_id = 0; build_ok(0)
fn handle_rollback_txn() → session.active_txn_id = 0; build_ok(0)
```
These are **no-ops** — they don't interact with the storage layer at all.

### Client (`src/net/client.rs`)
- **No `begin()`/`commit()`/`rollback()` methods** on the client
- Transaction operations can only be invoked via raw protocol messages (not SQL or client API)

### Transaction struct (`src/storage/txn.rs`)
Already exists with:
- `TxnState`: `Active`, `Committed`, `RolledBack`
- `Transaction`: id, state, started_at, had_writes
- `commit()`, `rollback()`, `mark_write()` methods
- **Not used anywhere** — no integration with Engine or Session

---

## 4. Transaction Hook Integration Points

To implement SQL-level `BEGIN`/`COMMIT`/`ROLLBACK`, the following integration points need changes:

### Layer 1: Lexer (Add keywords)
**File**: `src/sql/parser/lexer.rs`
- Add `Token::Begin`, `Token::Commit`, `Token::Rollback`, `Token::Transaction`, `Token::Work`
- Map keywords in `read_ident_or_keyword()`

### Layer 2: Parser (Parse transaction statements)
**File**: `src/sql/parser/parser.rs`
- Add `Token::Begin | Token::Commit | Token::Rollback` arms in `parse_statement()`
- Parse `BEGIN [WORK] [TRANSACTION]`, `COMMIT [WORK] [TRANSACTION]`, `ROLLBACK [WORK] [TRANSACTION]`

### Layer 3: AST (Define transaction nodes)
**File**: `src/sql/parser/ast.rs`
- Add to `Statement` enum:
  - `Begin`
  - `Commit`
  - `Rollback`

### Layer 4: Engine (Execute transaction statements)
**File**: `src/sql/execution/engine.rs`
- Add match arms in `execute_stmt()` for `Statement::Begin`, `Statement::Commit`, `Statement::Rollback`
- These should call transaction management functions that interact with storage

### Layer 5: Session (Track real transaction state)
**File**: `src/net/session.rs`
- Replace `active_txn_id: u64` with proper `Option<Transaction>` or a reference to a transaction manager
- Track whether the session is in a transaction, which tables were written, etc.

### Layer 6: Storage Integration (Commit/Rollback semantics)
**Files**: `src/storage/txn.rs`, `src/storage/batch_writer.rs`, `src/storage/memtable.rs`

**BEGIN semantics:**
- Create a new `Transaction` with allocated txn_id
- Set session into transactional mode
- Optionally defer memtable flushes until commit

**COMMIT semantics:**
- Mark transaction as committed
- Flush all memtables belonging to this transaction
- Write WAL commit marker
- Clear WAL entries for committed transaction

**ROLLBACK semantics:**
- Mark transaction as rolled back
- Discard all memtables written during this transaction (`clear_table` or similar)
- Clear WAL entries for rolled-back transaction
- This is the most complex part — currently MemTable doesn't track which transaction owns it

### Layer 7: DML Execution (Mark writes in transaction)
**Files**: `src/sql/execution/dml_insert.rs`, `dml_update.rs`, `dml_delete.rs`
- When executing within a transaction, call `txn.mark_write()`
- Tag memtable/wal entries with the active transaction ID

### Layer 8: Client API
**File**: `src/net/client.rs`
- Add `begin()`, `commit()`, `rollback()` async methods
- These can send SQL (`BEGIN`/`COMMIT`/`ROLLBACK`) or protocol messages

---

## 5. Minimal Implementation Strategy

For a first iteration (autocommit-only semantics with session-level tracking):

1. **Lexer + Parser + AST**: Add `BEGIN`, `COMMIT`, `ROLLBACK` as recognized SQL statements
2. **Engine**: Wire them to session transaction state (reuse existing `active_txn_id`)
3. **Session**: Properly store `Transaction` object from `src/storage/txn.rs`
4. **COMMIT**: Flush all buffers (call `engine.flush_all_buffers()`)
5. **ROLLBACK**: Clear pending memtables for tables written in this transaction
6. **No MVCC yet**: Reads always see the latest data; isolation is effectively "read uncommitted"

This provides SQL-level transaction syntax that clients expect, with real commit/rollback semantics around buffer management, without requiring full MVCC.

---

## 6. Key Gap: MemTable → Transaction Association

Currently `MemTable` has no `txn_id` field. For proper rollback support, memtables need to know which transaction created them. This requires:

- Adding `txn_id: Option<u64>` to `MemTable`
- Filtering memtables by transaction ID during scans (for isolation)
- On rollback: remove only memtables belonging to the rolled-back transaction
- On commit: mark memtables as committed (txn_id → None or Committed)

This is the most significant storage-layer change required.

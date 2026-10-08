//! Guard: every concrete SQL statement shown in docs/user-guide/sql-reference.md
//! must parse with the current grammar. Keeps the SQL reference from drifting
//! away from the implementation (statements with CJK placeholders, syntax
//! skeletons and protocol-only commands are skipped, not parseable here).

use std::path::Path;

use taibai::sql::parser::parser::Parser;

const DOC: &str = "docs/user-guide/sql-reference.md";

const STATEMENT_KEYWORDS: &[&str] = &[
    "CREATE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "INSERT", "REPLACE", "UPDATE",
    "DELETE", "SELECT", "WITH", "SHOW", "DESCRIBE", "USE", "EXPLAIN", "BEGIN", "COMMIT",
    "ROLLBACK", "FLUSH", "KILL", "OPTIMIZE", "ANALYZE", "REBUILD", "PAUSE", "RESUME", "RESET",
];

fn is_concrete_statement(stmt: &str) -> bool {
    let trimmed = stmt.trim();
    if trimmed.is_empty() {
        return false;
    }
    // Syntax skeletons carry CJK placeholders (列名 / 表达式 / ...).
    if trimmed.chars().any(|c| c as u32 >= 0x2E80) {
        return false;
    }
    // Protocol-level session commands are matched by string prefix in the
    // network handler, not parsed as SQL.
    if trimmed.starts_with("START BULK IMPORT") || trimmed.starts_with("STOP BULK IMPORT") {
        return false;
    }
    let upper = trimmed.to_ascii_uppercase();
    STATEMENT_KEYWORDS.iter().any(|kw| upper.starts_with(kw)) || upper.starts_with('(')
}

#[test]
fn sql_reference_examples_parse_with_current_grammar() {
    let doc_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(DOC);
    let text = std::fs::read_to_string(&doc_path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", doc_path.display()));

    let mut checked = 0usize;
    let mut failures = Vec::new();
    for (block_idx, block) in extract_sql_blocks(&text).iter().enumerate() {
        for stmt in split_statements(block) {
            if !is_concrete_statement(&stmt) {
                continue;
            }
            checked += 1;
            if let Err(e) = Parser::parse(&stmt) {
                failures.push(format!("sql block #{block_idx}: {stmt}\n  -> {e}"));
            }
        }
    }
    assert!(
        checked >= 20,
        "expected a healthy sample of concrete statements, found {checked}"
    );
    assert!(
        failures.is_empty(),
        "sql-reference.md contains statements the parser rejects:\n{}",
        failures.join("\n")
    );
}

fn extract_sql_blocks(text: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("```sql") {
        let after = &rest[start + 6..];
        let Some(end) = after.find("```") else { break };
        blocks.push(after[..end].to_string());
        rest = &after[end + 3..];
    }
    blocks
}

/// Naive split on `;` is safe for this document: no statement embeds a
/// semicolon inside a string literal.
fn split_statements(block: &str) -> Vec<String> {
    block.split(';').map(|s| s.to_string()).collect()
}

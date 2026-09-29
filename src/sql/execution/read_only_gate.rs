//! Returns true when the SQL statement is a read (SELECT/SHOW/EXPLAIN/USE/
//! transaction inspection). Used by the standby read-only gate: everything
//! else (writes, DDL, maintenance) is rejected on a standby. Best-effort by
//! design - the parser still decides the statement kind for non-SQL text.
pub fn starts_read_keyword(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let starts = |kw: &str| {
        trimmed.len() >= kw.len()
            && trimmed[..kw.len()].eq_ignore_ascii_case(kw)
            && trimmed[kw.len()..]
                .chars()
                .next()
                .map(|c| c.is_whitespace() || c == b'(' as char || c == ';')
                .unwrap_or(false)
    };
    starts("SELECT")
        || starts("SHOW")
        || starts("EXPLAIN")
        || starts("USE")
        || starts("DESCRIBE")
        || starts("DESC")
        || starts("KILL")
        || starts("PAUSE")
        || starts("RESUME")
        || trimmed.starts_with('(')
}

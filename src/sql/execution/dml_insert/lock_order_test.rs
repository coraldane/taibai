#[test]
fn replace_does_not_publish_catalog_while_holding_table_lock() {
    let source = include_str!("write.rs");
    let replace = source
        .split_once("pub fn exec_replace(")
        .unwrap()
        .1
        .split_once("pub fn exec_insert(")
        .unwrap()
        .0;
    let locked = replace
        .split_once("with_table_mutation_lock")
        .unwrap()
        .1
        .split_once("})?;")
        .unwrap()
        .0;
    assert!(!locked.contains("record_inserted_rows"));
    assert!(!locked.contains("persist_auto_increment_next_value"));
    assert!(!locked.contains("finalize_replace_delete("));
}

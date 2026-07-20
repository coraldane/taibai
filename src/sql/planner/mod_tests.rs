use super::*;
use crate::protocol::types::DataType;
use crate::sql::parser::parser::Parser;
use crate::types::batch::ColumnDef;

fn make_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("age", DataType::Int32),
    ])
}

#[test]
fn test_plan_select_star() {
    let schema = make_schema();
    let mut schemas = std::collections::HashMap::new();
    schemas.insert("users".into(), schema);

    let stmts = Parser::parse("SELECT * FROM users").unwrap();
    let plan = plan_select(&stmts[0], &schemas).unwrap();

    match &plan {
        LogicalPlan::Project(p) => {
            assert_eq!(p.expressions.len(), 3);
            assert!(matches!(&*p.input, LogicalPlan::Scan(_)));
        }
        _ => panic!("expected Project at root"),
    }
}

#[test]
fn test_plan_select_with_filter() {
    let schema = make_schema();
    let mut schemas = std::collections::HashMap::new();
    schemas.insert("users".into(), schema);

    let stmts = Parser::parse("SELECT id FROM users WHERE age > 30").unwrap();
    let plan = plan_select(&stmts[0], &schemas).unwrap();

    match &plan {
        LogicalPlan::Project(p) => match &*p.input {
            LogicalPlan::Filter(f) => {
                assert!(matches!(&*f.input, LogicalPlan::Scan(_)));
            }
            _ => panic!("expected Filter"),
        },
        _ => panic!("expected Project"),
    }
}

#[test]
fn test_plan_select_with_limit_order() {
    let schema = make_schema();
    let mut schemas = std::collections::HashMap::new();
    schemas.insert("users".into(), schema);

    let stmts = Parser::parse("SELECT * FROM users ORDER BY age DESC LIMIT 10").unwrap();
    let plan = plan_select(&stmts[0], &schemas).unwrap();

    match &plan {
        LogicalPlan::Project(p) => match &*p.input {
            LogicalPlan::Limit(l) => {
                assert_eq!(l.limit, 10);
                match &*l.input {
                    LogicalPlan::Sort(s) => {
                        assert_eq!(s.sort_keys.len(), 1);
                        assert!(s.sort_keys[0].desc);
                    }
                    _ => panic!("expected Sort"),
                }
            }
            _ => panic!("expected Limit"),
        },
        _ => panic!("expected Project"),
    }
}

#[test]
fn test_plan_aggregate() {
    let schema = make_schema();
    let mut schemas = std::collections::HashMap::new();
    schemas.insert("users".into(), schema);

    let stmts = Parser::parse("SELECT COUNT(*) FROM users").unwrap();
    let plan = plan_select(&stmts[0], &schemas).unwrap();

    fn has_aggregate(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Aggregate(_) => true,
            LogicalPlan::Filter(f) => has_aggregate(&f.input),
            LogicalPlan::Project(p) => has_aggregate(&p.input),
            LogicalPlan::Sort(s) => has_aggregate(&s.input),
            LogicalPlan::Limit(l) => has_aggregate(&l.input),
            _ => false,
        }
    }
    assert!(has_aggregate(&plan));
}

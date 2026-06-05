use crate::sql::planner::*;

use super::Rule;

pub struct JoinReorderRule;

impl Rule for JoinReorderRule {
    fn name(&self) -> &str {
        "join_reorder"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        (plan, false)
    }
}

use hawdb_cascades::{
    apply_rule_batch, ApplyOrder, Memo, OptimizationPipeline, OptimizationStage, OptimizerRule,
    RuleApplication, RuleId, RuleKind, RulePromise, RuleStage,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Expression(&'static str);

struct RewriteRule;

impl OptimizerRule<Expression> for RewriteRule {
    fn id(&self) -> RuleId {
        RuleId::new("rewrite_scan", RuleKind::Transformation)
    }

    fn promise(&self, expression: &Expression) -> RulePromise {
        if expression.0 == "scan" {
            RulePromise::new(10)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, _expression: &Expression) -> Option<RuleApplication<Expression>> {
        Some(RuleApplication::new(Expression("index_scan"), "indexed"))
    }
}

#[test]
fn external_consumer_can_build_a_domain_specific_pipeline() {
    let rule = RewriteRule;
    let pipeline = OptimizationPipeline::new(vec![RuleStage::new(
        OptimizationStage::new("rewrite", ApplyOrder::Once),
        vec![&rule],
    )]);
    let execution = pipeline.execute(Expression("scan"));

    assert_eq!(execution.expression(), &Expression("index_scan"));
    assert_eq!(execution.events().len(), 1);

    let batch = apply_rule_batch(&Expression("scan"), &[&rule]);
    let mut memo = Memo::default();
    let group = memo.insert_group(batch.expressions()[0].application().expression().clone());

    assert_eq!(
        memo.group(group).unwrap().first_expression(),
        Some(&Expression("index_scan"))
    );
}

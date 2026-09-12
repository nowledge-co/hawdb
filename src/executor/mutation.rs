//! Concrete-store entrypoint for storage-neutral mutation execution.

use super::*;

use skein_executor::mutation::execute_mutation_with_store;
pub use skein_executor::mutation::project_staged_mutation_return_rows;
pub use skein_executor::mutation::{is_mutation_plan, mutation_command};
pub(super) use skein_executor::mutation::{
    node_set_assignment, relationship_on_create_property_value,
};

pub fn execute_mutation_with_limits(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    execute_mutation_with_store(plan, catalog, store, limits, task_context)
}

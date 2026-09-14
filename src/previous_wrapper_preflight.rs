//! Compatibility re-exports for the readiness-owned previous-wrapper preflight.

pub use skein_readiness::previous_wrapper_preflight::*;

#[cfg(test)]
mod tests {
    use super::{
        nowledge_previous_wrapper_preflight_check_usage,
        run_nowledge_previous_wrapper_preflight_check,
        NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL,
    };

    #[test]
    fn root_compatibility_module_preserves_preflight_entrypoint() {
        let error = run_nowledge_previous_wrapper_preflight_check(std::iter::empty())
            .expect_err("missing inputs must retain the public usage error");
        assert_eq!(
            error.to_string(),
            format!(
                "semantic error: {}",
                nowledge_previous_wrapper_preflight_check_usage()
            )
        );
        assert_eq!(
            NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL,
            skein_readiness::previous_wrapper_preflight::NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL,
        );
    }
}

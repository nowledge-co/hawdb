.DEFAULT_GOAL := fuzz-help

BAZEL ?= bazel

FUZZ_SEED ?= 7
FUZZ_CASES ?=
FUZZ_CASE_INDEX ?=
FUZZ_STEPS ?=
FUZZ_LOG_DIRECTORY ?= target/fuzz-logs
FUZZ_SHARD_INDEX ?= 0
FUZZ_SHARD_COUNT ?= 1
FUZZ_PROGRESS_INTERVAL ?= 16
FUZZ_RESUME ?=
FUZZ_PRINT_REPORT ?=
FUZZ_OPTIMIZER_ARGS ?=
FUZZ_STORAGE_ARGS ?=
FUZZ_APPEND_ARGS ?=

FUZZ_CASES_ARG = $(if $(strip $(FUZZ_CASES)),--cases $(FUZZ_CASES),)
FUZZ_CASE_INDEX_ARG = $(if $(strip $(FUZZ_CASE_INDEX)),--case-index $(FUZZ_CASE_INDEX),)
FUZZ_STEPS_ARG = $(if $(strip $(FUZZ_STEPS)),--steps $(FUZZ_STEPS),)
FUZZ_RESUME_ARG = $(if $(strip $(FUZZ_RESUME)),--resume,)
FUZZ_PRINT_REPORT_ARG = $(if $(strip $(FUZZ_PRINT_REPORT)),--print-report,)
FUZZ_COMMON_ARGS = $(strip \
	--seed $(FUZZ_SEED) \
	$(FUZZ_CASES_ARG) \
	$(FUZZ_CASE_INDEX_ARG) \
	--log-directory "$(FUZZ_LOG_DIRECTORY)" \
	$(FUZZ_PRINT_REPORT_ARG))
FUZZ_CAMPAIGN_EXECUTION_ARGS = $(strip \
	--shard-index $(FUZZ_SHARD_INDEX) \
	--shard-count $(FUZZ_SHARD_COUNT) \
	--progress-interval $(FUZZ_PROGRESS_INTERVAL) \
	$(FUZZ_RESUME_ARG))

.PHONY: \
	fuzz \
	fuzz-append \
	fuzz-append-resume \
	fuzz-help \
	fuzz-optimizer \
	fuzz-optimizer-resume \
	fuzz-smoke \
	fuzz-storage \
	fuzz-test

fuzz: fuzz-optimizer fuzz-storage fuzz-append

fuzz-optimizer:
	$(BAZEL) run //crates/fuzz:skein_optimizer_fuzz -- $(FUZZ_COMMON_ARGS) $(FUZZ_CAMPAIGN_EXECUTION_ARGS) $(FUZZ_OPTIMIZER_ARGS)

fuzz-optimizer-resume: FUZZ_RESUME := 1
fuzz-optimizer-resume: fuzz-optimizer

fuzz-storage:
	$(BAZEL) run //crates/fuzz:skein_storage_fuzz -- $(FUZZ_COMMON_ARGS) $(FUZZ_STORAGE_ARGS)

fuzz-append:
	$(BAZEL) run //crates/fuzz:skein_append_fuzz -- $(FUZZ_COMMON_ARGS) $(FUZZ_STEPS_ARG) $(FUZZ_CAMPAIGN_EXECUTION_ARGS) $(FUZZ_APPEND_ARGS)

fuzz-append-resume: FUZZ_RESUME := 1
fuzz-append-resume: fuzz-append

fuzz-smoke:
	$(BAZEL) test //:skein_linux_ci_fuzz_smoke_test

fuzz-test:
	$(BAZEL) test \
		//crates/fuzz:skein_fuzz_tests \
		//crates/fuzz:skein_fuzz_cli_tests \
		//:skein_linux_ci_fuzz_smoke_test

fuzz-help:
	@printf '%s\n' \
		'Fuzz targets:' \
		'  make fuzz                    Run all native fuzz campaigns.' \
		'  make fuzz-optimizer          Run the optimizer/query campaign.' \
		'  make fuzz-optimizer-resume   Resume the matching optimizer campaign.' \
		'  make fuzz-storage            Run the storage corruption campaign.' \
		'  make fuzz-append             Run the Strict Append state-machine campaign.' \
		'  make fuzz-append-resume      Resume the matching Strict Append campaign.' \
		'  make fuzz-smoke              Run the Bazel fuzz smoke test.' \
		'  make fuzz-test               Run the Bazel fuzz regression suite.' \
		'' \
		'Common variables:' \
		'  FUZZ_SEED=7 FUZZ_CASES=128 FUZZ_CASE_INDEX=19 FUZZ_STEPS=256' \
		'  FUZZ_LOG_DIRECTORY=target/fuzz-logs FUZZ_PRINT_REPORT=1' \
		'  FUZZ_SHARD_INDEX=0 FUZZ_SHARD_COUNT=4 FUZZ_PROGRESS_INTERVAL=16' \
		'' \
		'Advanced runner arguments:' \
		'  FUZZ_OPTIMIZER_ARGS="..." FUZZ_STORAGE_ARGS="..." FUZZ_APPEND_ARGS="..."'

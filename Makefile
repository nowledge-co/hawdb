# Copyright 2026 Nowledge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.DEFAULT_GOAL := fuzz-help

BAZEL ?= bazel

MVCC_SECONDS ?= 30
MVCC_SEED ?= 7
MVCC_LOG_DIRECTORY ?= target/mvcc-fuzz
MVCC_FUZZ_ARGS ?=
SHIRO_SECONDS ?=
SHIRO_SEED ?= 7
SHIRO_LOG_DIRECTORY ?= target/fuzz-logs
SHIRO_FUZZ_ARGS ?=

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
FUZZ_PARSER_ARGS ?=

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
	shiro \
	shiro-fuzz \
	mvcc-fuzz \
	fuzz-append \
	fuzz-append-resume \
	fuzz-help \
	fuzz-optimizer \
	fuzz-optimizer-resume \
	fuzz-parser \
	fuzz-smoke \
	fuzz-storage \
	fuzz-test

fuzz: fuzz-optimizer fuzz-storage fuzz-append fuzz-parser

shiro: shiro-fuzz

mvcc-fuzz:
	$(BAZEL) run --//:compression_test_support=true //tools/mvcc-fuzz:hawdb_mvcc_fuzz -- run --seconds $(MVCC_SECONDS) --seed $(MVCC_SEED) --log-directory "$(MVCC_LOG_DIRECTORY)" --explain $(MVCC_FUZZ_ARGS)

shiro-fuzz:
	$(BAZEL) run --//:compression_test_support=true //tools/shiro:shiro -- $(if $(SHIRO_SECONDS),--timeout-seconds $(SHIRO_SECONDS),) --seed $(SHIRO_SEED) --log-directory "$(SHIRO_LOG_DIRECTORY)" $(SHIRO_FUZZ_ARGS)

fuzz-optimizer:
	$(BAZEL) run --//:compression_test_support=true //tools/shiro:shiro -- $(if $(or $(FUZZ_CASES),$(FUZZ_CASE_INDEX)),,--cases 128) $(FUZZ_COMMON_ARGS) $(FUZZ_CAMPAIGN_EXECUTION_ARGS) $(FUZZ_OPTIMIZER_ARGS)

fuzz-optimizer-resume: FUZZ_RESUME := 1
fuzz-optimizer-resume: fuzz-optimizer

fuzz-storage:
	$(BAZEL) run --//:compression_test_support=true //tools/shiro:shiro_storage -- $(FUZZ_COMMON_ARGS) $(FUZZ_STORAGE_ARGS)

fuzz-parser:
	$(BAZEL) run --//:compression_test_support=true //tools/shiro:shiro_parser -- $(FUZZ_COMMON_ARGS) $(FUZZ_PARSER_ARGS)

fuzz-append:
	$(BAZEL) run --//:compression_test_support=true //tools/shiro:shiro_append -- $(FUZZ_COMMON_ARGS) $(FUZZ_STEPS_ARG) $(FUZZ_CAMPAIGN_EXECUTION_ARGS) $(FUZZ_APPEND_ARGS)

fuzz-append-resume: FUZZ_RESUME := 1
fuzz-append-resume: fuzz-append

fuzz-smoke:
	$(BAZEL) test --//:compression_test_support=true //:hawdb_linux_ci_fuzz_smoke_test

fuzz-test:
	$(BAZEL) test --//:compression_test_support=true \
		//tools/shiro:shiro_tests \
		//tools/shiro:shiro_cli_tests \
		//:hawdb_linux_ci_fuzz_smoke_test

fuzz-help:
	@printf '%s\n' \
		'Fuzz targets:' \
		'  make mvcc-fuzz               Run the local MVCC CLI for about 30 seconds.' \
		'  make shiro-fuzz              Continuously fuzz SQL/Cypher oracles (Ctrl+C to stop).' \
		'  make fuzz                    Run all native fuzz campaigns.' \
		'  make fuzz-optimizer          Run the optimizer/query campaign.' \
		'  make fuzz-optimizer-resume   Resume the matching optimizer campaign.' \
		'  make fuzz-parser             Run the frontend parser byte campaign.' \
		'  make fuzz-storage            Run the storage corruption campaign.' \
		'  make fuzz-append             Run the Strict Append state-machine campaign.' \
		'  make fuzz-append-resume      Resume the matching Strict Append campaign.' \
		'  make fuzz-smoke              Run the Bazel fuzz smoke test.' \
		'  make fuzz-test               Run the Bazel fuzz regression suite.' \
		'' \
		'Common variables:' \
		'  MVCC_SECONDS=30 MVCC_SEED=7 MVCC_LOG_DIRECTORY=target/mvcc-fuzz' \
		'  SHIRO_SECONDS=30 SHIRO_SEED=7 SHIRO_LOG_DIRECTORY=target/fuzz-logs' \
		'  FUZZ_SEED=7 FUZZ_CASES=128 FUZZ_CASE_INDEX=19 FUZZ_STEPS=256' \
		'  FUZZ_LOG_DIRECTORY=target/fuzz-logs FUZZ_PRINT_REPORT=1' \
		'  FUZZ_SHARD_INDEX=0 FUZZ_SHARD_COUNT=4 FUZZ_PROGRESS_INTERVAL=16' \
		'' \
		'Advanced runner arguments:' \
		'  FUZZ_OPTIMIZER_ARGS="..." FUZZ_STORAGE_ARGS="..." FUZZ_APPEND_ARGS="..." FUZZ_PARSER_ARGS="..."'

# PostgreSQL SQL compatibility; the DSN stays in the caller's environment.
PG_REGRESS_ARGS ?=

.PHONY: pg-regress pg-regress-list pg-regress-hawdb
pg-regress-hawdb:
	cargo test --locked --manifest-path tests/pg-regress/Cargo.toml --no-default-features --test upstream_sql

pg-regress:
	cargo run --locked --manifest-path tests/pg-regress/Cargo.toml -- $(PG_REGRESS_ARGS)

pg-regress-list:
	cargo run --locked --manifest-path tests/pg-regress/Cargo.toml -- --list $(PG_REGRESS_ARGS)

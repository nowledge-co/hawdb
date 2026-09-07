"""tla_check — model-check a TLA+ spec with TLC as a `bazel test` target.

Derived from tomato-bazel/rules_tla v0.2.0 (see ../../README.md and LICENSE).
Implemented as a build action that runs TLC and writes a marker plus cacheable
evidence, gated by a `build_test`. Running TLC in an action (rather than a test binary) means we use
exec-config file paths directly — no runfiles/rlocation wiring — and the JDK comes
from Bazel's built-in java runtime toolchain.
"""

load("@bazel_skylib//lib:shell.bzl", "shell")
load("@bazel_skylib//rules:build_test.bzl", "build_test")
load(":providers.bzl", "TlaInfo")

# What TLC found, as this rule classifies it. `expect` is compared against these.
_OUTCOMES = [
    "ok",
    "invariant_violation",
    "deadlock",
    "temporal_violation",
]

def _rooted(path):
    """An exec-root-relative path made absolute, leaving absolute paths alone.

    `java_runtime.java_executable_exec_path` is absolute for a local_jdk and
    relative for a remote one, and the action below cd's away from the exec
    root, so every path it uses has to be resolved first.
    """
    if path.startswith("/"):
        return shell.quote(path)
    return '"$ROOT"/' + shell.quote(path)

def _tla_check_run_impl(ctx):
    java_runtime = ctx.toolchains["@bazel_tools//tools/jdk:runtime_toolchain_type"].java_runtime
    java = java_runtime.java_executable_exec_path

    jar = ctx.file._tla2tools
    module = ctx.file.module
    config = ctx.file.config

    dep_sources = depset(
        transitive = [dep[TlaInfo].transitive_sources for dep in ctx.attr.deps],
    ).to_list()

    # TLC's module search path (TLA-Library): every directory holding a
    # transitive dependency module, so `EXTENDS` resolves across packages. The
    # root module's own directory is the working directory (see below) and is
    # searched anyway. Standard modules (Naturals, Sequences, …) come from the
    # jar.
    lib_dirs = {}
    for f in dep_sources:
        lib_dirs[f.dirname] = True

    module_dir = module.dirname if module.dirname else "."
    if not module.basename.endswith(".tla"):
        fail("%s: module must be a .tla file, got %s" % (ctx.label, module.basename))
    module_name = module.basename[:-len(".tla")]

    marker = ctx.actions.declare_file(ctx.label.name + ".tlc.ok")
    evidence = ctx.actions.declare_directory(ctx.label.name + ".tlc-evidence")

    inputs = depset(
        [jar, module, config] + dep_sources,
        transitive = [java_runtime.files],
    )

    # ⛔ THE MODULE ARGUMENT MUST BE A BARE NAME, AND THE ACTION MUST cd TO ITS
    # DIRECTORY. `tlc2.TLC.main` does
    #
    #     String dir = FileUtil.parseDirname(tlc.getMainFile());
    #     if (!dir.isEmpty()) tlc.setResolver(new SimpleFilenameToStream(dir));
    #     else                tlc.setResolver(new SimpleFilenameToStream());
    #
    # and `SimpleFilenameToStream(String)` wraps its argument in a NON-NULL
    # String[], which makes `getLibraryPaths` take the branch that never calls
    # `System.getProperty("TLA-Library")`. So passing `pkg/Module.tla` — which is
    # what every path under Bazel looks like — SILENTLY DISCARDS -DTLA-Library and
    # every `deps` module becomes invisible, with the diagnostic
    #
    #     Cannot find source file for module Foo imported in module Bar
    #
    # ⚠ WHICH READS AS A MISSING DEP IN THE BUILD FILE, NOT AS THE RULE IGNORING
    # THE DEP IT WAS GIVEN. Verified against tla2tools 1.7.4 bytecode; it is why
    # `deps` never worked in 0.1.0/0.1.1 and why nothing noticed — the only
    # example in this repo had no deps. `examples/` now has one that does.
    jvm_args = [
        "-XX:+UseParallelGC",
        # ⛔ `-Djava.io.tmpdir` IS LOAD-BEARING AND ITS ABSENCE IS A CONCURRENCY BUG, NOT
        # A TIDINESS ISSUE. TLC does not read the standard modules (Naturals, Sequences,
        # FiniteSets, …) from the jar in place — it EXTRACTS them to `java.io.tmpdir`
        # under FIXED names and registers them for deleteOnExit. Two `tla_check` targets
        # building concurrently therefore extract to the same paths and delete each
        # other's copies, and the loser fails with
        #
        #     Error: source file 'Sequences.tla' has apparently been deleted.
        #     Error: Parsing or semantic analysis failed.
        #
        # ⚠ WHICH LOOKS LIKE A BROKEN SPEC, NOT A RACE. Measured in tomato-bazel/infra
        # with 11 tla_check targets: roughly one run in three had 2-3 spurious failures,
        # each naming a different module, and every one of them passed when re-run alone.
        '-Djava.io.tmpdir="$MD"',
    ]
    if lib_dirs:
        lib_path = ":".join(['"$ROOT"/' + shell.quote(d) for d in lib_dirs.keys()])
        jvm_args.append("-DTLA-Library=" + lib_path)

    launch = " ".join(
        ['"$JAVA"'] + jvm_args + [
            '-cp "$JAR"',
            "tlc2.TLC",
            '-metadir "$MD"',
        ] + [shell.quote(a) for a in ctx.attr.tlc_args] + [
            '-config "$CONFIG"',
            shell.quote(module_name),
        ],
    )

    if ctx.attr.timeout_seconds > 0:
        # No `timeout(1)`: it is GNU coreutils and macOS does not ship it. A
        # watchdog subshell is portable, and a build action has NO timeout of
        # its own — `size`/`timeout` on the wrapping build_test govern the
        # (trivial) test, not this action — so an unbounded spec would
        # otherwise hang the build forever rather than failing it.
        run = """{launch} > "$LOG" 2>&1 &
TLC_PID=$!
( sleep {t}; kill -9 "$TLC_PID" 2>/dev/null ) &
WATCHDOG=$!
wait "$TLC_PID"
rc=$?
kill "$WATCHDOG" 2>/dev/null
wait "$WATCHDOG" 2>/dev/null
""".format(launch = launch, t = ctx.attr.timeout_seconds)
    else:
        run = """{launch} > "$LOG" 2>&1
rc=$?
""".format(launch = launch)

    command = """
set -euo pipefail
ROOT="$PWD"
JAVA={java}
JAR={jar}
MODULE={module}
CONFIG={config}
MARKER={marker}
EVIDENCE={evidence}
MD="$(mktemp -d)"
trap 'rm -rf "$MD"' EXIT
mkdir -p "$EVIDENCE"
LOG="$EVIDENCE/tlc.log"
cp "$MODULE" "$EVIDENCE/module.tla"
cp "$CONFIG" "$EVIDENCE/model.cfg"
"$JAVA" -version > "$EVIDENCE/java-version.txt" 2>&1
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$JAR" | cut -d ' ' -f1 > "$EVIDENCE/tla2tools.sha256"
else
    shasum -a 256 "$JAR" | cut -d ' ' -f1 > "$EVIDENCE/tla2tools.sha256"
fi
printf '%s\\n' {tlc_args} > "$EVIDENCE/tlc-args.txt"

cd {module_dir} || exit 1
set +e
{run}
set -e
cat "$LOG"

# Classify by the FIRST error TLC printed. TLC stops at the first violation, so
# there is normally exactly one; taking the first keeps the verdict
# deterministic if -continue was passed in tlc_args.
FIRST_ERR="$(grep -m1 '^Error:' "$LOG" || true)"
case "$FIRST_ERR" in
    'Error: Deadlock reached'*)                  KIND=deadlock ;;
    'Error: Temporal properties were violated'*) KIND=temporal_violation ;;
    'Error: Invariant '*)                        KIND=invariant_violation ;;
    'Error: Action property '*)                  KIND=invariant_violation ;;
    '')                                          KIND=ok ;;
    *)                                           KIND=other_error ;;
esac
if [ "$rc" -eq 137 ]; then
    KIND=timeout
elif [ "$rc" -ne 0 ] && [ "$KIND" = ok ]; then
    KIND=other_error
elif [ "$KIND" = ok ] && ! grep -q 'Model checking completed' "$LOG"; then
    # ⛔ FAIL CLOSED. A run that reported no error AND never reached "Model
    # checking completed" checked nothing — a mistyped -config, an empty cfg, a
    # usage dump. Passing that is the failure mode this rule exists to prevent.
    # (-simulate does not print this marker; a simulation run needs its own
    # gate rather than expect = "ok".)
    KIND=other_error
fi

printf '%s\\n' "$KIND" "$rc" > "$EVIDENCE/result.txt"
if [ "$KIND" = {expect} ]; then
    if [ {expect} != ok ]; then
        echo "rules_tla: TLC reported the expected {expect}; the counterexample is above." >&2
    fi
    touch "$MARKER"
    exit 0
fi

echo "rules_tla: expected {expect}, TLC produced $KIND (exit $rc)" >&2
if [ -n "$FIRST_ERR" ]; then
    echo "rules_tla: first TLC error: $FIRST_ERR" >&2
fi
exit 1
""".format(
        java = _rooted(java),
        jar = _rooted(jar.path),
        module = _rooted(module.path),
        config = _rooted(config.path),
        marker = _rooted(marker.path),
        evidence = _rooted(evidence.path),
        tlc_args = " ".join([shell.quote(a) for a in ctx.attr.tlc_args]),
        module_dir = _rooted(module_dir),
        run = run,
        expect = ctx.attr.expect,
    )

    ctx.actions.run_shell(
        inputs = inputs,
        outputs = [marker, evidence],
        command = command,
        mnemonic = "TlcCheck",
        progress_message = "TLC checking %s" % module.short_path,
    )
    return [
        DefaultInfo(files = depset([marker])),
        OutputGroupInfo(tla_evidence = depset([evidence])),
    ]

_tla_check_run = rule(
    implementation = _tla_check_run_impl,
    attrs = {
        "module": attr.label(
            allow_single_file = [".tla"],
            mandatory = True,
            doc = "The root TLA+ module to check.",
        ),
        "config": attr.label(
            allow_single_file = [".cfg"],
            mandatory = True,
            doc = "The TLC configuration file.",
        ),
        "deps": attr.label_list(
            providers = [TlaInfo],
            doc = "tla_library targets the module EXTENDS.",
        ),
        "expect": attr.string(
            default = "ok",
            values = _OUTCOMES,
            doc = "The TLC outcome that makes this target pass.",
        ),
        "tlc_args": attr.string_list(
            doc = "Extra flags for tlc2.TLC (e.g. -workers, -difftrace, -depth).",
        ),
        "timeout_seconds": attr.int(
            default = 0,
            doc = "Kill TLC after this many seconds; 0 means no limit.",
        ),
        "_tla2tools": attr.label(
            default = "@tla2tools//file",
            allow_single_file = True,
        ),
    },
    toolchains = ["@bazel_tools//tools/jdk:runtime_toolchain_type"],
)

def tla_check(
        name,
        module,
        config,
        deps = [],
        expect = "ok",
        tlc_args = [],
        timeout_seconds = 0,
        **kwargs):
    """Model-check a TLA+ spec with TLC, as a `bazel test` target.

    Args:
      name: test target name.
      module: the root `.tla` module.
      config: the `.cfg` file (SPECIFICATION / INVARIANT / PROPERTY / CONSTANTS).
      deps: `tla_library` targets the module EXTENDS.
      expect: the TLC outcome that makes this target pass — one of `ok`
        (default), `invariant_violation`, `deadlock`, `temporal_violation`.
        Anything else fails, INCLUDING a violation of a different kind than the
        one named. Use this to assert that a design really does break the way
        it is claimed to: an expected counterexample that quietly stops being
        produced is a regression, and under `expect = "ok"` there is no way to
        state it, let alone notice.
      tlc_args: extra flags passed to `tlc2.TLC`, e.g. `["-workers", "auto"]`.
      timeout_seconds: kill TLC after this many seconds. 0 (the default) means
        no limit. A check runs in a build ACTION, and Bazel does not time
        actions out — `size`/`timeout` on the wrapping `build_test` govern the
        trivial test, not the model check — so a spec with an infinite state
        space hangs the build instead of failing it.
      **kwargs: forwarded to the wrapping `build_test` (e.g. tags, size).
    """
    if kwargs.pop("shard_count", 1) not in [0, 1]:
        fail("tla_check cannot shard a state graph; use tla_test_suite(shard_count = N) to shard complete models")
    _tla_check_run(
        name = name + ".run",
        module = module,
        config = config,
        deps = deps,
        expect = expect,
        tlc_args = tlc_args,
        timeout_seconds = timeout_seconds,
        testonly = True,
        tags = ["manual"],
    )
    build_test(
        name = name,
        targets = [":" + name + ".run"],
        **kwargs
    )

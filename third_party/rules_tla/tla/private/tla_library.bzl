"""tla_library — a group of TLA+ modules and their transitive dependencies."""

load(":providers.bzl", "TlaInfo")

def _tla_library_impl(ctx):
    transitive = [dep[TlaInfo].transitive_sources for dep in ctx.attr.deps]
    sources = depset(ctx.files.srcs, transitive = transitive)
    return [
        DefaultInfo(files = sources),
        TlaInfo(transitive_sources = sources),
    ]

tla_library = rule(
    implementation = _tla_library_impl,
    doc = "A group of TLA+ (.tla) modules plus transitive tla_library deps.",
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".tla"],
            doc = "TLA+ modules in this library.",
        ),
        "deps": attr.label_list(
            providers = [TlaInfo],
            doc = "Other tla_library targets this one EXTENDS.",
        ),
    },
)

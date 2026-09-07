"""Module extension that fetches the pinned, hermetic TLA+ tools jar."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")

# tla2tools.jar (SANY + TLC + PlusCal) from the tlaplus/tlaplus releases.
_VERSION = "1.7.4"
_SHA256 = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

def _tla2tools_impl(_module_ctx):
    http_file(
        name = "tla2tools",
        urls = [
            "https://github.com/tlaplus/tlaplus/releases/download/v{v}/tla2tools.jar".format(v = _VERSION),
        ],
        sha256 = _SHA256,
        downloaded_file_path = "tla2tools.jar",
    )

tla2tools = module_extension(
    implementation = _tla2tools_impl,
    doc = "Fetches the pinned tla2tools.jar as @tla2tools//file.",
)

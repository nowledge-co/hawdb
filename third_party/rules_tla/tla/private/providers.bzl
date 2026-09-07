"""Providers for rules_tla."""

TlaInfo = provider(
    doc = "Transitive set of TLA+ (.tla) module sources.",
    fields = {
        "transitive_sources": "depset of .tla File objects (this library + its deps).",
    },
)

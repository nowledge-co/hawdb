"""Public API for rules_tla.

    load("@rules_tla//tla:defs.bzl", "tla_library", "tla_check")
"""

load("//tla/private:tla_check.bzl", _tla_check = "tla_check")
load("//tla/private:tla_library.bzl", _tla_library = "tla_library")
load("//tla/private:tla_test_suite.bzl", _tla_test_suite = "tla_test_suite")

tla_library = _tla_library
tla_check = _tla_check
tla_test_suite = _tla_test_suite

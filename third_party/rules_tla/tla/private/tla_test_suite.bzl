"""Partition complete model checks without partitioning any TLC state graph."""

def tla_test_suite(name, tests, shard_count = 1, **kwargs):
    """Create a full suite, disjoint model shards, and their evidence targets.

    Args:
      name: full-suite target name.
      tests: ordered, unique, explicit labels of tla_check targets.
      shard_count: number of nonempty, zero-indexed model shards; defaults to 1.
      **kwargs: forwarded to each native test_suite, for example tags.

    Shard i owns tests[j] iff j % shard_count == i. Each shard target is named
    <name>_shard_<i>; each evidence filegroup appends _evidence. The unsharded
    <name> and <name>_evidence targets always cover the complete model set.
    """
    if type(shard_count) != "int" or shard_count < 1 or shard_count > len(tests):
        fail("shard_count must be an integer between 1 and the model count")
    for test in tests:
        if type(test) != "string" or ":" not in test:
            fail("tla_test_suite requires explicit tla_check labels (for example :counter_check)")
    if len({test: True for test in tests}) != len(tests):
        fail("tla_test_suite requires unique model-check labels")

    groups = [(name, tests)] + [
        (name + "_shard_" + str(shard), [test for index, test in enumerate(tests) if index % shard_count == shard])
        for shard in range(shard_count)
    ]
    for group_name, group_tests in groups:
        native.test_suite(name = group_name, tests = group_tests, **kwargs)
        native.filegroup(
            name = group_name + "_evidence",
            testonly = True,
            srcs = [test + ".run" for test in group_tests],
            output_group = "tla_evidence",
        )

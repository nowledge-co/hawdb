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

"""Manual fixtures for the benchmark-only Linux CPU discovery contract."""

import importlib.util
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    'topology', Path(__file__).resolve().parents[1] / 'concurrent_writer_topology.py')
TOPOLOGY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TOPOLOGY)


class CpuTopologyTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.proc, self.sys = self.root / 'proc', self.root / 'sys'
        (self.proc / 'self').mkdir(parents=True)
        for cpu in range(16):
            directory = self.sys / f'devices/system/cpu/cpu{cpu}/topology'
            directory.mkdir(parents=True)
            (directory / 'physical_package_id').write_text('0')
            (directory / 'core_id').write_text(str(cpu // 2))

    def v2(self):
        mount = self.root / 'cgroup'
        leaf = mount / 'tenant/job'
        leaf.mkdir(parents=True)
        (self.proc / 'self/cgroup').write_text('0::/tenant/job\n')
        (self.proc / 'self/mountinfo').write_text(f'30 1 0:30 / {mount} rw - cgroup2 cgroup rw\n')
        return mount, leaf

    def detect(self, affinity=range(16)):
        return TOPOLOGY.detect(self.proc, self.sys, affinity)

    def test_smt_is_not_counted_as_additional_physical_cores(self):
        self.v2()
        result = self.detect()
        self.assertEqual(result['effective_cpu_cores'], '8')
        self.assertEqual(result['writer_counts'], [1, 2, 4])
        self.assertEqual(TOPOLOGY.matrix(result)[-3:], [(4, 0), (4, 4), (4, 8)])

    def test_v2_parent_fractional_quota_caps_unlimited_child(self):
        mount, leaf = self.v2()
        (leaf / 'cpu.max').write_text('max 100000')
        (leaf.parent / 'cpu.max').write_text('150000 100000')
        (mount / 'cpu.max').write_text('600000 100000')
        result = self.detect()
        self.assertEqual(result['effective_cpu_cores'], '3/2')
        self.assertEqual(result['writer_counts'], [1])
        self.assertFalse(result['multi_writer_coverage'])

    def test_cpuset_and_affinity_intersect_before_core_counting(self):
        _, leaf = self.v2()
        (leaf / 'cpuset.cpus.effective').write_text('')
        (leaf.parent / 'cpuset.cpus').write_text('0-5,8-9')
        result = self.detect([2, 3, 4, 5, 6, 7])
        self.assertEqual(result['allowed_logical_cpus'], [2, 3, 4, 5])
        self.assertEqual(result['effective_cpu_cores'], '2')

    def test_v1_mount_root_mapping_and_ancestor_quota(self):
        cpu, cpuset = self.root / 'cpu', self.root / 'cpuset'
        for mount in (cpu, cpuset):
            (mount / 'job').mkdir(parents=True)
        (self.proc / 'self/cgroup').write_text('2:cpu,cpuacct:/tenant/job\n3:cpuset:/tenant/job\n')
        (self.proc / 'self/mountinfo').write_text(
            f'30 1 0:30 /tenant {cpu} rw - cgroup cgroup rw,cpu,cpuacct\n'
            f'31 1 0:31 /tenant {cpuset} rw - cgroup cgroup rw,cpuset\n')
        (cpu / 'job/cpu.cfs_quota_us').write_text('-1')
        (cpu / 'job/cpu.cfs_period_us').write_text('100000')
        (cpu / 'cpu.cfs_quota_us').write_text('700000')
        (cpu / 'cpu.cfs_period_us').write_text('100000')
        (cpuset / 'cpuset.cpus').write_text('0-15')
        result = self.detect()
        self.assertEqual(result['effective_cpu_cores'], '7')
        self.assertEqual(result['writer_counts'], [1, 2, 3])

    def test_subcore_quota_preserves_explicit_minimum_one(self):
        _, leaf = self.v2()
        (leaf / 'cpu.max').write_text('25000 100000')
        result = self.detect()
        self.assertEqual(result['effective_cpu_cores'], '1/4')
        self.assertEqual(result['maximum_writers'], 1)

    def test_malformed_or_missing_quota_period_is_not_unlimited(self):
        _, leaf = self.v2()
        for value in ('0 100000', '100000 0', 'invalid 100000', 'max'):
            (leaf / 'cpu.max').write_text(value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.detect()

    def test_missing_mount_and_unavailable_topology_fail_closed(self):
        self.v2()
        (self.proc / 'self/mountinfo').write_text('')
        with self.assertRaises(ValueError):
            self.detect()
        (self.proc / 'self/cgroup').write_text('1:name=systemd:/\n')
        (self.sys / 'devices/system/cpu/cpu0/topology/core_id').unlink()
        with self.assertRaises(FileNotFoundError):
            self.detect()

    def test_manual_counts_override_automatic_bound_and_reader_scaling(self):
        self.v2()
        topology = self.detect()
        self.assertEqual(TOPOLOGY.matrix(topology, [2, 8], [0, 3]),
                         [(2, 0), (2, 3), (8, 0), (8, 3)])
        self.assertEqual(TOPOLOGY.matrix(topology, [3]), [(3, 0), (3, 3), (3, 6)])
        self.assertEqual(TOPOLOGY.counts('4,1,4,2', 1), [1, 2, 4])
        for value in ('', '0', '-1', '1,,2'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                TOPOLOGY.counts(value, 1)


if __name__ == '__main__':
    unittest.main()

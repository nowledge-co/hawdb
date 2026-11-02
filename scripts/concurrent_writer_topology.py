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

"""Freeze Linux core, affinity, cpuset and hierarchical quota evidence for benchmarks."""

from fractions import Fraction
import os
from pathlib import Path, PurePosixPath
import re


def cpu_set(value):
    result = set()
    for item in value.strip().split(','):
        if not item:
            continue
        bounds = item.split('-')
        if len(bounds) not in (1, 2):
            raise ValueError(f"invalid CPU range: {item}")
        start, end = int(bounds[0]), int(bounds[-1])
        if start < 0 or end < start:
            raise ValueError(f"invalid CPU range: {item}")
        result.update(range(start, end + 1))
    return result


def unescape(value):
    return re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), value)


def absolute(value):
    path = PurePosixPath(value)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError(f"invalid cgroup path: {value}")
    return path


def controller_directories(membership, mountinfo):
    mounts = []
    for line in mountinfo.splitlines():
        before, after = line.split(' - ', 1)
        fields, fs = before.split(), after.split()
        if fs[0] in ('cgroup', 'cgroup2'):
            mounts.append((fs[0], absolute(unescape(fields[3])),
                           Path(unescape(fields[4])), set(fs[2].split(','))))
    result = []
    for line in membership.splitlines():
        hierarchy, names, raw_path = line.split(':', 2)
        unified = hierarchy == '0' and not names
        controllers = set(names.split(',')) & {'cpu', 'cpuset'}
        if not unified and not controllers:
            continue
        process_path = absolute(raw_path)
        for controller in (['unified'] if unified else sorted(controllers)):
            candidates = []
            for kind, root, mount, available in mounts:
                if (unified and kind != 'cgroup2') or (not unified and
                        (kind != 'cgroup' or controller not in available)):
                    continue
                if process_path.is_relative_to(root):
                    candidates.append((len(root.parts), mount / process_path.relative_to(root), mount))
            if not candidates:
                raise ValueError(f"cannot resolve {controller} cgroup membership")
            _, leaf, mount = max(candidates, key=lambda item: item[0])
            if not leaf.is_dir():
                raise ValueError(f"cgroup directory is inaccessible: {leaf}")
            result.append((controller, leaf, mount))
    if not membership.strip():
        raise ValueError('missing cgroup membership')
    return result


def optional_text(path):
    try:
        return path.read_text().strip()
    except FileNotFoundError:
        return None


def detect(proc=Path('/proc'), sys=Path('/sys'), affinity=None):
    # sched_getaffinity reflects the kernel's current cpuset restriction too;
    # record and intersect controller masks rather than assuming that relation.
    allowed = set(os.sched_getaffinity(0) if affinity is None else affinity)
    affinity_cpus = sorted(allowed)
    if not allowed:
        raise ValueError('empty CPU affinity')
    quotas, masks = [], []
    directories = controller_directories((proc / 'self/cgroup').read_text(),
                                         (proc / 'self/mountinfo').read_text())
    for controller, leaf, mount in directories:
        directory = leaf
        while True:
            if controller in ('unified', 'cpu'):
                filename = 'cpu.max' if controller == 'unified' else 'cpu.cfs_quota_us'
                raw = optional_text(directory / filename)
                if raw is not None:
                    if controller == 'unified':
                        quota, period = raw.split()
                    else:
                        quota, period = raw, (directory / 'cpu.cfs_period_us').read_text().strip()
                    if int(period) <= 0:
                        raise ValueError('CPU quota period must be positive')
                    unlimited = quota == ('max' if controller == 'unified' else '-1')
                    limit = None if unlimited else Fraction(int(quota), int(period))
                    if limit is not None and limit <= 0:
                        raise ValueError('CPU quota must be positive or unlimited')
                    quotas.append({'path': str(directory / filename), 'quota': quota,
                                   'period': period, 'cores': str(limit) if limit is not None else None})
            if controller in ('unified', 'cpuset'):
                names = ('cpuset.cpus.effective', 'cpuset.cpus') if controller == 'unified' else (
                    'cpuset.effective_cpus', 'cpuset.cpus')
                for name in names:
                    raw = optional_text(directory / name)
                    if raw:
                        mask = cpu_set(raw)
                        allowed &= mask
                        masks.append({'path': str(directory / name), 'cpus': sorted(mask)})
                        break
            if directory == mount:
                break
            directory = directory.parent
    if not allowed:
        raise ValueError('cpuset and affinity have no CPUs in common')
    cores = {}
    for cpu in sorted(allowed):
        topology = sys / f'devices/system/cpu/cpu{cpu}/topology'
        package = int((topology / 'physical_package_id').read_text())
        core = int((topology / 'core_id').read_text())
        if package < 0 or core < 0:
            raise ValueError('physical core topology is unavailable')
        cores.setdefault((package, core), []).append(cpu)
    effective = min([Fraction(len(cores))] + [Fraction(q['cores']) for q in quotas if q['cores']])
    maximum = max(1, effective // 2)
    writers, point = set([maximum]), 1
    while point <= maximum:
        writers.add(point)
        point *= 2
    return {
        'schema': 'writer-cpu-topology-v1', 'affinity_cpus': affinity_cpus,
        'allowed_logical_cpus': sorted(allowed),
        'physical_cores': [{'package': p, 'core': c, 'logical_cpus': ids}
                           for (p, c), ids in sorted(cores.items())],
        'cpuset_masks': masks, 'quota_limits': quotas,
        'effective_cpu_cores': str(effective), 'maximum_writers': maximum,
        'writer_counts': sorted(writers), 'multi_writer_coverage': maximum >= 2,
        'rule': 'max(1, floor(min(allowed physical cores, visible ancestor quota/period) / 2))',
        'visibility': 'visible cgroup mount ancestors only; physical topology as exposed by the host or VM',
    }


def counts(value, minimum):
    parsed = sorted(set(int(item.strip()) for item in value.split(',')))
    if not parsed or parsed[0] < minimum:
        raise ValueError(f'counts must be integers >= {minimum}')
    return parsed


def matrix(topology, writer_counts=None, reader_counts=None):
    return [(writers, readers) for writers in
            (topology['writer_counts'] if writer_counts is None else writer_counts)
            for readers in ((0, writers, writers * 2) if reader_counts is None else reader_counts)]

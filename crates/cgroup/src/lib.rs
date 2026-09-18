// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Typed, fail-closed Linux cgroup v2 resource discovery.

#![forbid(unsafe_code)]

use std::num::NonZeroUsize;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinuxCgroupVersion {
    None,
    V2,
    V1Unsupported,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinuxCgroupValue<T> {
    Absent,
    Unlimited,
    Value(T),
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinuxCgroupSnapshot {
    pub version: LinuxCgroupVersion,
    pub cpu_quota_parallelism: LinuxCgroupValue<NonZeroUsize>,
    pub cpuset_parallelism: LinuxCgroupValue<NonZeroUsize>,
    pub memory_limit_bytes: LinuxCgroupValue<u64>,
    pub memory_high_bytes: LinuxCgroupValue<u64>,
    pub memory_current_bytes: LinuxCgroupValue<u64>,
}

impl LinuxCgroupSnapshot {
    pub const fn host() -> Self {
        Self {
            version: LinuxCgroupVersion::None,
            cpu_quota_parallelism: LinuxCgroupValue::Absent,
            cpuset_parallelism: LinuxCgroupValue::Absent,
            memory_limit_bytes: LinuxCgroupValue::Absent,
            memory_high_bytes: LinuxCgroupValue::Absent,
            memory_current_bytes: LinuxCgroupValue::Absent,
        }
    }

    pub const fn fail_closed(version: LinuxCgroupVersion) -> Self {
        Self {
            version,
            cpu_quota_parallelism: LinuxCgroupValue::Invalid,
            cpuset_parallelism: LinuxCgroupValue::Invalid,
            memory_limit_bytes: LinuxCgroupValue::Invalid,
            memory_high_bytes: LinuxCgroupValue::Invalid,
            memory_current_bytes: LinuxCgroupValue::Invalid,
        }
    }

    #[cfg(target_os = "linux")]
    pub fn detect() -> Self {
        let self_cgroup = std::fs::read_to_string("/proc/self/cgroup");
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo");
        match (self_cgroup, mountinfo) {
            (Ok(self_cgroup), Ok(mountinfo)) => Self::detect_from_procfs(&self_cgroup, &mountinfo),
            _ => Self::fail_closed(LinuxCgroupVersion::Unknown),
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub const fn detect() -> Self {
        Self::host()
    }

    pub fn detect_from_procfs(self_cgroup: &str, mountinfo: &str) -> Self {
        let membership = match parse_membership(self_cgroup) {
            Ok(membership) => membership,
            Err(()) => return Self::fail_closed(LinuxCgroupVersion::Unknown),
        };
        if membership.has_v1_resource_controller {
            return Self::fail_closed(LinuxCgroupVersion::V1Unsupported);
        }
        let Some(process_path) = membership.unified_path else {
            return Self::host();
        };
        let Some(directory) = resolve_v2_directory(&process_path, mountinfo) else {
            return Self::fail_closed(LinuxCgroupVersion::Unknown);
        };
        if !directory.path.is_dir() {
            return Self::fail_closed(LinuxCgroupVersion::Unknown);
        }

        let memory_limit_bytes = read_value(directory.path.join("memory.max"), parse_memory_limit);
        let memory_high_bytes = read_value(directory.path.join("memory.high"), parse_memory_limit);
        let memory_current_bytes =
            read_value(directory.path.join("memory.current"), parse_memory_current);
        let (memory_limit_bytes, memory_high_bytes, memory_current_bytes) =
            normalize_memory_controller(
                memory_limit_bytes,
                memory_high_bytes,
                memory_current_bytes,
            );

        Self {
            version: LinuxCgroupVersion::V2,
            cpu_quota_parallelism: read_value(directory.path.join("cpu.max"), parse_cpu_max),
            cpuset_parallelism: read_cpuset(&directory),
            memory_limit_bytes,
            memory_high_bytes,
            memory_current_bytes,
        }
    }
}

#[derive(Debug)]
struct Membership {
    unified_path: Option<CgroupPath>,
    has_v1_resource_controller: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CgroupPath {
    components: Vec<String>,
}

impl CgroupPath {
    fn parse_absolute(value: &str) -> Option<Self> {
        if !value.starts_with('/') {
            return None;
        }
        let mut components = Vec::new();
        for component in value.split('/') {
            match component {
                "" | "." => {}
                ".." => return None,
                component => components.push(component.to_string()),
            }
        }
        Some(Self { components })
    }

    fn relative_to<'a>(&'a self, root: &Self) -> Option<&'a [String]> {
        self.components.strip_prefix(root.components.as_slice())
    }

    fn depth(&self) -> usize {
        self.components.len()
    }
}

#[derive(Debug)]
struct V2Directory {
    path: PathBuf,
    mount_point: PathBuf,
}

fn parse_membership(input: &str) -> Result<Membership, ()> {
    let mut unified_path = None;
    let mut has_v1_resource_controller = false;
    let mut saw_membership = false;
    for line in input.lines().filter(|line| !line.trim().is_empty()) {
        saw_membership = true;
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next().ok_or(())?;
        let controllers = fields.next().ok_or(())?;
        let raw_path = fields.next().ok_or(())?;
        if hierarchy == "0" && controllers.is_empty() {
            if unified_path.is_some() {
                return Err(());
            }
            unified_path = Some(CgroupPath::parse_absolute(raw_path).ok_or(())?);
            continue;
        }
        has_v1_resource_controller |= controllers
            .split(',')
            .any(|controller| matches!(controller, "cpu" | "cpuacct" | "cpuset" | "memory"));
    }
    if !saw_membership {
        return Err(());
    }
    Ok(Membership {
        unified_path,
        has_v1_resource_controller,
    })
}

fn resolve_v2_directory(process_path: &CgroupPath, mountinfo: &str) -> Option<V2Directory> {
    mountinfo
        .lines()
        .filter_map(parse_v2_mount)
        .filter_map(|(root, mount_point)| {
            let relative = process_path.relative_to(&root)?;
            let path = relative
                .iter()
                .fold(mount_point.clone(), |path, component| path.join(component));
            Some((root.depth(), V2Directory { path, mount_point }))
        })
        .max_by_key(|(specificity, _)| *specificity)
        .map(|(_, directory)| directory)
}

fn parse_v2_mount(line: &str) -> Option<(CgroupPath, PathBuf)> {
    let (mount_fields, file_system_fields) = line.split_once(" - ")?;
    let mount_fields = mount_fields.split_whitespace().collect::<Vec<_>>();
    let file_system_fields = file_system_fields.split_whitespace().collect::<Vec<_>>();
    if mount_fields.len() < 5 || file_system_fields.first().copied() != Some("cgroup2") {
        return None;
    }
    Some((
        CgroupPath::parse_absolute(&unescape_mountinfo_field(mount_fields[3]))?,
        validated_absolute_host_path(&unescape_mountinfo_field(mount_fields[4]))?,
    ))
}

fn read_value<T>(path: PathBuf, parse: fn(&str) -> LinuxCgroupValue<T>) -> LinuxCgroupValue<T> {
    match std::fs::read_to_string(path) {
        Ok(value) => parse(&value),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => LinuxCgroupValue::Absent,
        Err(_) => LinuxCgroupValue::Invalid,
    }
}

fn read_cpuset(directory: &V2Directory) -> LinuxCgroupValue<NonZeroUsize> {
    let effective = read_inherited_cpuset(directory, "cpuset.cpus.effective");
    if !matches!(effective, LinuxCgroupValue::Absent) {
        return effective;
    }
    read_inherited_cpuset(directory, "cpuset.cpus")
}

fn normalize_memory_controller(
    limit: LinuxCgroupValue<u64>,
    high: LinuxCgroupValue<u64>,
    current: LinuxCgroupValue<u64>,
) -> (
    LinuxCgroupValue<u64>,
    LinuxCgroupValue<u64>,
    LinuxCgroupValue<u64>,
) {
    if matches!(limit, LinuxCgroupValue::Absent)
        && matches!(high, LinuxCgroupValue::Absent)
        && matches!(current, LinuxCgroupValue::Absent)
    {
        return (limit, high, current);
    }
    (
        required_controller_value(limit),
        required_controller_value(high),
        required_controller_value(current),
    )
}

fn required_controller_value<T>(value: LinuxCgroupValue<T>) -> LinuxCgroupValue<T> {
    match value {
        LinuxCgroupValue::Absent => LinuxCgroupValue::Invalid,
        value => value,
    }
}

fn read_inherited_cpuset(
    directory: &V2Directory,
    file_name: &str,
) -> LinuxCgroupValue<NonZeroUsize> {
    let mut current = directory.path.clone();
    loop {
        match std::fs::read_to_string(current.join(file_name)) {
            Ok(value) if value.trim().is_empty() => {}
            Ok(value) => return parse_cpuset(&value),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return LinuxCgroupValue::Absent;
            }
            Err(_) => return LinuxCgroupValue::Invalid,
        }
        if current == directory.mount_point
            || !current.starts_with(&directory.mount_point)
            || !current.pop()
        {
            return LinuxCgroupValue::Absent;
        }
    }
}

fn parse_cpu_max(value: &str) -> LinuxCgroupValue<NonZeroUsize> {
    let mut fields = value.split_whitespace();
    let Some(quota) = fields.next() else {
        return LinuxCgroupValue::Invalid;
    };
    let Some(period) = fields.next().and_then(|value| value.parse::<usize>().ok()) else {
        return LinuxCgroupValue::Invalid;
    };
    if fields.next().is_some() || period == 0 {
        return LinuxCgroupValue::Invalid;
    }
    if quota == "max" {
        return LinuxCgroupValue::Unlimited;
    }
    let Some(quota) = quota.parse::<usize>().ok().filter(|quota| *quota > 0) else {
        return LinuxCgroupValue::Invalid;
    };
    LinuxCgroupValue::Value(
        NonZeroUsize::new((quota / period).max(1)).expect("bounded CPU quota is non-zero"),
    )
}

fn parse_cpuset(value: &str) -> LinuxCgroupValue<NonZeroUsize> {
    if value.trim().is_empty() {
        return LinuxCgroupValue::Absent;
    }
    let Some(mut ranges) = value
        .trim()
        .split(',')
        .map(|part| {
            let mut bounds = part.splitn(2, '-');
            let start = bounds.next()?.parse::<usize>().ok()?;
            let end = bounds
                .next()
                .map(str::parse::<usize>)
                .transpose()
                .ok()?
                .unwrap_or(start);
            (start <= end).then_some((start, end))
        })
        .collect::<Option<Vec<_>>>()
    else {
        return LinuxCgroupValue::Invalid;
    };
    ranges.sort_unstable();
    let mut count = 0usize;
    let mut current: Option<(usize, usize)> = None;
    for (start, end) in ranges {
        match current {
            Some((current_start, current_end)) if start <= current_end.saturating_add(1) => {
                current = Some((current_start, current_end.max(end)));
            }
            Some((current_start, current_end)) => {
                count = count
                    .saturating_add(current_end.saturating_sub(current_start).saturating_add(1));
                current = Some((start, end));
            }
            None => current = Some((start, end)),
        }
    }
    if let Some((start, end)) = current {
        count = count.saturating_add(end.saturating_sub(start).saturating_add(1));
    }
    NonZeroUsize::new(count)
        .map(LinuxCgroupValue::Value)
        .unwrap_or(LinuxCgroupValue::Invalid)
}

fn parse_memory_limit(value: &str) -> LinuxCgroupValue<u64> {
    let value = value.trim();
    if value == "max" {
        return LinuxCgroupValue::Unlimited;
    }
    match value.parse::<u64>() {
        Ok(value) => LinuxCgroupValue::Value(value),
        Err(_) => LinuxCgroupValue::Invalid,
    }
}

fn parse_memory_current(value: &str) -> LinuxCgroupValue<u64> {
    match value.trim().parse::<u64>() {
        Ok(value) => LinuxCgroupValue::Value(value),
        Err(_) => LinuxCgroupValue::Invalid,
    }
}

fn validated_absolute_host_path(value: &str) -> Option<PathBuf> {
    let path = Path::new(value);
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return None;
    }
    Some(path.to_path_buf())
}

fn unescape_mountinfo_field(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        root: PathBuf,
        mount_point: PathBuf,
        mountinfo: String,
    }

    impl Fixture {
        fn new(name: &str, mount_root: &str) -> Self {
            let id = FIXTURE_ID.fetch_add(1, Ordering::SeqCst);
            let root = std::env::temp_dir().join(format!(
                "hawdb-cgroup-v2-{name}-{}-{id}",
                std::process::id()
            ));
            let mount_point = root.join("cgroup2");
            std::fs::create_dir_all(&mount_point).unwrap();
            let mountinfo = format!(
                "30 1 0:30 {mount_root} {} rw - cgroup2 cgroup rw\n",
                mount_point.display()
            );
            Self {
                root,
                mount_point,
                mountinfo,
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn detects_v2_limits_from_mount_root() {
        let fixture = Fixture::new("valid", "/tenant");
        let directory = fixture.mount_point.join("job");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("cpu.max"), "300000 100000").unwrap();
        std::fs::write(directory.join("cpuset.cpus.effective"), "2-5").unwrap();
        std::fs::write(directory.join("memory.max"), "1073741824").unwrap();
        std::fs::write(directory.join("memory.high"), "805306368").unwrap();
        std::fs::write(directory.join("memory.current"), "268435456").unwrap();

        let snapshot =
            LinuxCgroupSnapshot::detect_from_procfs("0::/tenant/job\n", &fixture.mountinfo);

        assert_eq!(snapshot.version, LinuxCgroupVersion::V2);
        assert_eq!(
            snapshot.cpu_quota_parallelism,
            LinuxCgroupValue::Value(NonZeroUsize::new(3).unwrap())
        );
        assert_eq!(
            snapshot.cpuset_parallelism,
            LinuxCgroupValue::Value(NonZeroUsize::new(4).unwrap())
        );
        assert_eq!(
            snapshot.memory_limit_bytes,
            LinuxCgroupValue::Value(1_073_741_824)
        );
        assert_eq!(
            snapshot.memory_high_bytes,
            LinuxCgroupValue::Value(805_306_368)
        );
        assert_eq!(
            snapshot.memory_current_bytes,
            LinuxCgroupValue::Value(268_435_456)
        );
    }

    #[test]
    fn inherits_v2_cpuset_and_preserves_unlimited_values() {
        let fixture = Fixture::new("inherited", "/");
        let parent = fixture.mount_point.join("tenant");
        let directory = parent.join("job");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("cpu.max"), "max 100000").unwrap();
        std::fs::write(directory.join("cpuset.cpus.effective"), "").unwrap();
        std::fs::write(parent.join("cpuset.cpus.effective"), "0-3,2-5,8").unwrap();
        std::fs::write(directory.join("memory.max"), "max").unwrap();
        std::fs::write(directory.join("memory.high"), "max").unwrap();
        std::fs::write(directory.join("memory.current"), "0").unwrap();

        let snapshot =
            LinuxCgroupSnapshot::detect_from_procfs("0::/tenant/job\n", &fixture.mountinfo);

        assert_eq!(snapshot.cpu_quota_parallelism, LinuxCgroupValue::Unlimited);
        assert_eq!(
            snapshot.cpuset_parallelism,
            LinuxCgroupValue::Value(NonZeroUsize::new(7).unwrap())
        );
        assert_eq!(snapshot.memory_limit_bytes, LinuxCgroupValue::Unlimited);
        assert_eq!(snapshot.memory_high_bytes, LinuxCgroupValue::Unlimited);
        assert_eq!(snapshot.memory_current_bytes, LinuxCgroupValue::Value(0));
    }

    #[test]
    fn invalid_v2_values_remain_typed_and_fail_closed() {
        let fixture = Fixture::new("invalid", "/");
        let directory = fixture.mount_point.join("tenant/job");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("cpu.max"), "invalid 100000").unwrap();
        std::fs::write(directory.join("cpuset.cpus.effective"), "4-2").unwrap();
        std::fs::write(directory.join("memory.max"), "invalid").unwrap();
        std::fs::write(directory.join("memory.high"), "invalid").unwrap();
        std::fs::write(directory.join("memory.current"), "invalid").unwrap();

        let snapshot =
            LinuxCgroupSnapshot::detect_from_procfs("0::/tenant/job\n", &fixture.mountinfo);

        assert_eq!(snapshot.version, LinuxCgroupVersion::V2);
        assert_eq!(snapshot.cpu_quota_parallelism, LinuxCgroupValue::Invalid);
        assert_eq!(snapshot.cpuset_parallelism, LinuxCgroupValue::Invalid);
        assert_eq!(snapshot.memory_limit_bytes, LinuxCgroupValue::Invalid);
        assert_eq!(snapshot.memory_high_bytes, LinuxCgroupValue::Invalid);
        assert_eq!(snapshot.memory_current_bytes, LinuxCgroupValue::Invalid);
    }

    #[test]
    fn v1_resource_controllers_are_explicitly_unsupported() {
        let self_cgroup = concat!(
            "2:cpu,cpuacct:/tenant/job\n",
            "3:cpuset:/tenant/job\n",
            "4:memory:/tenant/job\n"
        );

        let snapshot = LinuxCgroupSnapshot::detect_from_procfs(self_cgroup, "");

        assert_eq!(snapshot.version, LinuxCgroupVersion::V1Unsupported);
        assert_eq!(
            snapshot,
            LinuxCgroupSnapshot::fail_closed(LinuxCgroupVersion::V1Unsupported)
        );
    }

    #[test]
    fn hybrid_resource_controllers_are_not_partially_detected() {
        let fixture = Fixture::new("hybrid", "/");
        let snapshot = LinuxCgroupSnapshot::detect_from_procfs(
            "0::/tenant/job\n4:memory:/tenant/job\n",
            &fixture.mountinfo,
        );

        assert_eq!(snapshot.version, LinuxCgroupVersion::V1Unsupported);
        assert_eq!(snapshot.memory_limit_bytes, LinuxCgroupValue::Invalid);
    }

    #[test]
    fn invalid_paths_and_missing_mounts_fail_closed() {
        let invalid_path = LinuxCgroupSnapshot::detect_from_procfs(
            "0::/../../escape\n",
            "30 1 0:30 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
        );
        let missing_mount = LinuxCgroupSnapshot::detect_from_procfs("0::/tenant/job\n", "");
        let missing_membership = LinuxCgroupSnapshot::detect_from_procfs("", "");

        assert_eq!(invalid_path.version, LinuxCgroupVersion::Unknown);
        assert_eq!(missing_mount.version, LinuxCgroupVersion::Unknown);
        assert_eq!(missing_membership.version, LinuxCgroupVersion::Unknown);
        assert_eq!(invalid_path.memory_limit_bytes, LinuxCgroupValue::Invalid);
        assert_eq!(
            missing_mount.cpu_quota_parallelism,
            LinuxCgroupValue::Invalid
        );
    }

    #[test]
    fn hosts_without_resource_controller_membership_are_unconstrained() {
        let snapshot = LinuxCgroupSnapshot::detect_from_procfs("1:name=systemd:/user.slice\n", "");

        assert_eq!(snapshot, LinuxCgroupSnapshot::host());
    }

    #[test]
    fn zero_is_a_valid_v2_memory_boundary() {
        assert_eq!(parse_memory_limit("0"), LinuxCgroupValue::Value(0));
    }
}

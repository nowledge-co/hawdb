use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageMediaKind {
    Rotational,
    NonRotational,
    Memory,
    Network,
    Virtual,
    #[default]
    Unknown,
}

impl StorageMediaKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rotational => "rotational",
            Self::NonRotational => "non_rotational",
            Self::Memory => "memory",
            Self::Network => "network",
            Self::Virtual => "virtual",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageDeviceDiscoverySource {
    LinuxSysfs,
    AppleFileSystem,
    HostProvided,
    #[default]
    Unsupported,
}

impl StorageDeviceDiscoverySource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LinuxSysfs => "linux_sysfs",
            Self::AppleFileSystem => "apple_filesystem",
            Self::HostProvided => "host_provided",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StorageDeviceProfile {
    pub media_kind: StorageMediaKind,
    pub queue_depth_hint: Option<NonZeroUsize>,
    pub discovery_source: StorageDeviceDiscoverySource,
}

impl StorageDeviceProfile {
    pub fn detect(database_path: impl AsRef<Path>) -> Self {
        let path =
            nearest_existing_path(database_path.as_ref()).unwrap_or_else(|| PathBuf::from("."));
        detect_platform_device(&path)
    }

    pub const fn host_provided(
        media_kind: StorageMediaKind,
        queue_depth_hint: Option<NonZeroUsize>,
    ) -> Self {
        Self {
            media_kind,
            queue_depth_hint,
            discovery_source: StorageDeviceDiscoverySource::HostProvided,
        }
    }
}

fn nearest_existing_path(path: &Path) -> Option<PathBuf> {
    path.ancestors()
        .find(|candidate| !candidate.as_os_str().is_empty() && candidate.exists())
        .map(Path::to_path_buf)
}

#[cfg(target_os = "linux")]
fn detect_platform_device(path: &Path) -> StorageDeviceProfile {
    use std::os::unix::fs::MetadataExt;

    let Some(device) = std::fs::metadata(path).ok().map(|metadata| metadata.dev()) else {
        return StorageDeviceProfile::default();
    };
    let (major, minor) = linux_device_numbers(device);
    let sysfs_path = PathBuf::from(format!("/sys/dev/block/{major}:{minor}"));
    detect_linux_sysfs_device(&sysfs_path).unwrap_or_default()
}

#[cfg(any(target_os = "linux", test))]
fn linux_device_numbers(device: u64) -> (u64, u64) {
    let major = ((device & 0x0000_0000_000f_ff00) >> 8) | ((device & 0xffff_f000_0000_0000) >> 32);
    let minor = (device & 0x0000_0000_0000_00ff) | ((device & 0x0000_0fff_fff0_0000) >> 12);
    (major, minor)
}

#[cfg(any(target_os = "linux", test))]
fn detect_linux_sysfs_device(path: &Path) -> Option<StorageDeviceProfile> {
    let mut current = std::fs::canonicalize(path).ok()?;
    loop {
        let queue = current.join("queue");
        let media_kind = std::fs::read_to_string(queue.join("rotational"))
            .ok()
            .and_then(|value| parse_linux_rotational(&value));
        let queue_depth_hint = std::fs::read_to_string(queue.join("nr_requests"))
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .and_then(NonZeroUsize::new);
        if media_kind.is_some() || queue_depth_hint.is_some() {
            return Some(StorageDeviceProfile {
                media_kind: media_kind.unwrap_or(StorageMediaKind::Unknown),
                queue_depth_hint,
                discovery_source: StorageDeviceDiscoverySource::LinuxSysfs,
            });
        }
        current = current.parent()?.to_path_buf();
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_rotational(value: &str) -> Option<StorageMediaKind> {
    match value.trim() {
        "0" => Some(StorageMediaKind::NonRotational),
        "1" => Some(StorageMediaKind::Rotational),
        _ => None,
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn detect_platform_device(path: &Path) -> StorageDeviceProfile {
    let Some(file_system) = apple_file_system_type(path) else {
        return StorageDeviceProfile::default();
    };
    StorageDeviceProfile {
        media_kind: classify_apple_file_system(&file_system),
        queue_depth_hint: None,
        discovery_source: StorageDeviceDiscoverySource::AppleFileSystem,
    }
}

#[cfg(any(target_os = "macos", target_os = "ios", test))]
fn classify_apple_file_system(file_system: &str) -> StorageMediaKind {
    match file_system {
        "apfs" | "hfs" => StorageMediaKind::NonRotational,
        "tmpfs" => StorageMediaKind::Memory,
        "nfs" | "smbfs" | "webdav" | "afpfs" => StorageMediaKind::Network,
        "devfs" | "procfs" => StorageMediaKind::Virtual,
        _ => StorageMediaKind::Unknown,
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn apple_file_system_type(path: &Path) -> Option<String> {
    use std::ffi::CString;
    use std::mem::MaybeUninit;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut statistics = MaybeUninit::<libc::statfs>::uninit();
    if unsafe { libc::statfs(path.as_ptr(), statistics.as_mut_ptr()) } != 0 {
        return None;
    }
    let statistics = unsafe { statistics.assume_init() };
    let bytes = statistics
        .f_fstypename
        .iter()
        .copied()
        .take_while(|byte| *byte != 0)
        .map(|byte| byte as u8)
        .collect::<Vec<_>>();
    String::from_utf8(bytes).ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
fn detect_platform_device(_path: &Path) -> StorageDeviceProfile {
    StorageDeviceProfile::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_rotational_values_require_exact_kernel_evidence() {
        assert_eq!(
            parse_linux_rotational("0\n"),
            Some(StorageMediaKind::NonRotational)
        );
        assert_eq!(
            parse_linux_rotational("1"),
            Some(StorageMediaKind::Rotational)
        );
        assert_eq!(parse_linux_rotational("unknown"), None);
    }

    #[test]
    fn linux_device_number_decoding_preserves_extended_major_and_minor_bits() {
        let major = 0x1abc;
        let minor = 0x23456;
        let device = (minor & 0xff)
            | ((major & 0xfff) << 8)
            | ((minor & !0xff) << 12)
            | ((major & !0xfff) << 32);

        assert_eq!(linux_device_numbers(device), (major, minor));
    }

    #[test]
    fn host_profile_is_explicit_and_does_not_copy_a_device_identifier() {
        let profile = StorageDeviceProfile::host_provided(
            StorageMediaKind::NonRotational,
            NonZeroUsize::new(16),
        );

        assert_eq!(
            profile.discovery_source,
            StorageDeviceDiscoverySource::HostProvided
        );
        assert_eq!(profile.queue_depth_hint, NonZeroUsize::new(16));
    }

    #[test]
    fn apple_file_system_classification_recognizes_local_storage() {
        assert_eq!(
            classify_apple_file_system("apfs"),
            StorageMediaKind::NonRotational
        );
        assert_eq!(
            classify_apple_file_system("hfs"),
            StorageMediaKind::NonRotational
        );
        assert_eq!(
            classify_apple_file_system("smbfs"),
            StorageMediaKind::Network
        );
        assert_eq!(
            classify_apple_file_system("tmpfs"),
            StorageMediaKind::Memory
        );
        assert_eq!(
            classify_apple_file_system("devfs"),
            StorageMediaKind::Virtual
        );
        assert_eq!(
            classify_apple_file_system("unknown"),
            StorageMediaKind::Unknown
        );
    }

    #[test]
    fn linux_partition_inherits_queue_evidence_from_parent_device() {
        let root =
            std::env::temp_dir().join(format!("skein-qos-device-sysfs-{}", std::process::id()));
        let device = root.join("nvme0n1");
        let partition = device.join("nvme0n1p1");
        let queue = device.join("queue");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&partition).unwrap();
        std::fs::create_dir_all(&queue).unwrap();
        std::fs::write(queue.join("rotational"), "0\n").unwrap();
        std::fs::write(queue.join("nr_requests"), "64\n").unwrap();

        let profile = detect_linux_sysfs_device(&partition).unwrap();

        assert_eq!(profile.media_kind, StorageMediaKind::NonRotational);
        assert_eq!(profile.queue_depth_hint, NonZeroUsize::new(64));
        assert_eq!(
            profile.discovery_source,
            StorageDeviceDiscoverySource::LinuxSysfs
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn detection_uses_nearest_existing_ancestor() {
        let nested = std::env::temp_dir()
            .join("skein-device-discovery-missing")
            .join("database");

        let profile = StorageDeviceProfile::detect(nested);

        assert_ne!(
            profile.discovery_source,
            StorageDeviceDiscoverySource::HostProvided
        );
    }
}

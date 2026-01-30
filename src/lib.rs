#![cfg_attr(target_os = "windows", feature(windows_by_handle))]

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;
use sysinfo::{DiskRefreshKind, Disks};
use tokio::sync::watch;

pub const KB: u64 = 1024;
pub const MB: u64 = KB * KB;
pub const GB: u64 = MB * KB;

/// Global disk manager
static DISK_MANAGER: LazyLock<DiskManager> = LazyLock::new(DiskManager::new);

/// Handle to reserved disk space.
/// Dropping this struct automatically decrements the global reservation counter.
pub struct DiskReserve {
    amount: u64,
    state: Arc<DiskState>,
}

struct DiskState {
    /// Total available space
    available: AtomicU64,
    /// Total reserved space
    reserved: AtomicU64,
    /// Channel to notify waiting tasks on changes
    notify: watch::Sender<()>,
}

struct DiskManager {
    state: Arc<DiskState>,
}

impl DiskReserve {
    /// Wait for the system to have enough free space.
    ///
    /// Dropping this handle free the reserved amount.
    ///
    /// # Arguments
    ///
    /// - `amount`: The space to reserve in bytes.
    pub async fn new(amount: u64) -> io::Result<Self> {
        DISK_MANAGER.init().await?;

        Self::reserve(&DISK_MANAGER, amount).await
    }

    async fn reserve(disk_manager: &DiskManager, amount: u64) -> io::Result<Self> {
        disk_manager.init().await?;

        let mut rx = disk_manager.state.notify.subscribe();

        loop {
            if disk_manager.try_reserve(amount) {
                return Ok(Self {
                    amount,
                    state: disk_manager.state.clone(),
                });
            }

            if rx.changed().await.is_err() {
                return Err(io::Error::other("Disk manager shut down"));
            }
        }
    }
}

impl Drop for DiskReserve {
    fn drop(&mut self) {
        self.state
            .reserved
            .fetch_sub(self.amount, Ordering::Relaxed);

        let _ = self.state.notify.send(());
    }
}

impl DiskManager {
    const MIN_FREE: u64 = 50 * GB;
    const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

    fn new() -> Self {
        let (tx, _) = watch::channel(());
        Self {
            state: Arc::new(DiskState {
                available: AtomicU64::new(0),
                reserved: AtomicU64::new(0),
                notify: tx,
            }),
        }
    }

    /// Initializes the background disk monitoring
    async fn init(&self) -> io::Result<()> {
        static INIT_RESULT: OnceLock<io::Result<()>> = OnceLock::new();

        let result = INIT_RESULT.get_or_init(|| self.setup_background_task());

        result
            .as_ref()
            .cloned()
            .map_err(|err| io::Error::new(err.kind(), err.to_string()))
    }

    fn try_reserve(&self, amount: u64) -> bool {
        let mut reserved = self.state.reserved.load(Ordering::Relaxed);
        let available = self.state.available.load(Ordering::Relaxed);

        loop {
            // Optimistic reservation
            if available >= Self::MIN_FREE + reserved + amount {
                match self.state.reserved.compare_exchange_weak(
                    reserved,
                    reserved + amount,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let _ = self.state.notify.send(());

                        return true;
                    }
                    Err(v) => reserved = v,
                }
            } else {
                return false;
            }
        }
    }

    fn setup_background_task(&self) -> io::Result<()> {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        #[cfg(windows)]
        use std::os::windows::fs::MetadataExt;

        let current_dir = std::env::current_dir()?;
        let metadata = std::fs::metadata(&current_dir)?;

        #[cfg(unix)]
        let disk_id = metadata.dev();
        #[cfg(windows)]
        let disk_id = metadata
            .volume_serial_number()
            .ok_or_else(|| io::Error::other("Volume ID undefined"))?;

        let state = self.state.clone();

        std::thread::Builder::new()
            .name("disk-monitor".into())
            .spawn(move || {
                let refresh_kind = DiskRefreshKind::nothing().with_storage();
                let mut disks = Disks::new();
                let mut target_disk_index: Option<usize> = None;

                loop {
                    match target_disk_index {
                        Some(index) => {
                            // The disc list never change over time
                            let disk = &mut disks.list_mut()[index];
                            disk.refresh_specifics(refresh_kind);
                        }
                        None => {
                            disks.refresh_specifics(true, refresh_kind);
                            for (i, disk) in disks.list().iter().enumerate() {
                                if let Ok(meta) = std::fs::metadata(disk.mount_point()) {
                                    #[cfg(unix)]
                                    let is_match = meta.dev() == disk_id;
                                    #[cfg(windows)]
                                    let is_match = meta.volume_serial_number() == Some(disk_id);

                                    if is_match {
                                        target_disk_index.replace(i);
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if let Some(disk) = target_disk_index.map(|i| &disks.list()[i]) {
                        let available_space = disk.available_space();
                        state.available.store(available_space, Ordering::Relaxed);

                        let _ = state.notify.send(());
                    }

                    std::thread::sleep(Self::REFRESH_INTERVAL);
                }
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{DiskManager, DiskReserve, DiskState, GB};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::sync::watch;

    #[test]
    fn disk_state_reserve() {
        let (tx, _) = watch::channel(());

        let disk_manager = DiskManager {
            state: Arc::new(DiskState {
                available: AtomicU64::new(100 * GB),
                reserved: AtomicU64::new(0),
                notify: tx,
            })
        };

        assert!(!disk_manager.try_reserve(100 * GB));
        assert!(disk_manager.try_reserve(50 * GB));
        assert_eq!(disk_manager.state.reserved.load(Ordering::Relaxed), 50 * GB);
        assert_eq!(disk_manager.state.available.load(Ordering::Relaxed), 100 * GB);
    }

    #[tokio::test]
    async fn disk_state_reserve_and_free() {
        let (tx, mut rx) = watch::channel(());

        let disk_manager = DiskManager {
            state: Arc::new(DiskState {
                available: AtomicU64::new(100 * GB),
                reserved: AtomicU64::new(0),
                notify: tx,
            })
        };

        assert!(!rx.has_changed().unwrap());

        let _reserve_10 = DiskReserve::reserve(&disk_manager, 10 * GB).await;
        assert_eq!(disk_manager.state.reserved.load(Ordering::Relaxed), 10 * GB);
        assert_eq!(disk_manager.state.available.load(Ordering::Relaxed), 100 * GB);

        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();

        let _reserve_20 = DiskReserve::reserve(&disk_manager, 20 * GB).await;
        assert_eq!(disk_manager.state.reserved.load(Ordering::Relaxed), 30 * GB);
        assert_eq!(disk_manager.state.available.load(Ordering::Relaxed), 100 * GB);

        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();

        assert!(!disk_manager.try_reserve(21 * GB));

        assert!(!rx.has_changed().unwrap());

        drop(_reserve_10);
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();

        assert_eq!(disk_manager.state.reserved.load(Ordering::Relaxed), 20 * GB);
        assert_eq!(disk_manager.state.available.load(Ordering::Relaxed), 100 * GB);

        drop(_reserve_20);
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();

        assert_eq!(disk_manager.state.reserved.load(Ordering::Relaxed), 0);
        assert_eq!(disk_manager.state.available.load(Ordering::Relaxed), 100 * GB);
    }
}

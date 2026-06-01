use std::{
    cmp,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

/// Central boundary for install scheduling decisions.
///
/// Filesystem batches use an explicit, OS-aware worker count instead of rayon.
/// The long term goal is to split all install work by resource class (`cpu`,
/// `fs`, `network`, `scripts`) so package-manager code describes the work it
/// needs done instead of choosing the executor directly.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct InstallScheduler;

impl InstallScheduler {
    pub(crate) fn current() -> Self {
        Self
    }

    pub(crate) fn run_fs_batch<T, E, F>(&self, items: &[T], work: F) -> Result<(), E>
    where
        T: Sync,
        F: Fn(&T) -> Result<(), E> + Send + Sync,
        E: Send,
    {
        self.run_blocking(|| run_bounded_fs_batch(items, work))
    }

    pub(crate) fn run_fs_batch_unchecked<T, F>(&self, items: &[T], work: F)
    where
        T: Sync,
        F: Fn(&T) + Send + Sync,
    {
        self.run_blocking(|| run_bounded_fs_batch_unchecked(items, work))
    }

    /// Run blocking package-manager work from either production's multi-thread
    /// tokio runtime or tests' current-thread runtimes.
    ///
    /// `tokio::task::block_in_place` is the right production boundary for a
    /// synchronous package batch inside an async install: it lets tokio move
    /// other futures off the worker before this thread blocks. It panics on
    /// current-thread runtimes, though, so tests and any single-thread caller
    /// run the closure inline.
    fn run_blocking<F, R>(&self, work: F) -> R
    where
        F: FnOnce() -> R,
    {
        let on_multi_thread = tokio::runtime::Handle::try_current().is_ok_and(|handle| {
            handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        });
        if on_multi_thread { tokio::task::block_in_place(work) } else { work() }
    }
}

fn run_bounded_fs_batch<T, E, F>(items: &[T], work: F) -> Result<(), E>
where
    T: Sync,
    F: Fn(&T) -> Result<(), E> + Send + Sync,
    E: Send,
{
    if items.is_empty() {
        return Ok(());
    }

    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let first_error = Mutex::new(None);
    let worker_count = cmp::min(fs_parallelism(), items.len());

    thread::scope(|scope| {
        for _ in 0..worker_count {
            scope.spawn(|| {
                loop {
                    if failed.load(Ordering::Relaxed) {
                        break;
                    }
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(item) = items.get(index) else {
                        break;
                    };
                    if let Err(error) = work(item) {
                        if !failed.swap(true, Ordering::Relaxed) {
                            *first_error.lock().expect("first fs batch error lock poisoned") =
                                Some(error);
                        }
                        break;
                    }
                }
            });
        }
    });

    match first_error.into_inner().expect("first fs batch error lock poisoned") {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn run_bounded_fs_batch_unchecked<T, F>(items: &[T], work: F)
where
    T: Sync,
    F: Fn(&T) + Send + Sync,
{
    if items.is_empty() {
        return;
    }

    let next = AtomicUsize::new(0);
    let worker_count = cmp::min(fs_parallelism(), items.len());
    thread::scope(|scope| {
        for _ in 0..worker_count {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(item) = items.get(index) else {
                        break;
                    };
                    work(item);
                }
            });
        }
    });
}

fn fs_parallelism() -> usize {
    if cfg!(target_os = "macos") {
        4
    } else {
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1)
            .saturating_mul(2)
            .max(4)
    }
}

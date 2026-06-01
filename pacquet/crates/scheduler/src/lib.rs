use std::{
    cmp,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Instant,
};
use tokio::sync::Semaphore;

/// Central boundary for install scheduling decisions.
///
/// Filesystem batches use an explicit, OS-aware worker count instead of rayon.
/// The long term goal is to split all install work by resource class (`cpu`,
/// `fs`, `network`, `scripts`) so call sites describe the work they need done
/// instead of choosing the executor directly.
#[derive(Debug, Default, Clone, Copy)]
pub struct InstallScheduler;

impl InstallScheduler {
    pub fn current() -> Self {
        Self
    }

    pub fn run_fs_batch<T, E, F>(&self, items: &[T], work: F) -> Result<(), E>
    where
        T: Sync,
        F: Fn(&T) -> Result<(), E> + Send + Sync,
        E: Send,
    {
        let started = Instant::now();
        let item_count = items.len();
        let worker_count = cmp::min(fs_parallelism(), item_count);
        let result = self.run_blocking(|| run_bounded_fs_batch(items, work));
        tracing::debug!(
            target: "pacquet::scheduler",
            resource = "fs",
            kind = "batch",
            item_count,
            worker_count,
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            "scheduler fs batch completed",
        );
        result
    }

    pub fn run_fs_batch_unchecked<T, F>(&self, items: &[T], work: F)
    where
        T: Sync,
        F: Fn(&T) + Send + Sync,
    {
        let started = Instant::now();
        let item_count = items.len();
        let worker_count = cmp::min(fs_parallelism(), item_count);
        self.run_blocking(|| run_bounded_fs_batch_unchecked(items, work));
        tracing::debug!(
            target: "pacquet::scheduler",
            resource = "fs",
            kind = "batch_unchecked",
            item_count,
            worker_count,
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            "scheduler fs batch completed",
        );
    }

    pub async fn run_cpu<F, R>(&self, work: F) -> Result<R, tokio::task::JoinError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let wait_started = Instant::now();
        let _permit = cpu_semaphore().acquire().await.expect("cpu semaphore should stay open");
        let wait_ms = wait_started.elapsed().as_secs_f64() * 1000.0;
        let run_started = Instant::now();
        let result = tokio::task::spawn_blocking(work).await;
        tracing::debug!(
            target: "pacquet::scheduler",
            resource = "cpu",
            wait_ms,
            run_ms = run_started.elapsed().as_secs_f64() * 1000.0,
            "scheduler cpu task completed",
        );
        result
    }

    pub async fn run_fs<F, R>(&self, work: F) -> Result<R, tokio::task::JoinError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let wait_started = Instant::now();
        let _permit = fs_semaphore().acquire().await.expect("fs semaphore should stay open");
        let wait_ms = wait_started.elapsed().as_secs_f64() * 1000.0;
        let run_started = Instant::now();
        let result = tokio::task::spawn_blocking(work).await;
        tracing::debug!(
            target: "pacquet::scheduler",
            resource = "fs",
            wait_ms,
            run_ms = run_started.elapsed().as_secs_f64() * 1000.0,
            "scheduler fs task completed",
        );
        result
    }

    /// Run blocking work from either production's multi-thread tokio runtime or
    /// tests' current-thread runtimes.
    ///
    /// `tokio::task::block_in_place` is the right production boundary for a
    /// synchronous batch inside an async install: it lets tokio move other
    /// futures off the worker before this thread blocks. It panics on
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

fn cpu_parallelism() -> usize {
    std::thread::available_parallelism().map(std::num::NonZeroUsize::get).unwrap_or(1).max(1)
}

fn cpu_semaphore() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(cpu_parallelism()))
}

fn fs_semaphore() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(fs_parallelism()))
}

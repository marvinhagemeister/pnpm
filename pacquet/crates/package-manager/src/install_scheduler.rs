use rayon::prelude::*;

/// Central boundary for install scheduling decisions.
///
/// Today this still delegates package batches to rayon, preserving the current
/// behavior while moving the tokio/rayon boundary out of call sites. The long
/// term goal is to split this by resource class (`cpu`, `fs`, `network`,
/// `scripts`) so package-manager code describes the work it needs done instead
/// of choosing the executor directly.
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
        self.run_blocking(|| items.par_iter().try_for_each(work))
    }

    pub(crate) fn run_fs_batch_unchecked<T, F>(&self, items: &[T], work: F)
    where
        T: Sync,
        F: Fn(&T) + Send + Sync,
    {
        self.run_blocking(|| items.par_iter().for_each(work))
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

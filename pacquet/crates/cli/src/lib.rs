mod cli_args;
mod config_overrides;
mod state;

use clap::Parser;
use cli_args::CliArgs;
use config_overrides::ConfigOverrides;
use miette::set_panic_hook;
use pacquet_diagnostics::enable_tracing_by_env;
use state::State;

pub async fn main() -> miette::Result<()> {
    enable_tracing_by_env();
    set_panic_hook();
    // Extract pnpm's `--config.<key>=<value>` tokens before clap sees
    // argv. Clap can't parse a dotted-key flag whose right-hand name is
    // arbitrary, so a `--config.registry=...` from pnpm's forwarded flags
    // would otherwise error out as "unexpected argument". Each extracted
    // token is layered onto `Config` after `.npmrc` / yaml run.
    let (config_overrides, argv) = ConfigOverrides::extract(std::env::args_os());
    // Run argument parsing *before* sizing the rayon pool so
    // `pacquet --help` / `--version` (and any clap parse error) exit
    // without spinning up worker threads. `clap::Parser::parse` calls
    // `std::process::exit` on those paths, so we never reach
    // `configure_rayon_pool` for them (Copilot review on <https://github.com/pnpm/pacquet/pull/292>).
    let args = CliArgs::parse_from(argv);
    configure_rayon_pool();
    args.run(&config_overrides).await
}

/// Size rayon's global pool for pacquet's remaining package-level
/// filesystem work. On macOS, the hot warm-install path now materializes
/// packages via one APFS `clonefile` per package-shaped cache tree, so
/// 4 threads keeps the metadata journal busy without multiplying blocked
/// syscall workers. That matches Deno's macOS npm-cache writer limit and
/// local package-tree measurements (`RAYON_NUM_THREADS=4` had the same
/// wall time as the default while burning less system time).
///
/// Use [`std::thread::available_parallelism`] rather than the
/// workspace's existing `num_cpus::get()` so cgroup / CPU-quota
/// limits in containers and CI runners are respected — `num_cpus`
/// reports the host's logical CPU count, which on a quota-limited
/// runner can spin up far more rayon threads than the kernel will
/// actually schedule onto our cores (Copilot review on [#292]).
///
/// Non-macOS still uses `max(4, 2 × available_parallelism)` for now.
/// Those platforms do not have the macOS directory-clone fast path yet,
/// so warm installs can still fall back to per-file hardlink/copy work
/// where some oversubscription hides blocking filesystem calls.
///
/// Honours an explicit `RAYON_NUM_THREADS` env var by skipping our
/// override (rayon's `build_global` errors if a pool is already set,
/// but env vars don't pre-init it — so we just apply a smaller
/// override only when nothing else has been configured). Best-effort:
/// if another part of the binary already initialised the pool, leave
/// it alone.
///
/// [#292]: https://github.com/pnpm/pacquet/pull/292
fn configure_rayon_pool() {
    if std::env::var_os("RAYON_NUM_THREADS").is_some() {
        return;
    }
    let n = configured_rayon_threads();
    let _ = rayon::ThreadPoolBuilder::new().num_threads(n).build_global();
}

fn configured_rayon_threads() -> usize {
    if cfg!(target_os = "macos") {
        return 4;
    }
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .saturating_mul(2)
        .max(4)
}

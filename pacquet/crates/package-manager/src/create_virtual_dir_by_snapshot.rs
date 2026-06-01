use crate::{
    ImportIndexedDirError, ImportIndexedDirOpts, SkippedSnapshots, SymlinkPackageError,
    VirtualStoreLayout, create_symlink_layout, import_indexed_dir,
};
use derive_more::{Display, Error};
use miette::Diagnostic;
use pacquet_config::PackageImportMethod;
use pacquet_lockfile::{PackageKey, SnapshotEntry};
use pacquet_reporter::{
    LogEvent, LogLevel, PackageImportMethod as WireImportMethod, ProgressLog, ProgressMessage,
    Reporter,
};
use std::{collections::HashMap, fs, io, path::PathBuf, sync::atomic::AtomicU8, time::Instant};

/// This subroutine creates the virtual-store slot for one package, imports its
/// files, and then creates the package's dependency symlinks. The caller drives
/// many snapshots in parallel; inside one snapshot, keeping the operations
/// serial avoids nested rayon work now that the macOS import fast path is one
/// package-tree clone instead of one task per file.
#[must_use]
pub struct CreateVirtualDirBySnapshot<'a> {
    /// Per-install precomputed slot-directory mapping. Replaces the
    /// previous `virtual_store_dir: &Path` field — the layout already
    /// holds the root and knows how to resolve a per-snapshot slot
    /// (legacy `<root>/<flat-name>` vs GVS-shaped
    /// `<root>/<scope>/<name>/<version>/<hash>`) through a single
    /// [`VirtualStoreLayout::slot_dir`] lookup. See
    /// [`crate::VirtualStoreLayout`] for how it's built.
    pub layout: &'a VirtualStoreLayout,
    pub cas_paths: &'a HashMap<String, PathBuf>,
    pub import_method: PackageImportMethod,
    /// Install-scoped dedupe state for `pnpm:package-import-method`.
    /// See the comment on `link_file::log_method_once` for why this
    /// is install-scoped rather than module-static.
    pub logged_methods: &'a AtomicU8,
    /// Install root, threaded into `pnpm:progress` `imported`'s
    /// `requester`. Same value as the `prefix` in
    /// [`pacquet_reporter::StageLog`].
    pub requester: &'a str,
    /// Stable identifier for the package, e.g. `"{name}@{version}"`.
    /// Currently unused by `imported` (whose payload doesn't carry
    /// `packageId`) but kept here so future progress channels (e.g.
    /// per-package counts) can read it without rethreading.
    pub package_id: &'a str,
    pub package_tree_dir: Option<PathBuf>,
    pub package_key: &'a PackageKey,
    pub snapshot: &'a SnapshotEntry,
    /// Snapshots whose slots were not materialized on this host —
    /// platform-mismatched optionals, `--no-optional` exclusions, and
    /// swallowed optional fetch failures. `create_symlink_layout`
    /// uses this to skip dangling symlinks to absent slots. Mirrors
    /// upstream's `!pkg.installable && pkg.optional` short-circuit in
    /// `linkAllModules` at
    /// <https://github.com/pnpm/pnpm/blob/f2981a316/installing/deps-installer/src/install/link.ts#L540>.
    pub skipped: &'a SkippedSnapshots,
}

/// Error type of [`CreateVirtualDirBySnapshot`].
#[derive(Debug, Display, Error, Diagnostic)]
pub enum CreateVirtualDirError {
    #[display("Failed to recursively create node_modules directory at {dir:?}: {error}")]
    #[diagnostic(code(pacquet_package_manager::create_node_modules_dir))]
    CreateNodeModulesDir {
        dir: PathBuf,
        #[error(source)]
        error: io::Error,
    },

    #[diagnostic(transparent)]
    ImportIndexedDir(#[error(source)] ImportIndexedDirError),

    #[diagnostic(transparent)]
    SymlinkPackage(#[error(source)] SymlinkPackageError),
}

impl<'a> CreateVirtualDirBySnapshot<'a> {
    /// Execute the subroutine.
    pub fn run<Reporter: self::Reporter>(self) -> Result<(), CreateVirtualDirError> {
        let CreateVirtualDirBySnapshot {
            layout,
            cas_paths,
            import_method,
            logged_methods,
            requester,
            package_id,
            package_tree_dir,
            package_key,
            snapshot,
            skipped,
        } = self;

        let trace_materialize =
            tracing::enabled!(target: "pacquet::materialize", tracing::Level::DEBUG);
        let total_start = trace_materialize.then(Instant::now);

        let virtual_node_modules_dir = layout.slot_dir(package_key).join("node_modules");
        let mkdir_start = trace_materialize.then(Instant::now);
        fs::create_dir_all(&virtual_node_modules_dir).map_err(|error| {
            CreateVirtualDirError::CreateNodeModulesDir {
                dir: virtual_node_modules_dir.clone(),
                error,
            }
        })?;
        let mkdir_ms = mkdir_start.map(elapsed_ms);

        let save_path = virtual_node_modules_dir.join(package_key.name.to_string());

        let import_start = trace_materialize.then(Instant::now);
        import_indexed_dir::<Reporter>(
            logged_methods,
            import_method,
            &save_path,
            cas_paths,
            ImportIndexedDirOpts { package_tree_dir, ..ImportIndexedDirOpts::default() },
        )
        .map_err(CreateVirtualDirError::ImportIndexedDir)?;
        let import_ms = import_start.map(elapsed_ms);

        let symlink_start = trace_materialize.then(Instant::now);
        create_symlink_layout(
            snapshot.dependencies.as_ref(),
            snapshot.optional_dependencies.as_ref(),
            &package_key.name,
            skipped,
            layout,
            &virtual_node_modules_dir,
        )
        .map_err(CreateVirtualDirError::SymlinkPackage)?;
        let symlink_ms = symlink_start.map(elapsed_ms);

        // `pnpm:progress imported` mirrors pnpm's emit at
        // <https://github.com/pnpm/pnpm/blob/086c5e91e8/installing/deps-installer/src/install/link.ts#L498>:
        // one event per (resolved + fetched) package once its CAFS
        // import has finished. `to` is the per-package directory
        // inside the virtual store. `method` is best-effort — pacquet
        // doesn't surface the per-package resolved method past
        // `link_file`'s install-scoped atomic, so we report the
        // optimistic value the configured method would resolve to in
        // a non-degraded environment (`Auto`/`CloneOrCopy` → `clone`,
        // explicit settings as-is). Refining to per-package resolution
        // would require threading the resolved method back from
        // `link_file`; tracked under <https://github.com/pnpm/pacquet/issues/347>.
        let progress_start = trace_materialize.then(Instant::now);
        Reporter::emit(&LogEvent::Progress(ProgressLog {
            level: LogLevel::Debug,
            message: ProgressMessage::Imported {
                method: optimistic_wire_method(import_method),
                requester: requester.to_owned(),
                to: save_path.to_string_lossy().into_owned(),
            },
        }));
        let progress_ms = progress_start.map(elapsed_ms);

        if let Some(total_start) = total_start {
            tracing::debug!(
                target: "pacquet::materialize",
                package_id,
                package_key = %package_key,
                mkdir_ms = mkdir_ms.unwrap_or_default(),
                import_ms = import_ms.unwrap_or_default(),
                symlink_ms = symlink_ms.unwrap_or_default(),
                progress_ms = progress_ms.unwrap_or_default(),
                total_ms = elapsed_ms(total_start),
                "materialized virtual-store package",
            );
        }

        Ok(())
    }
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

/// Map pacquet's configured [`PackageImportMethod`] to the value
/// `pnpm:progress imported`'s `method` field carries. pnpm only
/// distinguishes the three resolved methods; for `Auto` and
/// `CloneOrCopy` the optimistic first-attempt method is `clone`.
/// See the comment at the emit site for why this is best-effort.
pub(crate) fn optimistic_wire_method(method: PackageImportMethod) -> WireImportMethod {
    match method {
        PackageImportMethod::Auto
        | PackageImportMethod::Clone
        | PackageImportMethod::CloneOrCopy => WireImportMethod::Clone,
        PackageImportMethod::Hardlink => WireImportMethod::Hardlink,
        PackageImportMethod::Copy => WireImportMethod::Copy,
    }
}

#[cfg(test)]
mod tests;

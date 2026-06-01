use crate::{
    ImportIndexedDirOpts,
    import_indexed_dir::{
        clone_package_tree, import_indexed_dir as import_indexed_dir_fn, package_tree_fingerprint,
        package_tree_supported, pick_stage_path,
    },
    link_file::log_package_import_method_once,
};
use pacquet_config::PackageImportMethod;
use pacquet_crypto_hash::create_short_hash;
use pacquet_lockfile::PackageKey;
use pacquet_reporter::{PackageImportMethod as WireImportMethod, Reporter};
use std::{
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::AtomicU8,
};

pub(crate) fn package_instance_dir(
    store_root: &Path,
    package_key: &PackageKey,
    cas_paths: &HashMap<String, PathBuf>,
) -> PathBuf {
    let fingerprint = package_tree_fingerprint(cas_paths);
    let key = format!("v1\0{package_key}\0{fingerprint}");
    store_root.join("package-instances").join(create_short_hash(&key))
}

pub(crate) fn try_populate_from_package_instance<Reporter: self::Reporter>(
    logged_methods: &AtomicU8,
    import_method: PackageImportMethod,
    dir_path: &Path,
    cas_paths: &HashMap<String, PathBuf>,
    package_instance_dir: &Path,
    package_tree_dir: Option<&Path>,
) -> bool {
    if !package_tree_supported(import_method) {
        return false;
    }

    let trace_instance =
        tracing::enabled!(target: "pacquet::package_instance", tracing::Level::DEBUG);
    let total_start = trace_instance.then(std::time::Instant::now);
    let fingerprint_start = trace_instance.then(std::time::Instant::now);
    let fingerprint = package_tree_fingerprint(cas_paths);
    let fingerprint_ms = fingerprint_start.map(elapsed_ms);

    let sentinel_start = trace_instance.then(std::time::Instant::now);
    let initialized = matches!(
        fs::read_to_string(package_instance_dir.join(".initialized")),
        Ok(existing) if existing == fingerprint
    );
    let sentinel_ms = sentinel_start.map(elapsed_ms);

    let build_start = trace_instance.then(std::time::Instant::now);
    if !initialized
        && let Err(error) = build_package_instance::<Reporter>(
            logged_methods,
            import_method,
            cas_paths,
            package_instance_dir,
            package_tree_dir,
            &fingerprint,
        )
    {
        tracing::debug!(
            target: "pacquet::package_instance",
            ?package_instance_dir,
            ?error,
            "failed to build package instance cache; falling back to package-tree import",
        );
        return false;
    }
    let build_ms = build_start.map(elapsed_ms);

    let files_dir = package_instance_dir.join("files");
    let clone_start = trace_instance.then(std::time::Instant::now);
    match clone_package_tree(&files_dir, dir_path) {
        Ok(()) => {
            let clone_ms = clone_start.map(elapsed_ms);
            log_package_import_method_once::<Reporter>(logged_methods, WireImportMethod::Clone);
            if let Some(total_start) = total_start {
                tracing::debug!(
                    target: "pacquet::package_instance",
                    ?files_dir,
                    ?dir_path,
                    file_count = cas_paths.len(),
                    initialized,
                    fingerprint_ms = fingerprint_ms.unwrap_or_default(),
                    sentinel_ms = sentinel_ms.unwrap_or_default(),
                    build_ms = build_ms.unwrap_or_default(),
                    clone_ms = clone_ms.unwrap_or_default(),
                    total_ms = elapsed_ms(total_start),
                    "populated package from package-instance cache",
                );
            }
            true
        }
        Err(error) => {
            tracing::debug!(
                target: "pacquet::package_instance",
                ?files_dir,
                ?dir_path,
                ?error,
                "failed to clone package instance cache; falling back to package-tree import",
            );
            false
        }
    }
}

fn build_package_instance<Reporter: self::Reporter>(
    logged_methods: &AtomicU8,
    import_method: PackageImportMethod,
    cas_paths: &HashMap<String, PathBuf>,
    package_instance_dir: &Path,
    package_tree_dir: Option<&Path>,
    fingerprint: &str,
) -> io::Result<()> {
    if let Some(parent) = package_instance_dir.parent() {
        fs::create_dir_all(parent)?;
    }

    let stage = pick_stage_path(package_instance_dir);
    let stage_files = stage.join("files");
    if let Err(error) = import_indexed_dir_fn::<Reporter>(
        logged_methods,
        import_method,
        &stage_files,
        cas_paths,
        ImportIndexedDirOpts {
            package_tree_dir: package_tree_dir.map(Path::to_path_buf),
            ..ImportIndexedDirOpts::default()
        },
    ) {
        let _ = fs::remove_dir_all(&stage);
        return Err(io::Error::other(error.to_string()));
    }

    fs::write(stage.join(".initialized"), fingerprint)?;
    match fs::rename(&stage, package_instance_dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_dir_all(&stage);
            Ok(())
        }
        Err(error) => {
            let _ = fs::remove_dir_all(&stage);
            Err(error)
        }
    }
}

fn elapsed_ms(start: std::time::Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

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

pub(crate) struct PackageInstanceEntry {
    pub dir: PathBuf,
    pub fingerprint: String,
}

pub(crate) fn package_instance_entry(
    store_root: &Path,
    package_key: &PackageKey,
    cas_paths: &HashMap<String, PathBuf>,
) -> PackageInstanceEntry {
    let fingerprint = package_tree_fingerprint(cas_paths);
    let key = format!("v1\0{package_key}\0{fingerprint}");
    PackageInstanceEntry {
        dir: store_root.join("package-instances").join(create_short_hash(&key)),
        fingerprint,
    }
}

pub(crate) fn try_populate_from_package_instance<Reporter: self::Reporter>(
    logged_methods: &AtomicU8,
    import_method: PackageImportMethod,
    dir_path: &Path,
    cas_paths: &HashMap<String, PathBuf>,
    package_instance: &PackageInstanceEntry,
    package_key: &PackageKey,
    package_tree_dir: Option<&Path>,
) -> bool {
    if !package_tree_supported(import_method) {
        return false;
    }

    let trace_instance =
        tracing::enabled!(target: "pacquet::package_instance", tracing::Level::DEBUG);
    let total_start = trace_instance.then(std::time::Instant::now);

    let sentinel_start = trace_instance.then(std::time::Instant::now);
    let initialized = has_matching_metadata(
        &package_instance.dir,
        package_key,
        &package_instance.fingerprint,
        cas_paths.len(),
    );
    let sentinel_ms = sentinel_start.map(elapsed_ms);

    let build_start = trace_instance.then(std::time::Instant::now);
    if !initialized
        && let Err(error) = build_package_instance::<Reporter>(
            logged_methods,
            import_method,
            cas_paths,
            &package_instance.dir,
            package_key,
            package_tree_dir,
            &package_instance.fingerprint,
        )
    {
        tracing::debug!(
            target: "pacquet::package_instance",
            package_instance_dir = ?package_instance.dir,
            ?error,
            "failed to build package instance cache; falling back to package-tree import",
        );
        return false;
    }
    let build_ms = build_start.map(elapsed_ms);

    let files_dir = package_instance.dir.join("files");
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
    package_key: &PackageKey,
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

    write_metadata(&stage, package_key, fingerprint, cas_paths.len())?;
    fs::write(stage.join(".initialized"), fingerprint)?;
    match fs::rename(&stage, package_instance_dir) {
        Ok(()) => Ok(()),
        Err(error) if is_existing_dir_error(&error) => {
            if let Err(remove_error) = fs::remove_dir_all(package_instance_dir)
                && remove_error.kind() != io::ErrorKind::NotFound
            {
                let _ = fs::remove_dir_all(&stage);
                return Err(remove_error);
            }
            match fs::rename(&stage, package_instance_dir) {
                Ok(()) => Ok(()),
                Err(error) if is_existing_dir_error(&error) => {
                    let _ = fs::remove_dir_all(&stage);
                    Ok(())
                }
                Err(error) => {
                    let _ = fs::remove_dir_all(&stage);
                    Err(error)
                }
            }
        }
        Err(error) => {
            let _ = fs::remove_dir_all(&stage);
            Err(error)
        }
    }
}

fn is_existing_dir_error(error: &io::Error) -> bool {
    matches!(error.kind(), io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty)
}

fn has_matching_metadata(
    package_instance_dir: &Path,
    package_key: &PackageKey,
    fingerprint: &str,
    file_count: usize,
) -> bool {
    let package_key = package_key.to_string();
    let Ok(bytes) = fs::read(package_instance_dir.join("instance.json")) else {
        return false;
    };
    let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return false;
    };
    metadata.get("schema").and_then(serde_json::Value::as_u64) == Some(1)
        && metadata.get("package_key").and_then(serde_json::Value::as_str)
            == Some(package_key.as_str())
        && metadata.get("content_fingerprint").and_then(serde_json::Value::as_str)
            == Some(fingerprint)
        && metadata.get("file_count").and_then(serde_json::Value::as_u64) == Some(file_count as u64)
        && package_instance_dir.join("files").is_dir()
}

fn write_metadata(
    package_instance_dir: &Path,
    package_key: &PackageKey,
    fingerprint: &str,
    file_count: usize,
) -> io::Result<()> {
    let metadata = serde_json::json!({
        "schema": 1,
        "package_key": package_key.to_string(),
        "content_fingerprint": fingerprint,
        "file_count": file_count,
    });
    let bytes = serde_json::to_vec(&metadata).map_err(io::Error::other)?;
    fs::write(package_instance_dir.join("instance.json"), bytes)
}

fn elapsed_ms(start: std::time::Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::{has_matching_metadata, write_metadata};
    use pacquet_lockfile::PackageKey;
    use tempfile::tempdir;

    #[test]
    fn metadata_match_requires_expected_package_key_fingerprint_and_file_count() {
        let dir = tempdir().expect("tempdir");
        let package_key: PackageKey = "react@18.0.0".parse().expect("valid package key");
        std::fs::create_dir(dir.path().join("files")).expect("files dir");

        write_metadata(dir.path(), &package_key, "abc123", 2).expect("write metadata");

        assert!(has_matching_metadata(dir.path(), &package_key, "abc123", 2));
        assert!(!has_matching_metadata(dir.path(), &package_key, "different", 2));
        assert!(!has_matching_metadata(dir.path(), &package_key, "abc123", 3));

        let other_key: PackageKey = "preact@10.0.0".parse().expect("valid package key");
        assert!(!has_matching_metadata(dir.path(), &other_key, "abc123", 2));
    }
}

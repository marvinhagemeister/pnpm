use crate::import_indexed_dir::{clone_package_tree, pick_stage_path};
use pacquet_config::{Config, NodeLinker};
use pacquet_crypto_hash::create_short_hash;
use pacquet_modules_yaml::IncludedDependencies;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub(crate) struct ProjectLayoutCache {
    dir: PathBuf,
    key: String,
}

impl ProjectLayoutCache {
    pub(crate) fn new(
        config: &Config,
        namespace: &str,
        lockfile_path: Option<&Path>,
        node_linker: NodeLinker,
        included: IncludedDependencies,
    ) -> Option<Self> {
        if !cfg!(target_os = "macos") {
            return None;
        }

        let lockfile = lockfile_path.and_then(|path| fs::read_to_string(path).ok())?;
        let namespace = create_short_hash(namespace);
        let key = create_short_hash(&format!(
            "v1\0lockfile={lockfile}\0node_linker={node_linker:?}\0import_method={:?}\0included={}:{}:{}\0virtual_store_max={}\0symlink={}\0store_dir={}\0modules_dir={}\0virtual_store_dir={}\0hoist={}\0hoist_pattern={:?}\0public_hoist_pattern={:?}\0shamefully_hoist={}",
            config.package_import_method,
            included.dependencies,
            included.dev_dependencies,
            included.optional_dependencies,
            config.virtual_store_dir_max_length,
            config.symlink,
            config.store_dir.root().display(),
            config.modules_dir.display(),
            config.virtual_store_dir.display(),
            config.hoist,
            config.hoist_pattern,
            config.public_hoist_pattern,
            config.shamefully_hoist,
        ));

        Some(Self { dir: config.cache_dir.join("project-layouts").join(namespace), key })
    }

    pub(crate) fn restore(&self, modules_dir: &Path) -> bool {
        if !self.metadata_matches() {
            return false;
        }

        if let Err(error) = remove_dir_if_exists(modules_dir) {
            tracing::debug!(
                target: "pacquet::project_layout_cache",
                ?modules_dir,
                ?error,
                "failed to remove modules dir before project layout restore",
            );
            return false;
        }

        match clone_package_tree(&self.dir.join("layout"), modules_dir) {
            Ok(()) => true,
            Err(error) => {
                tracing::debug!(
                    target: "pacquet::project_layout_cache",
                    ?modules_dir,
                    ?error,
                    "failed to restore project layout cache",
                );
                false
            }
        }
    }

    pub(crate) fn store(&self, modules_dir: &Path) {
        if let Err(error) = self.store_inner(modules_dir) {
            tracing::debug!(
                target: "pacquet::project_layout_cache",
                cache_dir = ?self.dir,
                ?error,
                "failed to store project layout cache",
            );
        }
    }

    fn store_inner(&self, modules_dir: &Path) -> io::Result<()> {
        if !modules_dir.is_dir() {
            return Ok(());
        }
        if let Some(parent) = self.dir.parent() {
            fs::create_dir_all(parent)?;
        }

        let stage = pick_stage_path(&self.dir);
        clone_package_tree(modules_dir, &stage.join("layout"))?;
        fs::write(stage.join("metadata.json"), self.metadata_json())?;
        replace_dir(&stage, &self.dir)
    }

    fn metadata_matches(&self) -> bool {
        matches!(
            fs::read_to_string(self.dir.join("metadata.json")),
            Ok(existing) if existing == self.metadata_json()
        ) && self.dir.join("layout").is_dir()
    }

    fn metadata_json(&self) -> String {
        format!("{{\"schema\":1,\"key\":\"{}\"}}", self.key)
    }
}

fn replace_dir(stage: &Path, target: &Path) -> io::Result<()> {
    match fs::rename(stage, target) {
        Ok(()) => Ok(()),
        Err(error) if is_existing_dir_error(&error) => {
            remove_dir_if_exists(target)?;
            match fs::rename(stage, target) {
                Ok(()) => Ok(()),
                Err(error) if is_existing_dir_error(&error) => {
                    let _ = fs::remove_dir_all(stage);
                    Ok(())
                }
                Err(error) => {
                    let _ = fs::remove_dir_all(stage);
                    Err(error)
                }
            }
        }
        Err(error) => {
            let _ = fs::remove_dir_all(stage);
            Err(error)
        }
    }
}

fn remove_dir_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn is_existing_dir_error(error: &io::Error) -> bool {
    matches!(error.kind(), io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty)
}

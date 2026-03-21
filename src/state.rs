use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::cli::Opt;
use crate::layout::Layout;

#[derive(Serialize, Deserialize)]
pub struct JobState {
    pub total_size_bytes: u64,
    pub file_size_bytes: u64,
    pub num_directories: usize,
    pub files_per_directory: usize,
    /// `completed_files[dir][file]` — true once the file has been fully written
    pub completed_files: Vec<Vec<bool>>,
}

impl JobState {
    fn state_path(base_dir: &Path) -> PathBuf {
        base_dir.join("job_state.json")
    }

    pub fn load_or_create(opt: &Opt, layout: &Layout) -> Result<Self> {
        let path = Self::state_path(&opt.base_dir);

        if opt.resume && path.exists() {
            let file = File::open(&path)
                .with_context(|| format!("Cannot open state file {}", path.display()))?;
            let state: Self =
                serde_json::from_reader(file).context("Cannot parse state file")?;

            if state.total_size_bytes != layout.total_size_bytes
                || state.file_size_bytes != layout.file_size_bytes
                || state.num_directories != opt.num_directories
                || state.files_per_directory != layout.files_per_directory
            {
                anyhow::bail!(
                    "Resume failed: parameters differ from the previous run.\n\
                     Previous: {} dirs, {} files/dir, {} bytes/file\n\
                     Current:  {} dirs, {} files/dir, {} bytes/file",
                    state.num_directories,
                    state.files_per_directory,
                    state.file_size_bytes,
                    opt.num_directories,
                    layout.files_per_directory,
                    layout.file_size_bytes,
                );
            }

            println!("Resuming previous run from {}", path.display());
            Ok(state)
        } else {
            Ok(Self {
                total_size_bytes: layout.total_size_bytes,
                file_size_bytes: layout.file_size_bytes,
                num_directories: opt.num_directories,
                files_per_directory: layout.files_per_directory,
                completed_files: vec![
                    vec![false; layout.files_per_directory];
                    opt.num_directories
                ],
            })
        }
    }

    /// Atomically persist state: write to a temp file then rename so a crash
    /// during save cannot corrupt the existing state file.
    pub fn save(&self, base_dir: &Path) -> Result<()> {
        let path = Self::state_path(base_dir);
        let tmp = path.with_extension("json.tmp");
        let file = File::create(&tmp)
            .with_context(|| format!("Cannot create temp state file {}", tmp.display()))?;
        serde_json::to_writer(file, self).context("Cannot serialize state")?;
        fs::rename(&tmp, &path).with_context(|| {
            format!("Cannot rename {} → {}", tmp.display(), path.display())
        })?;
        Ok(())
    }
}

/// Shared, mutex-protected completion table used during a parallel run.
/// Each worker marks its file done immediately after a successful write so
/// that an in-progress state save sees the most current picture.
#[derive(Debug)]
pub struct CompletionTable(pub Mutex<Vec<Vec<bool>>>);

impl CompletionTable {
    pub fn from_state(state: &JobState) -> Self {
        Self(Mutex::new(state.completed_files.clone()))
    }

    pub fn mark_done(&self, dir_idx: usize, file_idx: usize) {
        self.0.lock().unwrap()[dir_idx][file_idx] = true;
    }

    pub fn is_done(&self, dir_idx: usize, file_idx: usize) -> bool {
        self.0.lock().unwrap()[dir_idx][file_idx]
    }

    pub fn into_inner(self) -> Vec<Vec<bool>> {
        self.0.into_inner().unwrap()
    }
}

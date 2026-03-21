use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use indicatif::ProgressBar;

use crate::layout::make_buffer;

pub struct DirConfig {
    pub file_size_bytes: u64,
    pub files_per_directory: usize,
    pub random_data: bool,
    pub verify: bool,
    pub io_limit: u64,
}

/// Write (and optionally verify) a single file.
///
/// `buffer` is the per-thread write buffer — it is *not* shared across
/// threads, so each thread generates distinct random content if `--random-data`
/// is set.
pub fn write_file(
    path: &Path,
    buffer: &[u8],
    file_size_bytes: u64,
    verify: bool,
    io_limit_mbs: u64,
) -> Result<()> {
    // ── Write ────────────────────────────────────────────────────────────────
    {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("Cannot create {}", path.display()))?;

        let mut writer = BufWriter::new(file);
        let chunk = buffer.len();
        let mut written: u64 = 0;
        let t0 = Instant::now();

        while written < file_size_bytes {
            let n = chunk.min(
                usize::try_from(file_size_bytes - written).unwrap_or(usize::MAX),
            );
            writer
                .write_all(&buffer[..n])
                .with_context(|| format!("Write error on {}", path.display()))?;
            written += n as u64;

            if io_limit_mbs > 0 {
                let target_ms = (written * 1_000) / (io_limit_mbs * 1024 * 1024);
                let elapsed_ms =
                    u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
                if elapsed_ms < target_ms {
                    std::thread::sleep(Duration::from_millis(target_ms - elapsed_ms));
                }
            }
        }

        writer
            .flush()
            .with_context(|| format!("Flush error on {}", path.display()))?;
    } // file is closed here before verification

    // ── Verify ───────────────────────────────────────────────────────────────
    if verify {
        let mut file = File::open(path)
            .with_context(|| format!("Cannot open {} for verification", path.display()))?;

        let chunk = buffer.len();
        let mut verify_buf = vec![0u8; chunk];
        let mut offset: u64 = 0;

        while offset < file_size_bytes {
            let n = chunk.min(
                usize::try_from(file_size_bytes - offset).unwrap_or(usize::MAX),
            );
            file.seek(SeekFrom::Start(offset))
                .with_context(|| format!("Seek failed in {}", path.display()))?;

            let read = file
                .read(&mut verify_buf[..n])
                .with_context(|| format!("Read error in {}", path.display()))?;

            if read != n {
                anyhow::bail!(
                    "Verification failed: unexpected EOF in {} at offset {offset}",
                    path.display()
                );
            }
            if verify_buf[..n] != buffer[..n] {
                anyhow::bail!(
                    "Verification failed: data mismatch in {} at offset {offset}",
                    path.display()
                );
            }
            offset += n as u64;
        }
    }

    Ok(())
}

/// Process one directory.  Returns the updated per-file completion vector.
///
/// Each file that succeeds is marked `true`; failures are collected and
/// returned so the caller can report them and a subsequent `--resume` can
/// retry. The overall progress counter is incremented atomically after every
/// completed file so the top-level bar stays accurate in real time.
pub fn process_directory(
    dir_path: &Path,
    config: &DirConfig,
    dir_bar: &ProgressBar,
    overall_bar: &ProgressBar,
    overall_written: &AtomicU64,
    mut completed: Vec<bool>,
) -> (Vec<bool>, Vec<anyhow::Error>) {
    fs::create_dir_all(dir_path)
        .with_context(|| format!("Cannot create {}", dir_path.display()))
        .expect("directory creation failed");

    // Per-thread buffer — generated fresh per directory worker so random
    // files differ across directories.
    let buf_size = usize::try_from(config.file_size_bytes)
        .unwrap_or(usize::MAX)
        .min(1024 * 1024);
    let buffer = make_buffer(buf_size, config.random_data);

    let mut errors = Vec::new();

    for (idx, done) in completed.iter_mut().enumerate().take(config.files_per_directory) {
        if *done {
            dir_bar.inc(1);
            // overall bar was already pre-advanced for already-done files
            continue;
        }

        let file_path = dir_path.join(format!("file_{}", idx + 1));

        match write_file(
            &file_path,
            &buffer,
            config.file_size_bytes,
            config.verify,
            config.io_limit,
        ) {
            Ok(()) => {
                *done = true;
                overall_written.fetch_add(config.file_size_bytes, Ordering::Relaxed);
                dir_bar.inc(1);
                overall_bar.inc(1);
            }
            Err(e) => {
                errors.push(e);
                dir_bar.inc(1); // still advance so the bar doesn't stall
            }
        }
    }

    dir_bar.finish();
    (completed, errors)
}

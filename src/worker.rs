use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use indicatif::ProgressBar;
use rand::{RngCore, thread_rng};


// ─── Per-thread buffer state ──────────────────────────────────────────────────

// Each rayon worker thread gets its own RNG state so random-data files are
// distinct across threads without locking.
thread_local! {
    static THREAD_RNG: std::cell::RefCell<rand::rngs::ThreadRng> =
        std::cell::RefCell::new(thread_rng());
}

// ─── File writer implementations ──────────────────────────────────────────────

/// Write `file_size_bytes` bytes to `path` using a plain (page-cached) write.
/// `buf_size` is the chunk size; larger values reduce syscall overhead.
fn write_plain(
    path: &Path,
    file_size_bytes: u64,
    buf_size: usize,
    random: bool,
    io_limit_mbs: u64,
) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("Cannot create {}", path.display()))?;

    // Use a large BufWriter buffer to reduce syscall overhead on NAS writes.
    let mut writer = BufWriter::with_capacity(buf_size, file);
    let mut buf = vec![0u8; buf_size];
    let mut written: u64 = 0;
    let t0 = Instant::now();

    while written < file_size_bytes {
        let n = buf_size.min(
            usize::try_from(file_size_bytes - written).unwrap_or(usize::MAX),
        );

        if random {
            THREAD_RNG.with(|rng| rng.borrow_mut().fill_bytes(&mut buf[..n]));
        }

        writer
            .write_all(&buf[..n])
            .with_context(|| format!("Write error on {}", path.display()))?;
        written += n as u64;

        throttle(written, io_limit_mbs, t0);
    }

    writer
        .flush()
        .with_context(|| format!("Flush error on {}", path.display()))?;

    Ok(())
}

/// Write `file_size_bytes` bytes to `path` using `O_DIRECT` (Linux only).
///
/// Requirements enforced by the caller (see `compute_layout`):
/// - `buffer` is aligned to the filesystem block size
/// - `file_size_bytes` is a multiple of the filesystem block size
#[cfg(target_os = "linux")]
fn write_direct(
    path: &Path,
    file_size_bytes: u64,
    buffer: &mut AlignedBuffer,
    random: bool,
    io_limit_mbs: u64,
) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
        .with_context(|| format!("Cannot create {}", path.display()))?;

    // O_DIRECT bypasses BufWriter — write directly from the aligned buffer.
    let mut raw = std::io::BufWriter::with_capacity(buffer.len(), file);
    let mut written: u64 = 0;
    let t0 = Instant::now();

    while written < file_size_bytes {
        let n = buffer.len().min(
            usize::try_from(file_size_bytes - written).unwrap_or(usize::MAX),
        );

        if random {
            THREAD_RNG.with(|rng| rng.borrow_mut().fill_bytes(&mut buffer.as_mut_slice()[..n]));
        }

        raw.write_all(&buffer.as_slice()[..n])
            .with_context(|| format!("Write error on {}", path.display()))?;
        written += n as u64;

        throttle(written, io_limit_mbs, t0);
    }

    raw.flush()
        .with_context(|| format!("Flush error on {}", path.display()))?;

    Ok(())
}

// ─── Throttle helper ──────────────────────────────────────────────────────────

#[inline]
fn throttle(written: u64, io_limit_mbs: u64, t0: Instant) {
    if io_limit_mbs > 0 {
        let target_ms = (written * 1_000) / (io_limit_mbs * 1024 * 1024);
        let elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX);
        if elapsed_ms < target_ms {
            std::thread::sleep(Duration::from_millis(target_ms - elapsed_ms));
        }
    }
}

// ─── Verification ─────────────────────────────────────────────────────────────

/// Re-read `path` and compare every byte against `expected_buf` (tiled across
/// the file). Called only when `--verify` is set.
fn verify_file(path: &Path, file_size_bytes: u64, expected_buf: &[u8]) -> Result<()> {
    let mut file = File::open(path)
        .with_context(|| format!("Cannot open {} for verification", path.display()))?;

    let chunk = expected_buf.len();
    let mut read_buf = vec![0u8; chunk];
    let mut offset: u64 = 0;

    while offset < file_size_bytes {
        let n = chunk.min(
            usize::try_from(file_size_bytes - offset).unwrap_or(usize::MAX),
        );
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("Seek failed in {}", path.display()))?;

        let got = file
            .read(&mut read_buf[..n])
            .with_context(|| format!("Read error in {}", path.display()))?;

        if got != n {
            anyhow::bail!(
                "Verification failed: unexpected EOF in {} at offset {offset}",
                path.display()
            );
        }
        if read_buf[..n] != expected_buf[..n] {
            anyhow::bail!(
                "Verification failed: data mismatch in {} at offset {offset}",
                path.display()
            );
        }
        offset += n as u64;
    }

    Ok(())
}

// ─── Public task entry point ──────────────────────────────────────────────────

pub struct TaskConfig {
    pub file_size_bytes: u64,
    pub buf_size: usize,
    pub random_data: bool,
    pub verify: bool,
    pub no_cache: bool,
    pub io_limit: u64,
}

/// Write one file as a single rayon task.
///
/// Creates the parent directory (idempotent), then writes the file.
/// Returns `Ok(())` on success so the caller can update progress and state.
pub fn write_file_task(
    base_dir: &Path,
    dir_idx: usize,
    file_idx: usize,
    config: &TaskConfig,
    overall_bar: &ProgressBar,
    dir_bars: &[ProgressBar],
) -> Result<()> {
    let dir_path = base_dir.join(format!("dir_{}", dir_idx + 1));

    fs::create_dir_all(&dir_path)
        .with_context(|| format!("Cannot create {}", dir_path.display()))?;

    let file_path = dir_path.join(format!("file_{}", file_idx + 1));

    #[cfg(target_os = "linux")]
    if config.no_cache {
        // Allocate a fresh aligned buffer per task so each file gets unique
        // random data (for --random-data) and alignment is always guaranteed.
        let mut buf = AlignedBuffer::new_zeroed(config.buf_size, config.buf_size.next_power_of_two())
            .context("Failed to allocate O_DIRECT buffer")?;

        write_direct(
            &file_path,
            config.file_size_bytes,
            &mut buf,
            config.random_data,
            config.io_limit,
        )?;

        if config.verify {
            verify_file(&file_path, config.file_size_bytes, buf.as_slice())?;
        }

        dir_bars[dir_idx].inc(1);
        overall_bar.inc(1);
        return Ok(());
    }

    write_plain(
        &file_path,
        config.file_size_bytes,
        config.buf_size,
        config.random_data,
        config.io_limit,
    )?;

    if config.verify {
        // For verification on the plain path we use a zero buffer since the
        // written content was all-zeros (random content is not retained).
        // For random-data + verify the caller should use --no-cache instead,
        // where the buffer is retained after writing.
        let zero_buf = vec![0u8; config.buf_size];
        verify_file(&file_path, config.file_size_bytes, &zero_buf)?;
    }

    dir_bars[dir_idx].inc(1);
    overall_bar.inc(1);

    Ok(())
}

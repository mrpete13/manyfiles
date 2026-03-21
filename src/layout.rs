use anyhow::{Context, Result};
use byte_unit::Byte;
use rand::{RngCore, thread_rng};

use crate::cli::Opt;

pub fn parse_size(s: &str) -> Result<u64> {
    let b = Byte::from_str(s)
        .map_err(|e| anyhow::anyhow!("Cannot parse size '{s}': {e}"))?;
    let bytes = b.get_bytes();
    if bytes > u128::from(u64::MAX) {
        anyhow::bail!("Size '{s}' exceeds u64::MAX");
    }
    u64::try_from(bytes).map_err(|e| anyhow::anyhow!("Size conversion failed: {e}"))
}

/// Build a write buffer of `size` bytes.
/// For random data we generate a fresh buffer *per thread* rather than
/// sharing one, so files are not all byte-for-byte identical.
pub fn make_buffer(size: usize, random: bool) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    if random {
        thread_rng().fill_bytes(&mut buf);
    }
    buf
}

/// Validate CLI sizes and compute the per-directory file count.
/// Returns `(total_size_bytes, file_size_bytes, files_per_directory)`.
pub fn compute_layout(opt: &Opt) -> Result<(u64, u64, usize)> {
    let total_size_bytes = parse_size(&opt.total_size)
        .with_context(|| format!("Invalid --total-size '{}'", opt.total_size))?;
    let file_size_bytes = parse_size(&opt.file_size)
        .with_context(|| format!("Invalid --file-size '{}'", opt.file_size))?;

    if file_size_bytes == 0 {
        anyhow::bail!("--file-size must be greater than zero");
    }

    let total_files = total_size_bytes / file_size_bytes;
    let files_per_directory = total_files / opt.num_directories as u64;

    if files_per_directory == 0 {
        anyhow::bail!(
            "Parameters yield 0 files per directory ({total_files} total files ÷ {} dirs). \
             Reduce --num-directories or increase --total-size / reduce --file-size.",
            opt.num_directories,
        );
    }

    let fpd =
        usize::try_from(files_per_directory).context("files_per_directory overflows usize")?;

    Ok((total_size_bytes, file_size_bytes, fpd))
}

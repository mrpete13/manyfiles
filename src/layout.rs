use anyhow::{Context, Result};
use byte_unit::Byte;

use crate::cli::Opt;

// ─── Size parsing ─────────────────────────────────────────────────────────────

pub fn parse_size(s: &str) -> Result<u64> {
    let b = Byte::from_str(s)
        .map_err(|e| anyhow::anyhow!("Cannot parse size '{s}': {e}"))?;
    let bytes = b.get_bytes();
    if bytes > u128::from(u64::MAX) {
        anyhow::bail!("Size '{s}' exceeds u64::MAX");
    }
    u64::try_from(bytes).map_err(|e| anyhow::anyhow!("Size conversion failed: {e}"))
}

// ─── Block size detection ─────────────────────────────────────────────────────

/// Returns the preferred I/O block size for the filesystem at `path`.
///
/// Uses `statfs(2)` rather than `ioctl(BLKSSZGET)` because `BLKSSZGET` only
/// works on block devices — NFS/SMB mounts are not block devices.
/// `statfs::f_bsize` is the filesystem-reported optimal transfer size and
/// works correctly on both local and network filesystems.
///
/// Only available on Linux; calling this on other platforms is a compile error.
#[cfg(target_os = "linux")]
pub fn detect_block_size(path: &std::path::Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .context("Path contains null byte")?;

    // SAFETY: statfs is always safe to call with a valid path and a properly
    // sized output buffer initialised to zero.
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statfs(c_path.as_ptr(), &raw mut stat) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("statfs failed on {}", path.display()));
    }

    // f_bsize is i64 on Linux
    let bsize = u64::try_from(stat.f_bsize)
        .context("statfs f_bsize is negative")?;

    // Guard against degenerate values (e.g. some FUSE filesystems report 0 or 1)
    if bsize < 512 || !bsize.is_power_of_two() {
        return Ok(4096);
    }

    Ok(bsize)
}

// ─── Buffer allocation ────────────────────────────────────────────────────────

/// An owned, heap-allocated buffer guaranteed to be aligned to `align` bytes.
///
/// Required for `O_DIRECT` I/O, which demands that both the buffer address and
/// the transfer length are multiples of the filesystem block size.
pub struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
    align: usize,
}

// SAFETY: AlignedBuffer owns its allocation exclusively and never aliases it.
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Allocate a zeroed buffer of `len` bytes aligned to `align` bytes.
    /// `align` must be a power of two and at least `size_of::<usize>()`.
    pub fn new_zeroed(len: usize, align: usize) -> Result<Self> {
        use std::alloc::{Layout, alloc_zeroed};

        let layout = Layout::from_size_align(len, align)
            .context("Invalid buffer layout")?;

        // SAFETY: layout is valid (checked above); we check for null.
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            anyhow::bail!("Failed to allocate {len}-byte aligned buffer");
        }

        Ok(Self { ptr, len, align })
    }

    pub const fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for len bytes and we hold exclusive ownership.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub const fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for len bytes and we hold exclusive ownership.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub const fn len(&self) -> usize {
        self.len
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        use std::alloc::{Layout, dealloc};
        // SAFETY: ptr and align are the same values used in the original alloc.
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.len, self.align);
            dealloc(self.ptr, layout);
        }
    }
}

// ─── Layout computation ───────────────────────────────────────────────────────

pub struct Layout {
    pub total_size_bytes: u64,
    pub file_size_bytes: u64,
    pub files_per_directory: usize,
    /// Alignment required for `O_DIRECT` (1 when `O_DIRECT` is not in use)
    pub block_size: u64,
}

/// Validate CLI sizes, optionally detect the filesystem block size, and
/// compute the per-directory file count.
pub fn compute_layout(opt: &Opt) -> Result<Layout> {
    let total_size_bytes = parse_size(&opt.total_size)
        .with_context(|| format!("Invalid --total-size '{}'", opt.total_size))?;
    let mut file_size_bytes = parse_size(&opt.file_size)
        .with_context(|| format!("Invalid --file-size '{}'", opt.file_size))?;

    if file_size_bytes == 0 {
        anyhow::bail!("--file-size must be greater than zero");
    }

    // ── O_DIRECT block-size alignment ────────────────────────────────────────
    let block_size = if opt.no_cache {
        #[cfg(not(target_os = "linux"))]
        anyhow::bail!("--no-cache requires Linux");

        #[cfg(target_os = "linux")]
        {
            let bs = detect_block_size(&opt.base_dir)?;
            println!("Detected block size: {bs} bytes");

            // Round file_size down to a multiple of block_size
            let aligned = (file_size_bytes / bs) * bs;
            if aligned == 0 {
                anyhow::bail!(
                    "--file-size ({file_size_bytes} bytes) is smaller than \
                     the filesystem block size ({bs} bytes)"
                );
            }
            if aligned != file_size_bytes {
                eprintln!(
                    "Warning: --file-size rounded down from {file_size_bytes} \
                     to {aligned} bytes to satisfy O_DIRECT alignment"
                );
                file_size_bytes = aligned;
            }
            bs
        }
    } else {
        1
    };

    let total_files = total_size_bytes / file_size_bytes;
    let files_per_directory = total_files / opt.num_directories as u64;

    if files_per_directory == 0 {
        anyhow::bail!(
            "Parameters yield 0 files per directory ({total_files} total files \
             ÷ {} dirs). Reduce --num-directories or increase --total-size / \
             reduce --file-size.",
            opt.num_directories,
        );
    }

    let files_per_directory = usize::try_from(files_per_directory)
        .context("files_per_directory overflows usize")?;

    Ok(Layout {
        total_size_bytes,
        file_size_bytes,
        files_per_directory,
        block_size,
    })
}

use std::path::PathBuf;

use clap::Parser;

#[allow(clippy::struct_excessive_bools)]
#[derive(Parser, Debug)]
#[command(
    name = "many_files",
    about = "Create many files across multiple directories",
    version
)]
pub struct Opt {
    /// Total size to write (e.g. "15GiB", "10GB", "1500MiB")
    #[arg(short, long, default_value = "15GiB")]
    pub total_size: String,

    /// Size of each individual file (e.g. "256KiB", "1MB")
    #[arg(short, long, default_value = "256KiB")]
    pub file_size: String,

    /// Number of directories to spread files across
    #[arg(short, long, default_value = "5")]
    pub num_directories: usize,

    /// Parallel worker threads (0 = one per logical CPU)
    #[arg(short = 'j', long, default_value = "0")]
    pub num_parallel_jobs: usize,

    /// Base directory under which subdirectories are created
    #[arg(short, long, default_value = "/tmp/dirs")]
    pub base_dir: PathBuf,

    /// Fill files with random data instead of zeros
    #[arg(short = 'R', long)]
    pub random_data: bool,

    /// Verify written data after each file completes
    #[arg(short, long)]
    pub verify: bool,

    /// Bypass the page cache using `O_DIRECT` (Linux only).
    /// File size and buffer size are rounded down to the filesystem block size.
    #[arg(long)]
    pub no_cache: bool,

    /// Save job state to disk so a run can be resumed later (see --resume)
    #[arg(long)]
    pub save_state: bool,

    /// Resume a previous run. Implies --save-state.
    #[arg(short, long)]
    pub resume: bool,

    /// Per-thread I/O throttle in MB/s (0 = unlimited)
    #[arg(long, default_value = "0")]
    pub io_limit: u64,
}

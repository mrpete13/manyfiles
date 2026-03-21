use anyhow::{Context, Result};
use byte_unit::Byte;
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rand::{RngCore, thread_rng};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "many_files",
    about = "Create many files across multiple directories",
    version
)]
struct Opt {
    /// Total size to write (e.g. "15GiB", "10GB", "1500MiB")
    #[arg(short, long, default_value = "15GiB")]
    total_size: String,

    /// Size of each individual file (e.g. "256KiB", "1MB")
    #[arg(short, long, default_value = "256KiB")]
    file_size: String,

    /// Number of directories to spread files across
    #[arg(short, long, default_value = "5")]
    num_directories: usize,

    /// Parallel worker threads (0 = one per logical CPU)
    #[arg(short = 'j', long, default_value = "0")]
    num_parallel_jobs: usize,

    /// Base directory under which subdirectories are created
    #[arg(short, long, default_value = "/tmp/dirs")]
    base_dir: PathBuf,

    /// Fill files with random data instead of zeros
    #[arg(short = 'R', long)]
    random_data: bool,

    /// Verify written data after each file completes
    #[arg(short, long)]
    verify: bool,

    /// Resume a previous interrupted run
    #[arg(short, long)]
    resume: bool,

    /// Per-thread I/O throttle in MB/s (0 = unlimited)
    #[arg(long, default_value = "0")]
    io_limit: u64,
}

// ─── RESUME STATE ────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct JobState {
    total_size_bytes: u64,
    file_size_bytes: u64,
    num_directories: usize,
    files_per_directory: usize,
    /// `completed_files[dir][file]` — true once the file has been fully written
    completed_files: Vec<Vec<bool>>,
}

impl JobState {
    fn state_path(base_dir: &Path) -> PathBuf {
        base_dir.join("job_state.json")
    }

    fn load_or_create(
        opt: &Opt,
        total_size_bytes: u64,
        file_size_bytes: u64,
        files_per_directory: usize,
    ) -> Result<Self> {
        let path = Self::state_path(&opt.base_dir);

        if opt.resume && path.exists() {
            let file = File::open(&path)
                .with_context(|| format!("Cannot open state file {}", path.display()))?;
            let state: Self =
                serde_json::from_reader(file).context("Cannot parse state file")?;

            if state.total_size_bytes != total_size_bytes
                || state.file_size_bytes != file_size_bytes
                || state.num_directories != opt.num_directories
                || state.files_per_directory != files_per_directory
            {
                anyhow::bail!(
                    "Resume failed: parameters differ from the previous run.\n\
                     Previous: {} dirs, {} files/dir, {} bytes/file\n\
                     Current:  {} dirs, {} files/dir, {} bytes/file",
                    state.num_directories,
                    state.files_per_directory,
                    state.file_size_bytes,
                    opt.num_directories,
                    files_per_directory,
                    file_size_bytes,
                );
            }

            println!("Resuming previous run from {}", path.display());
            Ok(state)
        } else {
            Ok(Self {
                total_size_bytes,
                file_size_bytes,
                num_directories: opt.num_directories,
                files_per_directory,
                completed_files: vec![vec![false; files_per_directory]; opt.num_directories],
            })
        }
    }

    fn save(&self, base_dir: &Path) -> Result<()> {
        let path = Self::state_path(base_dir);
        // Write to a temp file first, then rename — avoids a torn write
        // leaving the state file unreadable on a crash.
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

// ─── HELPERS ─────────────────────────────────────────────────────────────────

fn parse_size(s: &str) -> Result<u64> {
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
fn make_buffer(size: usize, random: bool) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    if random {
        thread_rng().fill_bytes(&mut buf);
    }
    buf
}

fn progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .unwrap()
        .progress_chars("##-")
}

// ─── CORE FILE WRITE ─────────────────────────────────────────────────────────

/// Write (and optionally verify) a single file.
///
/// `buffer` is the per-thread write buffer — it is *not* shared across
/// threads, so each thread generates distinct random content if `--random-data`
/// is set.
fn write_file(
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
    } // file is closed here before we verify

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

// ─── DIRECTORY WORKER ────────────────────────────────────────────────────────

struct DirConfig {
    file_size_bytes: u64,
    files_per_directory: usize,
    random_data: bool,
    verify: bool,
    io_limit: u64,
}

/// Process one directory.  Returns the updated per-file completion vector.
///
/// Each file that succeeds is marked `true`; failures are reported and the
/// slot stays `false` so a resume can retry.  The overall progress counter
/// (`overall_written`) is incremented atomically after every completed file
/// so the top-level bar stays accurate in real time.
fn process_directory(
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
            // overall bar was already advanced when state was loaded
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

// ─── SETUP HELPERS ───────────────────────────────────────────────────────────

fn compute_layout(opt: &Opt) -> Result<(u64, u64, usize)> {
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

fn build_progress_bars(
    multi: &MultiProgress,
    style: &ProgressStyle,
    state: &JobState,
    files_per_directory: usize,
) -> (ProgressBar, Vec<ProgressBar>) {
    // enable_steady_tick guarantees redraws at a fixed interval regardless of
    // how fast (or slow) the worker threads call inc(). Without it, a bar that
    // isn't incremented for a while will appear frozen.
    let tick = Duration::from_millis(100);

    let total_file_count =
        u64::try_from(files_per_directory * state.num_directories).unwrap_or(u64::MAX);

    let overall_bar = multi.add(ProgressBar::new(total_file_count));
    overall_bar.set_style(style.clone());
    overall_bar.set_message("overall");
    overall_bar.enable_steady_tick(tick);

    let already_done = u64::try_from(
        state
            .completed_files
            .iter()
            .flat_map(|d| d.iter())
            .filter(|&&v| v)
            .count(),
    )
    .unwrap_or(u64::MAX);
    overall_bar.inc(already_done);

    let dir_bars: Vec<ProgressBar> = (0..state.num_directories)
        .map(|i| {
            let pb = multi.add(ProgressBar::new(
                u64::try_from(files_per_directory).unwrap_or(u64::MAX),
            ));
            pb.set_style(style.clone());
            pb.set_message(format!("dir {}", i + 1));
            pb.enable_steady_tick(tick);
            let done = u64::try_from(
                state.completed_files[i].iter().filter(|&&v| v).count(),
            )
            .unwrap_or(u64::MAX);
            pb.inc(done);
            pb
        })
        .collect();

    (overall_bar, dir_bars)
}

// ─── MAIN ────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let opt = Opt::parse();

    let (total_size_bytes, file_size_bytes, files_per_directory) = compute_layout(&opt)?;

    println!(
        "Plan: {} dirs × {files_per_directory} files = {} files total",
        opt.num_directories,
        opt.num_directories * files_per_directory,
    );
    println!(
        "      {} / file  ·  {} total",
        Byte::from_bytes(u128::from(file_size_bytes)).get_appropriate_unit(true),
        Byte::from_bytes(u128::from(total_size_bytes)).get_appropriate_unit(true),
    );

    fs::create_dir_all(&opt.base_dir)
        .with_context(|| format!("Cannot create base dir {}", opt.base_dir.display()))?;

    let mut state =
        JobState::load_or_create(&opt, total_size_bytes, file_size_bytes, files_per_directory)?;

    // ── Thread pool ──────────────────────────────────────────────────────────
    let thread_count = if opt.num_parallel_jobs > 0 {
        opt.num_parallel_jobs
    } else {
        rayon::current_num_threads()
    };
    println!("Workers: {thread_count}");

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .context("Cannot build thread pool")?;

    // ── Progress bars ────────────────────────────────────────────────────────
    // All println! calls must happen before MultiProgress::new(). Anything
    // written to stderr after this point races with indicatif and causes
    // duplicate or garbled bar output.
    let multi = MultiProgress::new();
    let style = progress_style();
    let (overall_bar, dir_bars) =
        build_progress_bars(&multi, &style, &state, files_per_directory);

    let overall_written = Arc::new(AtomicU64::new(0));
    let overall_written_ref = overall_written.clone();

    // ── Parallel execution ───────────────────────────────────────────────────
    // MultiProgress in 0.17.x uses internal shared state — bars render
    // themselves via their steady-tick threads. No separate draw thread needed.
    let start = Instant::now();

    let results: Vec<(Vec<bool>, Vec<anyhow::Error>)> = pool.install(|| {
        (0..opt.num_directories)
            .into_par_iter()
            .map(|dir_idx| {
                let dir_path = opt.base_dir.join(format!("dir_{}", dir_idx + 1));
                let config = DirConfig {
                    file_size_bytes,
                    files_per_directory,
                    random_data: opt.random_data,
                    verify: opt.verify,
                    io_limit: opt.io_limit,
                };
                let completed = state.completed_files[dir_idx].clone();
                process_directory(
                    &dir_path,
                    &config,
                    &dir_bars[dir_idx],
                    &overall_bar,
                    &overall_written_ref,
                    completed,
                )
            })
            .collect()
    });

    // ── Collect results + save state ─────────────────────────────────────────
    let mut any_error = false;
    for (dir_idx, (completed, errors)) in results.into_iter().enumerate() {
        state.completed_files[dir_idx] = completed;
        for e in errors {
            eprintln!("Error in dir {}: {e:#}", dir_idx + 1);
            any_error = true;
        }
    }

    if let Err(e) = state.save(&opt.base_dir) {
        eprintln!("Warning: failed to save job state: {e:#}");
    }

    // ── Summary ──────────────────────────────────────────────────────────────
    let elapsed = start.elapsed();
    let bytes_written = overall_written.load(Ordering::Relaxed);
    #[allow(clippy::cast_precision_loss)]
    let mib_written = bytes_written as f64 / (1024.0 * 1024.0);
    let secs = elapsed.as_secs_f64();
    let mib_per_sec = if secs > 0.0 { mib_written / secs } else { 0.0 };

    overall_bar.finish_with_message(format!("done in {secs:.2}s"));

    println!("\n── Summary ─────────────────────────────────────────────");
    println!("  Dirs:          {}", opt.num_directories);
    println!("  Files/dir:     {files_per_directory}");
    println!("  Written:       {mib_written:.2} MiB");
    println!("  Elapsed:       {secs:.2}s");
    println!("  Throughput:    {mib_per_sec:.2} MiB/s");

    if any_error {
        eprintln!("\nSome files failed. Re-run with --resume to retry them.");
        std::process::exit(1);
    }

    Ok(())
}

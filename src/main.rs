mod cli;
mod layout;
mod progress;
mod state;
mod worker;

use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use byte_unit::Byte;
use clap::Parser;
use indicatif::MultiProgress;
use rayon::prelude::*;

use cli::Opt;
use layout::{Layout, compute_layout};
use progress::{build_progress_bars, progress_style};
use state::{CompletionTable, JobState};
use worker::{TaskConfig, write_file_task};

// ─── Setup helpers ────────────────────────────────────────────────────────────

/// Derive the write-buffer size from the layout.
///
/// For `O_DIRECT` the buffer must be a multiple of the block size, so we round
/// up to at least 512 KiB while keeping the result block-aligned and ≤ 1 MiB.
/// For plain (page-cached) writes 512 KiB is large enough to keep NAS
/// throughput high without excessive per-thread memory use.
fn buf_size(layout: &Layout) -> usize {
    if layout.block_size > 1 {
        let bs = usize::try_from(layout.block_size).unwrap_or(4096);
        (512 * 1024_usize).div_ceil(bs).saturating_mul(bs).min(1024 * 1024)
    } else {
        512 * 1024
    }
}

/// Compute how many files are already done globally and per-directory.
/// Returns `(total_done, per_dir_done)` as `u64` counts ready for progress bars.
fn resume_counts(job_state: Option<&JobState>, num_directories: usize) -> (u64, Vec<u64>) {
    job_state.map_or_else(
        || (0, vec![0; num_directories]),
        |s| {
            let total = s.completed_files.iter().flat_map(|d| d.iter()).filter(|&&v| v).count();
            let per_dir = s
                .completed_files
                .iter()
                .map(|d| u64::try_from(d.iter().filter(|&&v| v).count()).unwrap_or(u64::MAX))
                .collect();
            (u64::try_from(total).unwrap_or(u64::MAX), per_dir)
        },
    )
}

/// Flush and persist job state after the parallel run completes.
fn save_state(opt: &Opt, job_state: Option<JobState>, completion: Option<Arc<CompletionTable>>) {
    if let (Some(mut state), Some(table)) = (job_state, completion) {
        state.completed_files = Arc::try_unwrap(table)
            .expect("completion table still borrowed")
            .into_inner();
        if let Err(e) = state.save(&opt.base_dir) {
            eprintln!("Warning: failed to save job state: {e:#}");
        }
    }
}

// ─── Parallel execution ───────────────────────────────────────────────────────

/// Shared handles needed by every parallel file task.
struct RunContext<'a> {
    opt: &'a Opt,
    layout: &'a Layout,
    task_config: &'a Arc<TaskConfig>,
    completion: Option<&'a Arc<CompletionTable>>,
    overall_bar: &'a Arc<indicatif::ProgressBar>,
    dir_bars: &'a Arc<Vec<indicatif::ProgressBar>>,
    overall_written: Arc<AtomicU64>,
}

fn run_parallel(
    pool: &rayon::ThreadPool,
    ctx: &RunContext<'_>,
) -> Vec<(usize, usize, anyhow::Error)> {
    // Collect all (dir, file) pairs into a Vec so rayon can use work-stealing
    // across the full task set. par_bridge() serializes the source iterator on
    // a single thread and hands off tasks one at a time, which bottlenecks
    // throughput when tasks are small and fast.
    let tasks: Vec<(usize, usize)> = (0..ctx.opt.num_directories)
        .flat_map(|d| (0..ctx.layout.files_per_directory).map(move |f| (d, f)))
        .filter(|&(dir_idx, file_idx)| {
            !ctx.completion.is_some_and(|c| c.is_done(dir_idx, file_idx))
        })
        .collect();

    pool.install(|| {
        tasks
            .into_par_iter()
            .filter_map(|(dir_idx, file_idx)| {
                match write_file_task(
                    &ctx.opt.base_dir,
                    dir_idx,
                    file_idx,
                    ctx.task_config,
                    ctx.overall_bar,
                    ctx.dir_bars,
                ) {
                    Ok(()) => {
                        ctx.overall_written.fetch_add(ctx.layout.file_size_bytes, Ordering::Relaxed);
                        if let Some(c) = ctx.completion {
                            c.mark_done(dir_idx, file_idx);
                        }
                        None
                    }
                    Err(e) => Some((dir_idx, file_idx, e)),
                }
            })
            .collect()
    })
}

// ─── Summary output ───────────────────────────────────────────────────────────

fn print_summary(
    overall_bar: &indicatif::ProgressBar,
    elapsed: std::time::Duration,
    bytes_written: u64,
    opt: &Opt,
    layout: &Layout,
    errors: &[(usize, usize, anyhow::Error)],
) {
    #[allow(clippy::cast_precision_loss)]
    let mib_written = bytes_written as f64 / (1024.0 * 1024.0);
    let secs = elapsed.as_secs_f64();
    let mib_per_sec = if secs > 0.0 { mib_written / secs } else { 0.0 };

    overall_bar.finish_with_message(format!("done in {secs:.2}s"));

    println!("\n── Summary ─────────────────────────────────────────────");
    println!("  Dirs:          {}", opt.num_directories);
    println!("  Files/dir:     {}", layout.files_per_directory);
    println!("  Written:       {mib_written:.2} MiB");
    println!("  Elapsed:       {secs:.2}s");
    println!("  Throughput:    {mib_per_sec:.2} MiB/s");

    if !errors.is_empty() {
        eprintln!("\n{} file(s) failed:", errors.len());
        for (dir_idx, file_idx, e) in errors {
            eprintln!("  dir_{}/file_{}: {e:#}", dir_idx + 1, file_idx + 1);
        }
        if opt.save_state {
            eprintln!("Re-run with --resume to retry failed files.");
        }
        std::process::exit(1);
    }
}

// ─── Entry point ──────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let mut opt = Opt::parse();
    if opt.resume { opt.save_state = true; } // resume implies save

    let layout = compute_layout(&opt)?;

    println!(
        "Plan: {} dirs × {} files = {} files total",
        opt.num_directories,
        layout.files_per_directory,
        opt.num_directories * layout.files_per_directory,
    );
    println!(
        "      {} / file  ·  {} total",
        Byte::from_bytes(u128::from(layout.file_size_bytes)).get_appropriate_unit(true),
        Byte::from_bytes(u128::from(layout.total_size_bytes)).get_appropriate_unit(true),
    );

    fs::create_dir_all(&opt.base_dir)
        .with_context(|| format!("Cannot create base dir {}", opt.base_dir.display()))?;

    let job_state = opt.save_state
        .then(|| JobState::load_or_create(&opt, &layout))
        .transpose()?;
    let completion = job_state.as_ref().map(|s| Arc::new(CompletionTable::from_state(s)));

    let thread_count = if opt.num_parallel_jobs > 0 { opt.num_parallel_jobs } else { rayon::current_num_threads() };
    println!("Workers: {thread_count}");

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .context("Cannot build thread pool")?;

    let (already_done, dir_done) = resume_counts(job_state.as_ref(), opt.num_directories);

    // All println! must happen before MultiProgress::new() to avoid
    // duplicate/garbled bar output.
    let multi = MultiProgress::new();
    let style = progress_style();
    let (overall_bar, dir_bars) =
        build_progress_bars(&multi, &style, &layout, opt.num_directories, already_done, &dir_done);

    let overall_written = Arc::new(AtomicU64::new(0));
    let overall_bar = Arc::new(overall_bar);
    let dir_bars = Arc::new(dir_bars);
    let task_config = Arc::new(TaskConfig {
        file_size_bytes: layout.file_size_bytes,
        buf_size: buf_size(&layout),
        random_data: opt.random_data,
        verify: opt.verify,
        no_cache: opt.no_cache,
        io_limit: opt.io_limit,
    });

    let start = std::time::Instant::now();
    let errors = run_parallel(&pool, &RunContext {
        opt: &opt,
        layout: &layout,
        task_config: &task_config,
        completion: completion.as_ref(),
        overall_bar: &overall_bar,
        dir_bars: &dir_bars,
        overall_written: overall_written.clone(),
    });

    for pb in dir_bars.iter() { pb.finish(); }

    if opt.save_state { save_state(&opt, job_state, completion); }

    print_summary(&overall_bar, start.elapsed(), overall_written.load(Ordering::Relaxed), &opt, &layout, &errors);

    Ok(())
}

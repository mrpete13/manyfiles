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
use layout::compute_layout;
use progress::{build_progress_bars, progress_style};
use state::JobState;
use worker::{DirConfig, process_directory};

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
    let start = std::time::Instant::now();

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

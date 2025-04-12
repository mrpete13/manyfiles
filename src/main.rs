use anyhow::{Context, Result};
use byte_unit::Byte;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rand::{RngCore, thread_rng};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "many_files",
    about = "Create many files across multiple directories",
    version = env!("CARGO_PKG_VERSION")
)]
struct Opt {
    /// Total size (e.g., "15GiB", "10GB", "1500MiB")
    #[structopt(short, long, default_value = "15GiB")]
    total_size: String,

    /// Size of each file (e.g., "256KiB", "1MB")
    #[structopt(short, long, default_value = "256KiB")]
    file_size: String,

    /// Number of directories to create
    #[structopt(short, long, default_value = "5")]
    num_directories: usize,

    /// Number of parallel jobs
    #[structopt(short = "j", long, default_value = "0")]
    num_parallel_jobs: usize,

    /// Base directory where subdirectories will be created
    #[structopt(short, long, default_value = "/tmp/dirs")]
    base_dir: PathBuf,

    /// Use random data instead of zeros
    #[structopt(short = "R", long)]
    random_data: bool,

    /// Verify files after writing
    #[structopt(short, long)]
    verify: bool,

    /// Resume a previous run if possible
    #[structopt(short, long)]
    resume: bool,

    /// Throttle I/O to this MB/s per thread (0 = no limit)
    #[structopt(long, default_value = "0")]
    io_limit: u64,
}

#[derive(Serialize, Deserialize)]
struct JobState {
    total_size_bytes: u64,
    file_size_bytes: u64,
    num_directories: usize,
    files_per_directory: usize,
    completed_files: Vec<Vec<bool>>,
}

fn parse_size(size_str: &str) -> Result<u64> {
    let byte = Byte::from_str(size_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse size '{}': {}", size_str, e))?;

    let bytes_u128 = byte.get_bytes();
    if bytes_u128 > u64::MAX as u128 {
        return Err(anyhow::anyhow!("Size exceeds maximum value"));
    }

    Ok(bytes_u128 as u64)
}

fn get_buffer(size: usize, random: bool) -> Vec<u8> {
    let mut buffer = vec![0u8; size];
    if random {
        thread_rng().fill_bytes(&mut buffer);
    }
    buffer
}

fn create_or_load_state(
    opt: &Opt,
    total_size_bytes: u64,
    file_size_bytes: u64,
    files_per_directory: usize,
) -> Result<JobState> {
    let state_path = opt.base_dir.join("job_state.json");

    if opt.resume && state_path.exists() {
        let file = File::open(&state_path)
            .with_context(|| format!("Failed to open state file at {:?}", state_path))?;
        let state: JobState =
            serde_json::from_reader(file).with_context(|| "Failed to parse state file")?;

        // Validate the resumed state matches current parameters
        if state.total_size_bytes != total_size_bytes
            || state.file_size_bytes != file_size_bytes
            || state.num_directories != opt.num_directories
            || state.files_per_directory != files_per_directory
        {
            anyhow::bail!("Cannot resume: job parameters don't match previous run");
        }

        Ok(state)
    } else {
        // Initialize new state
        let completed_files = vec![vec![false; files_per_directory]; opt.num_directories];

        let state = JobState {
            total_size_bytes,
            file_size_bytes,
            num_directories: opt.num_directories,
            files_per_directory,
            completed_files,
        };

        Ok(state)
    }
}

fn save_state(state: &JobState, base_dir: &Path) -> Result<()> {
    let state_path = base_dir.join("job_state.json");
    let file = File::create(&state_path)
        .with_context(|| format!("Failed to create state file at {:?}", state_path))?;
    serde_json::to_writer(file, state).with_context(|| "Failed to write state file")?;
    Ok(())
}

fn create_file(
    dir_path: &Path,
    file_index: usize,
    buffer: &[u8],
    file_size_bytes: u64,
    verify: bool,
    io_limit: u64,
) -> Result<()> {
    let file_path = dir_path.join(format!("file_{}", file_index + 1));

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&file_path)
        .with_context(|| format!("Failed to create file at {:?}", file_path))?;

    let mut writer = BufWriter::new(file);

    let mut bytes_written = 0;
    let chunk_size = buffer.len();
    let start_time = Instant::now();

    while bytes_written < file_size_bytes {
        let bytes_to_write =
            std::cmp::min(chunk_size as u64, file_size_bytes - bytes_written) as usize;

        writer
            .write_all(&buffer[..bytes_to_write])
            .with_context(|| format!("Failed writing to file {:?}", file_path))?;

        bytes_written += bytes_to_write as u64;

        // Apply I/O throttling if requested
        if io_limit > 0 {
            let elapsed_ms = start_time.elapsed().as_millis() as u64;
            let target_ms = (bytes_written * 1000) / (io_limit * 1024 * 1024);

            if elapsed_ms < target_ms {
                std::thread::sleep(Duration::from_millis(target_ms - elapsed_ms));
            }
        }
    }

    writer
        .flush()
        .with_context(|| format!("Failed to flush file {:?}", file_path))?;

    // Verify file contents if requested
    if verify {
        let mut file = File::open(&file_path)
            .with_context(|| format!("Failed to open file for verification: {:?}", file_path))?;

        let mut verify_buf = vec![0u8; chunk_size];
        let mut offset = 0;

        while offset < file_size_bytes {
            let bytes_to_read = std::cmp::min(chunk_size as u64, file_size_bytes - offset) as usize;
            file.seek(SeekFrom::Start(offset))
                .with_context(|| format!("Failed to seek in file {:?}", file_path))?;

            let bytes_read = file
                .read(&mut verify_buf[..bytes_to_read])
                .with_context(|| {
                    format!("Failed to read file for verification: {:?}", file_path)
                })?;

            if bytes_read != bytes_to_read {
                anyhow::bail!("Verification failed: unexpected EOF in {:?}", file_path);
            }

            if &verify_buf[..bytes_read] != &buffer[..bytes_read] {
                anyhow::bail!("Verification failed: data mismatch in {:?}", file_path);
            }

            offset += bytes_read as u64;
        }
    }

    Ok(())
}

fn create_files_in_directory(
    dir_index: usize,
    dir_path: &Path,
    file_size_bytes: u64,
    files_per_directory: usize,
    buffer: Arc<Vec<u8>>,
    progress_bar: ProgressBar,
    completed_files: &mut [bool],
    verify: bool,
    io_limit: u64,
) -> Result<()> {
    // Create the directory if it doesn't exist
    fs::create_dir_all(dir_path)
        .with_context(|| format!("Failed to create directory: {:?}", dir_path))?;

    // Process each file in this directory
    for file_index in 0..files_per_directory {
        // Skip if already completed
        if completed_files[file_index] {
            progress_bar.inc(1);
            continue;
        }

        create_file(
            dir_path,
            file_index,
            &buffer,
            file_size_bytes,
            verify,
            io_limit,
        )?;

        completed_files[file_index] = true;
        progress_bar.inc(1);
    }

    Ok(())
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    // Parse sizes
    let total_size_bytes = parse_size(&opt.total_size)
        .with_context(|| format!("Invalid total size: {}", opt.total_size))?;
    let file_size_bytes = parse_size(&opt.file_size)
        .with_context(|| format!("Invalid file size: {}", opt.file_size))?;

    // Calculate files per directory
    let total_files = total_size_bytes / file_size_bytes;
    let files_per_directory = total_files / opt.num_directories as u64;

    if files_per_directory == 0 {
        anyhow::bail!("Parameters would result in 0 files per directory. Please adjust sizes.");
    }

    println!(
        "Creating {} directories with {} files each",
        opt.num_directories, files_per_directory
    );
    println!(
        "Each file: {}, Total size: {}",
        Byte::from_bytes(file_size_bytes as u128).get_appropriate_unit(true),
        Byte::from_bytes(total_size_bytes as u128).get_appropriate_unit(true)
    );

    // Create base directory
    fs::create_dir_all(&opt.base_dir)
        .with_context(|| format!("Failed to create base directory: {:?}", opt.base_dir))?;

    // Create or load job state
    let mut state = create_or_load_state(
        &opt,
        total_size_bytes,
        file_size_bytes,
        files_per_directory as usize,
    )?;

    // Configure and create progress bars
    let multi_progress = MultiProgress::new();
    let progress_style = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .unwrap()
        .progress_chars("##-");

    let overall_progress = multi_progress.add(ProgressBar::new(
        (files_per_directory * opt.num_directories as u64) as u64,
    ));
    overall_progress.set_style(progress_style.clone());
    overall_progress.set_message("Total progress");

    let dir_progress_bars: Vec<_> = (0..opt.num_directories)
        .map(|i| {
            let pb = multi_progress.add(ProgressBar::new(files_per_directory as u64));
            pb.set_style(progress_style.clone());
            pb.set_message(format!("Directory {}", i + 1));
            pb
        })
        .collect();

    // Count already completed files for progress
    let completed_count: u64 = state
        .completed_files
        .iter()
        .map(|dir| dir.iter().filter(|&&completed| completed).count() as u64)
        .sum();

    overall_progress.inc(completed_count);

    for (i, dir_files) in state.completed_files.iter().enumerate() {
        let completed = dir_files.iter().filter(|&&completed| completed).count() as u64;
        dir_progress_bars[i].inc(completed);
    }

    // Determine thread count
    let thread_count = if opt.num_parallel_jobs > 0 {
        opt.num_parallel_jobs
    } else {
        num_cpus::get()
    };
    println!("Using {} worker threads", thread_count);

    // Create thread pool configuration
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()?;

    // Buffer size based on file size (for efficiency)
    let buffer_size = std::cmp::min(file_size_bytes as usize, 1024 * 1024); // Max 1MB buffer
    let buffer = Arc::new(get_buffer(buffer_size, opt.random_data));

    let start_time = Instant::now();

    // Process directories in parallel
    let dir_results: Vec<_> = (0..opt.num_directories)
        .into_par_iter()
        .map(|dir_index| {
            let buffer = buffer.clone();
            let progress_bar = dir_progress_bars[dir_index].clone();
            let base_dir = opt.base_dir.clone();
            let dir_path = base_dir.join(format!("dir_{}", dir_index + 1));

            // Clone the completion state for this directory
            let mut dir_completed = state.completed_files[dir_index].clone();

            let result = create_files_in_directory(
                dir_index,
                &dir_path,
                file_size_bytes,
                files_per_directory as usize,
                buffer,
                progress_bar,
                &mut dir_completed,
                opt.verify,
                opt.io_limit,
            );

            // Return both the result and the updated completion state
            (result, dir_completed)
        })
        .collect();

    // Now update the state with the results
    for (dir_index, (result, dir_completed)) in dir_results.into_iter().enumerate() {
        if let Err(e) = result {
            eprintln!("Error in directory {}: {}", dir_index + 1, e);
        }
        state.completed_files[dir_index] = dir_completed;
    }

    // Save final state
    if let Err(e) = save_state(&state, &opt.base_dir) {
        eprintln!("Failed to save job state: {}", e);
    }

    let elapsed = start_time.elapsed();
    let bytes_written = total_size_bytes - (completed_count * file_size_bytes);
    let mb_written = bytes_written as f64 / (1024.0 * 1024.0);
    let seconds = elapsed.as_secs_f64();
    let mb_per_sec = if seconds > 0.0 {
        mb_written / seconds
    } else {
        0.0
    };

    overall_progress
        .finish_with_message(format!("Done in {:.2}s ({:.2} MB/s)", seconds, mb_per_sec));

    for pb in dir_progress_bars {
        pb.finish();
    }

    println!(
        "\nCreated {} directories with {} files each",
        opt.num_directories, files_per_directory
    );
    println!(
        "Total storage: {}",
        Byte::from_bytes(total_size_bytes as u128).get_appropriate_unit(true)
    );
    println!("Average write speed: {:.2} MB/s", mb_per_sec);

    Ok(())
}

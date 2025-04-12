use std::fs::{self, File};
use std::io::{self, Write};
use std::process;
use std::sync::{Arc, Mutex};
use std::thread;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "many_files",
    about = "Create many files across multiple directories"
)]
struct Opt {
    /// Total size in GiB
    #[structopt(short, long, default_value = "15")]
    total_size_gib: usize,

    /// Size of each file in KiB
    #[structopt(short, long, default_value = "256")]
    file_size_kib: usize,

    /// Number of directories to create
    #[structopt(short, long, default_value = "5")]
    num_directories: usize,

    /// Number of parallel jobs
    #[structopt(short = "j", long, default_value = "8")]
    num_parallel_jobs: usize,

    /// Base directory where subdirectories will be created
    #[structopt(short, long, default_value = "/tmp/dirs")]
    base_dir: String,
}

fn main() -> io::Result<()> {
    let opt = Opt::from_args();

    // Calculate the number of files per directory
    let file_count = (opt.total_size_gib * 1024 * 1024) / (opt.file_size_kib * opt.num_directories);

    // Create a zero buffer with the specified file size
    let buffer = vec![0u8; opt.file_size_kib * 1024];
    let buffer = Arc::new(buffer);

    // Create base directory if it doesn't exist
    fs::create_dir_all(&opt.base_dir)?;

    let mut handles = vec![];

    // Create directories and files
    for d in 1..=opt.num_directories {
        let dir_path = format!("{}/dir_{}", opt.base_dir, d);
        let buffer = buffer.clone();
        let num_parallel_jobs = opt.num_parallel_jobs;
        let file_count = file_count;

        let handle = thread::spawn(move || -> io::Result<()> {
            // Create the directory
            fs::create_dir_all(&dir_path)?;

            // Use a thread pool to create files
            let (tx, rx) = std::sync::mpsc::channel();
            let job_count = Arc::new(Mutex::new(0));

            for i in 1..=file_count {
                let dir_path = dir_path.clone();
                let buffer = buffer.clone();
                let job_count = job_count.clone();
                let tx = tx.clone();

                // Wait if we've hit our parallel job limit
                {
                    let mut count = job_count.lock().unwrap();
                    while *count >= num_parallel_jobs {
                        drop(count);
                        rx.recv().unwrap();
                        count = job_count.lock().unwrap();
                    }
                    *count += 1;
                }

                thread::spawn(move || {
                    let result = (|| -> io::Result<()> {
                        let file_path = format!("{}/file_{}", dir_path, i);
                        let mut file = File::create(file_path)?;
                        file.write_all(&buffer)?;
                        Ok(())
                    })();

                    // Decrement job count when done
                    {
                        let mut count = job_count.lock().unwrap();
                        *count -= 1;
                    }
                    tx.send(result).unwrap();
                });
            }

            // Wait for all jobs to complete
            drop(tx);
            while let Ok(result) = rx.recv() {
                result?;
            }

            Ok(())
        });

        handles.push(handle);
    }

    // Wait for all directories to be processed
    for handle in handles {
        if let Err(e) = handle.join().unwrap() {
            eprintln!("Error creating files: {}", e);
            process::exit(1);
        }
    }

    println!(
        "Created {} directories with identical filenames, totaling approximately {}GiB of storage.",
        opt.num_directories, opt.total_size_gib
    );

    Ok(())
}

use byte_unit::Byte;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn cpu_count() -> u16 {
    num_cpus::get() as u16
}

fn parse_size(size_string: &str) -> Result<u64, Box<dyn Error>> {
    let bytes = Byte::parse_str(size_string, true).unwrap().as_u64();
    let bytes_u128: u128 = bytes as u128;
    if bytes_u128 > u64::MAX as u128 {
        Err("Size exceeds maximum value")?
    }
    if bytes_u128 == 0 {
        Err("Size cannot be zero")?
    }
    Ok(bytes)
}

fn calc_files_per_dir(
    total_size: u64,
    file_size_bytes: u64,
    dir_count: u16,
) -> Result<u64, Box<dyn Error>> {
    // todo!("Implement Result<> for error handling");
    let total_files = total_size / file_size_bytes;
    Ok(total_files / dir_count as u64)
}

pub fn make_file(
    path: Option<&PathBuf>,
    file_index: u16,
    buffer: &Arc<Vec<u8>>,
    file_size: u64,
    _random_data: Option<bool>,
) -> Result<(), Box<dyn Error>> {
    let mut file: File = File::create(path.unwrap().join(format!("file_{}", file_index)))?;

    let start_time = Instant::now();
    let mut bytes_written = 0;

    while bytes_written < file_size {
        let bytes_to_write = std::cmp::min(file_size - bytes_written, buffer.len() as u64);
        file.write_all(&buffer[..bytes_to_write as usize])?;
        bytes_written += bytes_to_write;
    }

    let elapsed_time = start_time.elapsed();
    println!("Elapsed time: {:?}", elapsed_time);

    Ok(())
}

pub fn make_dirs(
    path: &Path,
    dir_index: u16,
    files_per_dir: u64,
    file_size_bytes: Option<String>,
) -> Result<(), Box<dyn Error>> {
    let file_size = parse_size(file_size_bytes.expect("File size is required").as_str())?;
    let buffer = Arc::new(vec![0; 1024 * 1024]);
    let dir_path: PathBuf = path.join(format!("dir_{}", dir_index));
    std::fs::create_dir_all(&dir_path)?;
    for file_index in 0..files_per_dir {
        make_file(
            Some(&dir_path),
            file_index.try_into().unwrap(),
            &buffer,
            file_size,
            None,
        )?;
    }
    Ok(())
}

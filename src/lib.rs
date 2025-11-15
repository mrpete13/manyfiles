use byte_unit::Byte;
use rand::Rng;
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::thread;

// pub struct FileGenerator {
// path: PathBuf,
// total_size_bytes: u64,
// file_size_bytes: u64,
// dir_count: u16,
// thread_count: u16,
// random_data: Option<bool>,
// }

pub fn gen_files(
    path: &Path,
    total_size: Option<String>,
    file_size_bytes: Option<String>,
    dir_count: u16,
    thread_count: u16,
    random_data: Option<bool>,
) -> Result<(), Box<dyn Error>> {
    let total_size_bytes = parse_size(total_size.unwrap())?;
    let fsize: u64 = parse_size(file_size_bytes.unwrap())?;
    let files_per_dir: u64 = calc_files_per_dir(total_size_bytes, fsize, dir_count)?;
    let mut handles = Vec::new();
    for dir_index in 0..thread_count {
        let path_buff = path.to_path_buf();
        let random = random_data;
        let handle = thread::spawn(move || {
            let file_index = 0;
            make_dirs(&path_buff, dir_index, files_per_dir, fsize);
            create_file(&path_buff, file_index, fsize, random);
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}

pub fn cpu_count() -> u16 {
    num_cpus::get() as u16
}

fn parse_size(size_string: String) -> Result<u64, Box<dyn Error>> {
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

fn create_file(
    path: &Path,
    file_index: u16,
    file_size: u64,
    random_data: Option<bool>,
) -> Result<(), Box<dyn Error>> {
    let f = File::create(path.join(format!("file_{}", file_index)))?;
    let mut writer = BufWriter::with_capacity(256 * 1024, f);
    let mut remaining = file_size;
    let mut buffer = vec![0u8; 256 * 1024];

    if let Some(true) = random_data {
        let mut rng = rand::thread_rng();
        while remaining > 0 {
            let to_write = remaining.min(buffer.len() as u64) as usize;
            rng.fill(&mut buffer[..to_write]);
            writer.write_all(&buffer[..to_write])?;
            remaining -= to_write as u64;
        }
        Ok(())
    } else {
        while remaining > 0 {
            let to_write = remaining.min(buffer.len() as u64) as usize;
            writer.write_all(&buffer[..to_write])?;
            remaining -= to_write as u64;
        }
        Ok(())
    }
}

fn make_dirs(
    path: &Path,
    dir_index: u16,
    files_per_dir: u64,
    file_size: u64,
) -> Result<(), Box<dyn Error>> {
    let dir_path: PathBuf = path.join(format!("dir_{}", dir_index));
    std::fs::create_dir_all(&dir_path)?;
    for file_index in 0..files_per_dir {
        create_file(&dir_path, file_index.try_into().unwrap(), file_size, None)?;
    }
    Ok(())
}

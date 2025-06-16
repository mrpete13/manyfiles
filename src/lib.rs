use byte_unit::Byte;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
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

fn calc_files_per_dir(total_size: u64, file_size_bytes: u64, dir_count: u16) -> u64 {
    // todo!("Implement Result<> for error handling");
    let total_files = total_size / file_size_bytes;
    total_files / dir_count as u64
}

pub fn make_files(
    path: Option<&PathBuf>,
    total_size_bytes: Option<String>,
    file_size_bytes: Option<String>,
    dir_count: Option<u16>,
    thread_count: Option<u16>,
    _random_data: Option<bool>,
) -> Result<(), Box<dyn Error>> {
    let total_size = parse_size(total_size_bytes.as_ref().unwrap())?;
    let file_size_str = file_size_bytes.as_ref().unwrap();
    let file_size = parse_size(file_size_str)?;
    println!("CPU Core count: {}", cpu_count());
    let threads = thread_count.unwrap_or(4);
    // todo!("Implement check to ensure system has enough RAM for buffer size of larger files");
    let buffer_size = file_size; // Use file_size directly
    let buffer = Arc::new(vec![0u8; buffer_size as usize]); // Create a Vec<u8> of proper size
    let start_time = Instant::now();

    let path = match path {
        Some(p) => p.clone(),
        None => PathBuf::from("/tmp/dirs"),
    };

    let dir_file_count = calc_files_per_dir(total_size, file_size, dir_count.unwrap_or(4));
    let chunk_size = dir_file_count / threads as u64;

    let mut handles: Vec<thread::JoinHandle<Result<(), std::io::Error>>> =
        Vec::with_capacity(threads as usize);
    for i in 0..threads {
        let start = i as u64;
        let end = start + chunk_size;
        let path_clone = path.clone();
        let buffer_clone = buffer.clone();
        let handle = thread::spawn(move || {
            for file_index in start..end {
                let file_name = format!("file_{}", file_index);
                let file_path = path_clone.join(file_name);
                let mut file = File::create(file_path)?;
                file.write_all(&buffer_clone)?;
            }
            Ok(())
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap()?;
    }

    let elapsed_time = start_time.elapsed();
    println!("Elapsed time: {:?}", elapsed_time);

    Ok(())
}

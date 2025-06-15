use std::error::Error;
use std::fs;
use std::io;
use std::process;
use std::thread;

pub struct Config {
    path: String,
    total_size: u32, // needs to be converted from String
    dir_count: u32,
    file_size: u32,
    thread_count: u16,
    random_data: bool,
}

fn cpu_count() -> u16 {
    num_cpus::get() as u16
}

fn to_bytes(size: u32) -> u32 {
    todo!("Convert human-readable size to bytes");
    // num * 1024 * 1024
}

fn calc_file_count(total_size: u32, file_size: u32) -> u32 {
    todo!("Implement file count calculation");
    //total_size.to_bytes() / file_size
}

fn make_files(config: &Config) -> Result<(), io::Error> {
    todo!("Implement make_files function");
    // Implementation of make_files function
    Ok(())
}

impl Config {
    pub fn build(args: &[String]) -> Result<Config, &'static str> {
        if args.is_empty() {
            return Err("Not enough arguments");
        }

        let path = args[1].clone();
        let total_size = args[2].parse().unwrap_or(1); //defaults to 1 GiB
        let dir_count = args[3].parse().unwrap_or(5); // defaults to 5 dirs
        let file_size = args[4].parse().unwrap_or(256); // defaults to 256 KiB
        let thread_count = args[4].parse().unwrap_or(cpu_count()); //defaults to max thread count
        let random_data = args[5].parse().unwrap_or(false);

        Ok(Config {
            path,
            total_size,
            dir_count,
            file_size,
            thread_count,
            random_data,
        })
    }
}

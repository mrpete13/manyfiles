use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short = 'p', long = "path", default_value = "/tmp/dirs")]
    path: PathBuf,

    #[clap(short = 's', long = "size", default_value = "1GiB")]
    total_size: Option<String>,

    #[clap(short = 'd', long = "numdirs", default_value = "4")]
    dir_count: u16,

    #[clap(short = 'f', long = "filesize", default_value = "256KiB")]
    file_size: Option<String>,

    #[clap(short = 'j', long = "threads", default_value = "4")]
    thread_count: u16,

    #[clap(short = 'r', long = "random", default_value = "false")]
    random_data: Option<bool>,
}

fn main() {
    let args = Args::parse();
    println!("Path: {:?}", args.path);
    println!("Total size: {:?}", args.total_size);
    println!("File size: {:?}", args.file_size);
    println!("Directory count: {}", args.dir_count);
    println!("Thread count: {}", args.thread_count);
    println!("Random data: {}", args.random_data.unwrap());
    if let Err(e) = manyfiles::gen_files(
        &args.path,
        args.total_size,
        args.file_size,
        args.dir_count,
        args.thread_count,
        args.random_data,
    ) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

use clap::Parser;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(short = 'p', long = "path", default_value = "/tmp/dirs")]
    path: Option<PathBuf>,

    #[clap(short = 's', long = "size", default_value = "1GiB")]
    total_size_bytes: Option<String>,

    #[clap(short = 'd', long = "numdirs", default_value = "4")]
    dir_count: Option<u16>,

    #[clap(short = 'f', long = "filesize", default_value = "256KiB")]
    file_size_bytes: Option<String>,

    #[clap(short = 'j', long = "threads", default_value = "4")]
    thread_count: Option<u16>,

    #[clap(short = 'r', long = "random", default_value = "false")]
    random_data: Option<bool>,
}

// todo!(
// "
// - Cumulative size isn't being created.
// - Files aren't being created in subdirectories.
// "
// );

fn main() {
    let args = Args::parse();
    if let Err(e) = manyfiles::make_file(
        args.path.as_ref(),
        args.total_size_bytes,
        args.file_size_bytes,
        args.dir_count,
        args.thread_count,
        args.random_data,
    ) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

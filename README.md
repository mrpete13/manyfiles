## manyfiles

This program creates many files of specified sizes in multiple directories.
This was *not* originally written to be a benchmarking tool but rather a utility for creating large amounts of files for other various testing purposes.

### Install
Prerequisites:
- Rust installed (https://www.rust-lang.org/tools/install)

Clone this repo and run `cargo build --release` from the root of this repo.
The binary file will then be located in target/release/manyfiles.

### Usage 
```
Usage: manyfiles [OPTIONS]

Options:
  -p, --path <PATH>                 [default: /tmp/dirs]
  -s, --size <TOTAL_SIZE_BYTES>     [default: 1GiB]
  -d, --numdirs <DIR_COUNT>         [default: 4]
  -f, --filesize <FILE_SIZE_BYTES>  [default: 256KiB]
  -j, --threads <THREAD_COUNT>      [default: 4]
  -r, --random <RANDOM_DATA>        [default: false] [possible values: true, false]
  -h, --help                        Print help
  -V, --version                     Print version
```

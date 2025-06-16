## manyfiles

This program creates many files of specified sizes in multiple directories.
This was *not* originally written to be a benchmarking tool but rather a utility for creating large amounts of files for other varioustesting purposes.

By default there are 5 directories that total 15GiB written to /tmp/dirs/.

### Install
Prerequisites:
- Rust installed (https://www.rust-lang.org/tools/install)

Clone this repo and run `cargo build --release` from the root of this repo.
The binary file will then be located in target/release/manyfiles.
Use `manyfiles --help` to see the available commands.

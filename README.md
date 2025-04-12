## manyfiles

This program creates many files of specified sizes in multiple directories.
This was *not* originally written to be a benchmarking tool but rather a utility for creating large amounts of files for other varioustesting purposes.

By default there are 5 directories that total 15GiB written to /tmp/dirs/.

### Install
Prerequisites:
- Rust installed (https://www.rust-lang.org/tools/install)

Clone this repo and running `cargo build --release`
Use `manyfiles --help` to see the available commands.

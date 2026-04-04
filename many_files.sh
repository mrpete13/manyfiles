#!/bin/bash

total_size_gib=15
file_size_kib=256
num_directories=5 
file_count=$(((total_size_gib * 1024 * 1024) / (file_size_kib * num_directories)))

num_parallel_jobs=10

create_files_in_directory() {
  local dir=$1
  mkdir -p "$dir"
  # Create files within the directory
  for ((i = 1; i <= $file_count; i++)); do
    dd if=/dev/zero of="$dir/file_$i" bs="$file_size_kib"K count=1 2>/dev/null &
    if ((i % num_parallel_jobs == 0)); then
      wait 
    fi
  done
}

# Create identical files in each directory
for ((d = 1; d <= num_directories; d++)); do
  create_files_in_directory "/tmp/dirs/dir_$d" &
done
wait

echo "Created $num_directories directories with identical filenames, totaling approximately ${total_size_gib}GiB of storage."

#!/bin/bash

# Define the total size and file parameters
total_size_gib=15
file_size_kib=256
num_directories=5 # Number of directories you want to create
file_count=$(((total_size_gib * 1024 * 1024) / (file_size_kib * num_directories)))

# Set the number of parallel jobs
num_parallel_jobs=10

# Function to create files in a directory
create_files_in_directory() {
  local dir=$1

  # Create the directory if it does not exist
  mkdir -p "$dir"

  # Create files within the directory
  for ((i = 1; i <= $file_count; i++)); do
    dd if=/dev/zero of="$dir/file_$i" bs="$file_size_kib"K count=1 2>/dev/null &

    # Limit the number of parallel jobs
    if ((i % num_parallel_jobs == 0)); then
      wait # Wait for all background jobs to finish
    fi
  done
}

# Create identical files in each directory
for ((d = 1; d <= num_directories; d++)); do
  # Call the function to create files in parallel for each directory
  create_files_in_directory "/tmp/dirs/dir_$d" &
done

# Wait for any remaining background jobs to finish
wait

echo "Created $num_directories directories with identical filenames, totaling approximately ${total_size_gib}GiB of storage."

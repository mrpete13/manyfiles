use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::state::JobState;

pub fn progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .unwrap()
        .progress_chars("##-")
}

/// Create and register all progress bars with `multi`.
///
/// `enable_steady_tick` guarantees redraws at a fixed interval regardless of
/// how fast (or slow) worker threads call `inc()`. Without it a bar that isn't
/// incremented for a while will appear frozen.
pub fn build_progress_bars(
    multi: &MultiProgress,
    style: &ProgressStyle,
    state: &JobState,
    files_per_directory: usize,
) -> (ProgressBar, Vec<ProgressBar>) {
    let tick = Duration::from_millis(100);

    let total_file_count =
        u64::try_from(files_per_directory * state.num_directories).unwrap_or(u64::MAX);

    let overall_bar = multi.add(ProgressBar::new(total_file_count));
    overall_bar.set_style(style.clone());
    overall_bar.set_message("overall");
    overall_bar.enable_steady_tick(tick);

    let already_done = u64::try_from(
        state
            .completed_files
            .iter()
            .flat_map(|d| d.iter())
            .filter(|&&v| v)
            .count(),
    )
    .unwrap_or(u64::MAX);
    overall_bar.inc(already_done);

    let dir_bars: Vec<ProgressBar> = (0..state.num_directories)
        .map(|i| {
            let pb = multi.add(ProgressBar::new(
                u64::try_from(files_per_directory).unwrap_or(u64::MAX),
            ));
            pb.set_style(style.clone());
            pb.set_message(format!("dir {}", i + 1));
            pb.enable_steady_tick(tick);
            let done = u64::try_from(
                state.completed_files[i].iter().filter(|&&v| v).count(),
            )
            .unwrap_or(u64::MAX);
            pb.inc(done);
            pb
        })
        .collect();

    (overall_bar, dir_bars)
}

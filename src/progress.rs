use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::layout::Layout;

pub fn progress_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .unwrap()
        .progress_chars("##-")
}

/// Create and register all progress bars with `multi`.
///
/// `enable_steady_tick` guarantees redraws at a fixed 100ms interval
/// regardless of how frequently workers call `inc()` — prevents bars from
/// appearing frozen during large writes.
pub fn build_progress_bars(
    multi: &MultiProgress,
    style: &ProgressStyle,
    layout: &Layout,
    num_directories: usize,
    already_done: u64,
    dir_done: &[u64],
) -> (ProgressBar, Vec<ProgressBar>) {
    let tick = Duration::from_millis(100);

    let total_file_count =
        u64::try_from(layout.files_per_directory * num_directories).unwrap_or(u64::MAX);

    let overall_bar = multi.add(ProgressBar::new(total_file_count));
    overall_bar.set_style(style.clone());
    overall_bar.set_message("overall");
    overall_bar.enable_steady_tick(tick);
    overall_bar.inc(already_done);

    let dir_bars: Vec<ProgressBar> = (0..num_directories)
        .map(|i| {
            let pb = multi.add(ProgressBar::new(
                u64::try_from(layout.files_per_directory).unwrap_or(u64::MAX),
            ));
            pb.set_style(style.clone());
            pb.set_message(format!("dir {}", i + 1));
            pb.enable_steady_tick(tick);
            pb.inc(dir_done[i]);
            pb
        })
        .collect();

    (overall_bar, dir_bars)
}

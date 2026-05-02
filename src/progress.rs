const DEFAULT_PROGRESS_WIDTH: usize = 18;

pub fn progress_bar(completed: usize, total: usize) -> String {
    progress_bar_u64(completed as u64, total as u64)
}

pub fn progress_bar_u64(completed: u64, total: u64) -> String {
    let total = total.max(1);
    let completed = completed.min(total);
    let filled = ((completed as f64 / total as f64) * DEFAULT_PROGRESS_WIDTH as f64).round()
        as usize;
    let empty = DEFAULT_PROGRESS_WIDTH.saturating_sub(filled);
    format!("[{}{}]", "#".repeat(filled), "-".repeat(empty))
}

pub fn progress_percent(completed: usize, total: usize) -> u8 {
    progress_percent_u64(completed as u64, total as u64)
}

pub fn progress_percent_u64(completed: u64, total: u64) -> u8 {
    let total = total.max(1);
    let completed = completed.min(total);
    ((completed as f64 / total as f64) * 100.0).round() as u8
}
//! Run-level counters kept separate from command-line configuration.

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RunMetrics {
    pub entries: usize,
    pub artifacts_attempted: usize,
    pub artifacts_with_rows: usize,
    pub files_written: usize,
    pub errors: usize,
}

impl RunMetrics {
    pub fn record_success(&mut self, rows: usize, files: usize) {
        self.entries += rows;
        self.artifacts_with_rows += usize::from(rows > 0);
        self.files_written += files;
    }
}

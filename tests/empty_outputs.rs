use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;

fn only_file_containing(dir: &Path, needle: &str, extension: &str) -> PathBuf {
    let matches: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains(needle) && name.ends_with(extension))
        })
        .collect();
    assert_eq!(matches.len(), 1, "expected one {needle}{extension} output");
    matches.into_iter().next().unwrap()
}

#[test]
fn attempted_empty_artifacts_write_schema_outputs_and_reconcile_summary() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let temp = tempfile::tempdir().unwrap();
    let csv_dir = temp.path().join("csv");
    let parquet_dir = temp.path().join("parquet");

    let output = Command::new(env!("CARGO_BIN_EXE_forensic-webhistory"))
        .env("RUST_LOG", "info")
        .args([
            "scan",
            "--dir",
            fixture.to_str().unwrap(),
            "--output",
            csv_dir.to_str().unwrap(),
            "--out",
            parquet_dir.to_str().unwrap(),
            "--artifacts",
            "history,downloads",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "webx failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let log = String::from_utf8_lossy(&output.stderr);
    let downloads_csv = only_file_containing(&csv_dir, "_downloads_", ".csv");
    let history_csv = only_file_containing(&csv_dir, "_history_", ".csv");
    let downloads_parquet = only_file_containing(&parquet_dir, "_downloads_", ".parquet");

    let mut downloads = csv::Reader::from_path(&downloads_csv).unwrap();
    assert!(!downloads.headers().unwrap().is_empty());
    assert_eq!(downloads.records().count(), 0);

    let mut history = csv::Reader::from_path(&history_csv).unwrap();
    assert_eq!(history.records().count(), 1);

    let reader = SerializedFileReader::new(File::open(downloads_parquet).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 0);

    let named_paths: Vec<_> = log
        .lines()
        .filter_map(|line| line.split_once(" -> ").map(|(_, path)| path.trim()))
        .collect();
    assert_eq!(named_paths.len(), 2);
    for path in named_paths {
        assert!(
            Path::new(path).exists(),
            "logged output does not exist: {path}"
        );
    }

    assert!(log.contains(
        "Complete: 1 total entries; 2 artifact(s) attempted, 1 with rows; 4 file(s) written (0 errors)"
    ));
}

use std::fs;
use std::path::Path;

#[test]
fn binary_entrypoint_is_thin_and_layers_are_explicit() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let main = fs::read_to_string(root.join("src/main.rs")).unwrap();
    assert!(
        main.lines().count() <= 30,
        "main.rs must remain a thin entrypoint"
    );
    assert!(!main.contains("fn cmd_scan"));
    for module in [
        "cli.rs",
        "discovery.rs",
        "model.rs",
        "metrics.rs",
        "parser/mod.rs",
    ] {
        assert!(
            root.join("src").join(module).is_file(),
            "missing src/{module}"
        );
    }
}

#[test]
fn parser_model_and_output_do_not_depend_on_cli_types() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for path in ["parser/mod.rs", "model.rs", "output.rs"] {
        let source = fs::read_to_string(root.join(path)).unwrap();
        assert!(
            !source.contains("crate::cli"),
            "{path} depends on CLI types"
        );
        assert!(!source.contains("Cli"), "{path} mentions CLI types");
    }
}

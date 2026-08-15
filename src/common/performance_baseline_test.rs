use super::{PerformanceBaselineStore, PerformanceSample};
use crate::common::config::PerformanceBaselineConfig;
use tempfile::TempDir;

#[test]
fn baseline_warms_up_detects_regression_and_survives_reload() {
    let temp = TempDir::new().unwrap();
    let store = PerformanceBaselineStore::load(temp.path());
    for _ in 0..3 {
        let entry = store.record(
            "main",
            "abc",
            "Scan(metrics)",
            PerformanceSample {
                elapsed_ms: 100.0,
                peak_memory_bytes: 1024,
                spill_bytes: 0,
                row_error: 1.0,
            },
        );
        assert_ne!(entry.status, "regression");
    }
    let regressed = store.record(
        "main",
        "abc",
        "Scan(metrics)",
        PerformanceSample {
            elapsed_ms: 200.0,
            peak_memory_bytes: 1024,
            spill_bytes: 128,
            row_error: 1.0,
        },
    );
    assert_eq!(regressed.status, "regression");
    assert_eq!(regressed.regression_reasons, vec!["elapsed", "spill"]);
    assert_eq!(regressed.baseline_elapsed_ms, 100.0);

    for _ in 0..3 {
        store.record(
            "main",
            "resource-heavy",
            "Aggregate(Scan(metrics))",
            PerformanceSample {
                elapsed_ms: 10.0,
                peak_memory_bytes: 1024,
                spill_bytes: 0,
                row_error: 1.0,
            },
        );
    }
    let resource_regression = store.record(
        "main",
        "resource-heavy",
        "Aggregate(Scan(metrics))",
        PerformanceSample {
            elapsed_ms: 10.0,
            peak_memory_bytes: 2 * 1024 * 1024,
            spill_bytes: 0,
            row_error: 4.0,
        },
    );
    assert_eq!(
        resource_regression.regression_reasons,
        vec!["memory", "cardinality"]
    );

    let baseline_path = temp.path().join("meta").join("performance_baselines.json");
    let backup_path = baseline_path.with_extension("json.bak");
    std::fs::copy(&baseline_path, &backup_path).unwrap();
    std::fs::write(&baseline_path, b"invalid json").unwrap();

    let reloaded = PerformanceBaselineStore::load(temp.path());
    let entries = reloaded.entries("main");
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|entry| entry.samples == 4));
    assert_eq!(reloaded.reset("main").unwrap(), 2);
    assert!(reloaded.entries("main").is_empty());
}

#[test]
fn configurable_baselines_are_isolated_by_environment_and_version() {
    let temp = TempDir::new().unwrap();
    let config = PerformanceBaselineConfig {
        max_entries: 1,
        warmup_samples: 2,
        ewma_alpha: 0.5,
        elapsed_regression_ratio: 1.1,
        elapsed_regression_min_ms: 0.0,
        environment: "ci".to_string(),
        version: "v1".to_string(),
        ..PerformanceBaselineConfig::default()
    };
    let store = PerformanceBaselineStore::load_with_config(temp.path(), config.clone());
    for _ in 0..2 {
        let entry = store.record(
            "main",
            "query-a",
            "Scan(metrics)",
            PerformanceSample {
                elapsed_ms: 100.0,
                ..PerformanceSample::default()
            },
        );
        assert_ne!(entry.status, "regression");
    }
    let regressed = store.record(
        "main",
        "query-a",
        "Scan(metrics)",
        PerformanceSample {
            elapsed_ms: 111.0,
            ..PerformanceSample::default()
        },
    );
    assert_eq!(regressed.status, "regression");
    assert_eq!(regressed.environment, "ci");
    assert_eq!(regressed.version, "v1");

    store.record(
        "main",
        "query-b",
        "Scan(events)",
        PerformanceSample::default(),
    );
    assert_eq!(store.entries("main").len(), 1);

    let v2 = PerformanceBaselineStore::load_with_config(
        temp.path(),
        PerformanceBaselineConfig {
            version: "v2".to_string(),
            ..config.clone()
        },
    );
    assert!(v2.entries("main").is_empty());
    v2.record(
        "main",
        "query-a",
        "Scan(metrics)",
        PerformanceSample::default(),
    );

    let production = PerformanceBaselineStore::load_with_config(
        temp.path(),
        PerformanceBaselineConfig {
            environment: "production".to_string(),
            ..config
        },
    );
    assert!(production.entries("main").is_empty());
}

#[test]
fn legacy_baseline_files_gain_default_labels_and_new_keys() {
    let temp = TempDir::new().unwrap();
    let store = PerformanceBaselineStore::load(temp.path());
    store.record(
        "main",
        "legacy",
        "Scan(metrics)",
        PerformanceSample::default(),
    );
    drop(store);

    let path = temp.path().join("meta").join("performance_baselines.json");
    let mut json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    for entry in json["entries"].as_object_mut().unwrap().values_mut() {
        let object = entry.as_object_mut().unwrap();
        object.remove("environment");
        object.remove("version");
    }
    std::fs::write(&path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();

    let migrated = PerformanceBaselineStore::load(temp.path());
    let entries = migrated.entries("main");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].environment, "default");
    assert_eq!(entries[0].version, env!("CARGO_PKG_VERSION"));
    assert_eq!(migrated.reset("main").unwrap(), 1);
}

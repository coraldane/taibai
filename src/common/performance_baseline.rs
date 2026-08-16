use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::common::config::PerformanceBaselineConfig;
use crate::common::error::{Result, TaibaiError};

#[derive(Clone, Copy, Debug, Default)]
pub struct PerformanceSample {
    pub elapsed_ms: f64,
    pub peak_memory_bytes: u64,
    pub spill_bytes: u64,
    pub row_error: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerformanceBaselineEntry {
    pub database: String,
    pub environment: String,
    pub version: String,
    pub fingerprint: String,
    pub query_shape: String,
    pub samples: u64,
    pub baseline_elapsed_ms: f64,
    pub last_elapsed_ms: f64,
    pub baseline_peak_memory_bytes: f64,
    pub last_peak_memory_bytes: u64,
    pub baseline_spill_bytes: f64,
    pub last_spill_bytes: u64,
    pub baseline_row_error: f64,
    pub last_row_error: f64,
    pub status: String,
    pub regression_reasons: Vec<String>,
    pub updated_at: i64,
}

#[derive(Default, Serialize, Deserialize)]
struct PerformanceBaselineState {
    entries: HashMap<String, PerformanceBaselineEntry>,
}

pub struct PerformanceBaselineStore {
    path: PathBuf,
    config: PerformanceBaselineConfig,
    state: Mutex<PerformanceBaselineState>,
}

impl PerformanceBaselineStore {
    pub fn load(data_dir: &Path) -> Self {
        Self::load_with_config(
            data_dir,
            crate::common::config::performance_baseline_config(),
        )
    }

    pub fn load_with_config(data_dir: &Path, config: PerformanceBaselineConfig) -> Self {
        let config = config.sanitized();
        let path = data_dir.join("meta").join("performance_baselines.json");
        let mut state = load_state(&path)
            .or_else(|| load_state(&backup_path(&path)))
            .unwrap_or_default();
        evict_oldest(&mut state.entries, config.max_entries);
        Self {
            path,
            config,
            state: Mutex::new(state),
        }
    }

    pub fn record(
        &self,
        database: &str,
        fingerprint: &str,
        query_shape: &str,
        sample: PerformanceSample,
    ) -> PerformanceBaselineEntry {
        let now = unix_timestamp();
        let key = baseline_key(
            database,
            &self.config.environment,
            &self.config.version,
            fingerprint,
        );
        let mut state = self.state.lock().unwrap();
        let entry = state
            .entries
            .entry(key)
            .or_insert_with(|| PerformanceBaselineEntry {
                database: database.to_string(),
                environment: self.config.environment.clone(),
                version: self.config.version.clone(),
                fingerprint: fingerprint.to_string(),
                query_shape: query_shape.to_string(),
                samples: 0,
                baseline_elapsed_ms: sample.elapsed_ms.max(0.0),
                last_elapsed_ms: sample.elapsed_ms.max(0.0),
                baseline_peak_memory_bytes: sample.peak_memory_bytes as f64,
                last_peak_memory_bytes: sample.peak_memory_bytes,
                baseline_spill_bytes: sample.spill_bytes as f64,
                last_spill_bytes: sample.spill_bytes,
                baseline_row_error: sample.row_error.max(1.0),
                last_row_error: sample.row_error.max(1.0),
                status: "warming".to_string(),
                regression_reasons: Vec::new(),
                updated_at: now,
            });
        entry.query_shape = query_shape.to_string();
        entry.last_elapsed_ms = sample.elapsed_ms.max(0.0);
        entry.last_peak_memory_bytes = sample.peak_memory_bytes;
        entry.last_spill_bytes = sample.spill_bytes;
        entry.last_row_error = sample.row_error.max(1.0);
        entry.updated_at = now;
        entry.regression_reasons.clear();

        if entry.samples < self.config.warmup_samples {
            let previous = entry.samples as f64;
            let next = previous + 1.0;
            entry.baseline_elapsed_ms = running_mean(
                entry.baseline_elapsed_ms,
                previous,
                entry.last_elapsed_ms,
                next,
            );
            entry.baseline_peak_memory_bytes = running_mean(
                entry.baseline_peak_memory_bytes,
                previous,
                entry.last_peak_memory_bytes as f64,
                next,
            );
            entry.baseline_spill_bytes = running_mean(
                entry.baseline_spill_bytes,
                previous,
                entry.last_spill_bytes as f64,
                next,
            );
            entry.baseline_row_error = running_mean(
                entry.baseline_row_error,
                previous,
                entry.last_row_error,
                next,
            );
            entry.samples += 1;
            entry.status = if entry.samples < self.config.warmup_samples {
                "warming".to_string()
            } else {
                "stable".to_string()
            };
        } else {
            collect_regression_reasons(entry, &self.config);
            entry.samples += 1;
            if entry.regression_reasons.is_empty() {
                entry.status = "stable".to_string();
                entry.baseline_elapsed_ms = ewma(
                    entry.baseline_elapsed_ms,
                    entry.last_elapsed_ms,
                    self.config.ewma_alpha,
                );
                entry.baseline_peak_memory_bytes = ewma(
                    entry.baseline_peak_memory_bytes,
                    entry.last_peak_memory_bytes as f64,
                    self.config.ewma_alpha,
                );
                entry.baseline_spill_bytes = ewma(
                    entry.baseline_spill_bytes,
                    entry.last_spill_bytes as f64,
                    self.config.ewma_alpha,
                );
                entry.baseline_row_error = ewma(
                    entry.baseline_row_error,
                    entry.last_row_error,
                    self.config.ewma_alpha,
                );
            } else {
                entry.status = "regression".to_string();
            }
        }

        let result = entry.clone();
        evict_oldest(&mut state.entries, self.config.max_entries);
        if let Err(error) = save_state(&self.path, &state) {
            tracing::warn!(error = %error, "failed to persist performance baselines");
        }
        result
    }

    pub fn entries(&self, database: &str) -> Vec<PerformanceBaselineEntry> {
        let state = self.state.lock().unwrap();
        let mut entries = state
            .entries
            .values()
            .filter(|entry| {
                entry.database.eq_ignore_ascii_case(database)
                    && entry.environment == self.config.environment
                    && entry.version == self.config.version
            })
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            status_rank(&right.status)
                .cmp(&status_rank(&left.status))
                .then_with(|| right.updated_at.cmp(&left.updated_at))
                .then_with(|| left.fingerprint.cmp(&right.fingerprint))
        });
        entries
    }

    pub fn reset(&self, database: &str) -> Result<u64> {
        let mut state = self.state.lock().unwrap();
        let before = state.entries.len();
        state.entries.retain(|_, entry| {
            !entry.database.eq_ignore_ascii_case(database)
                || entry.environment != self.config.environment
                || entry.version != self.config.version
        });
        let removed = before.saturating_sub(state.entries.len()) as u64;
        save_state(&self.path, &state)?;
        Ok(removed)
    }
}

fn collect_regression_reasons(
    entry: &mut PerformanceBaselineEntry,
    config: &PerformanceBaselineConfig,
) {
    if exceeds(
        entry.last_elapsed_ms,
        entry.baseline_elapsed_ms,
        config.elapsed_regression_ratio,
        config.elapsed_regression_min_ms,
    ) {
        entry.regression_reasons.push("elapsed".to_string());
    }
    if exceeds(
        entry.last_peak_memory_bytes as f64,
        entry.baseline_peak_memory_bytes,
        config.memory_regression_ratio,
        config.memory_regression_min_bytes as f64,
    ) {
        entry.regression_reasons.push("memory".to_string());
    }
    let spill_regressed = if entry.baseline_spill_bytes <= 0.0 {
        entry.last_spill_bytes > 0
    } else {
        entry.last_spill_bytes as f64 > entry.baseline_spill_bytes * config.spill_regression_ratio
    };
    if spill_regressed {
        entry.regression_reasons.push("spill".to_string());
    }
    if entry.last_row_error
        > (entry.baseline_row_error * config.row_error_regression_ratio)
            .max(entry.baseline_row_error + config.row_error_regression_min)
    {
        entry.regression_reasons.push("cardinality".to_string());
    }
}

fn exceeds(current: f64, baseline: f64, ratio: f64, absolute_delta: f64) -> bool {
    current > (baseline * ratio).max(baseline + absolute_delta)
}

fn running_mean(previous: f64, previous_count: f64, value: f64, next_count: f64) -> f64 {
    if previous_count == 0.0 {
        value
    } else {
        (previous * previous_count + value) / next_count
    }
}

fn ewma(baseline: f64, value: f64, alpha: f64) -> f64 {
    baseline * (1.0 - alpha) + value * alpha
}

fn evict_oldest(entries: &mut HashMap<String, PerformanceBaselineEntry>, max_entries: usize) {
    while entries.len() > max_entries {
        let oldest = entries
            .iter()
            .min_by(|(left_key, left), (right_key, right)| {
                left.updated_at
                    .cmp(&right.updated_at)
                    .then_with(|| left_key.cmp(right_key))
            })
            .map(|(key, _)| key.clone());
        if let Some(oldest) = oldest {
            entries.remove(&oldest);
        } else {
            break;
        }
    }
}

fn save_state(path: &Path, state: &PerformanceBaselineState) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|error| TaibaiError::Internal(format!("serialize baselines: {error}")))?;
    let temp = path.with_extension("json.tmp");
    let backup = backup_path(path);
    std::fs::write(&temp, bytes)?;
    if backup.exists() {
        std::fs::remove_file(&backup)?;
    }
    if path.exists() {
        std::fs::rename(path, &backup)?;
    }
    if let Err(error) = std::fs::rename(&temp, path) {
        if backup.exists() && !path.exists() {
            let _ = std::fs::rename(&backup, path);
        }
        return Err(error.into());
    }
    if backup.exists() {
        std::fs::remove_file(backup)?;
    }
    Ok(())
}

fn load_state(path: &Path) -> Option<PerformanceBaselineState> {
    std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

fn backup_path(path: &Path) -> PathBuf {
    path.with_extension("json.bak")
}

fn baseline_key(database: &str, environment: &str, version: &str, fingerprint: &str) -> String {
    format!(
        "{}\0{}\0{}\0{}",
        database.to_ascii_lowercase(),
        environment,
        version,
        fingerprint
    )
}

fn status_rank(status: &str) -> u8 {
    match status {
        "regression" => 2,
        "warming" => 1,
        _ => 0,
    }
}

fn unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
#[path = "performance_baseline_test.rs"]
mod tests;

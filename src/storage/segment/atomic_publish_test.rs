//! 原子发布 + 坏段隔离回归测试（2026-11-07 官网 /market 事故的内核侧根治）。
//!
//! 事故链：进程在 `SegmentWriter::create` 与 `seal` 之间被杀 → 半截
//! `seg_NNNN.dat` 留在表目录 → `discover_segments` 只看文件名就收编 →
//! 该表所有查询（含 DELETE）报 `bad scalar tag: 0`，整表不可用。
//!
//! 根治三件套：
//! 1. 写入全程落在 `.partial`，`seal` 原子 rename 发布——崩溃只留 .partial；
//! 2. 发现段时先验证 footer，坏段重命名隔离（`.corrupt-<epoch>`）；
//! 3. 读侧 footer 解析错误一律带文件路径。

use super::store;
use super::writer::SegmentWriter;
use super::SegmentReader;
use crate::protocol::types::DataType;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;
use std::path::Path;

fn schema_two() -> Schema {
    Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("payload", DataType::VarChar),
    ])
}

fn batch_two(rows: i64) -> RecordBatch {
    let schema = schema_two();
    let mut batch = RecordBatch::new(schema.clone());
    for i in 0..rows {
        let _ = batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(i)));
        let _ = batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(format!("row-{i}"))));
    }
    batch
}

/// 写入未 seal（模拟进程被杀）：最终路径不得出现任何文件，
/// `.partial` 残骸不得被 discovery 收编；之后正常 seal 的段可用。
#[test]
fn unsealed_writer_leaves_no_adoptable_segment() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let final_path = dir.join("seg_00001.dat");

    {
        // 模拟崩溃：写了一半、从未 seal、writer 被 drop
        let mut writer = SegmentWriter::create(&final_path, schema_two(), 8192, 3).unwrap();
        writer.write_batch(&batch_two(10)).unwrap();
        // drop —— 不调用 seal
    }

    assert!(
        !final_path.exists(),
        "unsealed write must never publish the final segment path"
    );
    let partial = dir.join("seg_00001.dat.partial");
    assert!(partial.exists(), "staged bytes stay in the .partial file");

    let discovered = store::discover_segments(dir).unwrap();
    assert!(
        discovered.is_empty(),
        "discovery must not adopt .partial leftovers, got {discovered:?}"
    );

    // 随后正常写入并 seal：发布成功、可读
    let mut writer = SegmentWriter::create(&final_path, schema_two(), 8192, 3).unwrap();
    writer.write_batch(&batch_two(5)).unwrap();
    writer.seal().unwrap();
    assert!(final_path.exists());
    let discovered = store::discover_segments(dir).unwrap();
    assert_eq!(discovered, vec![final_path.clone()]);
    let reader = SegmentReader::open(&final_path).unwrap();
    assert_eq!(reader.row_count(), 5);
}

/// seal 后最终路径出现且 `.partial` 被清理。
#[test]
fn seal_publishes_final_and_cleans_staging() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let final_path = dir.join("seg_00001.dat");
    let mut writer = SegmentWriter::create(&final_path, schema_two(), 8192, 3).unwrap();
    writer.write_batch(&batch_two(3)).unwrap();
    writer.seal().unwrap();
    assert!(final_path.exists());
    assert!(!dir.join("seg_00001.dat.partial").exists());
    let reader = SegmentReader::open(&final_path).unwrap();
    assert_eq!(reader.row_count(), 3);
}

/// 坏段（footer 区域被零填充）在读打开时被隔离：
/// 报错带文件名、文件被改名保留、后续 discovery 只剩健康段。
#[test]
fn corrupt_segment_is_quarantined_at_reader_open() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let good_path = dir.join("seg_00001.dat");
    let bad_path = dir.join("seg_00002.dat");

    for (path, rows) in [(&good_path, 4i64), (&bad_path, 6i64)] {
        let mut writer = SegmentWriter::create(path, schema_two(), 8192, 3).unwrap();
        writer.write_batch(&batch_two(rows)).unwrap();
        writer.seal().unwrap();
    }

    // 破坏 seg_00002：整体清零，模拟半截写入的零填充。
    let len = std::fs::metadata(&bad_path).unwrap().len() as usize;
    let zeros = vec![0u8; len.saturating_sub(64)];
    std::fs::write(&bad_path, &zeros).unwrap();

    let mgr = crate::storage::segment_manager::SegmentManager::new(dir).unwrap();
    let err = match mgr.open_reader(&bad_path) {
        Err(err) => err.to_string(),
        Ok(_) => panic!("open must fail on a zero-filled segment"),
    };
    assert!(
        err.contains("seg_00002.dat"),
        "error must name the file, got: {err}"
    );
    assert!(
        !bad_path.exists(),
        "corrupt file must be moved out of the segment pattern"
    );
    let quarantine: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.contains(".corrupt-"))
        .collect();
    assert_eq!(
        quarantine.len(),
        1,
        "corrupt file is renamed aside, got {quarantine:?}"
    );

    // 后续 discovery 只剩健康段；健康段照常可读
    let discovered = store::discover_segments(dir).unwrap();
    assert_eq!(discovered, vec![good_path.clone()]);
    mgr.open_reader(&good_path).expect("good segment stays readable");
}

/// 隔离的坏文件内容保留（可取证），名字不再匹配段文件模式。
#[test]
fn quarantined_file_keeps_content_and_exits_segment_pattern() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let bad_path = dir.join("seg_00009.dat");
    std::fs::write(&bad_path, vec![0u8; 4096]).unwrap();
    let mgr = crate::storage::segment_manager::SegmentManager::new(dir).unwrap();
    let _ = mgr.open_reader(&bad_path);
    assert!(!bad_path.exists(), "corrupt file is renamed aside");
    let quarantined: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("seg_00009.dat.corrupt-"))
        .collect();
    assert_eq!(quarantined.len(), 1);
    let kept = dir.join(&quarantined[0]);
    assert_eq!(
        std::fs::metadata(&kept).unwrap().len(),
        4096,
        "quarantined bytes are preserved for forensics"
    );
    assert!(!super::store::is_segment_filename(&quarantined[0]));
}

/// 打开坏段（绕过 discovery）时报错必须带文件路径。
#[test]
fn reader_open_error_names_the_file() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("seg_00003.dat");
    std::fs::write(&path, vec![0u8; 4096]).unwrap();
    let err = match SegmentReader::open(&path) {
        Err(err) => err.to_string(),
        Ok(_) => panic!("open must fail on a zero-filled segment"),
    };
    assert!(
        err.contains("seg_00003.dat"),
        "open error must name the file, got: {err}"
    );
}

#[allow(dead_code)]
fn touch(_: &Path) {}

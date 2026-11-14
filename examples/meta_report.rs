//! One-off diagnostic: report what a real segment footer is actually made of.
//!
//! Usage: cargo run --release --example meta_report -- /tmp/sample.dat
//! (expects `/tmp/sample.meta` next to it)

use std::path::PathBuf;

use taibai::storage::segment::footer::{parse_segment_footer, parse_segment_footer_index};
use taibai::storage::segment::meta::read_latest_sidecar_footer;
use taibai::storage::segment::RowGroupMeta;

fn main() {
    let segment = PathBuf::from(std::env::args().nth(1).expect("segment path"));
    let sidecar = read_latest_sidecar_footer(&segment)
        .expect("read sidecar")
        .expect("sidecar not found");
    let footer_len = sidecar.footer.len();
    let parsed = parse_segment_footer(&sidecar.footer).expect("parse footer");

    let rg_count = parsed.row_groups.len();
    let col_count = parsed.schema.columns.len();

    let mut bloom_count = 0usize;
    let mut bloom_bytes = 0usize;
    let mut value_set_cols = 0usize;
    let mut value_set_entries = 0usize;
    let mut value_set_bytes = 0usize;
    let mut zone_map_bytes = 0usize;
    let mut sizes_bytes = 0usize;

    for rg in &parsed.row_groups {
        for filter in rg.bloom_filters.iter().flatten() {
            bloom_count += 1;
            bloom_bytes += filter.heap_bytes();
        }
        for set in rg.value_sets.iter().flatten() {
            value_set_cols += 1;
            for value in set {
                value_set_entries += 1;
                value_set_bytes += std::mem::size_of::<taibai::types::scalar::ScalarValue>()
                    + value.heap_bytes();
            }
        }
        zone_map_bytes += rg.zone_maps.len()
            * (std::mem::size_of::<taibai::storage::partition::pruning::ZoneMap>()
                + 2 * std::mem::size_of::<taibai::types::scalar::ScalarValue>());
        sizes_bytes += (rg.column_compressed_sizes.len()
            + rg.compressed_sizes.len()
            + rg.uncompressed_sizes.len())
            * 8
            + rg.column_crcs.len() * 4
            + rg.col_sums.len() * 8;
    }

    let mib = |bytes: usize| bytes as f64 / (1024.0 * 1024.0);
    println!("segment        : {}", segment.display());
    println!("footer bytes   : {:.2} MiB", mib(footer_len));
    println!("row groups     : {rg_count}");
    println!("columns        : {col_count}");
    println!("rows           : {}", parsed.row_count);
    println!("avg rows/rg    : {}", parsed.row_count as usize / rg_count.max(1));
    println!("--- parsed heap (approx) ---");
    println!(
        "bloom          : {bloom_count} filters, {:.2} MiB",
        mib(bloom_bytes)
    );
    println!(
        "value sets     : {value_set_cols} columns, {value_set_entries} values, {:.2} MiB",
        mib(value_set_bytes)
    );
    println!("zone maps      : {:.2} MiB", mib(zone_map_bytes));
    println!("sizes+crc+sums : {:.2} MiB", mib(sizes_bytes));
    if rg_count > 0 {
        let uncompressed: u64 = parsed
            .row_groups
            .iter()
            .flat_map(|rg| rg.uncompressed_sizes.iter())
            .sum();
        let compressed: u64 = parsed
            .row_groups
            .iter()
            .flat_map(|rg| rg.compressed_sizes.iter())
            .sum();
        println!("--- per row group ---");
        println!("footer bytes   : {:.1} KiB", mib(footer_len) * 1024.0 / rg_count as f64);
        println!(
            "value sets     : {:.1} KiB",
            mib(value_set_bytes) * 1024.0 / rg_count as f64
        );
        println!(
            "uncompressed   : {:.1} KiB / rg (total {:.2} GiB)",
            uncompressed as f64 / 1024.0 / rg_count as f64,
            uncompressed as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        println!(
            "compressed     : {:.1} KiB / rg (total {:.2} GiB)",
            compressed as f64 / 1024.0 / rg_count as f64,
            compressed as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        println!(
            "metadata/row   : {:.1} B heap per row (parsed)",
            (zone_map_bytes + sizes_bytes + bloom_bytes + value_set_bytes) as f64
                / parsed.row_count as f64
        );
    }

    // --- resident index (what a reader keeps after the lazy-footer change) ---
    let index = parse_segment_footer_index(&sidecar.footer).expect("parse footer index");
    assert_eq!(
        index.num_row_groups(),
        rg_count,
        "eager and lazy parses disagree on the row-group count"
    );
    let mut probe = RowGroupMeta::empty();
    for i in 0..rg_count {
        index
            .decode_row_group_into(&sidecar.footer, i, &mut probe)
            .unwrap_or_else(|err| panic!("decode row group {i}: {err}"));
        assert_eq!(
            probe.row_count, parsed.row_groups[i].row_count,
            "row group {i}: lazy decode disagrees on row count"
        );
        assert_eq!(
            probe.offset, parsed.row_groups[i].offset,
            "row group {i}: lazy decode disagrees on file offset"
        );
        assert_eq!(
            probe.zone_maps.len(),
            parsed.row_groups[i].zone_maps.len(),
            "row group {i}: lazy decode disagrees on column count"
        );
    }

    let eager_heap = taibai::storage::segment::row_groups_heap_bytes(&parsed.row_groups)
        + taibai::storage::segment::schema_heap_bytes(&parsed.schema)
        + taibai::storage::segment::zone_maps_heap_bytes(&parsed.segment_zone_maps)
        + parsed.delete_bitmap.capacity() as u64;
    let index_heap = index.heap_bytes();
    println!("--- resident index (lazy footer) ---");
    println!("eager heap     : {:.2} MiB", mib(eager_heap as usize));
    println!(
        "index heap     : {:.2} MiB ({} B/row group)",
        mib(index_heap as usize),
        index_heap / (rg_count.max(1) as u64)
    );
    println!(
        "reduction      : {:.1}x",
        eager_heap as f64 / index_heap.max(1) as f64
    );
    println!(
        "per row group  : {:.1} KiB eager vs {:.1} B index",
        eager_heap as f64 / 1024.0 / rg_count.max(1) as f64,
        index_heap as f64 / rg_count.max(1) as f64
    );
}

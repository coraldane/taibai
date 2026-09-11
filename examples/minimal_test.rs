fn main() {
    eprintln!("Starting benchmark...");

    let start = std::time::Instant::now();

    let mut data = Vec::with_capacity(100_000);
    for i in 0..100_000 {
        data.push(i as i64);
    }

    let elapsed = start.elapsed();
    eprintln!("Created {} i64 values in {:?}", data.len(), elapsed);

    let compressed = zstd::bulk::compress(
        &data
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<u8>>(),
        1,
    )
    .unwrap();
    eprintln!(
        "Compressed to {} bytes in {:?}",
        compressed.len(),
        start.elapsed() - elapsed
    );

    eprintln!("Benchmark complete!");
}

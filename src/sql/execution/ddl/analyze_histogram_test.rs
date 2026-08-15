use super::{AnalyzeColumnCollector, ANALYZE_HISTOGRAM_BUCKETS, ANALYZE_HISTOGRAM_SAMPLE_SIZE};
use crate::types::scalar::ScalarValue;

#[test]
fn histogram_sampling_and_bucket_count_are_bounded() {
    let mut collector = AnalyzeColumnCollector::new();
    for value in 0..10_000i64 {
        collector.non_null_count += 1;
        collector.sample_value(ScalarValue::Int64(Some(value)));
    }
    assert_eq!(collector.samples.len(), ANALYZE_HISTOGRAM_SAMPLE_SIZE);

    let statistics = collector.finish();
    assert!(statistics.histogram.len() <= ANALYZE_HISTOGRAM_BUCKETS);
    assert_eq!(
        statistics
            .histogram
            .iter()
            .map(|bucket| bucket.sample_count as usize)
            .sum::<usize>(),
        ANALYZE_HISTOGRAM_SAMPLE_SIZE
    );
}

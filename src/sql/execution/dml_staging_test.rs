use super::*;

#[test]
fn watermark_is_opaque_and_defaults_empty() {
    let watermark = ImageWatermark::default();
    assert!(watermark.as_slice().is_empty());
}

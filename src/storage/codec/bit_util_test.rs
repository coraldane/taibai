use super::*;

#[test]
fn bit_roundtrip_mixed() {
    let mut w = BitWriter::with_capacity(4);
    w.write_bit(true);
    w.write_bit(false);
    w.write_bits(0b1011_0110, 8);
    w.write_bits(u64::MAX, 64);
    w.write_bits(1, 3);
    let bytes = w.finish();

    let mut r = BitReader::new(&bytes);
    assert!(r.read_bit().unwrap());
    assert!(!r.read_bit().unwrap());
    assert_eq!(r.read_bits(8).unwrap(), 0b1011_0110);
    assert_eq!(r.read_bits(64).unwrap(), u64::MAX);
    assert_eq!(r.read_bits(3).unwrap(), 1);
}

#[test]
fn bit_reader_reports_exhaustion() {
    let mut w = BitWriter::with_capacity(1);
    w.write_bits(0b1010_1010, 8);
    let bytes = w.finish();
    assert_eq!(bytes.len(), 1);
    let mut r = BitReader::new(&bytes);
    for _ in 0..8 {
        assert!(r.read_bit().is_ok());
    }
    assert!(r.read_bit().is_err());
}

#[test]
fn partial_byte_is_flushed() {
    let mut w = BitWriter::with_capacity(1);
    w.write_bits(0b1, 1);
    let bytes = w.finish();
    assert_eq!(bytes.len(), 1);
    assert_eq!(bytes[0], 1);
}

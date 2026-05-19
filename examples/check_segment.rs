use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args.get(1).cloned().unwrap_or("main/benchmark_test/seg_00001.dat".to_string());
    
    let mut file = File::open(&path).unwrap();
    let file_len = file.metadata().unwrap().len();
    
    println!("File: {} ({} bytes)", path, file_len);
    
    let mut header = [0u8; 32];
    file.read_exact(&mut header).unwrap();
    println!("First 32 bytes: {}", hex_dump(&header));
    
    file.seek(SeekFrom::End(-8)).unwrap();
    let mut footer_offset_buf = [0u8; 8];
    file.read_exact(&mut footer_offset_buf).unwrap();
    let footer_offset = u64::from_le_bytes(footer_offset_buf);
    println!("Footer offset: {}", footer_offset);
    
    file.seek(SeekFrom::Start(footer_offset)).unwrap();
    let mut footer_header = [0u8; 32];
    file.read_exact(&mut footer_header).unwrap();
    println!("Footer first 32 bytes: {}", hex_dump(&footer_header));
    
    let magic = u32::from_le_bytes(footer_header[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(footer_header[4..8].try_into().unwrap());
    println!("Magic: 0x{:08X} (expected: 0x54414948)", magic);
    println!("Version: {}", version);
}

fn hex_dump(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(" ")
}
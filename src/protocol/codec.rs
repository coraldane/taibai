use bytes::{Bytes, BytesMut, Buf};
use tokio_util::codec::{Decoder, Encoder};

use crate::common::error::{MonaxError, Result};
use crate::protocol::message::{Message, MessageHeader, HEADER_SIZE, MSG_TYPE_QUERY};

pub struct MessageCodec {
    max_message_size: usize,
    compress_threshold: usize,
}

impl MessageCodec {
    pub fn new(max_message_size: usize) -> Self {
        Self {
            max_message_size,
            compress_threshold: 4096,
        }
    }

    pub fn with_compression(mut self, threshold: usize) -> Self {
        self.compress_threshold = threshold;
        self
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = MonaxError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }

        let header_bytes: [u8; HEADER_SIZE] = src[..HEADER_SIZE].try_into().unwrap();
        let header = MessageHeader::decode(&header_bytes)?;

        let payload_len = header.payload_len as usize;
        if payload_len > self.max_message_size {
            return Err(MonaxError::Protocol(format!(
                "payload length {payload_len} exceeds max {}",
                self.max_message_size
            )));
        }

        let total_len = HEADER_SIZE + payload_len;
        if src.len() < total_len {
            src.reserve(total_len - src.len());
            return Ok(None);
        }

        src.advance(HEADER_SIZE);
        let payload = if payload_len > 0 {
            let raw = src.split_to(payload_len);
            if raw.len() >= 2 && raw[0] == 0x78 {
                match crate::protocol::compress::zlib_decompress(&raw) {
                    Ok(decompressed) => Bytes::from(decompressed),
                    Err(_) => raw.freeze(),
                }
            } else {
                raw.freeze()
            }
        } else {
            Bytes::new()
        };

        Ok(Some(Message {
            msg_type: header.msg_type,
            payload,
        }))
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = MonaxError;

    fn encode(&mut self, msg: Message, dst: &mut BytesMut) -> Result<()> {
        let is_bulk = msg.msg_type == 0x20 || msg.msg_type == 0x21 || msg.msg_type == 0x23;
        let is_query = msg.msg_type == MSG_TYPE_QUERY;
        let should_compress = !is_bulk && !is_query && msg.payload.len() > self.compress_threshold;

        let payload = if should_compress {
            match crate::protocol::compress::zlib_compress(&msg.payload) {
                Ok(compressed) => {
                    tracing::trace!(
                        original = msg.payload.len(),
                        compressed = compressed.len(),
                        "payload compressed"
                    );
                    Bytes::from(compressed)
                }
                Err(_) => msg.payload,
            }
        } else {
            msg.payload
        };

        let header = MessageHeader {
            msg_type: msg.msg_type,
            payload_len: payload.len() as u32,
        };

        let header_bytes = header.encode();
        dst.reserve(HEADER_SIZE + payload.len());
        dst.extend_from_slice(&header_bytes);
        dst.extend_from_slice(&payload);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_small_payload() {
        let mut codec = MessageCodec::new(1024 * 1024);
        let msg = Message::new(0x01, Bytes::from("hello"));
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.msg_type, 0x01);
        assert_eq!(&decoded.payload[..], b"hello");
    }

    #[test]
    fn test_encode_decode_large_payload_compressed() {
        let mut codec = MessageCodec::new(1024 * 1024).with_compression(100);
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let msg = Message::new(0x02, Bytes::from(data));
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.msg_type, 0x02);
        assert_eq!(decoded.payload.len(), 10000);
    }

    #[test]
    fn test_decode_incomplete() {
        let mut codec = MessageCodec::new(1024 * 1024);
        let mut buf = BytesMut::from(&[0x01, 0xDB, 0x01, 0x00, 0x00, 0x00][..]);
        let result = codec.decode(&mut buf).unwrap();
        assert!(result.is_none());
    }
}

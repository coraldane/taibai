use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::common::error::{Result, TaibaiError};
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
    type Error = TaibaiError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }

        let header_bytes: [u8; HEADER_SIZE] = src[..HEADER_SIZE].try_into().unwrap();
        let header = MessageHeader::decode(&header_bytes)?;

        let payload_len = header.payload_len as usize;
        if payload_len > self.max_message_size {
            return Err(TaibaiError::Protocol(format!(
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
            if raw.len() >= 8 && raw[0] == 0x78 {
                match crate::protocol::compress::zlib_decompress_complete(&raw) {
                    Some(decompressed) => Bytes::from(decompressed),
                    None => raw.freeze(),
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
    type Error = TaibaiError;

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
#[path = "codec_test.rs"]
mod tests;

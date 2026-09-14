use crate::common::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingType {
    Plain,
    Rle,
    Dictionary,
    Delta,
    DeltaOfDelta,
    BitPacked,
    GorillaXor,
    DeltaRle,
}

impl EncodingType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(EncodingType::Plain),
            1 => Ok(EncodingType::Rle),
            2 => Ok(EncodingType::Dictionary),
            3 => Ok(EncodingType::Delta),
            4 => Ok(EncodingType::DeltaOfDelta),
            5 => Ok(EncodingType::BitPacked),
            6 => Ok(EncodingType::GorillaXor),
            7 => Ok(EncodingType::DeltaRle),
            _ => Err(crate::common::error::TaibaiError::Storage(format!(
                "unknown encoding type: {}",
                v
            ))),
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            EncodingType::Plain => 0,
            EncodingType::Rle => 1,
            EncodingType::Dictionary => 2,
            EncodingType::Delta => 3,
            EncodingType::DeltaOfDelta => 4,
            EncodingType::BitPacked => 5,
            EncodingType::GorillaXor => 6,
            EncodingType::DeltaRle => 7,
        }
    }
}

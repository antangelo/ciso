use std::fmt::{Display, Debug};

use super::util;
use arbitrary_int::u31;
use bitbybit::bitfield;

const CISO_MAGIC: u32 = 0x4F534943;

#[repr(C)]
#[repr(packed)]
#[derive(Debug, Clone, Copy)]
pub struct CSOHeader {
    pub magic: u32,
    pub header_size: u32,
    pub uncompressed_size: u64,
    pub block_size: u32,
    pub version: u8,
    pub alignment: u8,
    pub reserved0: u8,
    pub reserved1: u8,
}

#[derive(Clone, Debug)]
pub enum Error<E> {
    UnsupportedVersion,
    InvalidHeader,
    Other(E),
}

impl<E> From<E> for Error<E> {
    fn from(value: E) -> Self {
        Self::Other(value)
    }
}

impl<E: Display> Display for Error<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion => write!(f, "Unsupported CSO version"),
            Self::InvalidHeader => write!(f, "Invalid CSO header"),
            Self::Other(e) => e.fmt(f),
        }
    }
}

impl<E: Display + Debug> std::error::Error for Error<E> {}

#[bitfield(u32, default: 0)]
pub struct IndexTableEntry {
    #[bits(0..=30, rw)]
    pub position: u31,

    #[bit(31, rw)]
    pub compression_type: bool,
}

impl CSOHeader {
    fn deserialize_unchecked(header: &[u8; 24]) -> CSOHeader {
        let magic = util::deserialize_u32_le(&header[0..4]);
        let header_size = util::deserialize_u32_le(&header[4..8]);
        let uncompressed_size = util::deserialize_u64_le(&header[8..16]);
        let block_size = util::deserialize_u32_le(&header[16..20]);
        let version = header[20];
        let alignment = header[21];
        let reserved0 = header[22];
        let reserved1 = header[23];

        CSOHeader {
            magic,
            header_size,
            uncompressed_size,
            block_size,
            version,
            alignment,
            reserved0,
            reserved1,
        }
    }

    pub fn deserialize<E>(header: &[u8; 24]) -> Result<CSOHeader, Error<E>> {
        let header = Self::deserialize_unchecked(header);

        if header.version != 2 {
            return Err(Error::UnsupportedVersion);
        }

        if header.magic != CISO_MAGIC || header.header_size != 24 {
            return Err(Error::InvalidHeader);
        }

        Ok(header)
    }

    pub fn serialize(&self) -> [u8; 24] {
        let mut out = [0; 24];
        util::serialize_u32_le(self.magic, &mut out[0..4]);
        util::serialize_u32_le(self.header_size, &mut out[4..8]);
        util::serialize_u64_le(self.uncompressed_size, &mut out[8..16]);
        util::serialize_u32_le(self.block_size, &mut out[16..20]);
        out[20] = self.version;
        out[21] = self.alignment;
        out[22] = self.reserved0;
        out[23] = self.reserved1;

        out
    }

    pub fn new() -> Self {
        Self {
            magic: CISO_MAGIC,
            header_size: 24,
            uncompressed_size: 0,
            block_size: 2048,
            version: 2,
            alignment: 2,
            reserved0: 0,
            reserved1: 0,
        }
    }

    pub fn index_table_len(&self) -> usize {
        (self.uncompressed_size / self.block_size as u64) as usize + 1
    }
}

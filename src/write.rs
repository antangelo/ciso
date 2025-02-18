use maybe_async::maybe_async;
use std::{
    fmt::{Debug, Display},
    io::Write,
};

use crate::{index, layout};
use arbitrary_int::u31;

#[derive(Debug)]
pub enum CSOCreationError<ReadError, WriteError> {
    LZ4Error(lz4_flex::frame::Error),
    CompressionError(std::io::Error),
    ReadError(ReadError),
    WriteError(WriteError),
}

impl<RE: Display, WE: Display> Display for CSOCreationError<RE, WE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LZ4Error(e) => Display::fmt(e, f),
            Self::CompressionError(e) => Display::fmt(e, f),
            Self::ReadError(e) => e.fmt(f),
            Self::WriteError(e) => e.fmt(f),
        }
    }
}

impl<RE: Display + Debug, WE: Display + Debug> std::error::Error for CSOCreationError<RE, WE> {}

#[non_exhaustive]
pub enum ProgressInfo {
    SectorCount(usize),
    SectorFinished,
    Finished,
}

#[maybe_async]
async fn write_ciso_data<I: SectorReader, O: AsyncWriter>(
    input: &mut I,
    output: &mut O,
    header: &layout::CSOHeader,
    index_table: &mut index::IndexTable,
    mut progress_callback: impl FnMut(ProgressInfo),
) -> Result<(), CSOCreationError<I::ReadError, O::WriteError>> {
    let mut position: u64 = 24 + 4 * index_table.len() as u64;
    let cfg = lz4_flex::frame::FrameInfo::new()
        .block_mode(lz4_flex::frame::BlockMode::Independent)
        .block_size(lz4_flex::frame::BlockSize::Max64KB)
        .content_checksum(false)
        .block_checksums(false)
        .legacy_frame(true)
        .content_size(None);

    let align_b = 1 << header.alignment;
    let align_m = align_b - 1;

    for sector in 0..(index_table.len() - 1) {
        let align = position & (align_m as u64);
        if align != 0 {
            let align = (align_b as u64) - align;
            let align_bytes = vec![0; align as usize];
            output
                .atomic_write(position, &align_bytes)
                .await
                .map_err(CSOCreationError::WriteError)?;
            position += align;
        }

        let data = input
            .read_sector(sector, header.block_size)
            .await
            .map_err(CSOCreationError::ReadError)?;

        let mut data_compressed =
            lz4_flex::frame::FrameEncoder::with_frame_info(cfg.clone(), Vec::new());
        data_compressed
            .write_all(&data)
            .map_err(CSOCreationError::CompressionError)?;
        let data_compressed = data_compressed
            .finish()
            .map_err(CSOCreationError::LZ4Error)?;

        // Strip header and footer
        let data_compressed = &data_compressed[7..(data_compressed.len() - 4)];

        let compressed_len = data_compressed.len();
        let is_compressed = compressed_len + 12 < data.len();

        index_table[sector] = layout::IndexTableEntry::default()
            .with_position(u31::new((position >> header.alignment) as u32))
            .with_compression_type(is_compressed);

        let data = if is_compressed {
            data_compressed
        } else {
            &data
        };
        output
            .atomic_write(position, data)
            .await
            .map_err(CSOCreationError::WriteError)?;
        position += data.len() as u64;

        progress_callback(ProgressInfo::SectorFinished);
    }

    let index_table_len = index_table.len();
    index_table[index_table_len - 1] = layout::IndexTableEntry::default()
        .with_position(u31::new((position >> header.alignment) as u32));
    progress_callback(ProgressInfo::SectorFinished);

    Ok(())
}

#[maybe_async]
pub async fn write_ciso_image<I: SectorReader, O: AsyncWriter>(
    input: &mut I,
    output: &mut O,
    mut progress_callback: impl FnMut(ProgressInfo),
) -> Result<(), CSOCreationError<I::ReadError, O::WriteError>> {
    let header = {
        let mut header = layout::CSOHeader::new();
        header.uncompressed_size = input
            .size()
            .await
            .map_err(CSOCreationError::ReadError)?;
        header
    };
    let mut index_table = index::IndexTable::new(&header);
    progress_callback(ProgressInfo::SectorCount(index_table.len()));

    output
        .atomic_write(0, &header.serialize())
        .await
        .map_err(CSOCreationError::WriteError)?;
    write_ciso_data(
        input,
        output,
        &header,
        &mut index_table,
        &mut progress_callback,
    )
    .await?;

    let index_table_data = index_table.serialize();
    assert_eq!(index_table_data.len(), index_table.len() * 4);
    output
        .atomic_write(24, &index_table_data)
        .await
        .map_err(CSOCreationError::WriteError)?;

    progress_callback(ProgressInfo::Finished);

    Ok(())
}

#[maybe_async]
pub trait AsyncWriter: Send + Sync {
    type WriteError;
    async fn atomic_write(&mut self, position: u64, data: &[u8]) -> Result<(), Self::WriteError>;
}

#[maybe_async]
impl<T> AsyncWriter for T
where
    T: std::io::Write + std::io::Seek + Send + Sync,
{
    type WriteError = std::io::Error;

    async fn atomic_write(&mut self, position: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.seek(std::io::SeekFrom::Start(position))?;
        self.write_all(data)?;
        Ok(())
    }
}

#[maybe_async]
pub trait SectorReader: Send + Sync {
    type ReadError;

    async fn size(&mut self) -> Result<u64, Self::ReadError>;
    async fn read_sector(
        &mut self,
        sector: usize,
        sector_size: u32,
    ) -> Result<Vec<u8>, Self::ReadError>;
}

#[maybe_async]
impl<T> SectorReader for T
where
    T: std::io::Read + std::io::Seek + Send + Sync,
{
    type ReadError = std::io::Error;

    async fn size(&mut self) -> Result<u64, std::io::Error> {
        self.seek(std::io::SeekFrom::End(0))
    }

    async fn read_sector(
        &mut self,
        sector: usize,
        sector_size: u32,
    ) -> Result<Vec<u8>, std::io::Error> {
        let pos = (sector as u64) * (sector_size as u64);
        self.seek(std::io::SeekFrom::Start(pos))?;

        let mut buf: Vec<u8> = vec![0; sector_size as usize];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }
}

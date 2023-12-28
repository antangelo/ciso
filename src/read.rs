use crate::{index, layout};
use maybe_async::maybe_async;

const LZ4_HEADER: &[u8] = &[0x4, 0x22, 0x4d, 0x18, 0x60, 0x40, 0x82];

/// Asynchronous read interface
#[maybe_async]
pub trait Read<E>: Send + Sync {
    async fn size(&mut self) -> Result<u64, E>;

    /// Read bytes from a given position
    async fn read(&mut self, pos: u64, buf: &mut [u8]) -> Result<(), E>;
}

#[maybe_async]
impl<T: std::io::Read + std::io::Seek + Send + Sync> Read<std::io::Error> for T {
    async fn size(&mut self) -> Result<u64, std::io::Error> {
        self.seek(std::io::SeekFrom::End(0))
    }

    async fn read(&mut self, pos: u64, buf: &mut [u8]) -> Result<(), std::io::Error> {
        self.seek(std::io::SeekFrom::Start(pos))?;
        std::io::Read::read(self, buf)?;
        Ok(())
    }
}

#[maybe_async]
impl<E> Read<E> for Box<dyn Read<E>> {
    async fn size(&mut self) -> Result<u64, E> {
        self.as_mut().size().await
    }

    async fn read(&mut self, pos: u64, buf: &mut [u8]) -> Result<(), E> {
        self.as_mut().read(pos, buf).await
    }
}

pub struct CSOReader<E, R: Read<E>> {
    read: R,
    header: layout::CSOHeader,
    index_table: index::IndexTable,

    err_t: core::marker::PhantomData<E>,
}

impl<E, R: Read<E>> CSOReader<E, R> {
    #[maybe_async]
    pub async fn new(mut read: R) -> Result<CSOReader<E, R>, layout::Error<E>> {
        let mut header = [0; 24];
        read.read(0, &mut header).await?;
        let header = layout::CSOHeader::deserialize(&header)?;

        let mut index_table = vec![0; header.index_table_len() * 4];
        read.read(24, &mut index_table).await?;
        let index_table = index::IndexTable::deserialize(index_table);

        Ok(Self {
            read,
            header,
            index_table,
            err_t: core::marker::PhantomData,
        })
    }

    pub fn file_size(&self) -> u64 {
        self.header.uncompressed_size
    }

    #[maybe_async]
    pub async fn read_offset(&mut self, pos: u64, buf: &mut [u8]) -> Result<(), layout::Error<E>> {
        let mut sector = pos / (self.header.block_size as u64);
        let position = pos % (self.header.block_size as u64);
        let mut position = position as usize;

        let mut len_remaining = buf.len();
        let mut buf_pos = 0;

        while len_remaining > 0 {
            let index_entry = self.index_table[sector as usize];
            let sector_pos = index_entry.position();
            let data_len = self.index_table[(sector + 1) as usize].position() - sector_pos;
            let sector_pos: u32 = sector_pos.into();
            let sector_pos = (sector_pos as u64) << self.header.alignment;
            let data_len: u32 = data_len.into();
            let data_len = data_len << self.header.alignment;

            if !index_entry.compression_type() {
                let to_read = core::cmp::min(len_remaining, data_len as usize);
                self.read
                    .read(sector_pos, &mut buf[buf_pos..(buf_pos + to_read)])
                    .await?;
                buf_pos += to_read;
                len_remaining -= to_read;
            } else {
                let mut data = vec![0; data_len as usize + 4 + 7];
                data[0..7].copy_from_slice(LZ4_HEADER);
                self.read
                    .read(sector_pos, &mut data[7..(7 + data_len as usize)])
                    .await?;

                use std::io::Read;
                let mut lz4 = lz4_flex::frame::FrameDecoder::new(data.as_slice());
                let mut data = vec![];
                let read = lz4.read_to_end(&mut data).unwrap();
                assert_eq!(read, 2048);

                let to_read = core::cmp::min(len_remaining, data.len());
                let data = &data[position..(position + to_read)];
                buf[buf_pos..(buf_pos + to_read)].copy_from_slice(data);
                buf_pos += to_read;
                len_remaining -= to_read;
            }

            position = 0;
            sector += 1;
        }

        Ok(())
    }
}

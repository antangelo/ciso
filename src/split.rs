use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
};

use maybe_async::maybe_async;

use crate::write::AsyncWriter;

const FILE_SPLIT_POINT: u64 = 0xffbf6000;

#[maybe_async]
pub trait SplitFilesystem<E, H: AsyncWriter<WriteError = E>>: Send + Sync {
    async fn create_file(&mut self, name: &OsStr) -> Result<H, E>;
    async fn close(&mut self, file: H);
}

pub struct SplitOutput<E: Send + Sync, H: AsyncWriter<WriteError = E>, S: SplitFilesystem<E, H>> {
    fs: S,
    file_name: std::path::PathBuf,
    splits: std::collections::BTreeMap<u64, H>,

    err_t: core::marker::PhantomData<E>,
}

impl<E, H, S> SplitOutput<E, H, S>
where
    H: AsyncWriter<WriteError = E>,
    S: SplitFilesystem<E, H>,
    E: Send + Sync,
{
    pub fn new(fs: S, file_name: std::path::PathBuf) -> Self {
        Self {
            fs,
            file_name,
            splits: std::collections::BTreeMap::new(),
            err_t: core::marker::PhantomData,
        }
    }

    fn split_name(&self, index: u64) -> OsString {
        self.file_name
            .with_extension(format!("{}.cso", index + 1))
            .file_name()
            .unwrap()
            .to_os_string()
    }

    #[maybe_async]
    async fn handle_for_position(&mut self, position: u64) -> Result<&mut H, E> {
        let index = position / FILE_SPLIT_POINT;

        if self.splits.contains_key(&index) {
            return Ok(self.splits.get_mut(&index).unwrap());
        }

        let file = self.split_name(index);
        let file = self.fs.create_file(&file).await?;
        self.splits.insert(index, file);
        Ok(self.splits.get_mut(&index).unwrap())
    }

    #[maybe_async]
    pub async fn close(mut self) {
        for (_, writer) in self.splits.into_iter() {
            self.fs.close(writer).await;
        }
    }
}

#[maybe_async]
impl<E, H, S> AsyncWriter for SplitOutput<E, H, S>
where
    H: AsyncWriter<WriteError = E>,
    S: SplitFilesystem<E, H>,
    E: Send + Sync,
{
    type WriteError = E;

    async fn atomic_write(&mut self, position: u64, data: &[u8]) -> Result<(), E> {
        let mut written = 0;

        while written < data.len() {
            let handle = self.handle_for_position(position + written as u64).await?;
            let bytes_to_split = (position + written as u64) % FILE_SPLIT_POINT;
            let bytes_to_split = if bytes_to_split == 0 {
                FILE_SPLIT_POINT
            } else {
                bytes_to_split
            };
            let to_write = core::cmp::min((data.len() - written) as u64, bytes_to_split);
            assert_ne!(to_write, 0);

            handle
                .atomic_write(
                    position + written as u64,
                    &data[written..(written + to_write as usize)],
                )
                .await?;
            written += to_write as usize;
        }

        Ok(())
    }
}

pub struct SplitFileReader<E, R: crate::read::Read<ReadError = E>> {
    files: BTreeMap<u64, R>,
    last_position: u64,

    err_t: core::marker::PhantomData<E>,
}

impl<E, R: crate::read::Read<ReadError = E>> SplitFileReader<E, R> {
    #[maybe_async]
    pub async fn new(readers: Vec<R>) -> Result<SplitFileReader<E, R>, E> {
        let mut position = 0;
        let mut files = BTreeMap::new();

        for mut reader in readers {
            let size = reader.size().await?;
            files.insert(position, reader);
            position += size;
        }

        Ok(Self {
            files,
            last_position: position,
            err_t: core::marker::PhantomData,
        })
    }
}

#[maybe_async]
impl<E: Send + Sync, R: crate::read::Read<ReadError = E>> crate::read::Read
    for SplitFileReader<E, R>
{
    type ReadError = E;

    async fn size(&mut self) -> Result<u64, E> {
        Ok(match self.files.last_entry() {
            Some(mut entry) => *entry.key() + entry.get_mut().size().await?,
            None => 0,
        })
    }

    async fn read(&mut self, pos: u64, buf: &mut [u8]) -> Result<(), E> {
        let mut bytes_read = 0;

        while bytes_read < buf.len() {
            let pos = pos + bytes_read as u64;

            let next_pos = match self.files.range((pos + 1)..).next() {
                Some((pos, _)) => *pos,
                None => self.last_position,
            };

            let mut handle_iter = self.files.range_mut(..=pos);
            // As long as the B-tree is not empty, this should never fail
            let handle = handle_iter.next_back().unwrap();

            assert!(next_pos > *handle.0);
            let handle_size = next_pos - handle.0;

            let handle_offset = pos - handle.0;
            let handle: &mut R = handle.1;

            assert!(handle_size > handle_offset);
            let bytes_to_split = handle_size - handle_offset;

            let to_read = core::cmp::min(buf.len() - bytes_read, bytes_to_split as usize);
            assert_ne!(to_read, 0);

            handle
                .read(pos, &mut buf[bytes_read..(bytes_read + to_read)])
                .await?;
            bytes_read += to_read;
        }

        Ok(())
    }
}

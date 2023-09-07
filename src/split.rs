use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
};

use maybe_async::maybe_async;

use crate::write::AsyncWriter;

const FILE_SPLIT_POINT: u64 = 0xffbf6000;

#[maybe_async(?Send)]
pub trait SplitFilesystem<E, H: AsyncWriter<E>> {
    async fn create_file(&mut self, name: &OsStr) -> Result<H, E>;
    async fn close(&mut self, file: H);
}

pub struct SplitOutput<E, H: AsyncWriter<E>, S: SplitFilesystem<E, H>> {
    fs: S,
    file_name: std::path::PathBuf,
    splits: std::collections::BTreeMap<u64, H>,

    err_t: core::marker::PhantomData<E>,
}

impl<E, H, S> SplitOutput<E, H, S>
where
    H: AsyncWriter<E>,
    S: SplitFilesystem<E, H>,
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

    #[maybe_async(?Send)]
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

    #[maybe_async(?Send)]
    pub async fn close(mut self) {
        for (_, writer) in self.splits.into_iter() {
            self.fs.close(writer).await;
        }
    }
}

#[maybe_async(?Send)]
impl<E, H, S> AsyncWriter<E> for SplitOutput<E, H, S>
where
    H: AsyncWriter<E>,
    S: SplitFilesystem<E, H>,
{
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

pub struct SplitFileReader<E, R: crate::read::Read<E>> {
    files: BTreeMap<u64, R>,
    last_position: u64,

    err_t: core::marker::PhantomData<E>,
}

impl<E, R: crate::read::Read<E>> SplitFileReader<E, R> {
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

#[maybe_async(?Send)]
impl<E, R: crate::read::Read<E>> crate::read::Read<E> for SplitFileReader<E, R> {
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

            let (pos, to_read) = if handle_size > handle_offset {
                let bytes_to_split = handle_size - handle_offset;
                (
                    pos,
                    core::cmp::min(buf.len() - bytes_read, bytes_to_split as usize),
                )
            } else {
                // If the handle is out of range, read over the handle size so it throws an
                // error. The alternative is wrapping this in its own error enum, but that
                // likely overlaps with the Reader's own error type.
                (2 * handle_size, buf.len() - bytes_read)
            };

            assert_ne!(to_read, 0);

            handle
                .read(pos, &mut buf[bytes_read..(bytes_read + to_read)])
                .await?;
            bytes_read += to_read;
        }

        Ok(())
    }
}

use std::ops::Range;

use crate::medium::{file, mapping, Bytes, BytesMut, File};

/// Represents the fundamental unit of data storage in ThetaDB.
///
/// # Safety
///
/// Page will map directly to memory, so you need to ensure that the layout
/// of the data structures inside the Page is safe and aligned.
pub(crate) unsafe trait Page<B>: Sized
where
    B: Bytes,
{
    fn from_bytes(bytes: B) -> mapping::Result<Self>;
}

/// A unique identifier for a page in ThetaDB.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct PageId(u32);

impl PageId {
    #[inline]
    pub(crate) const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub(crate) const fn raw(&self) -> u32 {
        self.0
    }

    #[inline]
    pub(crate) fn from_bytes(bytes: &[u8]) -> mapping::Result<Self> {
        bytes
            .try_into()
            .map(u32::from_le_bytes)
            .map(Self)
            .map_err(|_| mapping::Error::Size)
    }

    #[inline]
    pub(crate) fn to_bytes(self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }

    /// Increments the page id by one.
    #[inline]
    pub(crate) fn incr(&mut self) -> PageId {
        let res = *self;
        self.0 += 1;
        res
    }
}

impl From<u32> for PageId {
    #[inline]
    fn from(value: u32) -> Self {
        Self(value)
    }
}

/// Represents the storage system of the ThetaDB and is responsible for managing the data
/// storage file.
pub(crate) struct Storage {
    file: File,
    page_size: u32,
}

impl Storage {
    #[inline]
    pub(crate) fn new(file: File, page_size: u32) -> Self {
        Self { file, page_size }
    }

    #[inline]
    pub(crate) fn page_size(&self) -> u32 {
        self.page_size
    }

    #[inline]
    pub(crate) fn allocate(&mut self, page_count: u32) -> file::Result<()> {
        let len = page_count * self.page_size;
        self.file.allocate(len as usize).map_err(Into::into)
    }

    #[inline]
    pub(crate) fn sync(&mut self) -> file::Result<()> {
        self.file.sync()
    }

    #[inline]
    pub(crate) fn page<'a, T>(&'a self, id: PageId) -> mapping::Result<T>
    where
        T: Page<&'a [u8]>,
    {
        let bytes = self.page_raw(id)?;
        T::from_bytes(bytes)
    }

    #[inline]
    pub(crate) fn page_mut<'a, T>(&'a mut self, id: PageId) -> mapping::Result<T>
    where
        T: Page<&'a mut [u8]>,
    {
        let bytes = self.page_raw_mut(id)?;
        T::from_bytes(bytes)
    }

    #[inline]
    pub(crate) fn copy_page_to_bytes<B>(&self, id: PageId, mut dest: B) -> mapping::Result<()>
    where
        B: BytesMut,
    {
        self.page_raw(id).map(|p| dest.copy_from_slice(p))
    }

    #[inline]
    pub(crate) fn copy_page_from_bytes<B>(&mut self, id: PageId, src: B) -> mapping::Result<()>
    where
        B: Bytes,
    {
        self.page_raw_mut(id).map(|p| p.copy_from_slice(&src))
    }

    #[inline]
    fn page_raw(&self, id: PageId) -> mapping::Result<&[u8]> {
        let range = self.range(id)?;
        Ok(&self.file[range])
    }

    #[inline]
    fn page_raw_mut(&mut self, id: PageId) -> mapping::Result<&mut [u8]> {
        let range = self.range(id)?;
        Ok(&mut self.file[range])
    }

    #[inline]
    fn range(&self, id: PageId) -> mapping::Result<Range<usize>> {
        let offset = id.raw() as usize * self.page_size as usize;
        let range = offset..offset + self.page_size as usize;
        mapping::check_range(&range, &self.file.as_ref())?;
        Ok(range)
    }
}

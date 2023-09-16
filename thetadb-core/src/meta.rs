use std::{
    mem,
    ops::{Deref, DerefMut},
    slice,
};

use thiserror::Error;

use crate::{
    medium::{mapping, os_page_size, Bytes, BytesMut, Mapping},
    storage::{Page, PageId},
};

/// The current format version of the ThetaDB file.
const VERSION: u32 = 1;

/// A special sequence of bytes that is used at the beginning of the ThetaDB file for validation.
const MAGIC: u32 = 0xDB314159;

/// The minimum page size (4 KB) of ThetaDB.
const MIN_PAGE_SIZE: u32 = 4 * 1024;

/// Represents the header of a ThetaDB file.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Header {
    magic: u32,
    version: u32,
    page_size: u32,
}

/// Represents the index of pages in a ThetaDB file.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageIndex {
    /// The id of the B+Tree root page.
    pub(crate) root: PageId,
    /// The id of the free list page.
    pub(crate) freelist: PageId,
    /// The id of the next available page.
    pub(crate) next: PageId,
}

type Checksum = u32;

/// Represents the metadata of the ThetaDB file.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Meta {
    header: Header,
    page_index: PageIndex,
    checksum: Checksum,
}

#[derive(Error, Debug)]
pub(crate) enum ValidationError {
    #[error("the ThetaDB file is invalid")]
    FileInvalid,
    #[error("the file format version is mismatched")]
    VersionMismatched,
    #[error("the metadata checksum is mismatched")]
    ChecksumMismatched,
}

impl Header {
    #[inline]
    fn new(page_size: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            page_size: page_size.max(MIN_PAGE_SIZE),
        }
    }

    /// Validates the header by checking the magic number and version.
    fn validate(&self) -> Result<(), ValidationError> {
        if self.magic != MAGIC {
            Err(ValidationError::FileInvalid)
        } else if self.version != VERSION {
            Err(ValidationError::VersionMismatched)
        } else {
            Ok(())
        }
    }
}

impl PageIndex {
    pub(crate) const META: PageId = PageId::from_raw(0);
    pub(crate) const DEFAULT_ROOT: PageId = PageId::from_raw(1);
    pub(crate) const DEFAULT_FREELIST: PageId = PageId::from_raw(2);
    pub(crate) const DEFAULT_NEXT: PageId = PageId::from_raw(3);

    #[inline]
    pub(crate) fn page_count(&self) -> u32 {
        self.next.raw()
    }
}

impl Default for PageIndex {
    #[inline]
    fn default() -> Self {
        Self {
            root: Self::DEFAULT_ROOT,
            freelist: Self::DEFAULT_FREELIST,
            next: Self::DEFAULT_NEXT,
        }
    }
}

impl Meta {
    pub(crate) const SIZE: usize = mem::size_of::<Self>();

    pub(crate) fn new(page_size: u32) -> Self {
        let header = Header::new(page_size);
        let page_index = PageIndex::default();
        let checksum = Checksum::default();

        let mut meta = Self {
            header,
            page_index,
            checksum,
        };

        meta.check_page_size();
        meta.update_checksum();
        meta
    }

    #[inline]
    pub(crate) fn page_size(&self) -> u32 {
        self.header.page_size
    }

    #[inline]
    pub(crate) fn page_index(&self) -> &PageIndex {
        &self.page_index
    }

    #[inline]
    pub(crate) fn set_page_index(&mut self, page_index: PageIndex) {
        self.page_index = page_index;
        self.update_checksum();
    }

    /// Validates the metadata by validating the header and checking the checksum.
    pub(crate) fn validate(&self) -> Result<(), ValidationError> {
        self.header.validate()?;
        // Validate checksum.
        if self.checksum != self.calc_checksum() {
            Err(ValidationError::ChecksumMismatched)
        } else {
            Ok(())
        }
    }

    /// Updates the checksum of the metadata.
    #[inline]
    fn update_checksum(&mut self) {
        self.checksum = self.calc_checksum();
    }

    /// Calculates a checksum of the metadata using the CRC32 algorithm.
    fn calc_checksum(&self) -> u32 {
        // The byte slice that points to the metadata (without the checksum field).
        let bytes = unsafe {
            // The length of the metadata without the checksum field.
            let len = mem::size_of_val(self) - mem::size_of_val(&self.checksum);
            slice::from_raw_parts(self as *const _ as *const u8, len)
        };
        crc32fast::hash(bytes)
    }

    /// Check page size, prevent data damage in ThetaDB.
    #[inline]
    fn check_page_size(&self) {
        assert!(
            self.header.page_size as usize >= Self::SIZE,
            "page size is too small, should not be smaller than than metadata size"
        );
    }
}

impl Default for Meta {
    #[inline]
    fn default() -> Self {
        // Uses os page size as default.
        Self::new(
            os_page_size()
                .try_into()
                .expect("the page size is too large"),
        )
    }
}

#[repr(transparent)]
pub(crate) struct MetaPage<B>(Mapping<B, Meta>);

unsafe impl<B> Page<B> for MetaPage<B>
where
    B: Bytes,
{
    #[inline]
    fn from_bytes(bytes: B) -> mapping::Result<Self> {
        unsafe { Mapping::new(bytes) }.map(MetaPage)
    }
}

impl<B> Deref for MetaPage<B>
where
    B: Bytes,
{
    type Target = Meta;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<B> DerefMut for MetaPage<B>
where
    B: BytesMut,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{Header, Meta, PageIndex, VERSION};
    use crate::medium::os_page_size;

    #[test]
    fn test_default() {
        let header = Header {
            magic: 0xDB314159,
            version: VERSION,
            page_size: os_page_size() as u32,
        };
        let page_index = PageIndex {
            root: 1.into(),
            freelist: 2.into(),
            next: 3.into(),
        };
        let mut meta = Meta {
            header,
            page_index,
            checksum: 0,
        };
        meta.update_checksum();
        assert_eq!(Meta::default(), meta);
    }
}

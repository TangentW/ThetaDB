use std::mem;

use crate::{
    medium::{mapping, Bytes, BytesMut, Mapping},
    storage::{Page, PageId},
};

/// Represents a chunk of a page chain in ThetaDB.
///
/// A page chain is a sequence of pages that are linked together to store
/// a large piece of data (e.g., Freelist, Overflow page) that cannot fit into a single page.
///
/// # Chunk Page Chain
///
/// ```plain
///           ┌────────────────────────┐           ┌────────────────────────┐        
/// ┌─────┬───┴──┬───────────────────┐ │ ┌─────┬───┴──┬───────────────────┐ │ ┌─────┐
/// │ Len │ Next │      Payload      │ └▶│ Len │ Next │      Payload      │ └▶│ ••• │
/// └─────┴──────┴───────────────────┘   └─────┴──────┴───────────────────┘   └─────┘
/// ```
pub(crate) struct Chunk<B> {
    len: Mapping<B, Len>,
    next: Mapping<B, PageId>,
    body: B,
}

type Len = u32;

/// Indicates that a chunk a overflowed, i.e., the length of the data exceeds the capacity
/// of the chunk. When a chunk is overflowed, the `len` field is set to `LEN_OVERFLOW_FLAG`,
/// and the remaining data is stored in the next chunk in the chain.
const LEN_OVERFLOW_FLAG: Len = Len::MAX;

unsafe impl<B> Page<B> for Chunk<B>
where
    B: Bytes,
{
    #[inline]
    fn from_bytes(bytes: B) -> mapping::Result<Self> {
        assert!(
            bytes.len() < LEN_OVERFLOW_FLAG as usize,
            "page size is too large"
        );

        let (len, remaining) = unsafe { Mapping::split(bytes)? };
        let (next, body) = unsafe { Mapping::split(remaining)? };
        Ok(Self { len, next, body })
    }
}

impl<B> Chunk<B>
where
    B: Bytes,
{
    /// The length of the chunk.
    #[inline]
    pub(crate) fn len(&self) -> u32 {
        if self.is_overflow() {
            self.body.len() as u32
        } else {
            *self.len
        }
    }

    /// The id of the next chunk page in the chain.
    #[inline]
    pub(crate) fn next(&self) -> Option<PageId> {
        self.is_overflow().then(|| *self.next)
    }

    /// The data of the chunk.
    #[inline]
    pub(crate) fn body(&self) -> mapping::Result<&[u8]> {
        let range = ..self.len() as usize;
        mapping::check_range(&range, &self.body)?;
        Ok(&self.body[range])
    }

    /// Checks if the chunk is overflowed.
    #[inline]
    fn is_overflow(&self) -> bool {
        *self.len == LEN_OVERFLOW_FLAG
    }
}

impl<B> Chunk<B>
where
    B: BytesMut,
{
    /// Assign a slice of bytes to the chunk.
    pub(crate) fn assign<'a>(&mut self, slice: &'a [u8]) -> Option<(&'a [u8], &mut PageId)> {
        if slice.len() > self.body.len() {
            *self.len = LEN_OVERFLOW_FLAG;

            let (body, remaining) = slice.split_at(self.body.len());
            self.body.copy_from_slice(body);

            Some((remaining, &mut self.next))
        } else {
            *self.len = slice.len() as u32;

            self.body[..slice.len()].copy_from_slice(slice);
            None
        }
    }
}

impl Chunk<()> {
    /// Reads a page chain into a byte vector.
    pub(crate) fn read<'a, F>(id: PageId, mut obtain: F) -> mapping::Result<Vec<u8>>
    where
        F: FnMut(PageId) -> mapping::Result<Chunk<&'a [u8]>>,
    {
        let (mut res, mut next_id) = (Vec::new(), Some(id));

        while let Some(id) = next_id {
            let chunk = obtain(id)?;
            res.extend_from_slice(chunk.body()?);
            next_id = chunk.next();
        }

        Ok(res)
    }

    /// Writes a byte slice into a page chain.
    pub(crate) fn write<'a, F>(mut slice: &[u8], mut alloc: F) -> mapping::Result<PageId>
    where
        F: FnMut() -> mapping::Result<(PageId, Chunk<&'a mut [u8]>)>,
    {
        let (id, mut chunk) = alloc()?;

        while let Some((remaining, next_id)) = chunk.assign(slice) {
            (*next_id, chunk) = alloc()?;
            slice = remaining;
        }

        Ok(id)
    }

    /// Deletes a page chain.
    pub(crate) fn delete<'a, O, D>(id: PageId, mut obtain: O, mut delete: D) -> mapping::Result<()>
    where
        O: FnMut(PageId) -> mapping::Result<Chunk<&'a [u8]>>,
        D: FnMut(PageId) -> mapping::Result<()>,
    {
        let mut next_id = Some(id);

        while let Some(id) = next_id {
            let chunk = obtain(id)?;
            next_id = chunk.next();
            delete(id)?;
        }

        Ok(())
    }

    /// Counts the number of chunks needed to store a given length of data.
    #[inline]
    pub(crate) fn count(len: u32, page_size: u32) -> u32 {
        let capacity = page_size - mem::size_of::<Len>() as u32 - mem::size_of::<PageId>() as u32;
        (len - 1) / capacity + 1
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::Chunk;
    use crate::{
        medium::{mapping::Result, mempool::MemoryPool},
        storage::{Page, PageId},
    };

    #[test]
    fn test_chunk() -> Result<()> {
        const PAGE_SIZE: usize = 20;

        let mut id = PageId::from_raw(0);
        let mut pages = HashMap::new();
        let pool = MemoryPool::new(PAGE_SIZE, 0);

        let bytes = b"ThetaDB is suitable for use on mobile clients with \"High-Read, Low-Write\" demands, it uses B+ Tree as the foundational layer for index management.";

        // Write
        let id = Chunk::write(bytes.as_ref(), || {
            let id = id.incr();
            let cell = pages.entry(id).or_insert(pool.obtain_cell());
            let chunk = Chunk::from_bytes(unsafe { cell.as_mut_slice() })?;
            Ok((id, chunk))
        })?;

        assert_eq!(
            Chunk::count(bytes.len() as u32, PAGE_SIZE as u32),
            pages.len() as u32
        );

        // Read
        let res = Chunk::read(id, |id| {
            let cell = pages.get(&id).unwrap();
            Chunk::from_bytes(unsafe { cell.as_slice() })
        })?;

        assert_eq!(bytes, res.as_slice());

        // Delete
        let mut page_ids = HashSet::new();

        Chunk::delete(
            id,
            |id| {
                let cell = pages.get(&id).unwrap();
                Chunk::from_bytes(unsafe { cell.as_slice() })
            },
            |id| {
                page_ids.insert(id);
                Ok(())
            },
        )?;

        assert_eq!(pages.into_keys().collect::<HashSet<_>>(), page_ids);

        Ok(())
    }
}

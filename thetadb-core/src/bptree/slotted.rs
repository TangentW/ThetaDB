use std::{iter::zip, mem, ops::Range, u32};

use crate::medium::{mapping, Bytes, BytesMut, Mapping};

/// Represents a `Slotted Page` layout scheme.
///
/// A slotted page is a common data storage structure, it divides a page into two parts:
///
/// * `Records` (i.e., slots)
/// * `Position and length infomation of each record` (i.e., slot)
///
/// # Structure
///
/// ```plain
///     ┌──────────Free Space End───────────┐                    
/// ┌───┴────┬───┬───┬───┬──────────────────▼──────┬───┬────────┐
/// │ Header │Ptr│Ptr│Ptr│    Free Space    │      │   │        │
/// └────────┴─┬─┴─┬─┴─┬─┴──────────────────┴──▲───┴─▲─┴───▲────┘
///            └───┼───┼───────────────────────┼─────┼─────┘     
///                └───┼───────────────────────┘     │           
///                    └───────Record Offset─────────┘           
/// ```
pub(crate) struct Slotted<B> {
    header: Mapping<B, Header>,
    body: B,
}

/// The header of a slotted page.
#[repr(C)]
struct Header {
    /// The number of records (i.e., slots) in the page.
    num_slots: u32,
    /// The end of the free space in the page.
    free_end: u32,
}

/// A pointer to a record in the page.
#[derive(Clone, Copy)]
#[repr(C)]
struct Pointer {
    /// The start position of the record.
    offset: u32,
    /// The length of the record.
    len: u32,
}

impl Pointer {
    /// The size of a `Pointer` in bytes.
    const SIZE: u32 = mem::size_of::<Self>() as u32;

    /// The range of the record in the slotted page body.
    #[inline]
    fn range(&self) -> Range<usize> {
        let start = self.offset as usize;
        let end = start + self.len as usize;
        start..end
    }
}

type Pointers<B> = Mapping<B, [Pointer]>;

impl<B> Slotted<B>
where
    B: Bytes,
{
    #[inline]
    pub(crate) fn new(bytes: B) -> mapping::Result<Self> {
        unsafe { Mapping::split(bytes) }.map(|(header, body)| Self { header, body })
    }

    /// The number of records in the page.
    #[inline]
    pub(crate) fn count(&self) -> usize {
        self.header.num_slots as usize
    }

    /// The fill rate of the page body.
    #[inline]
    pub(crate) fn fill_rate(&self) -> f64 {
        let body_len = self.body.len() as f64;
        assert_ne!(body_len, 0f64, "body length should not be zero");
        (self.pointers_len() + self.records_len()) as f64 / body_len
    }

    /// The amount of free space in the page.
    #[inline]
    pub(crate) fn free_space(&self) -> u32 {
        self.header.free_end.wrapping_sub(self.pointers_len())
    }

    /// Obtain the record at the given index.
    #[inline]
    pub(crate) fn get(&self, index: usize) -> mapping::Result<&[u8]> {
        let pointer = &self.pointers()?[index];
        let range = pointer.range();

        mapping::check_range(&range, &self.body)?;
        Ok(&self.body[range])
    }

    #[inline]
    fn pointers(&self) -> mapping::Result<Pointers<&[u8]>> {
        let range = ..self.pointers_len() as usize;
        mapping::check_range(&range, &self.body)?;
        unsafe { Mapping::new_slice(&self.body[range]) }
    }

    #[inline]
    fn pointers_len(&self) -> u32 {
        self.header.num_slots * Pointer::SIZE
    }

    #[inline]
    fn records_len(&self) -> u32 {
        (self.body.len() as u32).wrapping_sub(self.header.free_end)
    }
}

impl<B> Slotted<B>
where
    B: BytesMut,
{
    /// Initializes the page.
    ///
    /// The slotted page must call this method before using.
    #[inline]
    pub(crate) fn init(&mut self) {
        self.header.num_slots = 0;
        self.header.free_end = self.body.len() as u32;
    }

    /// Obtain the mutable record at the given index.
    #[inline]
    pub(crate) fn get_mut(&mut self, index: usize) -> mapping::Result<&mut [u8]> {
        let pointer = &self.pointers()?[index];
        let range = pointer.range();

        mapping::check_range(&range, &self.body)?;
        Ok(&mut self.body[range])
    }

    /// Inserts a record of the given length at the end of the page.
    #[inline]
    pub(crate) fn put(&mut self, len: u32) -> mapping::Result<Option<&mut [u8]>> {
        self.insert(self.count(), len)
    }

    /// Inserts a record of the given length at the given index.
    pub(crate) fn insert(&mut self, index: usize, len: u32) -> mapping::Result<Option<&mut [u8]>> {
        let count = self.count();
        assert!(
            index <= count,
            "insertion index ({index}) should be less than or equal to count ({count})"
        );

        // There is not enough space for insertion.
        if Pointer::SIZE + len > self.free_space() {
            return Ok(None);
        }

        let pointer_offset = self.header.free_end - len;

        self.header.num_slots += 1;
        self.header.free_end = pointer_offset;

        // If `index == len`, there are no pointers need shifting.
        let mut pointers = self.pointers_mut()?;
        if index < count {
            pointers.copy_within(index..count, index + 1);
        }

        let pointer = &mut pointers[index];
        pointer.offset = pointer_offset;
        pointer.len = len;

        let range = pointer.range();
        mapping::check_range(&range, &self.body)?;
        Ok(Some(&mut self.body[range]))
    }

    /// Sets the value of the record at the given index.
    pub(crate) fn set(&mut self, index: usize, new_len: u32) -> mapping::Result<Option<&mut [u8]>> {
        let count = self.count();
        assert!(
            index < count,
            "target index ({index}) should be less than count ({count})"
        );

        let pointer = self.pointers()?[index];
        let offset_orig = pointer.offset;
        let len_incr = (new_len as i64) - (pointer.len as i64);

        // There is not enough space for resizing.
        if len_incr > self.free_space() as i64 {
            return Ok(None);
        }

        let body_shift_range = (self.header.free_end as usize)..(offset_orig as usize);
        let free_end = (self.header.free_end as i64 - len_incr) as u32;

        self.body.copy_within(body_shift_range, free_end as usize);
        self.header.free_end = free_end;

        self.pointers_mut()?
            .iter_mut()
            .filter(|p| p.offset <= offset_orig)
            .for_each(|p| p.offset = (p.offset as i64 - len_incr) as u32);

        let pointer = &mut self.pointers_mut()?[index];
        pointer.len = new_len;

        let range = pointer.range();
        mapping::check_range(&range, &self.body)?;
        Ok(Some(&mut self.body[range]))
    }

    /// Removes the record at the given index.
    pub(crate) fn remove(&mut self, index: usize) -> mapping::Result<()> {
        let count = self.count();
        assert!(
            index < count,
            "removal index ({index}) should be less than count ({count})"
        );

        let pointer = self.pointers()?[index];
        let offset_orig = pointer.offset;
        let len_decr = pointer.len;

        let body_shift_range = (self.header.free_end as usize)..(offset_orig as usize);
        let free_end = self.header.free_end + len_decr;

        self.body.copy_within(body_shift_range, free_end as usize);
        self.header.free_end = free_end;

        if index < count - 1 {
            self.pointers_mut()?.copy_within((index + 1)..count, index);
        }
        self.header.num_slots -= 1;

        self.pointers_mut()?
            .iter_mut()
            .filter(|p| p.offset <= offset_orig)
            .for_each(|p| p.offset += len_decr);

        Ok(())
    }

    /// Merges this page with the next page.
    pub(crate) fn merge<T>(&mut self, other: &Slotted<T>, with_next: bool) -> mapping::Result<bool>
    where
        T: Bytes,
    {
        if self.free_space() < other.pointers_len() + other.records_len() {
            // There is no enough space for merging.
            return Ok(false);
        }

        let (count_orig, records_len_orig) = (self.count(), self.records_len());
        self.header.num_slots += other.header.num_slots;

        let mut pointers = self.pointers_mut()?;
        let new_pointers = if with_next {
            &mut pointers[count_orig..]
        } else {
            pointers.copy_within(..count_orig, other.count());
            &mut pointers[0..other.count()]
        };

        zip(new_pointers, other.pointers()?.as_ref())
            .for_each(|(l, r)| (l.len, l.offset) = (r.len, r.offset - records_len_orig));

        let free_end_orig = self.header.free_end;
        self.header.free_end -= other.records_len();

        self.body[self.header.free_end as usize..free_end_orig as usize]
            .copy_from_slice(&other.body[other.header.free_end as usize..]);

        Ok(true)
    }

    /// Splits this page into two pages.
    pub(crate) fn split(&mut self, new: &mut Self) -> mapping::Result<usize> {
        assert_ne!(self.count(), 0, "the slotted page should not be empty");
        assert_eq!(new.count(), 0, "the new slotted page should be empty");

        let index = loop {
            let last_idx = self.count() - 1;

            let record_space_len = (Pointer::SIZE + self.pointers()?[last_idx].len) as isize;
            let free_space_diff = self.free_space() as isize - new.free_space() as isize;

            // Check if we still need to move the records to the new slotted page.
            if free_space_diff.abs() <= (free_space_diff + 2 * record_space_len).abs() {
                break last_idx + 1;
            }

            let bytes = self.get(last_idx)?;

            new.put(bytes.len() as u32)?
                .map(|b| b.copy_from_slice(bytes))
                .expect("the slotted page should have enough space for insertion");

            self.remove(last_idx)?
        };

        new.pointers_mut()?.reverse();

        Ok(index)
    }

    #[inline]
    pub(crate) fn split_insert<'a>(
        &'a mut self,
        new: &'a mut Self,
        index: usize,
        len: u32,
    ) -> mapping::Result<Option<&mut [u8]>> {
        let mid_idx = self.split(new)?;
        if index < mid_idx {
            self.insert(index, len)
        } else {
            new.insert(index - mid_idx, len)
        }
    }

    #[inline]
    pub(crate) fn split_set<'a>(
        &'a mut self,
        new: &'a mut Self,
        index: usize,
        new_len: u32,
    ) -> mapping::Result<Option<&mut [u8]>> {
        let mid_idx = self.split(new)?;
        if index < mid_idx {
            self.set(index, new_len)
        } else {
            new.set(index - mid_idx, new_len)
        }
    }

    #[inline]
    fn pointers_mut(&mut self) -> mapping::Result<Pointers<&mut [u8]>> {
        let range = ..self.pointers_len() as usize;
        mapping::check_range(&range, &self.body)?;
        unsafe { Mapping::new_slice(&mut self.body[range]) }
    }
}

#[cfg(test)]
mod tests {

    use super::Slotted;
    use crate::medium::mapping::Result;

    #[test]
    #[should_panic(expected = "insertion index (1) should be less than or equal to count (0)")]
    fn test_insert_panic() {
        let mut bytes = [0; 256];
        if let Ok(mut slotted) = Slotted::new(bytes.as_mut()) {
            slotted.init();
            slotted.insert(1, 1).unwrap();
        }
    }

    #[test]
    fn test_insert() -> Result<()> {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut())?;
        slotted.init();

        slotted.insert(0, 2)?.unwrap().copy_from_slice(&[5, 6]);
        assert_eq!(slotted.get(0)?, &[5, 6]);

        slotted.insert(1, 3)?.unwrap().copy_from_slice(&[7, 8, 9]);
        assert_eq!(slotted.get(0)?, &[5, 6]);
        assert_eq!(slotted.get(1)?, &[7, 8, 9]);

        slotted.insert(0, 3)?.unwrap().copy_from_slice(&[1, 2, 3]);
        assert_eq!(slotted.get(0)?, &[1, 2, 3]);
        assert_eq!(slotted.get(1)?, &[5, 6]);
        assert_eq!(slotted.get(2)?, &[7, 8, 9]);

        slotted.insert(2, 1)?.unwrap().copy_from_slice(&[4]);
        assert_eq!(slotted.get(0)?, &[1, 2, 3]);
        assert_eq!(slotted.get(1)?, &[5, 6]);
        assert_eq!(slotted.get(2)?, &[4]);
        assert_eq!(slotted.get(3)?, &[7, 8, 9]);

        Ok(())
    }

    #[test]
    fn test_insert_layout() -> Result<()> {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut())?;
        slotted.init();

        // 256 - 8 (Header)
        assert_eq!(slotted.header.free_end, 248);

        assert!(slotted.insert(0, 200)?.is_some());
        // 256 - 8 (Header) - 8 (Slot) - 200
        assert_eq!(slotted.free_space(), 40);
        // 256 - 8 (Header) - 200
        assert_eq!(slotted.header.free_end, 48);

        assert!(slotted.insert(0, 32)?.is_some());
        // 256 - 8 (Header) - 2 * 8 (Slot) - 232
        assert_eq!(slotted.free_space(), 0);
        // 256 - 8 (Header) - 232
        assert_eq!(slotted.header.free_end, 16);

        // Overflow
        assert!(slotted.insert(0, 1)?.is_none());

        Ok(())
    }

    #[test]
    #[should_panic(expected = "target index (0) should be less than count (0)")]
    fn test_set_panic() {
        let mut bytes = [0; 256];
        if let Ok(mut slotted) = Slotted::new(bytes.as_mut()) {
            slotted.init();
            slotted.set(0, 1).unwrap();
        }
    }

    #[test]
    fn test_set() -> Result<()> {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut())?;
        slotted.init();

        slotted.insert(0, 2)?.unwrap().copy_from_slice(&[4, 5]);
        slotted.insert(0, 3)?.unwrap().copy_from_slice(&[1, 2, 3]);

        slotted.set(0, 4)?.unwrap().copy_from_slice(&[6, 7, 8, 9]);
        assert_eq!(slotted.get(0)?, &[6, 7, 8, 9]);
        assert_eq!(slotted.get(1)?, &[4, 5]);

        slotted.set(1, 2)?.unwrap().copy_from_slice(&[1, 2]);
        assert_eq!(slotted.get(0)?, &[6, 7, 8, 9]);
        assert_eq!(slotted.get(1)?, &[1, 2]);

        slotted.insert(1, 1)?.unwrap().copy_from_slice(&[10]);
        assert_eq!(slotted.get(0)?, &[6, 7, 8, 9]);
        assert_eq!(slotted.get(1)?, &[10]);
        assert_eq!(slotted.get(2)?, &[1, 2]);

        Ok(())
    }

    #[test]
    fn test_set_layout() -> Result<()> {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut())?;
        slotted.init();

        assert!(slotted.insert(0, 100)?.is_some());
        assert!(slotted.insert(1, 50)?.is_some());
        // 256 - 8 (Header) - 2 * 8 (Slot) - 150
        assert_eq!(slotted.free_space(), 82);
        // 256 - 8 (Header) - 150
        assert_eq!(slotted.header.free_end, 98);

        assert!(slotted.set(1, 80)?.is_some());
        assert!(slotted.set(0, 55)?.is_some());
        assert_eq!(slotted.free_space(), 97);
        assert_eq!(slotted.header.free_end, 113);

        assert!(slotted.set(0, 152)?.is_some());
        assert_eq!(slotted.free_space(), 0);
        assert_eq!(slotted.header.free_end, 16);

        assert!(slotted.set(1, 152)?.is_none());

        Ok(())
    }

    #[test]
    #[should_panic(expected = "removal index (1) should be less than count (1)")]
    fn test_remove_panic() {
        let mut bytes = [0; 256];
        if let Ok(mut slotted) = Slotted::new(bytes.as_mut()) {
            slotted.init();
            assert!(slotted.insert(0, 1).unwrap().is_some());
            slotted.remove(1).unwrap();
        }
    }

    #[test]
    fn test_remove() -> Result<()> {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut())?;
        slotted.init();

        slotted.insert(0, 2)?.unwrap().copy_from_slice(&[1, 2]);
        slotted.insert(1, 3)?.unwrap().copy_from_slice(&[3, 4, 5]);
        slotted.insert(2, 3)?.unwrap().copy_from_slice(&[6, 7, 8]);
        slotted.insert(3, 1)?.unwrap().copy_from_slice(&[9]);

        slotted.remove(1)?;
        assert_eq!(slotted.get(0)?, &[1, 2]);
        assert_eq!(slotted.get(1)?, &[6, 7, 8]);
        assert_eq!(slotted.get(2)?, &[9]);

        slotted.remove(2)?;
        assert_eq!(slotted.get(0)?, &[1, 2]);
        assert_eq!(slotted.get(1)?, &[6, 7, 8]);

        slotted.remove(0)?;
        assert_eq!(slotted.get(0)?, &[6, 7, 8]);

        Ok(())
    }

    #[test]
    fn test_remove_layout() -> Result<()> {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut())?;
        slotted.init();

        assert!(slotted.insert(0, 100)?.is_some());
        assert!(slotted.insert(1, 50)?.is_some());

        // 256 - 8 (Header) - 2 * 8 (Slot) - 150
        assert_eq!(slotted.free_space(), 82);
        // 256 - 8 (Header) - 150
        assert_eq!(slotted.header.free_end, 98);

        slotted.remove(0)?;
        // 256 - 8 (Header) - 8 (Slot) - 50
        assert_eq!(slotted.free_space(), 190);
        // 256 - 8 (Header) - 50
        assert_eq!(slotted.header.free_end, 198);

        slotted.remove(0)?;
        // 256 - 8 (Header)
        assert_eq!(slotted.free_space(), 248);
        assert_eq!(slotted.header.free_end, 248);

        Ok(())
    }

    #[test]
    fn test_merge_failed() -> Result<()> {
        let mut bytes = [0; 32];
        let mut slotted = Slotted::new(bytes.as_mut())?;
        slotted.init();
        assert!(slotted.put(8)?.is_some());

        let mut next_bytes = [0; 32];
        let mut next_slotted = Slotted::new(next_bytes.as_mut())?;
        next_slotted.init();
        assert!(next_slotted.put(1)?.is_some());

        assert!(!slotted.merge(&next_slotted, true)?);
        Ok(())
    }

    #[test]
    fn test_merge() -> Result<()> {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut())?;
        let mut next_bytes = [0; 256];
        let mut next_slotted = Slotted::new(next_bytes.as_mut())?;

        slotted.init();
        assert!(slotted.insert(0, 3)?.is_some());
        assert!(slotted.insert(1, 2)?.is_some());
        assert!(slotted.insert(1, 1)?.is_some());

        next_slotted.init();
        assert!(next_slotted.insert(0, 7)?.is_some());
        assert!(next_slotted.insert(1, 5)?.is_some());
        assert!(next_slotted.insert(1, 6)?.is_some());

        assert!(slotted.merge(&next_slotted, true)?);

        assert_eq!(slotted.count(), 6);
        assert_eq!(slotted.get(0)?.len(), 3);
        assert_eq!(slotted.get(1)?.len(), 1);
        assert_eq!(slotted.get(2)?.len(), 2);
        assert_eq!(slotted.get(3)?.len(), 7);
        assert_eq!(slotted.get(4)?.len(), 6);
        assert_eq!(slotted.get(5)?.len(), 5);

        slotted.init();
        assert!(slotted.insert(0, 3)?.is_some());
        assert!(slotted.insert(1, 2)?.is_some());
        assert!(slotted.insert(1, 1)?.is_some());

        next_slotted.init();
        assert!(next_slotted.insert(0, 7)?.is_some());
        assert!(next_slotted.insert(1, 5)?.is_some());
        assert!(next_slotted.insert(1, 6)?.is_some());

        assert!(slotted.merge(&next_slotted, false)?);

        assert_eq!(slotted.count(), 6);
        assert_eq!(slotted.get(0)?.len(), 7);
        assert_eq!(slotted.get(1)?.len(), 6);
        assert_eq!(slotted.get(2)?.len(), 5);
        assert_eq!(slotted.get(3)?.len(), 3);
        assert_eq!(slotted.get(4)?.len(), 1);
        assert_eq!(slotted.get(5)?.len(), 2);

        Ok(())
    }

    #[test]
    fn test_merge_layout() -> Result<()> {
        fn test(merge_next: bool) -> Result<()> {
            let mut bytes = [0; 256];
            let mut slotted = Slotted::new(bytes.as_mut())?;
            slotted.init();
            assert!(slotted.put(27)?.is_some());

            let mut next_bytes = [0; 256];
            let mut next_slotted = Slotted::new(next_bytes.as_mut())?;
            next_slotted.init();
            assert!(next_slotted.put(39)?.is_some());

            assert!(slotted.merge(&next_slotted, merge_next)?);

            // 256 - 8 (Header) - 2 * 8 (Slot) - 27 - 39
            assert_eq!(slotted.free_space(), 166);

            // 256 - 8 (Header) - 27 - 39
            assert_eq!(slotted.header.free_end, 182);

            Ok(())
        }

        test(true).and_then(|_| test(false))
    }

    #[test]
    #[should_panic(expected = "the slotted page should not be empty")]
    fn test_split_empty() {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut()).unwrap();
        slotted.init();

        let mut new_bytes = [0; 256];
        let mut new_slotted = Slotted::new(new_bytes.as_mut()).unwrap();
        new_slotted.init();

        slotted.split(&mut new_slotted).unwrap();
    }

    #[test]
    #[should_panic(expected = "the new slotted page should be empty")]
    fn test_split_empty_new() {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut()).unwrap();
        slotted.init();
        slotted.put(32).unwrap();

        let mut new_bytes = [0; 256];
        let mut new_slotted = Slotted::new(new_bytes.as_mut()).unwrap();
        new_slotted.init();
        new_slotted.put(32).unwrap();

        slotted.split(&mut new_slotted).unwrap();
    }

    #[test]
    fn test_split() -> Result<()> {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut())?;
        slotted.init();

        let mut new_bytes = [0; 256];
        let mut new_slotted = Slotted::new(new_bytes.as_mut())?;
        new_slotted.init();

        assert!(slotted.put(64)?.is_some());
        assert!(slotted.put(8)?.is_some());
        assert!(slotted.put(16)?.is_some());
        assert!(slotted.put(32)?.is_some());

        assert_eq!(slotted.split(&mut new_slotted)?, 1);
        assert_eq!(slotted.count(), 1);
        assert_eq!(slotted.get(0)?.len(), 64);
        assert_eq!(new_slotted.count(), 3);
        assert_eq!(new_slotted.get(0)?.len(), 8);
        assert_eq!(new_slotted.get(1)?.len(), 16);
        assert_eq!(new_slotted.get(2)?.len(), 32);

        slotted.init();
        new_slotted.init();

        assert!(slotted.put(16)?.is_some());
        assert!(slotted.put(8)?.is_some());
        assert!(slotted.put(16)?.is_some());
        assert!(slotted.put(32)?.is_some());

        assert_eq!(slotted.split(&mut new_slotted)?, 3);
        assert_eq!(slotted.count(), 3);
        assert_eq!(slotted.get(0)?.len(), 16);
        assert_eq!(slotted.get(1)?.len(), 8);
        assert_eq!(slotted.get(2)?.len(), 16);
        assert_eq!(new_slotted.count(), 1);
        assert_eq!(new_slotted.get(0)?.len(), 32);

        Ok(())
    }

    #[test]
    fn test_split_insert_set() -> Result<()> {
        let mut bytes = [0; 256];
        let mut slotted = Slotted::new(bytes.as_mut())?;
        slotted.init();

        let mut new_bytes = [0; 256];
        let mut new_slotted = Slotted::new(new_bytes.as_mut())?;
        new_slotted.init();

        assert!(slotted.put(16)?.is_some());
        assert!(slotted.put(16)?.is_some());
        assert!(slotted.put(32)?.is_some());

        assert!(slotted.split_insert(&mut new_slotted, 2, 8)?.is_some());
        assert_eq!(slotted.count(), 2);
        assert_eq!(slotted.get(0)?.len(), 16);
        assert_eq!(slotted.get(1)?.len(), 16);
        assert_eq!(new_slotted.count(), 2);
        assert_eq!(new_slotted.get(0)?.len(), 8);
        assert_eq!(new_slotted.get(1)?.len(), 32);

        slotted.init();
        new_slotted.init();

        assert!(slotted.put(16)?.is_some());
        assert!(slotted.put(8)?.is_some());
        assert!(slotted.put(16)?.is_some());
        assert!(slotted.put(32)?.is_some());

        assert!(slotted.split_set(&mut new_slotted, 1, 10)?.is_some());
        assert_eq!(slotted.count(), 3);
        assert_eq!(slotted.get(0)?.len(), 16);
        assert_eq!(slotted.get(1)?.len(), 10);
        assert_eq!(slotted.get(2)?.len(), 16);
        assert_eq!(new_slotted.count(), 1);
        assert_eq!(new_slotted.get(0)?.len(), 32);

        Ok(())
    }
}

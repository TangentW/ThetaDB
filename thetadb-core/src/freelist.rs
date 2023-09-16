use std::mem;

use crate::storage::PageId;

/// Represents a free list that keeps track of free pages in ThetaDB.
///
/// Freelist uses a bitmap to record the ids of free pages, each bit in the bitmap
/// represents whether a page is free (1) or not (0).
#[derive(Default)]
#[repr(transparent)]
pub(crate) struct Freelist {
    bitmap: Vec<BitmapWord>,
}

type BitmapWord = u64;

const BITMAP_WORD_LEN: usize = mem::size_of::<BitmapWord>();

impl Freelist {
    #[inline]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The number of free pages in the free list.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.bitmap
            .iter()
            .fold(0, |acc, word| acc + word.count_ones() as usize)
    }

    /// Marks the specified pages as free in the free list.
    pub(crate) fn free(&mut self, page_id: PageId, count: u32) {
        if count == 0 {
            return;
        }
        let last_idx = page_id.raw() + count - 1;
        let words_len = last_idx / BitmapWord::BITS + 1;

        // Automatically resize the Vec of words.
        if words_len as usize > self.bitmap.len() {
            self.bitmap.resize(words_len as usize, 0);
        }

        for word_idx in (page_id.raw() / BitmapWord::BITS)..words_len {
            let index = page_id.raw().max(word_idx * BitmapWord::BITS);
            let last_idx = last_idx.min((word_idx + 1) * BitmapWord::BITS - 1);
            let count = last_idx - index + 1;

            self.bitmap[(word_idx) as usize] |= ((1 << count) - 1) << (index % BitmapWord::BITS);
        }
    }

    /// Takes a certain number of consecutive pages from the freelist.
    ///
    /// If `page_count` is 1, it uses a fast path method, otherwise, it uses a slow path method.
    #[inline]
    pub(crate) fn take(&mut self, page_count: u32) -> Option<PageId> {
        if page_count == 1 {
            self.take_one_fastpath()
        } else {
            self.take_mult_slowpath(page_count)
        }
    }

    /// Takes a single page from the free list.
    fn take_one_fastpath(&mut self) -> Option<PageId> {
        let (word_idx, word) = self
            .bitmap
            .iter_mut()
            .enumerate()
            .rev()
            .find(|(_, w)| **w != 0)?;

        let idx = BitmapWord::BITS - word.leading_zeros() - 1;
        *word &= !(1 << idx);

        if *word == 0 {
            self.bitmap.pop();
        }

        Some((word_idx as u32 * BitmapWord::BITS + idx).into())
    }

    /// Takes multiple consecutive pages from the free list.
    fn take_mult_slowpath(&mut self, page_count: u32) -> Option<PageId> {
        struct Window {
            start: u32,
            end: u32,
        }

        impl Window {
            #[inline]
            fn len(&self) -> u32 {
                self.end - self.start + 1
            }
        }

        let mut window: Option<Window> = None;

        'outer: for (word_idx, word) in self.bitmap.iter().enumerate().rev() {
            if *word == 0 {
                window = None;
                continue;
            }
            for idx in (0..BitmapWord::BITS).rev() {
                if (1 << idx) & word == 0 {
                    window = None;
                    continue;
                }

                let idx = word_idx as u32 * BitmapWord::BITS + idx;
                let window = window.get_or_insert(Window {
                    start: idx,
                    end: idx,
                });
                window.start = idx;

                if window.len() == page_count {
                    break 'outer;
                }
            }
        }

        if let Some(window) = &window {
            for idx in window.start..=window.end {
                self.bitmap[(idx / BitmapWord::BITS) as usize] &= !(1 << (idx % BitmapWord::BITS));
            }
            while let Some(last) = self.bitmap.last() && *last == 0 {
                self.bitmap.pop();
            }
        }

        window.map(|w| w.start.into())
    }

    /// Resizes the free list to accommodate a specified number of bytes.
    #[inline]
    pub(crate) fn resize(&mut self, bytes_len: usize) {
        let len = bytes_len / BITMAP_WORD_LEN;
        self.bitmap.resize(len, 0);
    }

    /// The number of bytes needed to store a free list up to the specified page id.
    #[inline]
    pub(crate) fn bytes_len_for_storing(id: PageId) -> u32 {
        let words_len = id.raw() / BitmapWord::BITS + 1;
        words_len * BITMAP_WORD_LEN as u32
    }

    /// Creates a free list from a byte slice.
    #[inline]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        let bitmap = bytes
            .chunks_exact(BITMAP_WORD_LEN)
            .map(|c| BitmapWord::from_le_bytes(c.try_into().unwrap()))
            .collect();
        Self { bitmap }
    }

    /// Converts a free list into a byte vector.
    #[inline]
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bitmap
            .into_iter()
            .flat_map(|w| w.to_le_bytes())
            .collect()
    }

    /// The number of bytes used to store the free list.
    #[inline]
    pub(crate) fn bytes_len(&self) -> usize {
        self.bitmap.len() * BITMAP_WORD_LEN
    }
}

impl<T> From<T> for Freelist
where
    T: Into<Vec<BitmapWord>>,
{
    #[inline]
    fn from(value: T) -> Self {
        Self {
            bitmap: value.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Freelist;

    #[test]
    fn test_bytes_len() {
        assert_eq!(Freelist::bytes_len_for_storing(63.into()), 8);
        assert_eq!(Freelist::bytes_len_for_storing(64.into()), 16);

        let mut freelist = Freelist::new();
        freelist.resize(8);
        assert_eq!(freelist.bitmap.len(), 1);
        assert_eq!(freelist.bytes_len(), 8);

        freelist.resize(16);
        assert_eq!(freelist.bitmap.len(), 2);
        assert_eq!(freelist.bytes_len(), 16);
    }

    #[test]
    fn test_set_bit() {
        let mut freelist = Freelist::new();
        freelist.free(52.into(), 3);
        assert_eq!(
            freelist.bitmap,
            [0b00000000_01110000_00000000_00000000_00000000_00000000_00000000_00000000,]
        );

        freelist.free(61.into(), 5);
        assert_eq!(
            freelist.bitmap,
            [
                0b11100000_01110000_00000000_00000000_00000000_00000000_00000000_00000000,
                0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000011,
            ]
        );

        freelist.free(66.into(), 65);
        assert_eq!(
            freelist.bitmap,
            [
                0b11100000_01110000_00000000_00000000_00000000_00000000_00000000_00000000,
                0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11111111,
                0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000111,
            ]
        );

        freelist.free(9.into(), 1);
        assert_eq!(
            freelist.bitmap,
            [
                0b11100000_01110000_00000000_00000000_00000000_00000000_00000010_00000000,
                0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11111111,
                0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000111,
            ]
        );
    }

    #[test]
    fn test_take_one() {
        let mut freelist = Freelist::from([
            0b00000000_00000000_00000000_01000000_00000000_00001000_00000000_00000000,
            0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000010,
        ]);

        assert_eq!(freelist.take(1), Some(65.into()));
        assert_eq!(
            freelist.bitmap,
            [0b00000000_00000000_00000000_01000000_00000000_00001000_00000000_00000000,]
        );

        assert_eq!(freelist.take(1), Some(38.into()));
        assert_eq!(
            freelist.bitmap,
            [0b00000000_00000000_00000000_00000000_00000000_00001000_00000000_00000000,]
        );

        assert_eq!(freelist.take(1), Some(19.into()));
        assert_eq!(freelist.bitmap, []);

        assert_eq!(freelist.take(1), None);
        assert_eq!(freelist.bitmap, []);
    }

    #[test]
    fn test_take_mult() {
        let mut freelist = Freelist::from([
            0b00000000_00000000_00000000_00000000_00000001_11001000_00000000_00111000,
            0b00000000_00000000_00000000_00000111_10000000_00000000_00000000_01110010,
        ]);

        assert_eq!(freelist.take(3), Some(96.into()));
        assert_eq!(
            freelist.bitmap,
            [
                0b00000000_00000000_00000000_00000000_00000001_11001000_00000000_00111000,
                0b00000000_00000000_00000000_00000000_10000000_00000000_00000000_01110010,
            ]
        );

        assert_eq!(freelist.take(3), Some(68.into()));
        assert_eq!(
            freelist.bitmap,
            [
                0b00000000_00000000_00000000_00000000_00000001_11001000_00000000_00111000,
                0b00000000_00000000_00000000_00000000_10000000_00000000_00000000_00000010,
            ]
        );

        assert_eq!(freelist.take(1), Some(95.into()));
        assert_eq!(
            freelist.bitmap,
            [
                0b00000000_00000000_00000000_00000000_00000001_11001000_00000000_00111000,
                0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000010,
            ]
        );

        assert_eq!(freelist.take(1), Some(65.into()));
        assert_eq!(
            freelist.bitmap,
            [0b00000000_00000000_00000000_00000000_00000001_11001000_00000000_00111000,]
        );

        assert_eq!(freelist.take(2), Some(23.into()));
        assert_eq!(
            freelist.bitmap,
            [0b00000000_00000000_00000000_00000000_00000000_01001000_00000000_00111000,]
        );

        assert_eq!(freelist.take(1), Some(22.into()));
        assert_eq!(freelist.take(1), Some(19.into()));
        assert_eq!(
            freelist.bitmap,
            [0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00111000,]
        );

        assert_eq!(freelist.take(3), Some(3.into()));
        assert_eq!(freelist.bitmap, []);
    }
}

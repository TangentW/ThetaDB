use std::{
    marker::PhantomData,
    mem,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    result, slice,
};

use thiserror::Error;

/// A struct that is used to align memory when it is directly mapped to a struct.
///
/// By adjusting the value of `N`, you can control the alignment of the struct in memory.
#[repr(transparent)]
pub(crate) struct Padding<const N: usize>([u8; N]);

/// Represents an immutable byte slice.
pub(crate) trait Bytes: Deref<Target = [u8]> + Sized {
    fn split_at(self, mid: usize) -> (Self, Self);
}

/// Represents a mutable byte slice.
pub(crate) trait BytesMut: Bytes + DerefMut<Target = [u8]> {}

impl<'a> Bytes for &'a [u8] {
    #[inline]
    fn split_at(self, mid: usize) -> (Self, Self) {
        <[u8]>::split_at(self, mid)
    }
}

impl<'a> Bytes for &'a mut [u8] {
    #[inline]
    fn split_at(self, mid: usize) -> (Self, Self) {
        <[u8]>::split_at_mut(self, mid)
    }
}

impl<'a> BytesMut for &'a mut [u8] {}

pub(crate) type Result<T> = result::Result<T, Error>;

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("bytes are not enough")]
    Size,
    #[error("bytes are not aligned")]
    Alignment,
}

#[inline]
pub(crate) fn check_range<R, B>(range: &R, bytes: &B) -> Result<()>
where
    R: RangeBounds<usize>,
    B: Bytes,
{
    if matches!(range.end_bound(), Bound::Excluded(&end) if end <= bytes.len()) {
        Ok(())
    } else {
        Err(Error::Size)
    }
}

/// A memory-to-struct mapping.
///
/// Conveniently mapping a byte slice to a struct of type `T` without copying.
#[repr(transparent)]
pub(crate) struct Mapping<B, T: ?Sized> {
    bytes: B,
    _marker: PhantomData<T>,
}

impl<B, T> Mapping<B, T> {
    const TARGET_SIZE: usize = mem::size_of::<T>();
    const TARGET_ALIGN: usize = mem::align_of::<T>();

    #[inline]
    fn check_align<P>(ptr: *const P) -> Result<()> {
        if Self::TARGET_SIZE != 0 && ptr as usize % Self::TARGET_ALIGN == 0 {
            Ok(())
        } else {
            Err(Error::Alignment)
        }
    }

    #[inline]
    fn check_size(bytes_size: usize) -> Result<()> {
        if bytes_size >= Self::TARGET_SIZE {
            Ok(())
        } else {
            Err(Error::Size)
        }
    }
}

impl<B, T> Mapping<B, T>
where
    B: Bytes,
{
    #[inline]
    pub(crate) unsafe fn new(bytes: B) -> Result<Self> {
        Self::check_size(bytes.len())?;
        Self::check_align(bytes.as_ptr())?;

        Ok(Self {
            bytes,
            _marker: PhantomData,
        })
    }

    #[inline]
    pub(crate) unsafe fn split(bytes: B) -> Result<(Self, B)> {
        Self::check_size(bytes.len())?;
        Self::check_align(bytes.as_ptr())?;

        let (bytes, remaining) = bytes.split_at(Self::TARGET_SIZE);
        Ok((
            Self {
                bytes,
                _marker: PhantomData,
            },
            remaining,
        ))
    }
}

impl<B, T> Mapping<B, [T]>
where
    B: Bytes,
{
    #[inline]
    pub(crate) unsafe fn new_slice(bytes: B) -> Result<Self> {
        Mapping::<B, T>::check_align(bytes.as_ptr())?;

        Ok(Mapping {
            bytes,
            _marker: PhantomData,
        })
    }
}

impl<B, T> Deref for Mapping<B, T>
where
    B: Bytes,
{
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*(self.bytes.as_ptr() as *const T) }
    }
}

impl<B, T> DerefMut for Mapping<B, T>
where
    B: BytesMut,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *(self.bytes.as_mut_ptr() as *mut T) }
    }
}

impl<B, T> Deref for Mapping<B, [T]>
where
    B: Bytes,
{
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        let len = self.bytes.len() / Mapping::<B, T>::TARGET_SIZE;
        unsafe { slice::from_raw_parts(self.bytes.as_ptr() as *const T, len) }
    }
}

impl<B, T> DerefMut for Mapping<B, [T]>
where
    B: BytesMut,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        let len = self.bytes.len() / Mapping::<B, T>::TARGET_SIZE;
        unsafe { slice::from_raw_parts_mut(self.bytes.as_mut_ptr() as *mut T, len) }
    }
}

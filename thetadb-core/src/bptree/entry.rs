use std::{borrow::Cow, cmp::Ordering, mem, ops::Deref};

use crate::{
    medium::{mapping, Bytes, BytesMut, Mapping},
    storage::PageId,
};

#[repr(transparent)]
pub(crate) struct Key<B>(B);

impl<B> Key<B>
where
    B: Bytes,
{
    #[inline]
    pub(crate) fn new(key: B) -> Self {
        Self(key)
    }

    #[inline]
    pub(crate) fn from_bytes(bytes: B) -> mapping::Result<Self> {
        Self::split_from_bytes(bytes).map(|t| t.0)
    }

    #[inline]
    pub(crate) fn split_from_bytes(bytes: B) -> mapping::Result<(Self, B)> {
        let (len, remaining) = unsafe { Mapping::<B, u8>::split(bytes)? };
        let (body, remaining) = remaining.split_at(*len as usize);
        Ok((Self(body), remaining))
    }

    #[inline]
    pub(crate) fn len(&self) -> u32 {
        (self.0.len() + mem::size_of::<u8>()) as u32
    }

    pub(crate) fn split_assign_to<T>(&self, bytes: T) -> mapping::Result<T>
    where
        T: BytesMut,
    {
        let (mut len, remaining) = unsafe { Mapping::<T, u8>::split(bytes)? };

        *len = self
            .0
            .len()
            .try_into()
            .expect("the key length cannot exceed 255");

        let (mut body, remaining) = remaining.split_at(*len as usize);
        body.copy_from_slice(&self.0);

        Ok(remaining)
    }
}

impl<B> Deref for Key<B>
where
    B: Bytes,
{
    type Target = B::Target;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<B, T> PartialEq<T> for Key<B>
where
    B: Bytes,
    T: AsRef<[u8]> + ?Sized,
{
    #[inline]
    fn eq(&self, other: &T) -> bool {
        self.0.eq(other.as_ref())
    }
}

impl<B, T> PartialOrd<T> for Key<B>
where
    B: Bytes,
    T: AsRef<[u8]> + ?Sized,
{
    #[inline]
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        self.0.partial_cmp(other.as_ref())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Value<B> {
    Bytes(B),
    Overflowed { page_id: PageId },
}

impl<B> Value<B>
where
    B: Bytes,
{
    #[inline]
    pub(crate) fn from_bytes(bytes: B) -> mapping::Result<Self> {
        let (overflowed, raw) = unsafe { Mapping::<B, bool>::split(bytes)? };
        if *overflowed {
            let page_id = PageId::from_bytes(&raw)?;
            Ok(Self::Overflowed { page_id })
        } else {
            Ok(Self::Bytes(raw))
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> u32 {
        let raw_len = match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::Overflowed { .. } => mem::size_of::<PageId>(),
        };
        (raw_len + mem::size_of::<bool>()) as u32
    }

    pub(crate) fn assign_to<T>(&self, bytes: T) -> mapping::Result<()>
    where
        T: BytesMut,
    {
        let (overflowed, raw) = match self {
            Self::Bytes(bytes) => (false, Cow::Borrowed(bytes.as_ref())),
            Self::Overflowed { page_id } => (true, Cow::Owned(page_id.to_bytes())),
        };

        let (mut new_overflowed, mut new_raw) = unsafe { Mapping::<T, bool>::split(bytes)? };
        *new_overflowed = overflowed;
        new_raw.copy_from_slice(&raw);

        Ok(())
    }
}

pub(crate) struct Entry<'a> {
    pub(crate) key: Key<&'a [u8]>,
    pub(crate) value: Value<&'a [u8]>,
}

impl<'a> Entry<'a> {
    #[inline]
    pub(crate) fn new(key: Key<&'a [u8]>, value: Value<&'a [u8]>) -> Self {
        Self { key, value }
    }
}

#[cfg(test)]
mod tests {

    use super::Key;
    use crate::medium::mapping::Result;

    #[test]
    fn test_key() -> Result<()> {
        let raw = b"Hello World";

        let key = Key::new(raw.as_slice());
        assert_eq!(key.len(), 12);

        let mut bytes = [0u8; 12];
        let remaining = key.split_assign_to(bytes.as_mut_slice())?;
        assert_eq!(remaining, &[]);

        let (key, remaining) = Key::split_from_bytes(bytes.as_ref())?;
        assert_eq!(remaining, &[]);
        assert_eq!(key.0.len(), raw.len());
        assert_eq!(key.as_ref(), raw);

        Ok(())
    }
}

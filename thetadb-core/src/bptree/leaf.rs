use crate::{
    bptree::{
        entry::{Entry, Key, Value},
        search::SearchIndex,
        slotted::Slotted,
    },
    medium::{mapping, Bytes, BytesMut},
};

#[repr(transparent)]
pub(crate) struct Leaf<B>(Slotted<B>);

struct Record<B> {
    key: Key<B>,
    value: Value<B>,
}

impl<B> Record<B>
where
    B: Bytes,
{
    #[inline]
    fn new(key: Key<B>, value: Value<B>) -> Self {
        Self { key, value }
    }

    #[inline]
    fn from_bytes(bytes: B) -> mapping::Result<Self> {
        let (key, remaining) = Key::split_from_bytes(bytes)?;
        let value = Value::from_bytes(remaining)?;
        Ok(Self { key, value })
    }

    #[inline]
    fn len(&self) -> u32 {
        self.key.len() + self.value.len()
    }

    #[inline]
    fn assign_to<T>(&self, bytes: T) -> mapping::Result<()>
    where
        T: BytesMut,
    {
        self.key
            .split_assign_to(bytes)
            .and_then(|b| self.value.assign_to(b))
    }
}

impl<'a> Record<&'a [u8]> {
    #[inline]
    fn entry(self) -> Entry<'a> {
        Entry::new(self.key, self.value)
    }
}

impl<B> Leaf<B>
where
    B: Bytes,
{
    #[inline]
    pub(crate) fn new(bytes: B) -> mapping::Result<Self> {
        Slotted::new(bytes).map(Self)
    }

    #[inline]
    pub(crate) fn entry(&self, index: usize) -> mapping::Result<Entry> {
        self.record(index).map(|r| r.entry())
    }

    pub(crate) fn search(&self, key: &[u8]) -> mapping::Result<SearchIndex> {
        let index = search!(*key, self.0.count(), idx => {
             Key::from_bytes(self.0.get(idx)?)?
        });
        Ok(index)
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.count() == 0
    }

    #[inline]
    pub(crate) fn count(&self) -> usize {
        self.0.count()
    }

    #[inline]
    pub(crate) fn fill_rate(&self) -> f64 {
        self.0.fill_rate()
    }

    #[inline]
    fn record(&self, index: usize) -> mapping::Result<Record<&[u8]>> {
        Record::from_bytes(self.0.get(index)?)
    }
}

impl<B> Leaf<B>
where
    B: BytesMut,
{
    #[inline]
    pub(crate) fn init(&mut self) {
        self.0.init();
    }

    pub(crate) fn put(
        &mut self,
        index: SearchIndex,
        key: &[u8],
        value: Value<&[u8]>,
    ) -> mapping::Result<bool> {
        let record = Record::new(Key::new(key), value);

        let bytes = match index {
            Ok(idx) => self.0.set(idx, record.len())?,
            Err(idx) => self.0.insert(idx, record.len())?,
        };

        if let Some(bytes) = bytes {
            record.assign_to(bytes)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn split_put(
        &mut self,
        new: &mut Self,
        index: SearchIndex,
        key: &[u8],
        value: Value<&[u8]>,
    ) -> mapping::Result<Vec<u8>> {
        let record = Record::new(Key::new(key), value);

        let bytes = match index {
            Ok(idx) => self.0.split_set(&mut new.0, idx, record.len())?,
            Err(idx) => self.0.split_insert(&mut new.0, idx, record.len())?,
        }
        .expect("the value size is too large");

        record.assign_to(bytes)?;

        // Extract middle key.
        let mid_key = new.record(0)?.key.to_vec();
        Ok(mid_key)
    }

    #[inline]
    pub(crate) fn delete(&mut self, index: usize) -> mapping::Result<()> {
        self.0.remove(index)
    }

    #[inline]
    pub(crate) fn merge<T>(&mut self, other: &Leaf<T>, with_next: bool) -> mapping::Result<bool>
    where
        T: Bytes,
    {
        self.0.merge(&other.0, with_next)
    }
}

use crate::{
    bptree::{entry::Key, slotted::Slotted},
    medium::{mapping, Bytes, BytesMut},
    storage::PageId,
};

#[repr(transparent)]
pub(crate) struct Branch<B>(Slotted<B>);

struct Record<B> {
    key: Key<B>,
    page_id: B,
}

impl<B> Record<B>
where
    B: Bytes,
{
    #[inline]
    fn new(key: Key<B>, page_id: B) -> Self {
        Self { key, page_id }
    }

    #[inline]
    fn from_bytes(bytes: B) -> mapping::Result<Self> {
        Key::split_from_bytes(bytes).map(|(key, page_id)| Self { key, page_id })
    }

    #[inline]
    fn len(&self) -> u32 {
        self.key.len() + self.page_id.len() as u32
    }

    #[inline]
    fn page_id(&self) -> mapping::Result<PageId> {
        PageId::from_bytes(&self.page_id)
    }

    #[inline]
    fn assign_to<T>(&self, bytes: T) -> mapping::Result<()>
    where
        T: BytesMut,
    {
        self.key
            .split_assign_to(bytes)
            .map(|mut b| b.copy_from_slice(&self.page_id))
    }
}

impl<B> Record<B>
where
    B: BytesMut,
{
    #[inline]
    fn set_page_id(&mut self, id: PageId) {
        self.page_id.copy_from_slice(&id.to_bytes())
    }
}

impl<B> Branch<B>
where
    B: Bytes,
{
    #[inline]
    pub(crate) fn new(bytes: B) -> mapping::Result<Self> {
        Slotted::new(bytes).map(Self)
    }

    pub(crate) fn search(&self, key: &[u8]) -> mapping::Result<usize> {
        assert!(
            self.0.count() >= 1,
            "the number of entities within a normal branch cannot be less than 1"
        );

        let index = search!(*key, self.0.count() - 1, idx => {
            Key::from_bytes(self.0.get(idx + 1)?)?
        })
        .map(|i| i + 1)
        .unwrap_or_else(|i| i);

        Ok(index)
    }

    #[inline]
    pub(crate) fn page_id(&self, index: usize) -> mapping::Result<PageId> {
        self.record(index).and_then(|r| r.page_id())
    }

    #[inline]
    pub(crate) fn key(&self, index: usize) -> mapping::Result<Vec<u8>> {
        self.record(index).map(|r| r.key.to_vec())
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
    pub(crate) fn sibling(&self, index: usize, with_next: bool) -> mapping::Result<Option<PageId>> {
        if with_next {
            (index < self.0.count() - 1).then(|| self.page_id(index + 1))
        } else {
            (index > 1).then(|| self.page_id(index - 1))
        }
        .transpose()
    }

    #[inline]
    fn record(&self, index: usize) -> mapping::Result<Record<&[u8]>> {
        Record::from_bytes(self.0.get(index)?)
    }
}

impl<B> Branch<B>
where
    B: BytesMut,
{
    #[inline]
    pub(crate) fn init(&mut self) {
        self.0.init();
    }

    #[inline]
    pub(crate) fn init_root(
        &mut self,
        key: &[u8],
        left: PageId,
        right: PageId,
    ) -> mapping::Result<()> {
        self.put(0, &[], left)?;
        self.put(1, key, right)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn set_page_id(&mut self, index: usize, id: PageId) -> mapping::Result<()> {
        self.record_mut(index).map(|mut r| r.set_page_id(id))
    }

    pub(crate) fn put(
        &mut self,
        index: usize,
        key: &[u8],
        page_id: PageId,
    ) -> mapping::Result<bool> {
        let page_id = &page_id.to_bytes();
        let record = Record::new(Key::new(key), page_id);

        if let Some(bytes) = self.0.insert(index, record.len())? {
            record.assign_to(bytes)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn split_put(
        &mut self,
        new: &mut Self,
        index: usize,
        key: &[u8],
        page_id: PageId,
    ) -> mapping::Result<Vec<u8>> {
        let page_id = &page_id.to_bytes();
        let record = Record::new(Key::new(key), page_id);

        let bytes = self
            .0
            .split_insert(&mut new.0, index, record.len())?
            .expect("the value size is too large");

        record.assign_to(bytes)?;

        // Extract middle record
        let mid_record = new.record(0)?;
        let (mid_key, mid_page_id) = (mid_record.key.to_vec(), &mid_record.page_id()?.to_bytes());

        let mid_record = Record::new(Key::new(&[] as &[u8]), mid_page_id);
        let mid_bytes = new.0.set(0, mid_record.len())?.expect("impossible");
        mid_record.assign_to(mid_bytes)?;

        Ok(mid_key)
    }

    #[inline]
    pub(crate) fn delete(&mut self, index: usize) -> mapping::Result<()> {
        self.0.remove(index)
    }

    pub(crate) fn merge<T>(
        &mut self,
        mid_key: &[u8],
        other: &Branch<T>,
        with_next: bool,
    ) -> mapping::Result<bool>
    where
        T: Bytes,
    {
        let mid_index = if with_next {
            self.0.count()
        } else {
            other.0.count()
        };

        if !self.0.merge(&other.0, with_next)? {
            return Ok(false);
        }

        let mid_page_id = &self.record(mid_index)?.page_id()?.to_bytes();

        let mid_record = Record::new(Key::new(mid_key), mid_page_id);
        let Some(mid_bytes) = self.0.set(mid_index, mid_record.len())? else {
            return Ok(false);
        };
        mid_record.assign_to(mid_bytes)?;

        Ok(true)
    }

    #[inline]
    fn record_mut(&mut self, index: usize) -> mapping::Result<Record<&mut [u8]>> {
        Record::from_bytes(self.0.get_mut(index)?)
    }
}

#[cfg(test)]
mod tests {

    use super::Branch;
    use crate::medium::mapping::Result;

    #[test]
    fn test_search() -> Result<()> {
        let mut bytes = [0; 256];
        let mut branch = Branch::new(bytes.as_mut_slice())?;
        branch.init();

        branch.put(0, &[], 0.into())?;
        branch.put(1, b"1", 1.into())?;
        branch.put(2, b"3", 2.into())?;

        assert_eq!(branch.search(b"0")?, 0);
        assert_eq!(branch.search(b"1")?, 1);
        assert_eq!(branch.search(b"2")?, 1);
        assert_eq!(branch.search(b"3")?, 2);
        assert_eq!(branch.search(b"4")?, 2);

        Ok(())
    }

    #[test]
    fn test_split_put() -> Result<()> {
        let mut bytes = [0; 256];
        let mut branch = Branch::new(bytes.as_mut_slice())?;
        branch.init();

        let mut new_bytes = [0; 256];
        let mut new_branch = Branch::new(new_bytes.as_mut_slice())?;
        new_branch.init();

        branch.put(0, &[], 0.into())?;
        branch.put(1, b"1", 1.into())?;
        branch.put(2, b"3", 3.into())?;
        branch.put(3, b"5", 5.into())?;

        let key = branch.split_put(&mut new_branch, 2, b"2", 2.into())?;
        assert_eq!(key, b"2");
        assert_eq!(branch.page_id(branch.0.count() - 1)?, 1.into());

        assert_eq!(new_branch.record(0)?.key.as_ref(), &[]);
        assert_eq!(new_branch.page_id(0)?, 2.into());

        assert_eq!(new_branch.record(1)?.key.as_ref(), b"3");
        assert_eq!(new_branch.page_id(1)?, 3.into());

        Ok(())
    }
}

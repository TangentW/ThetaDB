use std::sync::RwLockReadGuard;

use crate::{
    bptree::TreeIndex,
    medium::mapping,
    meta::{MetaPage, PageIndex},
    storage::{Page, PageId, Storage},
};

pub(crate) struct Readonly<'a> {
    page_index: PageIndex,
    storage: RwLockReadGuard<'a, Storage>,
}

impl<'a> Readonly<'a> {
    #[inline]
    pub(crate) fn new(storage: RwLockReadGuard<'a, Storage>) -> mapping::Result<Self> {
        let page_index = storage
            .page::<MetaPage<_>>(PageIndex::META)?
            .page_index()
            .clone();

        Ok(Self {
            page_index,
            storage,
        })
    }
}

impl<'a> TreeIndex for Readonly<'a> {
    #[inline]
    fn root_id(&self) -> PageId {
        self.page_index.root
    }

    #[inline]
    fn page<'b, P>(&'b self, id: PageId) -> mapping::Result<P>
    where
        P: Page<&'b [u8]>,
    {
        self.storage.page(id)
    }
}

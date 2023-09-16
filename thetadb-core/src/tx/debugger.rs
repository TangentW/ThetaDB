use std::fmt::{Debug, Formatter};

use crate::{
    bptree::{BPTree, TreeIndex},
    chunk::Chunk,
    freelist::Freelist,
    meta::{Meta, MetaPage, PageIndex},
    tx::readonly::Readonly,
    Result, ThetaDB,
};

pub struct Debugger<'a> {
    _db: &'a ThetaDB,
    bptree: BPTree<Readonly<'a>>,
}

impl<'a> Debugger<'a> {
    pub fn new(db: &'a ThetaDB) -> Result<Self> {
        let storage = db.storage.read().unwrap();
        let bptree = Readonly::new(storage).map(BPTree::new)?;
        Ok(Self { _db: db, bptree })
    }

    #[inline]
    pub fn freelist_len(&self) -> Result<usize> {
        self.freelist().map(|f| f.len())
    }

    #[inline]
    pub fn page_size(&self) -> Result<u32> {
        self.meta().map(|m| m.page_size())
    }

    #[inline]
    fn freelist(&self) -> Result<Freelist> {
        Chunk::read(self.meta()?.page_index().freelist, |id| {
            self.bptree.as_index().page(id)
        })
        .map(|bytes| Freelist::from_bytes(&bytes))
        .map_err(Into::into)
    }

    #[inline]
    fn meta(&self) -> Result<Meta> {
        self.bptree
            .as_index()
            .page::<MetaPage<_>>(PageIndex::META)
            .map(|m| m.clone())
            .map_err(Into::into)
    }
}

impl Debug for Debugger<'_> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.bptree.dump(f)
    }
}

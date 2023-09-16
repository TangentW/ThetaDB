use crate::{
    bptree::{branch::Branch, entry::Value, leaf::Leaf, node::Node, NodePage},
    chunk::Chunk,
    medium::mapping,
    storage::{Page, PageId},
};

/// Defines how the B+ Tree reads data from the underlying storage.
pub(crate) trait TreeIndex {
    fn root_id(&self) -> PageId;

    fn page<'a, P>(&'a self, id: PageId) -> mapping::Result<P>
    where
        P: Page<&'a [u8]>;
}

/// Defines how the B+ Tree writes data from the underlying storage, it is also a TreeIndex.
pub(crate) trait TreeIndexMut: TreeIndex {
    fn page_size(&self) -> u32;

    fn set_root_id(&self, id: PageId);

    fn alloc<'a, P>(&'a self) -> mapping::Result<(PageId, P)>
    where
        P: Page<&'a mut [u8]>;

    fn shadow<'a, P>(&'a self, id: PageId) -> mapping::Result<(PageId, P)>
    where
        P: Page<&'a mut [u8]>;

    fn delete(&self, id: PageId);
}

pub(crate) trait TreeIndexExt: TreeIndex {
    #[inline]
    fn child(&self, branch: &Branch<&[u8]>, index: usize) -> mapping::Result<Node<&[u8]>> {
        let page_id = branch.page_id(index)?;
        self.node(page_id)
    }

    #[inline]
    fn value(&self, value: Value<&[u8]>) -> mapping::Result<Vec<u8>> {
        match value {
            Value::Bytes(bytes) => Ok(bytes.to_vec()),
            Value::Overflowed { page_id } => self.chunk(page_id),
        }
    }

    #[inline]
    fn root_node(&self) -> mapping::Result<Node<&[u8]>> {
        self.node(self.root_id())
    }

    #[inline]
    fn node(&self, id: PageId) -> mapping::Result<Node<&[u8]>> {
        self.page::<NodePage<_>>(id).and_then(|p| p.into_node())
    }

    #[inline]
    fn chunk(&self, id: PageId) -> mapping::Result<Vec<u8>> {
        Chunk::read(id, |id| self.page(id))
    }
}

impl<T> TreeIndexExt for T where T: TreeIndex {}

pub(crate) trait TreeIndexMutExt: TreeIndexMut {
    #[inline]
    fn alloc_branch_root(&self) -> mapping::Result<Branch<&mut [u8]>> {
        let (id, branch) = self.alloc_branch()?;
        self.set_root_id(id);
        Ok(branch)
    }

    #[inline]
    fn alloc_branch(&self) -> mapping::Result<(PageId, Branch<&mut [u8]>)> {
        let (id, page) = self.alloc::<NodePage<_>>()?;
        let branch = page.init_branch()?;
        Ok((id, branch))
    }

    #[inline]
    fn alloc_leaf(&self) -> mapping::Result<(PageId, Leaf<&mut [u8]>)> {
        let (id, page) = self.alloc::<NodePage<_>>()?;
        let leaf = page.init_leaf()?;
        Ok((id, leaf))
    }

    #[inline]
    fn shadow_root(&self) -> mapping::Result<(PageId, Node<&mut [u8]>)> {
        let (id, node) = self.shadow_node(self.root_id())?;
        self.set_root_id(id);
        Ok((id, node))
    }

    #[inline]
    fn shadow_node(&self, id: PageId) -> mapping::Result<(PageId, Node<&mut [u8]>)> {
        let (new_id, page) = self.shadow::<NodePage<_>>(id)?;
        let node = page.into_node()?;
        Ok((new_id, node))
    }

    #[inline]
    fn set_chunk(&self, slice: &[u8]) -> mapping::Result<PageId> {
        Chunk::write(slice, || self.alloc())
    }

    #[inline]
    fn delete_chunk(&self, id: PageId) -> mapping::Result<()> {
        Chunk::delete(id, |id| self.page(id), |id| Ok(self.delete(id)))
    }
}

impl<T> TreeIndexMutExt for T where T: TreeIndexMut {}

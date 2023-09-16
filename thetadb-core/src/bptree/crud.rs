use crate::{
    bptree::{
        branch::Branch,
        entry::Value,
        index::{TreeIndex, TreeIndexExt, TreeIndexMut, TreeIndexMutExt},
        node::Node,
        BPTree,
    },
    medium::mapping,
    storage::PageId,
};

impl<Index> BPTree<Index>
where
    Index: TreeIndex,
{
    pub(crate) fn contains(&self, key: &[u8]) -> mapping::Result<bool> {
        let mut node = self.index.root_node()?;
        loop {
            match node {
                Node::Branch(branch) => {
                    let index = branch.search(key)?;
                    node = self.index.child(&branch, index)?;
                }
                Node::Leaf(leaf) => break leaf.search(key).map(|i| i.is_ok()),
            }
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> mapping::Result<Option<Vec<u8>>> {
        let mut node = self.index.root_node()?;
        loop {
            match node {
                Node::Branch(branch) => {
                    let index = branch.search(key)?;
                    node = self.index.child(&branch, index)?;
                }
                Node::Leaf(leaf) => {
                    let Ok(index) = leaf.search(key)? else {
                        break Ok(None);
                    };
                    break leaf
                        .entry(index)
                        .and_then(|e| self.index.value(e.value))
                        .map(Some);
                }
            }
        }
    }
}

impl<Index> BPTree<Index>
where
    Index: TreeIndexMut,
{
    const VALUE_OVERFLOW_RATIO: f64 = 0.25;

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> mapping::Result<()> {
        let (root_id, root) = self.index.shadow_root()?;

        if let Some(mid) = self.put_inner(root, key, value)? {
            let mut new_root = self.index.alloc_branch_root()?;
            new_root.init_root(&mid.1, root_id, mid.0)?;
        }

        Ok(())
    }

    fn put_inner<'a>(
        &'a self,
        mut node: Node<&'a mut [u8]>,
        key: &[u8],
        value: &[u8],
    ) -> mapping::Result<Option<(PageId, Vec<u8>)>> {
        match &mut node {
            Node::Leaf(leaf) => {
                let index = leaf.search(key)?;

                // Delete overflow chunk of key.
                if let Ok(idx) = index && let Value::Overflowed { page_id } = leaf.entry(idx)?.value {
                    self.index.delete_chunk(page_id)?;
                }

                // If the value is overflow, then store it in new overflow pages (Chunk pages).
                let value = if self.is_value_overflow(value) {
                    let page_id = self.index.set_chunk(value)?;
                    Value::Overflowed { page_id }
                } else {
                    Value::Bytes(value)
                };

                // Try inserting data to see if there is enough space.
                if leaf.put(index, key, value)? {
                    return Ok(None);
                }

                // Obtain a new page.
                let (new_id, mut new) = self.index.alloc_leaf()?;
                // Split then put data
                let mid_key = leaf.split_put(&mut new, index, key, value)?;

                Ok(Some((new_id, mid_key)))
            }

            Node::Branch(branch) => {
                let index = branch.search(key)?;

                let child_id = branch.page_id(index)?;
                let (child_id, child) = self.index.shadow_node(child_id)?;

                // Update page id for shadow child page.
                branch.set_page_id(index, child_id)?;

                // Recursively add data to the following child nodes.
                let Some(mid) = self.put_inner(child, key, value)? else {
                    return Ok(None);
                };

                // Try inserting data to see if there is enough space.
                if branch.put(index + 1, &mid.1, mid.0)? {
                    return Ok(None);
                }

                // Obtain a new page
                let (new_id, mut new) = self.index.alloc_branch()?;
                // Split then put data
                let mid_key = branch.split_put(&mut new, index + 1, &mid.1, mid.0)?;

                Ok(Some((new_id, mid_key)))
            }
        }
    }

    #[inline]
    fn is_value_overflow(&self, value: &[u8]) -> bool {
        (value.len() as f64 / self.index.page_size() as f64) > Self::VALUE_OVERFLOW_RATIO
    }
}

/// Represents whether it is the next sibling.
type NextSibling = bool;

/// Represents a sibling of the node.
type Sibling<'a> = (PageId, Node<&'a [u8]>, NextSibling);

impl<Index> BPTree<Index>
where
    Index: TreeIndexMut,
{
    const NODE_UNDERFLOW_RATIO: f64 = 0.35;

    pub(crate) fn delete(&self, key: &[u8]) -> mapping::Result<()> {
        let (root_id, mut root) = self.index.shadow_root()?;

        self.delete_inner(&mut root, key)?;

        if let Node::Branch(root) = root && root.count() == 1 {
            self.index.set_root_id(root.page_id(0)?);
            self.index.delete(root_id);
        }

        Ok(())
    }

    fn delete_inner(&self, node: &mut Node<&mut [u8]>, key: &[u8]) -> mapping::Result<()> {
        match node {
            Node::Leaf(leaf) => {
                if let Ok(index) = leaf.search(key)? {
                    if let Value::Overflowed { page_id } = leaf.entry(index)?.value {
                        self.index.delete_chunk(page_id)?;
                    }
                    leaf.delete(index)?;
                }
            }

            Node::Branch(branch) => {
                let index = branch.search(key)?;

                let child_id = branch.page_id(index)?;
                let (child_id, mut child) = self.index.shadow_node(child_id)?;

                // Update page id for shadow child page.
                branch.set_page_id(index, child_id)?;

                self.delete_inner(&mut child, key)?;

                if child.fill_rate() > Self::NODE_UNDERFLOW_RATIO {
                    return Ok(());
                }

                // Only merge with siblings and don't borrow records from them.
                let Some((sibling_id, sibling, is_next)) = self.underflow_sibling(index, branch)?
                else {
                    if child.is_empty() {
                        self.index.delete(child_id);
                        branch.delete(index)?;
                    }
                    return Ok(());
                };
                let deleted_index = if is_next { index + 1 } else { index };

                // We assume that the key len won't exceed (2 * NODE_UNDERFLOW_RATIO * page size).
                // In fact, the key is already limited to a maximum len of 255,
                // so we just need to make sure the page size doesn't get smaller than 510.
                match &mut child {
                    Node::Leaf(child) => {
                        if let Some(sibling) = sibling.leaf() {
                            let res = child.merge(&sibling, is_next)?;
                            assert!(res, "should have enough space for merging");
                        }
                    }
                    Node::Branch(child) => {
                        if let Some(sibling) = sibling.branch() {
                            let mid_key = branch.key(deleted_index)?;
                            let res = child.merge(&mid_key, &sibling, is_next)?;
                            assert!(res, "should have enough space for merging");
                        }
                    }
                }

                self.index.delete(sibling_id);
                branch.delete(deleted_index)?;
                if !is_next {
                    branch.set_page_id(index - 1, child_id)?;
                }
            }
        }
        Ok(())
    }

    fn underflow_sibling(
        &self,
        index: usize,
        parent: &Branch<&mut [u8]>,
    ) -> mapping::Result<Option<Sibling>> {
        let sibling = |with_next| {
            if let Some(id) = parent.sibling(index, with_next)? {
                let sibling = self.index.node(id)?;

                if sibling.fill_rate() <= Self::NODE_UNDERFLOW_RATIO {
                    return Ok(Some((id, sibling, with_next)));
                }
            }
            Ok(None)
        };

        let prev = sibling(false)?;
        if prev.is_some() {
            Ok(prev)
        } else {
            sibling(true)
        }
    }
}

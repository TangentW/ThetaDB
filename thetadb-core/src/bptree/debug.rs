use std::fmt::Formatter;

use crate::{
    bptree::{branch::Branch, index::TreeIndexExt, leaf::Leaf, node::Node, BPTree, TreeIndex},
    medium::mapping,
};

impl<Index> BPTree<Index>
where
    Index: TreeIndex,
{
    #[inline]
    pub(crate) fn dump(&self, f: &mut Formatter) -> std::fmt::Result {
        writeln!(f, "┓")?;
        self.dump_node(self.index.root_node()?, String::new(), f)
    }

    #[inline]
    fn dump_node(
        &self,
        node: Node<&[u8]>,
        prefix: String,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        match node {
            Node::Branch(branch) => self.dump_branch(branch, prefix, f),
            Node::Leaf(leaf) => self.dump_leaf(leaf, prefix, f),
        }
    }

    fn dump_branch(
        &self,
        branch: Branch<&[u8]>,
        prefix: String,
        f: &mut Formatter,
    ) -> std::fmt::Result {
        for index in 0..branch.count() {
            let is_last = index == branch.count() - 1;
            let page_id = branch.page_id(index)?;

            if index > 0 {
                let key = branch.key(index)?;
                writeln!(f, "{}┣━ {:?}", prefix, String::from_utf8_lossy(&key))?;
            }

            f.write_str(&prefix)?;
            if is_last {
                writeln!(f, "┗━━━━━━┓ ({})", page_id.raw())?;
            } else {
                writeln!(f, "┣━━━━━━┓ ({})", page_id.raw())?;
            }

            let next = self.index.node(page_id)?;
            let next_prefix = prefix.clone() + if is_last { "       " } else { "┃      " };
            self.dump_node(next, next_prefix, f)?;
        }
        Ok(())
    }

    fn dump_leaf(&self, leaf: Leaf<&[u8]>, prefix: String, f: &mut Formatter) -> std::fmt::Result {
        for index in 0..leaf.count() {
            let is_last = index == leaf.count() - 1;
            let weld = if is_last { "┗" } else { "┣" };
            let entry = leaf.entry(index)?;
            let key = String::from_utf8_lossy(&entry.key);
            writeln!(f, "{}{}━ {:?}", prefix, weld, key)?;
        }
        Ok(())
    }
}

impl From<mapping::Error> for std::fmt::Error {
    #[inline]
    fn from(_: mapping::Error) -> Self {
        Self
    }
}

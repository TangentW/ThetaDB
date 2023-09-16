use std::{mem, ops::Range};

use crate::{
    bptree::{branch::Branch, leaf::Leaf},
    medium::{mapping, Bytes, BytesMut, Mapping, Padding},
    storage::Page,
};

pub(crate) enum Node<B> {
    Leaf(Leaf<B>),
    Branch(Branch<B>),
}

impl<B> Node<B>
where
    B: Bytes,
{
    #[inline]
    pub(crate) fn leaf(self) -> Option<Leaf<B>> {
        match self {
            Self::Leaf(leaf) => Some(leaf),
            Self::Branch(_) => None,
        }
    }

    #[inline]
    pub(crate) fn branch(self) -> Option<Branch<B>> {
        match self {
            Self::Branch(branch) => Some(branch),
            Self::Leaf(_) => None,
        }
    }

    #[inline]
    pub(crate) fn fill_rate(&self) -> f64 {
        match self {
            Self::Branch(branch) => branch.fill_rate(),
            Self::Leaf(leaf) => leaf.fill_rate(),
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Branch(branch) => branch.is_empty(),
            Self::Leaf(leaf) => leaf.is_empty(),
        }
    }

    #[inline]
    pub(crate) fn count(&self) -> usize {
        match self {
            Self::Branch(branch) => branch.count(),
            Self::Leaf(leaf) => leaf.count(),
        }
    }

    #[allow(clippy::unnecessary_lazy_evaluations)]
    #[inline]
    pub(crate) fn step_index(&self, index: usize, forward: bool) -> Option<usize> {
        if forward {
            self.last_index()
                .and_then(|m| (index < m).then_some(index + 1))
        } else {
            // Operation may overflow, so we use `then` instead of `then_some`.
            self.first_index()
                .and_then(|m| (index > m).then(|| index - 1))
        }
    }

    #[inline]
    pub(crate) fn first_index(&self) -> Option<usize> {
        self.index_range().min()
    }

    #[inline]
    pub(crate) fn last_index(&self) -> Option<usize> {
        self.index_range().max()
    }

    #[inline]
    fn index_range(&self) -> Range<usize> {
        0..self.count()
    }

    #[inline]
    fn new_leaf(bytes: B) -> mapping::Result<Self> {
        Leaf::new(bytes).map(Self::Leaf)
    }

    #[inline]
    fn new_branch(bytes: B) -> mapping::Result<Self> {
        Branch::new(bytes).map(Self::Branch)
    }
}

pub(crate) struct NodePage<B> {
    header: Mapping<B, PageHeader>,
    body: B,
}

#[derive(PartialEq, Eq)]
#[repr(u8)]
enum NodeType {
    Branch,
    Leaf,
}

const NODE_ALIGN: usize = mem::align_of::<u32>();
const PAGE_HEADER_PADDING: usize = NODE_ALIGN - mem::size_of::<NodeType>();

#[repr(C)]
struct PageHeader {
    node_type: NodeType,
    _padding: Padding<PAGE_HEADER_PADDING>,
}

impl<B> NodePage<B>
where
    B: Bytes,
{
    #[inline]
    pub(crate) fn into_node(self) -> mapping::Result<Node<B>> {
        match self.header.node_type {
            NodeType::Leaf => Node::new_leaf(self.body),
            NodeType::Branch => Node::new_branch(self.body),
        }
    }
}

impl<B> NodePage<B>
where
    B: BytesMut,
{
    #[inline]
    pub(crate) fn init_branch(mut self) -> mapping::Result<Branch<B>> {
        self.header.node_type = NodeType::Branch;
        let mut branch = Branch::new(self.body)?;
        branch.init();
        Ok(branch)
    }

    #[inline]
    pub(crate) fn init_leaf(mut self) -> mapping::Result<Leaf<B>> {
        self.header.node_type = NodeType::Leaf;
        let mut leaf = Leaf::new(self.body)?;
        leaf.init();
        Ok(leaf)
    }
}

unsafe impl<B> Page<B> for NodePage<B>
where
    B: Bytes,
{
    #[inline]
    fn from_bytes(bytes: B) -> mapping::Result<Self> {
        unsafe { Mapping::split(bytes) }.map(|(header, body)| Self { header, body })
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        bptree::{entry::Value, node::Node, NodePage},
        medium::mapping::Result,
        storage::Page,
    };

    #[test]
    fn test_node() -> Result<()> {
        let mut bytes = [0; 256];

        let mut branch = NodePage::from_bytes(bytes.as_mut())?.init_branch()?;
        branch.put(0, b"abc", 123.into())?;
        assert_eq!(branch.page_id(0)?, 123.into());

        let node = NodePage::from_bytes(bytes.as_ref())?.into_node()?;
        let Node::Branch(branch) = node else {
            panic!();
        };
        assert_eq!(branch.page_id(0)?, 123.into());

        let mut leaf = NodePage::from_bytes(bytes.as_mut())?.init_leaf()?;
        leaf.put(Err(0), b"abc", Value::Bytes(b"123"))?;
        assert!(matches!(leaf.entry(0)?.value, Value::Bytes(b"123")));

        let node = NodePage::from_bytes(bytes.as_ref())?.into_node()?;
        let Node::Leaf(leaf) = node else {
            panic!();
        };
        assert!(matches!(leaf.entry(0)?.value, Value::Bytes(b"123")));

        Ok(())
    }
}

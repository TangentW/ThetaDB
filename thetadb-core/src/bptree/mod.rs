pub(crate) use cursor::Cursor;
pub(crate) use index::{TreeIndex, TreeIndexMut};
pub(crate) use node::NodePage;

#[macro_use]
mod search;

mod branch;
mod crud;
mod cursor;
mod debug;
mod entry;
mod index;
mod leaf;
mod node;
mod slotted;

/// Represents a B+ Tree, All B+ tree algorithms in ThetaDB will be implemented here.
///
/// Its structure consists of [`node`]s, which are divided into two types: [`branch`] and [`leaf`].
///
/// Different kinds of algorithms will be implemented in different `mod`s.
/// See [`crud`], [`cursor`], [`debug`] for more details.
pub(crate) struct BPTree<Index> {
    /// The `Index` acts as a bridge between the B+ Tree and the underlying storage,
    /// determining the logic of the interaction between them. See [`index`] mod for more details.
    index: Index,
}

impl<Index> BPTree<Index> {
    #[inline]
    pub(crate) fn new(index: Index) -> Self {
        Self { index }
    }

    #[inline]
    pub(crate) fn as_index(&self) -> &Index {
        &self.index
    }

    #[inline]
    pub(crate) fn into_index(self) -> Index {
        self.index
    }
}

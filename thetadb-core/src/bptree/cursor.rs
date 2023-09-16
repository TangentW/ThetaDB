use crate::{
    bptree::{
        index::{TreeIndex, TreeIndexExt},
        node::Node,
        BPTree,
    },
    medium::mapping,
    storage::PageId,
};

pub(crate) struct Cursor<Index> {
    bptree: BPTree<Index>,
    track: Option<Track>,
}

pub(crate) type Track = Vec<Location>;

#[derive(Clone, Copy)]
pub(crate) struct Location {
    pub(crate) page_id: PageId,
    pub(crate) index: usize,
}

impl Location {
    #[inline]
    pub(crate) fn new(page_id: PageId, index: usize) -> Self {
        Self { page_id, index }
    }
}

impl<Index> Cursor<Index> {
    #[inline]
    pub(crate) fn new(bptree: BPTree<Index>) -> Self {
        Self {
            bptree,
            track: None,
        }
    }
}

impl<Index> Cursor<Index>
where
    Index: TreeIndex,
{
    pub(crate) fn key(&self) -> mapping::Result<Option<Vec<u8>>> {
        let Some(location) = self.entry_location() else {
            return Ok(None);
        };
        let Node::Leaf(leaf) = self.bptree.index.node(location.page_id)? else {
            return Ok(None);
        };
        Ok(Some(leaf.entry(location.index)?.key.to_vec()))
    }

    pub(crate) fn value(&self) -> mapping::Result<Option<Vec<u8>>> {
        let Some(location) = self.entry_location() else {
            return Ok(None);
        };
        let Node::Leaf(leaf) = self.bptree.index.node(location.page_id)? else {
            return Ok(None);
        };
        let value = self.bptree.index.value(leaf.entry(location.index)?.value)?;
        Ok(Some(value))
    }

    pub(crate) fn key_value(&self) -> mapping::Result<Option<(Vec<u8>, Vec<u8>)>> {
        let Some(location) = self.entry_location() else {
            return Ok(None);
        };
        let Node::Leaf(leaf) = self.bptree.index.node(location.page_id)? else {
            return Ok(None);
        };
        let entry = leaf.entry(location.index)?;

        let key = entry.key.to_vec();
        let value = self.bptree.index.value(entry.value)?;
        Ok(Some((key, value)))
    }

    #[inline]
    pub(crate) fn first(&mut self) -> mapping::Result<bool> {
        self.track = self.bptree.edge_track(true)?;
        Ok(self.track.is_some())
    }

    #[inline]
    pub(crate) fn last(&mut self) -> mapping::Result<bool> {
        self.track = self.bptree.edge_track(false)?;
        Ok(self.track.is_some())
    }

    #[inline]
    pub(crate) fn seek(&mut self, key: &[u8]) -> mapping::Result<bool> {
        self.track = self.bptree.track(key)?;
        Ok(self.track.is_some())
    }

    #[inline]
    pub(crate) fn step(&mut self, forward: bool) -> mapping::Result<bool> {
        let Some(track) = self.track.take() else {
            return Ok(false);
        };
        self.track = self.bptree.step_track(track, forward)?;
        Ok(self.track.is_some())
    }

    #[inline]
    fn entry_location(&self) -> Option<Location> {
        self.track.as_ref().and_then(|t| t.last().cloned())
    }
}

impl<Index> BPTree<Index>
where
    Index: TreeIndex,
{
    #[inline]
    fn track(&self, key: &[u8]) -> mapping::Result<Option<Track>> {
        let mut track = Vec::new();
        let (mut page_id, mut node) = (self.index.root_id(), self.index.root_node()?);

        loop {
            match &node {
                Node::Branch(branch) => {
                    let index = branch.search(key)?;
                    track.push(Location::new(page_id, index));

                    page_id = branch.page_id(index)?;
                    node = self.index.node(page_id)?;
                }
                Node::Leaf(leaf) => {
                    let Ok(index) = leaf.search(key)? else {
                        break Ok(None);
                    };
                    track.push(Location::new(page_id, index));
                    break Ok(Some(track));
                }
            }
        }
    }

    fn edge_track(&self, first: bool) -> mapping::Result<Option<Track>> {
        let mut track = Vec::new();
        let (mut page_id, mut node) = (self.index.root_id(), self.index.root_node()?);

        loop {
            let index = if first {
                node.first_index()
            } else {
                node.last_index()
            };
            let Some(index) = index else {
                break Ok(None);
            };

            track.push(Location::new(page_id, index));

            match &node {
                Node::Branch(branch) => {
                    page_id = branch.page_id(index)?;
                    node = self.index.node(page_id)?;
                }
                Node::Leaf(_) => break Ok(Some(track)),
            }
        }
    }

    fn step_track(&self, mut track: Track, forward: bool) -> mapping::Result<Option<Track>> {
        let Some(mut location) = track.pop() else {
            return Ok(None);
        };

        let node = self.index.node(location.page_id)?;
        if let Some(next_index) = node.step_index(location.index, forward) {
            location.index = next_index;
            track.push(location);
            return Ok(Some(track));
        }

        track = if let Some(track) = self.step_track(track, forward)? {
            track
        } else {
            return Ok(None);
        };

        let parent = track.last().unwrap();

        location.page_id = match &self.index.node(parent.page_id)? {
            Node::Branch(branch) => branch.page_id(parent.index)?,
            // Parent node should not be leaf, so this code branch should not be reachable.
            Node::Leaf(_) => return Ok(None),
        };

        let node = self.index.node(location.page_id)?;
        let index = if forward {
            node.first_index()
        } else {
            node.last_index()
        };

        location.index = if let Some(index) = index {
            index
        } else {
            return Ok(None);
        };

        track.push(location);
        Ok(Some(track))
    }
}

use std::{
    cell::RefCell,
    collections::{hash_map, HashMap},
    iter,
    sync::{MutexGuard, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{
    bptree::{TreeIndex, TreeIndexMut},
    chunk::Chunk,
    db::TxCoordinator,
    freelist::Freelist,
    medium::{mapping, mempool::MemoryCell},
    meta::{MetaPage, PageIndex},
    storage::{Page, PageId, Storage},
    Result,
};

pub(crate) struct ReadWrite<'a> {
    context: RefCell<Context>,
    coordinator: MutexGuard<'a, TxCoordinator>,
    storage: RwLockReadGuard<'a, Storage>,
}

type DirtyPages = HashMap<PageId, DirtyPage>;

enum DirtyPage {
    Allocated { memcell: MemoryCell },
    Deleted,
}

struct Context {
    page_index: PageIndex,
    freelist: Freelist,
    dirty_pages: DirtyPages,
}

impl Context {
    #[inline]
    fn new(page_index: PageIndex, freelist: Freelist) -> Self {
        Self {
            page_index,
            freelist,
            dirty_pages: DirtyPages::new(),
        }
    }

    #[inline]
    fn allocated_page(&self, id: PageId) -> Option<&MemoryCell> {
        self.dirty_pages.get(&id).map(|p| match p {
            DirtyPage::Allocated { memcell } => memcell,
            DirtyPage::Deleted => panic!("the page has been deleted"),
        })
    }

    #[inline]
    fn allocated_page_mut(&mut self, id: PageId) -> Option<&mut MemoryCell> {
        self.dirty_pages.get_mut(&id).map(|p| match p {
            DirtyPage::Allocated { memcell } => memcell,
            DirtyPage::Deleted => panic!("the page has been deleted"),
        })
    }

    #[inline]
    fn deleted_pages(&self) -> impl Iterator<Item = PageId> + '_ {
        self.dirty_pages
            .iter()
            .filter_map(|(id, page)| matches!(page, DirtyPage::Deleted).then_some(id))
            .cloned()
    }

    #[inline]
    fn alloc_id(&mut self) -> PageId {
        self.freelist
            .take(1)
            .unwrap_or_else(|| self.page_index.next.incr())
    }

    fn alloc(&mut self, memcell: MemoryCell) -> (PageId, &mut MemoryCell) {
        let id = self.alloc_id();
        let entry = self.dirty_pages.entry(id);

        assert!(
            matches!(entry, hash_map::Entry::Vacant(_)),
            "page was dirty before"
        );

        let memcell = match entry.or_insert(DirtyPage::Allocated { memcell }) {
            DirtyPage::Allocated { memcell } => memcell,
            DirtyPage::Deleted => unreachable!(),
        };

        (id, memcell)
    }

    fn delete(&mut self, id: PageId) {
        if let Some(dirty_page) = self.dirty_pages.remove(&id) {
            match dirty_page {
                DirtyPage::Allocated { .. } => self.freelist.free(id, 1),
                DirtyPage::Deleted => panic!("the page has been deleted"),
            }
        } else {
            self.dirty_pages.insert(id, DirtyPage::Deleted);
        }
    }

    #[inline]
    fn freelist_len(&self) -> u32 {
        self.deleted_pages()
            .max()
            .map(Freelist::bytes_len_for_storing)
            .unwrap_or(0)
            .max(self.freelist.bytes_len() as u32)
    }
}

impl<'a> ReadWrite<'a> {
    #[inline]
    pub(crate) fn new(
        coordinator: MutexGuard<'a, TxCoordinator>,
        storage: RwLockReadGuard<'a, Storage>,
    ) -> mapping::Result<Self> {
        let page_index = storage
            .page::<MetaPage<_>>(PageIndex::META)?
            .page_index()
            .clone();

        let freelist = Chunk::read(page_index.freelist, |id| storage.page(id))
            .map(|bytes| Freelist::from_bytes(&bytes))?;

        let context = Context::new(page_index, freelist);

        Ok(Self {
            context: context.into(),
            coordinator,
            storage,
        })
    }

    #[inline]
    pub(crate) fn commit<F>(self, force_sync: bool, writable_storage: F) -> Result<()>
    where
        F: FnOnce() -> RwLockWriteGuard<'a, Storage>,
    {
        // If there are no dirty pages, then nothing is required next.
        if self.context.borrow().dirty_pages.is_empty() {
            return Ok(());
        }

        let mut context = self.context.into_inner();

        // Alloc new pages for freelist.
        let freelist_len = context.freelist_len();
        let freelist_ids = iter::repeat_with(|| context.alloc_id())
            .take(Chunk::count(freelist_len, self.storage.page_size()) as usize)
            .collect::<Vec<_>>();

        // Update freelist id.
        Chunk::delete(
            context.page_index.freelist,
            |id| self.storage.page(id),
            |id| Ok(context.delete(id)),
        )?;
        context.page_index.freelist = *freelist_ids
            .first()
            .expect("should have at least one page to store freelist");

        // Acquires the storage with write access.
        drop(self.storage);
        let mut storage = writable_storage();

        // Allocate enough space for storage.
        storage.allocate(context.page_index.page_count())?;

        // Write dirty pages into storage.
        for (id, dirty_page) in context.dirty_pages {
            match dirty_page {
                DirtyPage::Allocated { memcell } => {
                    storage.copy_page_from_bytes(id, memcell.as_ref())?;
                }
                DirtyPage::Deleted => {
                    context.freelist.free(id, 1);
                }
            }
        }

        // Write freelist into storage.
        context.freelist.resize(freelist_len as usize);

        let freelist_bytes = context.freelist.into_bytes();
        let mut freelist_slice = freelist_bytes.as_slice();

        for (idx, id) in freelist_ids.iter().enumerate() {
            let mut chunk = storage.page_mut::<Chunk<_>>(*id)?;
            let Some((remaining, next)) = chunk.assign(freelist_slice) else {
                break;
            };

            freelist_slice = remaining;
            if let Some(next_id) = (idx < freelist_ids.len() - 1).then(|| freelist_ids[idx + 1]) {
                *next = next_id;
            }
        }

        if force_sync {
            storage.sync()?;
        }

        // Write meta into storage.
        storage
            .page_mut::<MetaPage<_>>(PageIndex::META)?
            .set_page_index(context.page_index);

        if force_sync {
            storage.sync()?;
        }

        Ok(())
    }
}

impl<'a> TreeIndex for ReadWrite<'a> {
    #[inline]
    fn root_id(&self) -> PageId {
        self.context.borrow().page_index.root
    }

    #[inline]
    fn page<'b, P>(&'b self, id: PageId) -> mapping::Result<P>
    where
        P: Page<&'b [u8]>,
    {
        self.context
            .borrow()
            .allocated_page(id)
            .map(|p| P::from_bytes(unsafe { p.as_slice() }))
            .unwrap_or_else(|| self.storage.page(id))
    }
}

impl<'a> TreeIndexMut for ReadWrite<'a> {
    #[inline]
    fn page_size(&self) -> u32 {
        self.storage.page_size()
    }

    #[inline]
    fn set_root_id(&self, id: PageId) {
        self.context.borrow_mut().page_index.root = id;
    }

    fn alloc<'b, P>(&'b self) -> mapping::Result<(PageId, P)>
    where
        P: Page<&'b mut [u8]>,
    {
        let mut context = self.context.borrow_mut();
        let (id, memcell) = context.alloc(self.coordinator.mempool.obtain_cell());

        let page = P::from_bytes(unsafe { memcell.as_mut_slice() })?;
        Ok((id, page))
    }

    fn shadow<'b, P>(&'b self, id: PageId) -> mapping::Result<(PageId, P)>
    where
        P: Page<&'b mut [u8]>,
    {
        let mut context = self.context.borrow_mut();

        let (id, memcell) = if let Some(memcell) = context.allocated_page_mut(id) {
            (id, memcell)
        } else {
            context.delete(id);

            let (new_id, memcell) = context.alloc(self.coordinator.mempool.obtain_cell());
            self.storage.copy_page_to_bytes(id, memcell.as_mut())?;
            (new_id, memcell)
        };

        let page = P::from_bytes(unsafe { memcell.as_mut_slice() })?;
        Ok((id, page))
    }

    #[inline]
    fn delete(&self, id: PageId) {
        self.context.borrow_mut().delete(id);
    }
}

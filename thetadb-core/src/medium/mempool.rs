use core::slice;
use std::{
    alloc,
    alloc::Layout,
    cell::RefCell,
    mem,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    rc::Rc,
};

/// A struct representing a memory pool that manages memory cells.
///
/// It recycles memory of cell when cell is dropped and reuses it
/// when new cell is requested. This can be more efficient than constantly
/// allocating and deallocating memory.
#[derive(Clone)]
pub(crate) struct MemoryPool {
    inner: Rc<Inner>,
}

/// A struct representing a memory cell.
///
/// We can obtain a memory cell from the pool.
pub(crate) struct MemoryCell {
    pool: MemoryPool,
    raw: ManuallyDrop<RawCell>,
}

struct Inner {
    capacity: usize,
    cell_layout: Layout,
    recycle_chain: RefCell<RecycleChain>,
}

impl MemoryPool {
    const CELL_ALIGN: usize = mem::align_of::<usize>();

    /// Constructor. It takes the size of each memory cell
    /// and the maximum capacity of the pool.
    pub(crate) fn new(cell_len: usize, capacity: usize) -> Self {
        let cell_layout = Layout::from_size_align(cell_len, Self::CELL_ALIGN).expect("impossible");

        let recycle_chain = RefCell::new(RecycleChain::new());

        let inner = Inner {
            capacity,
            cell_layout,
            recycle_chain,
        };

        Self {
            inner: Rc::new(inner),
        }
    }

    /// Obtains a memory cell from the pool.
    #[inline]
    pub(crate) fn obtain_cell(&self) -> MemoryCell {
        let raw_cell = self.obtain_raw_cell();
        MemoryCell::new(self, raw_cell)
    }

    /// Obtains a raw cell from the pool.
    ///
    /// If there are no recycled cells available, it creates a new one.
    #[inline]
    fn obtain_raw_cell(&self) -> RawCell {
        self.inner
            .recycle_chain
            .borrow_mut()
            .pop_cell()
            .unwrap_or_else(|| RawCell::new(self.inner.cell_layout))
    }

    /// Recycles a raw cell back into the pool.
    ///
    /// If the pool is already at capacity, the cell is dropped.
    #[inline]
    fn recycle_raw_cell(&self, raw_cell: RawCell) {
        let mut recycle_chain = self.inner.recycle_chain.borrow_mut();
        if recycle_chain.len < self.inner.capacity {
            recycle_chain.push_cell(raw_cell);
        }
    }
}

impl MemoryCell {
    #[inline]
    fn new(pool: &MemoryPool, raw: RawCell) -> Self {
        Self {
            pool: pool.clone(),
            raw: ManuallyDrop::new(raw),
        }
    }

    /// Returns the size of the memory cell.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.raw.layout.size()
    }

    /// Returns a slice to the memory cell.
    #[inline]
    pub(crate) unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        slice::from_raw_parts(self.as_ptr(), self.len())
    }

    /// Returns a mutable slice to the memory cell.
    #[inline]
    pub(crate) unsafe fn as_mut_slice<'a>(&self) -> &'a mut [u8] {
        slice::from_raw_parts_mut(self.as_mut_ptr(), self.len())
    }

    /// Returns a raw pointer to the memory cell.
    #[inline]
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.raw.ptr.as_ptr()
    }

    /// Returns a mutable raw pointer to the memory cell.
    #[inline]
    pub(crate) fn as_mut_ptr(&self) -> *mut u8 {
        self.raw.ptr.as_ptr()
    }
}

impl Deref for MemoryCell {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
}

impl DerefMut for MemoryCell {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }
}

impl Drop for MemoryCell {
    #[inline]
    fn drop(&mut self) {
        // Recycles the raw cell back into the pool.
        let raw = unsafe { ManuallyDrop::take(&mut self.raw) };
        self.pool.recycle_raw_cell(raw);
    }
}

struct RawCell {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl RawCell {
    fn new(layout: Layout) -> Self {
        // Allocates memory for the cell.
        let ptr = unsafe {
            let ptr = alloc::alloc(layout);
            if ptr.is_null() {
                alloc::handle_alloc_error(layout);
            }
            NonNull::new_unchecked(ptr)
        };

        Self { ptr, layout }
    }
}

impl Drop for RawCell {
    #[inline]
    fn drop(&mut self) {
        // Deallocates the memory of the cell.
        unsafe {
            alloc::dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

/// A struct representing a node in the recycle chain.
struct RecycleNode {
    cell: RawCell,
    next: Option<Box<RecycleNode>>,
}

/// A simple singly linked list representing a recycle chain.
#[derive(Default)]
struct RecycleChain {
    len: usize,
    head: Option<Box<RecycleNode>>,
}

impl RecycleChain {
    #[inline]
    fn new() -> Self {
        Default::default()
    }

    fn push_cell(&mut self, cell: RawCell) {
        let node = RecycleNode {
            cell,
            next: self.head.take(),
        };
        self.head = Some(Box::new(node));
        self.len += 1;
    }

    fn pop_cell(&mut self) -> Option<RawCell> {
        if let Some(mut node) = self.head.take() {
            self.len -= 1;
            self.head = node.next.take();
            Some(node.cell)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryPool;

    #[test]
    fn test_mempool() {
        const CAPACITY: usize = 4;
        let pool = MemoryPool::new(20, CAPACITY);
        assert_eq!(pool.inner.recycle_chain.borrow().len, 0);

        let cell_0 = pool.obtain_cell();
        let cell_1 = pool.obtain_cell();
        assert_eq!(pool.inner.recycle_chain.borrow().len, 0);

        drop(cell_0);
        drop(cell_1);
        assert_eq!(pool.inner.recycle_chain.borrow().len, 2);

        let cell_0 = pool.obtain_cell();
        assert_eq!(pool.inner.recycle_chain.borrow().len, 1);

        let cell_1 = pool.obtain_cell();
        let cell_2 = pool.obtain_cell();
        let cell_3 = pool.obtain_cell();
        let cell_4 = pool.obtain_cell();
        assert_eq!(pool.inner.recycle_chain.borrow().len, 0);

        drop(cell_0);
        drop(cell_1);
        drop(cell_2);
        drop(cell_3);
        drop(cell_4);
        assert_eq!(pool.inner.recycle_chain.borrow().len, CAPACITY);
    }
}

use std::{
    path::Path,
    sync::{Mutex, RwLock},
};

use crate::{
    bptree::NodePage,
    chunk::Chunk,
    error::Result,
    freelist::Freelist,
    medium::{mempool::MemoryPool, File},
    meta::{Meta, MetaPage, PageIndex, ValidationError},
    storage::{Page, Storage},
    tx::{CursorTx, Debugger, Tx, TxMut},
};

/// The options for configuring a ThetaDB instance.
#[derive(Debug, Clone)]
pub struct Options {
    pub(crate) page_size: Option<u32>,
    pub(crate) force_sync: bool,
    pub(crate) mempool_capacity: usize,
}

impl Options {
    /// Creates a new Options instance with default values.
    #[inline]
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the size of a page in the ThetaDB.
    ///
    /// By default, it is the operating system's memory page size. And the minimum
    /// page size is 4 KB.
    #[inline]
    pub fn page_size(&mut self, page_size: Option<u32>) -> &mut Self {
        self.page_size = page_size;
        self
    }

    /// Decide whether to force synchronization on every commit of the read write transaction.
    ///
    /// If it is true, every commit of the read write transaction will be immediately followed
    /// by a sync operation. If it is false, sync operation will be performed according to the
    /// operating system's internal logic.
    #[inline]
    pub fn force_sync(&mut self, flag: bool) -> &mut Self {
        self.force_sync = flag;
        self
    }

    /// Set the capacity of the memory pool. Represents the number of pages that can be reused.
    ///
    /// By default, it is 4.
    #[inline]
    pub fn mempool_capacity(&mut self, capacity: usize) -> &mut Self {
        self.mempool_capacity = capacity;
        self
    }

    /// Open a ThetaDB instance with the current options.
    #[inline]
    pub fn open(&self, path: impl AsRef<Path>) -> Result<ThetaDB> {
        ThetaDB::open_with_options(path, self.clone())
    }
}

impl Default for Options {
    #[inline]
    fn default() -> Self {
        Self {
            page_size: None,
            force_sync: false,
            mempool_capacity: 4,
        }
    }
}

/// The main database struct, all entry points are here.
pub struct ThetaDB {
    pub(crate) options: Options,
    pub(crate) storage: RwLock<Storage>,
    pub(crate) rw_coordinator: Mutex<TxCoordinator>,
}

pub(crate) struct TxCoordinator {
    pub(crate) mempool: MemoryPool,
}

impl ThetaDB {
    /// Open a ThetaDB instance at the given file path with default options.
    #[inline]
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, Options::default())
    }

    /// Open a ThetaDB instance at the given file path with the provided options.
    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let file = File::open(path)?;
        if file.is_empty() {
            Self::init(options, file)
        } else {
            Self::bind(options, file)
        }
    }

    /// Options used to configure the ThetaDB.
    #[inline]
    pub fn options(&self) -> Options {
        self.options.clone()
    }

    /// Check if the ThetaDB contains a given key.
    #[inline]
    pub fn contains(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.begin_tx()?.contains(key)
    }

    /// Get the value associated with a given key.
    #[inline]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.begin_tx()?.get(key)
    }

    /// Insert or update a key-value pair into the ThetaDB.
    #[inline]
    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let mut tx = self.begin_tx_mut()?;
        tx.put(key, value)?;
        tx.commit()
    }

    /// Delete a key-value pair from the ThetaDB.
    #[inline]
    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        let mut tx = self.begin_tx_mut()?;
        tx.delete(key)?;
        tx.commit()
    }

    /// Perform a read-only transaction using closure on the ThetaDB.
    #[inline]
    pub fn view<T>(&self, f: impl FnOnce(&Tx) -> Result<T>) -> Result<T> {
        let tx = self.begin_tx()?;
        f(&tx)
    }

    /// Perform a read-write transaction using closure on the ThetaDB.
    #[inline]
    pub fn update<T>(&self, f: impl FnOnce(&mut TxMut) -> Result<T>) -> Result<T> {
        let mut tx = self.begin_tx_mut()?;
        let value = f(&mut tx)?;
        tx.commit().map(|_| value)
    }

    /// Start a read-only transaction.
    #[inline]
    pub fn begin_tx(&self) -> Result<Tx> {
        Tx::new(self)
    }

    /// Start a read-write transaction.
    #[inline]
    pub fn begin_tx_mut(&self) -> Result<TxMut> {
        TxMut::new(self)
    }

    /// Get the cursor pointing to the first record in the ThetaDB.
    #[inline]
    pub fn first_cursor(&self) -> Result<CursorTx> {
        let mut cursor = CursorTx::new(self)?;
        cursor.first().map(|_| cursor)
    }

    /// Get the cursor pointing to the last record in the ThetaDB.
    #[inline]
    pub fn last_cursor(&self) -> Result<CursorTx> {
        let mut cursor = CursorTx::new(self)?;
        cursor.last().map(|_| cursor)
    }

    /// Get the cursor pointing to the specific record in the ThetaDB with the given key.
    #[inline]
    pub fn cursor_from_key(&self, key: &[u8]) -> Result<CursorTx> {
        let mut cursor = CursorTx::new(self)?;
        cursor.seek(key).map(|_| cursor)
    }

    /// Initialize a new ThetaDB file with the given options.
    fn init(options: Options, file: File) -> Result<Self> {
        let meta = options.page_size.map(Meta::new).unwrap_or_default();

        let mempool = MemoryPool::new(meta.page_size() as usize, 4);

        let mut storage = Storage::new(file, meta.page_size());
        storage.allocate(2 * meta.page_index().page_count())?;

        // Initialize root node page.
        storage
            .page_mut::<NodePage<_>>(meta.page_index().root)?
            .init_leaf()?;

        // Initialize freelist page.
        storage
            .page_mut::<Chunk<_>>(meta.page_index().freelist)?
            .assign(&Freelist::new().into_bytes());

        // Initialize meta page.
        *storage.page_mut::<MetaPage<_>>(PageIndex::META)? = meta;

        Ok(Self {
            options,
            storage: storage.into(),
            rw_coordinator: TxCoordinator { mempool }.into(),
        })
    }

    /// Bind to an existing ThetaDB file with the given options.
    fn bind(options: Options, file: File) -> Result<Self> {
        let meta = MetaPage::from_bytes(file.as_ref())
            .map_err(|_| ValidationError::FileInvalid)
            .and_then(|m| m.validate().and(Ok(m)))?
            .clone();

        let storage = Storage::new(file, meta.page_size());
        let mempool = MemoryPool::new(meta.page_size() as usize, options.mempool_capacity);

        Ok(Self {
            options,
            storage: storage.into(),
            rw_coordinator: TxCoordinator { mempool }.into(),
        })
    }
}

impl ThetaDB {
    /// Get a debugger for the ThetaDB.
    #[inline]
    pub fn debugger(&self) -> Result<Debugger> {
        Debugger::new(self)
    }
}

use std::fmt::Debug;

use thiserror::Error;

use crate::{
    bptree::{BPTree, Cursor},
    tx::{readonly::Readonly, readwrite::ReadWrite},
    Result, ThetaDB, MAX_KEY_LEN, MAX_VALUE_LEN,
};

mod debugger;
mod readonly;
mod readwrite;

pub use debugger::Debugger;

/// Represents the read-only transaction in ThetaDB.
pub struct Tx<'a>(BPTree<Readonly<'a>>);

impl<'a> Tx<'a> {
    /// Start a read-only transaction.
    pub fn new(db: &'a ThetaDB) -> Result<Self> {
        let storage = db.storage.read().unwrap();
        let bptree = Readonly::new(storage).map(BPTree::new)?;
        Ok(Self(bptree))
    }

    /// Check if the ThetaDB contains a given key.
    #[inline]
    pub fn contains(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.0.contains(key.as_ref()).map_err(Into::into)
    }

    /// Get the value associated with a given key.
    #[inline]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.0.get(key.as_ref()).map_err(Into::into)
    }
}

#[derive(Error, Debug)]
pub(crate) enum InputInvalid {
    #[error("the length of the key cannot exceed {}", MAX_KEY_LEN)]
    KeyInvalid,
    #[error("the length of the value cannot exceed {}", MAX_VALUE_LEN)]
    ValueInvalid,
}

/// Represents the read-write transaction in ThetaDB.
pub struct TxMut<'a> {
    db: &'a ThetaDB,
    bptree: BPTree<ReadWrite<'a>>,
}

impl<'a> TxMut<'a> {
    /// Start a read-write transaction.
    pub fn new(db: &'a ThetaDB) -> Result<Self> {
        let coordinator = db.rw_coordinator.lock().unwrap();
        let storage = db.storage.read().unwrap();
        let bptree = ReadWrite::new(coordinator, storage).map(BPTree::new)?;
        Ok(Self { db, bptree })
    }

    /// Check if the ThetaDB contains a given key.
    #[inline]
    pub fn contains(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.bptree.contains(key.as_ref()).map_err(Into::into)
    }

    /// Get the value associated with a given key.
    #[inline]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.bptree.get(key.as_ref()).map_err(Into::into)
    }

    /// Insert or update a key-value pair into the ThetaDB.
    #[inline]
    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let (key, value) = (key.as_ref(), value.as_ref());
        Self::validate_input(key, value)?;
        self.bptree.put(key, value).map_err(Into::into)
    }

    /// Delete a key-value pair from the ThetaDB.
    #[inline]
    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
        self.bptree.delete(key.as_ref()).map_err(Into::into)
    }

    /// Commit the read-write transaction, which means it has done all its work.
    #[inline]
    pub fn commit(self) -> Result<()> {
        self.bptree
            .into_index()
            .commit(self.db.options.force_sync, || {
                self.db.storage.write().unwrap()
            })
    }

    fn validate_input(key: &[u8], value: &[u8]) -> Result<()> {
        if key.as_ref().len() > MAX_KEY_LEN {
            return Err(InputInvalid::KeyInvalid.into());
        }
        if value.as_ref().len() > MAX_VALUE_LEN {
            return Err(InputInvalid::ValueInvalid.into());
        }
        Ok(())
    }
}

/// Represents a cursor for navigating through the ThetaDB.
pub struct CursorTx<'a>(Cursor<Readonly<'a>>);

impl<'a> CursorTx<'a> {
    /// Start a cursor transaction.
    pub fn new(db: &'a ThetaDB) -> Result<Self> {
        let storage = db.storage.read().unwrap();
        let bptree = Readonly::new(storage).map(BPTree::new).map(Cursor::new)?;
        Ok(Self(bptree))
    }

    /// Gets the key of the current record pointed by the cursor.
    #[inline]
    pub fn key(&self) -> Result<Option<Vec<u8>>> {
        self.0.key().map_err(Into::into)
    }

    /// Gets the value of the current record pointed by the cursor.
    #[inline]
    pub fn value(&self) -> Result<Option<Vec<u8>>> {
        self.0.value().map_err(Into::into)
    }

    /// Gets the key-value pair of the current record pointed by the cursor.
    #[inline]
    pub fn key_value(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        self.0.key_value().map_err(Into::into)
    }

    /// Moves the cursor to the first record.
    #[inline]
    pub fn first(&mut self) -> Result<bool> {
        self.0.first().map_err(Into::into)
    }

    /// Moves the cursor to the last record.
    #[inline]
    pub fn last(&mut self) -> Result<bool> {
        self.0.last().map_err(Into::into)
    }

    /// Moves the cursor to the specific record with the given key.
    #[inline]
    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.0.seek(key).map_err(Into::into)
    }

    /// Moves the cursor to the next record.
    #[allow(clippy::should_implement_trait)]
    #[inline]
    pub fn next(&mut self) -> Result<bool> {
        self.0.step(true).map_err(Into::into)
    }

    /// Moves the cursor to the previous record.
    #[inline]
    pub fn prev(&mut self) -> Result<bool> {
        self.0.step(false).map_err(Into::into)
    }
}

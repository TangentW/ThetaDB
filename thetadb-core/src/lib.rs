//! A lightweight, embedded key-value database for mobile clients (i.e., iOS, Android),
//! written in Rust.
//!
//! `ThetaDB` is suitable for use on mobile clients with **"High-Read, Low-Write"** demands,
//! it uses B+ Tree as the foundational layer for index management.
//!
//! Inspired by Go's [BoltDB](https://github.com/boltdb/bolt), ThetaDB uses `mmap`, relying
//! on the operating system to keep memory and database files in sync. ThetaDB also implements
//! `shadow paging` to guarantee atomicity and durability of transactions, preventing data loss
//! or damage to the internal structures of database.
//!
//! # Open Database
//!
//! Use following way to open the database at the specified path. If the database file does not
//! exist, ThetaDB will automatically create and initialize it.
//!
//! ```
//! use thetadb::{Options, ThetaDB, Result};
//! # fn try_main() -> Result<()> {
//!
//! let path = "target/db.theta";
//!
//! // The simplest way to open with default `Options`:
//! let db = ThetaDB::open(path)?;
//!
//! // Open with `Options`:
//! let db = Options::new()
//!     .force_sync(true)
//!     .mempool_capacity(8)
//!     .open(path)?;
//! # Ok(())
//! # }
//!
//! # fn main() { try_main().unwrap(); }
//! ```
//! ThetaDB will automatically close when the database instance is destroyed.
//!
//! # Get, Insert, Update, Delete
//!
//! ```
//! # use thetadb::{ThetaDB, Result};
//! # fn try_main() -> Result<()> {
//! # let db = ThetaDB::open("target/db.theta")?;
//! // Insert a new key-value pair into database.
//! db.put(b"foo", b"foo")?;
//!
//! // Check if the database contains a given key.
//! assert!(db.contains(b"foo")?);
//! assert!(!db.contains(b"unknown")?);
//!
//! // Get the value associated with a given key.
//! assert_eq!(
//!     db.get(b"foo")?,
//!     Some(b"foo".to_vec())
//! );
//! assert_eq!(
//!     db.get(b"unknown")?,
//!     None
//! );
//!
//! // Update an existing value associated with a given key.
//! db.put(b"foo", b"bar")?;
//! assert_eq!(
//!     db.get(b"foo")?,
//!     Some(b"bar".to_vec())
//! );
//!
//! // Delete an existing key-value pair from database.
//! db.delete(b"foo")?;
//! assert!(!db.contains(b"foo")?);
//! assert_eq!(
//!     db.get(b"foo")?,
//!     None
//! );
//! # Ok(())
//! # }
//! # fn main() { try_main().unwrap(); }
//! ```
//!
//! # Transaction
//!
//! ThetaDB has two kinds of transactions: `Read-Only Transaction` and `Read-Write Transaction`.
//! The read-only transaction allows for read-only access and the read-write transaction allows
//! modification.
//!
//! ThetaDB allows a number of read-only transactions at a time but allows at most one read-write
//! transaction at a time. When a read-write transaction is committing, it has exclusive access
//! to the database until the commit is completed, at which point other transactions trying to
//! access the database will be blocked. You can think of this situation as `shared access` and
//! `exclusive access` to reader-writer lock.
//!
//! ## Read-Only Transaction
//!
//! ```
//! # use thetadb::{ThetaDB, Result};
//! # fn try_main() -> Result<()> {
//! # let db = ThetaDB::open("target/db.theta")?;
//! // Start a read-only transaction.
//! let tx = db.begin_tx()?;
//!
//! // Then perform read-only access.
//! _ = tx.contains(b"foo")?;
//! _ = tx.get(b"foo")?;
//!
//! // Or you can perform a read-only transaction using closure,
//! // with `view` method:
//! db.view(|tx| {
//!     _ = tx.contains(b"foo")?;
//!     _ = tx.get(b"foo")?;
//!     Ok(())
//! })?;
//! # Ok(())
//! # }
//! # fn main() { try_main().unwrap(); }
//! ```
//!
//! ## Read-Write Transaction
//!
//! ThetaBD's read-write transactions are designed to automatically rollback, and therefore any
//! changes made to the transaction will be discarded unless you explicity call the `commit`
//! method.
//!
//! Or you can perform a read-write transaction using closure, if no errors occur, then the
//! transaction will be commit automatically after the closure call.
//!
//! ```
//! # use thetadb::{ThetaDB, Result};
//! # fn try_main() -> Result<()> {
//! # let db = ThetaDB::open("target/db.theta")?;
//! // Start a read-write transaction.
//! let mut tx = db.begin_tx_mut()?;
//!
//! // Then perform read-write access.
//! tx.put(b"hello", b"world")?;
//! _ = tx.get(b"hello")?;
//!
//! // Finally, commit the transaction.
//! tx.commit()?;
//!
//! // Or you can perform a read-write transaction using closure,
//! // with `update` method:
//! db.update(|tx| {
//!     tx.put(b"hello", b"world")?;
//!     _ = tx.get(b"hello")?;
//!     Ok(())
//! })?;
//! # Ok(())
//! # }
//! # fn main() { try_main().unwrap(); }
//! ```
//!
//! ## Attention
//!
//! ❗️ Transaction instances are nonsendable, which means it's not safe to send them to another
//! thread. Rust leverages `Ownership` system and the `Send` and `Sync` traits to enforce
//! requirements automatically, whereas Swift requires us to manually ensure these guarantees.
//!
//! ❗️ Read-only transactions and read-write transaction must not overlap, otherwise a deadlock
//! will be occurred.
//!
//! 😺 So ThetaDB recommends that if you want to use transactions, use the APIs with closure
//! parameter (i.e., `view`, `update`).
//!
//! # Cursor
//!
//! We can freely traverse the data in the ThetaDB using `Cursor`.
//!
//! For instance, we can iterate over all the key-value pairs in the ThetaDB like this:
//!
//! ```
//! # use thetadb::{ThetaDB, Result};
//! # fn try_main() -> Result<()> {
//! # let db = ThetaDB::open("target/db.theta")?;
//! // Forward traversal.
//! let mut cursor = db.first_cursor()?;
//! while let Some((key, value)) = cursor.key_value()? {
//!     println!("{:?} => {:?}", key, value);
//!     cursor.next()?;
//! }
//!
//! // Backward traversal.
//! let mut cursor = db.last_cursor()?;
//! while let Some((key, value)) = cursor.key_value()? {
//!     println!("{:?} => {:?}", key, value);
//!     cursor.prev()?;
//! }
//! # Ok(())
//! # }
//! # fn main() { try_main().unwrap(); }
//! ```
//!
//! Or we can perform range queries on ThetaDB in this way:
//!
//! ```
//! # #![feature(let_chains)]
//! # use thetadb::{ThetaDB, Result};
//! # fn try_main() -> Result<()> {
//! # let db = ThetaDB::open("target/db.theta")?;
//! let mut cursor = db.cursor_from_key(b"C")?;
//! // Enable `let_chains` feature, should add `#![feature(let_chains)]`
//! // to the crate attributes.
//! while let Some((key, value)) = cursor.key_value()? && key != b"G" {
//!     println!("{:?} => {:?}", key, value);
//!     cursor.next()?;
//! }
//! # Ok(())
//! # }
//! # fn main() { try_main().unwrap(); }
//! ```
//!
//! ## Attention
//!
//! ❗️ Cursor is also a transaction (can be understood as a read-only transaction), so it alse
//! follows the transaction considerations mentioned above.
//!

#![feature(let_chains)]
#![allow(clippy::unit_arg)]

mod bptree;
mod chunk;
mod db;
mod error;
mod freelist;
mod medium;
mod meta;
mod storage;
mod tx;

pub use crate::{
    db::{Options, ThetaDB},
    error::{Error, ErrorCode, Result},
    tx::{CursorTx, Debugger, Tx, TxMut},
};

/// The maximum length of a key that can be put into the database.
pub const MAX_KEY_LEN: usize = 255;
/// The maximum length of a value that can be put into the database.
pub const MAX_VALUE_LEN: usize = 10 * 1024 * 1024;

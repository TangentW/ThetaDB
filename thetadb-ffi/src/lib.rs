#![allow(clippy::missing_safety_doc)]

mod bytes;
mod ffi_call;

macro_rules! check_null_ptr {
    ($ptr:expr) => {
        if $ptr.is_null() {
            return Ok(crate::ffi_call::FFIDefault::default());
        }
    };
}

pub mod db {
    use std::ffi::c_void;

    use thetadb::ThetaDB;

    use crate::{
        bytes::{FFIBytes, FFIBytesRef},
        ffi_call::{ffi_call, FFICallState},
    };

    #[repr(C)]
    pub struct ThetaDBOptions {
        page_size: u32,
        force_sync: u8,
        mempool_capacity: u64,
    }

    impl From<ThetaDBOptions> for thetadb::Options {
        fn from(value: ThetaDBOptions) -> Self {
            let mut options = Self::new();
            options
                .page_size((value.page_size != 0).then_some(value.page_size))
                .force_sync(value.force_sync != 0)
                .mempool_capacity(value.mempool_capacity as usize);
            options
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_new(
        path: FFIBytesRef,
        options: ThetaDBOptions,
        call_state: &mut FFICallState,
    ) -> *mut c_void {
        ffi_call(call_state, || {
            ThetaDB::open_with_options(path.into_str(), options.into())
                .map(Box::new)
                .map(Box::into_raw)
                .map(|ptr| ptr as *mut c_void)
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_dealloc(db: *mut c_void, call_state: &mut FFICallState) {
        ffi_call(call_state, || {
            check_null_ptr!(db);
            drop(Box::from_raw(db as *mut ThetaDB));
            Ok(())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_contains(
        db: *const c_void,
        key: FFIBytesRef,
        call_state: &mut FFICallState,
    ) -> u8 {
        ffi_call(call_state, || {
            check_null_ptr!(db);
            let db = &*(db as *const ThetaDB);
            db.contains(key.into_slice()).map(|b| if b { 1 } else { 0 })
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_get(
        db: *const c_void,
        key: FFIBytesRef,
        call_state: &mut FFICallState,
    ) -> FFIBytes {
        ffi_call(call_state, || {
            check_null_ptr!(db);
            let db = &*(db as *const ThetaDB);
            db.get(key.into_slice())
                .map(|value| value.map(FFIBytes::new).unwrap_or_default())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_put(
        db: *const c_void,
        key: FFIBytesRef,
        value: FFIBytesRef,
        call_state: &mut FFICallState,
    ) {
        ffi_call(call_state, || {
            check_null_ptr!(db);
            let db = &*(db as *const ThetaDB);
            db.put(key.into_slice(), value.into_slice())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_delete(
        db: *const c_void,
        key: FFIBytesRef,
        call_state: &mut FFICallState,
    ) {
        ffi_call(call_state, || {
            check_null_ptr!(db);
            let db = &*(db as *const ThetaDB);
            db.delete(key.into_slice())
        })
    }
}

pub mod tx {
    use std::ffi::c_void;

    use thetadb::{ThetaDB, Tx};

    use crate::{
        bytes::{FFIBytes, FFIBytesRef},
        ffi_call::{ffi_call, FFICallState},
    };

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_begin_tx(
        db: *const c_void,
        call_state: &mut FFICallState,
    ) -> *mut c_void {
        ffi_call(call_state, || {
            check_null_ptr!(db);
            let db = &*(db as *const ThetaDB);
            db.begin_tx()
                .map(Box::new)
                .map(Box::into_raw)
                .map(|ptr| ptr as *mut c_void)
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_tx_dealloc(tx: *mut c_void, call_state: &mut FFICallState) {
        ffi_call(call_state, || {
            check_null_ptr!(tx);
            drop(Box::from_raw(tx as *mut Tx));
            Ok(())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_tx_contains(
        tx: *const c_void,
        key: FFIBytesRef,
        call_state: &mut FFICallState,
    ) -> u8 {
        ffi_call(call_state, || {
            check_null_ptr!(tx);
            let tx = &*(tx as *const Tx);
            tx.contains(key.into_slice()).map(|b| if b { 1 } else { 0 })
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_tx_get(
        tx: *const c_void,
        key: FFIBytesRef,
        call_state: &mut FFICallState,
    ) -> FFIBytes {
        ffi_call(call_state, || {
            check_null_ptr!(tx);
            let tx = &*(tx as *const Tx);
            tx.get(key.into_slice())
                .map(|value| value.map(FFIBytes::new).unwrap_or_default())
        })
    }
}

pub mod tx_mut {
    use std::ffi::c_void;

    use thetadb::{ThetaDB, TxMut};

    use crate::{
        bytes::{FFIBytes, FFIBytesRef},
        ffi_call::{ffi_call, FFICallState},
    };

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_begin_tx_mut(
        db: *const c_void,
        call_state: &mut FFICallState,
    ) -> *mut c_void {
        ffi_call(call_state, || {
            check_null_ptr!(db);
            let db = &*(db as *const ThetaDB);
            db.begin_tx_mut()
                .map(Box::new)
                .map(Box::into_raw)
                .map(|ptr| ptr as *mut c_void)
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_tx_mut_dealloc(
        tx: *mut c_void,
        call_state: &mut FFICallState,
    ) {
        ffi_call(call_state, || {
            check_null_ptr!(tx);
            drop(Box::from_raw(tx as *mut TxMut));
            Ok(())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_tx_mut_contains(
        tx: *const c_void,
        key: FFIBytesRef,
        call_state: &mut FFICallState,
    ) -> u8 {
        ffi_call(call_state, || {
            check_null_ptr!(tx);
            let tx = &*(tx as *const TxMut);
            tx.contains(key.into_slice()).map(|b| if b { 1 } else { 0 })
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_tx_mut_get(
        tx: *const c_void,
        key: FFIBytesRef,
        call_state: &mut FFICallState,
    ) -> FFIBytes {
        ffi_call(call_state, || {
            check_null_ptr!(tx);
            let tx = &*(tx as *const TxMut);
            tx.get(key.into_slice())
                .map(|value| value.map(Into::into).unwrap_or_default())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_tx_mut_put(
        tx: *mut c_void,
        key: FFIBytesRef,
        value: FFIBytesRef,
        call_state: &mut FFICallState,
    ) {
        ffi_call(call_state, || {
            check_null_ptr!(tx);
            let tx = &mut *(tx as *mut TxMut);
            tx.put(key.into_slice(), value.into_slice())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_tx_mut_delete(
        tx: *mut c_void,
        key: FFIBytesRef,
        call_state: &mut FFICallState,
    ) {
        ffi_call(call_state, || {
            check_null_ptr!(tx);
            let tx = &mut *(tx as *mut TxMut);
            tx.delete(key.into_slice())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_tx_mut_commit(tx: *mut c_void, call_state: &mut FFICallState) {
        ffi_call(call_state, || {
            check_null_ptr!(tx);
            let tx = Box::from_raw(tx as *mut TxMut);
            tx.commit()
        })
    }
}

pub mod cursor {
    use std::ffi::c_void;

    use thetadb::{CursorTx, ThetaDB};

    use crate::{
        bytes::{FFIBytes, FFIBytesRef},
        ffi_call::{ffi_call, FFICallState},
    };

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_first_cursor(
        db: *const c_void,
        call_state: &mut FFICallState,
    ) -> *mut c_void {
        ffi_call(call_state, || {
            check_null_ptr!(db);
            let db = &*(db as *const ThetaDB);
            db.first_cursor()
                .map(Box::new)
                .map(Box::into_raw)
                .map(|ptr| ptr as *mut c_void)
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_last_cursor(
        db: *const c_void,
        call_state: &mut FFICallState,
    ) -> *mut c_void {
        ffi_call(call_state, || {
            check_null_ptr!(db);
            let db = &*(db as *const ThetaDB);
            db.last_cursor()
                .map(Box::new)
                .map(Box::into_raw)
                .map(|ptr| ptr as *mut c_void)
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_cursor_from_key(
        db: *const c_void,
        key: FFIBytesRef,
        call_state: &mut FFICallState,
    ) -> *mut c_void {
        ffi_call(call_state, || {
            check_null_ptr!(db);
            let db = &*(db as *const ThetaDB);
            db.cursor_from_key(key.into_slice())
                .map(Box::new)
                .map(Box::into_raw)
                .map(|ptr| ptr as *mut c_void)
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_cursor_dealloc(
        cursor: *const c_void,
        call_state: &mut FFICallState,
    ) {
        ffi_call(call_state, || {
            check_null_ptr!(cursor);
            drop(Box::from_raw(cursor as *mut CursorTx));
            Ok(())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_cursor_next(
        cursor: *mut c_void,
        call_state: &mut FFICallState,
    ) -> u8 {
        ffi_call(call_state, || {
            check_null_ptr!(cursor);
            let cursor = &mut *(cursor as *mut CursorTx);
            cursor.next().map(|b| if b { 1 } else { 0 })
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_cursor_prev(
        cursor: *mut c_void,
        call_state: &mut FFICallState,
    ) -> u8 {
        ffi_call(call_state, || {
            check_null_ptr!(cursor);
            let cursor = &mut *(cursor as *mut CursorTx);
            cursor.prev().map(|b| if b { 1 } else { 0 })
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_cursor_key(
        cursor: *mut c_void,
        call_state: &mut FFICallState,
    ) -> FFIBytes {
        ffi_call(call_state, || {
            check_null_ptr!(cursor);
            let cursor = &mut *(cursor as *mut CursorTx);
            cursor
                .key()
                .map(|key| key.map(Into::into).unwrap_or_default())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_cursor_value(
        cursor: *mut c_void,
        call_state: &mut FFICallState,
    ) -> FFIBytes {
        ffi_call(call_state, || {
            check_null_ptr!(cursor);
            let cursor = &mut *(cursor as *mut CursorTx);
            cursor
                .value()
                .map(|key| key.map(Into::into).unwrap_or_default())
        })
    }

    #[no_mangle]
    pub unsafe extern "C" fn thetadb_cursor_key_value(
        cursor: *mut c_void,
        key: *mut FFIBytes,
        value: *mut FFIBytes,
        call_state: &mut FFICallState,
    ) {
        ffi_call(call_state, || {
            check_null_ptr!(cursor);
            let cursor = &mut *(cursor as *mut CursorTx);
            (*key, *value) = cursor
                .key_value()?
                .map(|(k, v)| (k.into(), v.into()))
                .unwrap_or_else(|| (FFIBytes::null(), FFIBytes::null()));
            Ok(())
        })
    }
}

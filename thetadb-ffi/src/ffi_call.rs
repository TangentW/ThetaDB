use std::{panic, ptr, thread};

use crate::bytes::FFIBytes;

#[repr(C)]
pub enum FFICallCode {
    FFICallSuccess = 0,
    FFICallPanic,
    FFICallErrIO,
    FFICallErrInputInvalid,
    FFICallErrFileUnexpected,
    FFICallErrDBCorrupted,
}

use FFICallCode::*;

#[repr(C)]
pub struct FFICallState {
    code: FFICallCode,
    err_desc: FFIBytes,
}

pub(crate) fn ffi_call<T, F>(state: &mut FFICallState, call: F) -> T
where
    T: FFIDefault,
    F: FnOnce() -> thetadb::Result<T> + panic::UnwindSafe,
{
    let result = panic::catch_unwind(call);
    *state = result.call_state();
    result
        .ok()
        .and_then(|r| r.ok())
        .unwrap_or(FFIDefault::default())
}

trait FFICallResult {
    fn call_state(&self) -> FFICallState;
}

impl<T> FFICallResult for thread::Result<T>
where
    T: FFICallResult,
{
    fn call_state(&self) -> FFICallState {
        match self {
            Ok(result) => result.call_state(),
            Err(err) => {
                let err_desc = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                    if let Some(s) = err.downcast_ref::<&'static str>() {
                        s.to_string()
                    } else if let Some(s) = err.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "panic!".to_string()
                    }
                    .into()
                }))
                .unwrap_or_default();

                FFICallState {
                    code: FFICallPanic,
                    err_desc,
                }
            }
        }
    }
}

impl<T> FFICallResult for thetadb::Result<T> {
    #[inline]
    fn call_state(&self) -> FFICallState {
        FFICallState {
            code: FFICallSuccess,
            err_desc: FFIBytes::null(),
        }
    }
}

impl FFICallResult for thetadb::Error {
    fn call_state(&self) -> FFICallState {
        let code = match self.code() {
            thetadb::ErrorCode::IO => FFICallErrIO,
            thetadb::ErrorCode::InputInvalid => FFICallErrInputInvalid,
            thetadb::ErrorCode::FileUnexpected => FFICallErrFileUnexpected,
            thetadb::ErrorCode::DatabaseCorrupted => FFICallErrDBCorrupted,
        };
        let err_desc = self.to_string().into();

        FFICallState { code, err_desc }
    }
}

pub(crate) trait FFIDefault {
    fn default() -> Self;
}

impl<T> FFIDefault for *mut T {
    #[inline]
    fn default() -> Self {
        ptr::null_mut()
    }
}

impl FFIDefault for () {
    #[inline]
    fn default() -> Self {}
}

impl FFIDefault for u8 {
    #[inline]
    fn default() -> Self {
        0
    }
}

impl FFIDefault for FFIBytes {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

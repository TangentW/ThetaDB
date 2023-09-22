use std::{ffi::c_void, mem::ManuallyDrop, ptr, slice, str};

use crate::ffi_call::{ffi_call, FFICallState};

#[repr(C)]
pub struct FFIBytesRef {
    ptr: *const c_void,
    length: u32,
}

impl FFIBytesRef {
    #[inline]
    pub(crate) unsafe fn into_slice<'a>(self) -> &'a [u8] {
        slice::from_raw_parts(self.ptr as *const u8, self.length as usize)
    }

    #[inline]
    pub(crate) unsafe fn into_str<'a>(self) -> &'a str {
        str::from_utf8(self.into_slice()).unwrap()
    }
}

#[repr(C)]
pub struct FFIBytes {
    ptr: *mut c_void,
    length: u32,
    capacity: u32,
}

impl FFIBytes {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self {
            length: bytes.len().try_into().expect("length cannot fit into u32"),
            capacity: bytes
                .capacity()
                .try_into()
                .expect("capacity cannot fit into u32"),
            ptr: ManuallyDrop::new(bytes).as_mut_ptr() as *mut c_void,
        }
    }

    #[inline]
    pub(crate) const fn null() -> Self {
        Self {
            ptr: ptr::null_mut(),
            length: 0,
            capacity: 0,
        }
    }

    #[inline]
    pub(crate) unsafe fn dealloc(self) {
        drop(self.lift());
    }

    unsafe fn lift(self) -> Vec<u8> {
        if self.ptr.is_null() {
            return Vec::new();
        }
        assert!(self.length <= self.capacity);

        let length = self
            .length
            .try_into()
            .expect("length negative or overflowed");
        let capacity = self
            .capacity
            .try_into()
            .expect("capacity negative or overflowed");

        Vec::from_raw_parts(self.ptr as *mut u8, length, capacity)
    }
}

impl Default for FFIBytes {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

impl<T> From<T> for FFIBytes
where
    T: Into<Vec<u8>>,
{
    #[inline]
    fn from(value: T) -> Self {
        Self::new(value.into())
    }
}

#[no_mangle]
pub unsafe extern "C" fn thetadb_bytes_dealloc(bytes: FFIBytes, call_state: &mut FFICallState) {
    ffi_call(call_state, || {
        bytes.dealloc();
        Ok(())
    })
}

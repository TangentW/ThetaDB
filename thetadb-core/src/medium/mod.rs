use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) use file::File;
pub(crate) use mapping::{Bytes, BytesMut, Mapping, Padding};

pub(crate) mod file;
pub(crate) mod mapping;
pub(crate) mod mempool;

/// Align the given size to the operating system's memory page size.
///
/// We can choose to align either upwards or downwards.
/// This function will return a value that is a multiple of the system's memory page size.
pub(crate) fn align_to_page_size(size: usize, upwards: bool) -> usize {
    let page_size = os_page_size();
    if upwards {
        ((size - 1) / page_size + 1) * page_size
    } else {
        size / page_size * page_size
    }
}

/// Obtains the operating system's memory page size.
pub(crate) fn os_page_size() -> usize {
    static PAGE_SIZE: AtomicUsize = AtomicUsize::new(0);

    match PAGE_SIZE.load(Ordering::Acquire) {
        0 => {
            let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) } as usize;
            PAGE_SIZE.store(page_size, Ordering::Release);
            page_size
        }
        page_size => page_size,
    }
}

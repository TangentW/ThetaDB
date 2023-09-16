use std::{
    fs, io,
    ops::{Deref, DerefMut},
    os::fd::{AsRawFd, RawFd},
    path::Path,
    ptr::{self, NonNull},
    result, slice,
};

use thiserror::Error;

use super::align_to_page_size;

pub(crate) type Result<T> = result::Result<T, Error>;

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("the file size exceeded the limit")]
    SizeOverflow,
    #[error(transparent)]
    IO(#[from] io::Error),
}

/// A handle to a file stored on disk.
///
/// It allows us to read and write disk file as easily as memory,
/// using mmap internally for mapping.
pub(crate) struct File {
    inner: fs::File,
    mmap: Option<Mmap>,
    len: usize,
}

impl File {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        // Create all necessary intermediate directories.
        if let Some(parent_dir) = path.parent() {
            fs::create_dir_all(parent_dir)?;
        }

        // Open the database file.
        let inner = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let len = inner.metadata()?.len() as usize;
        let mut file = Self {
            inner,
            len,
            mmap: None,
        };

        // If the file is not mapped, we consider the current file to be empty.
        if len > 0 {
            file.allocate(len)?;
        }

        Ok(file)
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

impl File {
    // We assume that the target platform is 64-bit or higher.
    // 4 GB
    const MAX_LENGTH: usize = 1 << 32;
    // 4 MB
    const MAX_INCREMENTAL_LENGTH: usize = 1 << 22;

    pub(crate) fn allocate(&mut self, len: usize) -> Result<()> {
        debug_assert_ne!(len, 0, "len is meaningless");
        if self.len >= len && self.mmap.is_some() {
            return Ok(());
        }

        let len = Self::adjust_length(self.len, len)?;

        // Truncate the file.
        if self.len != len {
            self.inner.set_len(len as u64)?;
            self.len = len;
        }

        // Unmap the previous mmap.
        drop(self.mmap.take());
        // mmap the file.
        self.mmap = Some(Mmap::new(self.inner.as_raw_fd(), self.len)?);

        Ok(())
    }

    #[inline]
    pub(crate) fn sync(&mut self) -> Result<()> {
        self.mmap
            .as_mut()
            .map_or(Ok(()), |m| m.sync())
            .map_err(Into::into)
    }

    fn adjust_length(mut len: usize, expected: usize) -> Result<usize> {
        // When the len is 0, we use the expected len as the default.
        if len == 0 {
            len = expected;
        }

        // Double the length or increase the length by the max incremental length.
        while len < expected {
            let increment = len.min(Self::MAX_INCREMENTAL_LENGTH);
            len += increment
        }

        // Ensure that the length is a multiple of the page size.
        len = align_to_page_size(len, true);

        // Ensure that the length is smaller than the max length.
        len = len.min(align_to_page_size(Self::MAX_LENGTH, false));

        if len >= expected {
            Ok(len)
        } else {
            Err(Error::SizeOverflow)
        }
    }
}

impl Deref for File {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.mmap
            .as_ref()
            .map(|m| unsafe { slice::from_raw_parts(m.as_ptr(), m.len()) })
            .unwrap_or_default()
    }
}

impl DerefMut for File {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.mmap
            .as_mut()
            .map(|m| unsafe { slice::from_raw_parts_mut(m.as_mut_ptr(), m.len()) })
            .unwrap_or_default()
    }
}

/// A handle to a fixed-length memory mapped buffer of the entire file.
struct Mmap {
    ptr: NonNull<u8>,
    len: usize,
}

impl Mmap {
    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Mmap {
    fn new(file: RawFd, len: usize) -> io::Result<Self> {
        unsafe {
            // The entire file will be mapped.
            let raw_ptr = libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file,
                0,
            );
            if raw_ptr == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }

            // Expects to access the mmap randomly.
            if libc::madvise(raw_ptr, len, libc::MADV_RANDOM) != 0 {
                return Err(io::Error::last_os_error());
            }

            let ptr = NonNull::new_unchecked(raw_ptr as *mut u8);
            Ok(Self { ptr, len })
        }
    }

    fn sync(&mut self) -> io::Result<()> {
        let ptr = self.as_mut_ptr() as *mut libc::c_void;
        unsafe {
            if libc::msync(ptr, self.len, libc::MS_SYNC) == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        let ptr = self.as_mut_ptr() as *mut libc::c_void;
        unsafe {
            _ = libc::munmap(ptr, self.len);
        }
    }
}

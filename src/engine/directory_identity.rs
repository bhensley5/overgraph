//! Read-only, handle-pinned filesystem identity. No pathname identity fallback.
#![allow(dead_code)] // Private preflight substrate; engine entry routing is a3.

use std::fs::File;
use std::io;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DirectoryIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(windows)]
    volume: u64,
    #[cfg(windows)]
    file_id: [u8; 16],
}

pub(super) struct DirectoryPin {
    identity: DirectoryIdentity,
    _file: File,
}

/// Open a directory handle with the platform flags used for identity custody.
/// Missing-database admission pins its existing parent through the same route.
pub(super) fn open_directory(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        File::open(path)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .access_mode(0x80) // FILE_READ_ATTRIBUTES only.
            .custom_flags(0x0200_0000) // FILE_FLAG_BACKUP_SEMANTICS for directories.
            .open(path)
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unsupported directory identity target",
        ))
    }
}

impl DirectoryPin {
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        let file = open_directory(path)?;
        #[cfg(not(any(unix, windows)))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unsupported directory identity target",
        ));
        #[cfg(any(unix, windows))]
        {
            let metadata = file.metadata()?;
            if !metadata.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "database identity requires a directory",
                ));
            }
            #[cfg(unix)]
            let identity = {
                use std::os::unix::fs::MetadataExt;
                DirectoryIdentity {
                    device: metadata.dev(),
                    inode: metadata.ino(),
                }
            };
            #[cfg(windows)]
            let identity = windows_identity(&file)?;
            Ok(Self {
                identity,
                _file: file,
            })
        }
    }

    pub(super) fn identity(&self) -> DirectoryIdentity {
        self.identity
    }
}

#[cfg(windows)]
fn windows_identity(file: &File) -> io::Result<DirectoryIdentity> {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle;
    // FILE_ID_INFO: https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_id_info
    #[repr(C)]
    struct FileIdInfo {
        volume: u64,
        file_id: [u8; 16],
    }
    const _: () = assert!(std::mem::size_of::<FileIdInfo>() == 24);
    const _: () = assert!(std::mem::align_of::<FileIdInfo>() == 8);
    #[link(name = "kernel32")]
    extern "system" {
        fn GetFileInformationByHandleEx(
            handle: *mut c_void,
            class: i32,
            info: *mut c_void,
            size: u32,
        ) -> i32;
    }
    let mut info = FileIdInfo {
        volume: 0,
        file_id: [0; 16],
    };
    // SAFETY: the owned File keeps a valid borrowed handle alive for this
    // synchronous call. FileIdInfo=18 selects the documented 24-byte repr(C)
    // buffer, passed writable with its exact length. No pointer/handle escapes;
    // BOOL is checked before reading results. This is the selected fixed FFI,
    // not an additional collector/allocator safety exception.
    let succeeded = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            18,
            (&mut info as *mut FileIdInfo).cast(),
            24,
        )
    };
    if succeeded == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(DirectoryIdentity {
        volume: info.volume,
        file_id: info.file_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn p45a2_directory_identity_rejects_files_and_shares_directory_aliases() {
        let dir = tempfile::TempDir::new().unwrap();
        let direct = DirectoryPin::open(dir.path()).unwrap();
        let dot = DirectoryPin::open(&dir.path().join(".")).unwrap();
        assert_eq!(direct.identity(), dot.identity());
        let other = tempfile::TempDir::new().unwrap();
        assert_ne!(
            direct.identity(),
            DirectoryPin::open(other.path()).unwrap().identity()
        );
        let file = dir.path().join("ordinary-file");
        std::fs::write(&file, b"unchanged").unwrap();
        assert!(DirectoryPin::open(&file).is_err());
        assert_eq!(std::fs::read(&file).unwrap(), b"unchanged");
        #[cfg(unix)]
        {
            let alias = other.path().join("alias");
            std::os::unix::fs::symlink(dir.path(), &alias).unwrap();
            assert_eq!(
                direct.identity(),
                DirectoryPin::open(&alias).unwrap().identity()
            );
        }
    }
}

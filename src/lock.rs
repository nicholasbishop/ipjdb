use crate::error::Error;
use fs2::FileExt;
use std::fs;
use std::path::Path;

pub struct FileLock {
    file: fs::File,
    is_locked: bool,
}

impl FileLock {
    pub fn exclusive(path: &Path) -> Result<FileLock, Error> {
        let file = fs::File::open(path)?;
        file.lock_exclusive()?;
        Ok(FileLock {
            file,
            is_locked: true,
        })
    }

    pub fn shared(path: &Path) -> Result<FileLock, Error> {
        let file = fs::File::open(path)?;
        file.lock_shared()?;
        Ok(FileLock {
            file,
            is_locked: true,
        })
    }

    pub fn unlock(&mut self) -> Result<(), Error> {
        self.file.unlock()?;
        self.is_locked = false;
        Ok(())
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if self.is_locked {
            self.unlock().expect("failed to unlock file");
        }
    }
}

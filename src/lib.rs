use fs2::FileExt;
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum DbError {
    InvalidObjectId,
    IoError(io::Error),
    JsonError(serde_json::error::Error),
}

impl From<io::Error> for DbError {
    fn from(error: io::Error) -> Self {
        DbError::IoError(error)
    }
}

impl From<serde_json::error::Error> for DbError {
    fn from(error: serde_json::error::Error) -> Self {
        DbError::JsonError(error)
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            DbError::InvalidObjectId => write!(f, "InvalidObjectId"),
            DbError::IoError(e) => write!(f, "IoError: {}", e),
            DbError::JsonError(e) => write!(f, "JsonError: {}", e),
        }
    }
}

impl Error for DbError {}

const OBJECT_ID_SIZE: usize = 16;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObjectId([u8; OBJECT_ID_SIZE]);

impl ObjectId {
    fn from_str(s: &str) -> Result<ObjectId, DbError> {
        let b = s.as_bytes();
        if b.len() == OBJECT_ID_SIZE {
            let mut arr: [u8; OBJECT_ID_SIZE] = Default::default();
            arr.copy_from_slice(b);
            Ok(ObjectId(arr))
        } else {
            Err(DbError::InvalidObjectId)
        }
    }

    fn to_str(&self) -> Result<&str, DbError> {
        std::str::from_utf8(&self.0).map_err(|_| DbError::InvalidObjectId)
    }
}

struct FileLock {
    file: fs::File,
    is_locked: bool,
}

impl FileLock {
    fn exclusive(path: &Path) -> Result<FileLock, DbError> {
        let file = fs::File::open(path)?;
        file.lock_exclusive()?;
        Ok(FileLock {
            file,
            is_locked: true,
        })
    }

    fn shared(path: &Path) -> Result<FileLock, DbError> {
        let file = fs::File::open(path)?;
        file.lock_shared()?;
        Ok(FileLock {
            file,
            is_locked: true,
        })
    }

    fn unlock(&mut self) -> Result<(), DbError> {
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

#[derive(Debug)]
pub struct Collection {
    root: PathBuf,
}

impl Collection {
    fn object_path(&self, id: &ObjectId) -> Result<PathBuf, DbError> {
        Ok(self.root.join(id.to_str()?))
    }

    pub fn get_all<T>(&self) -> Result<HashMap<ObjectId, T>, DbError>
    where
        for<'de> T: Deserialize<'de>,
    {
        let mut lock = FileLock::shared(&self.root)?;
        let mut result = HashMap::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .expect("failed to convert file name to string");
            let id = ObjectId::from_str(&name)?;
            let path = entry.path();
            let file = fs::File::open(path)?;
            let reader = io::BufReader::new(file);
            if let Ok(val) = serde_json::from_reader(reader) {
                result.insert(id, val);
            }
        }
        lock.unlock()?;
        Ok(result)
    }

    pub fn get_one<T>(&self, id: &ObjectId) -> Result<T, DbError>
    where
        for<'de> T: Deserialize<'de>,
    {
        let mut lock = FileLock::shared(&self.root)?;
        let path = self.object_path(id)?;
        let file = fs::File::open(path)?;
        let reader = io::BufReader::new(file);
        let val = serde_json::from_reader(reader)?;
        lock.unlock()?;
        Ok(val)
    }

    // Precondition: an exclusive lock must be taken before calling
    // this function
    fn gen_object_id(&self) -> ObjectId {
        loop {
            let chars = b"0123456789abcdef";
            let mut rng = thread_rng();
            let mut arr: [u8; OBJECT_ID_SIZE] = Default::default();
            for index in 0..arr.len() {
                arr[index] = *chars.choose(&mut rng).unwrap();
            }
            let id = ObjectId(arr);
            // Check if the ID is already in use
            if !self.object_path(&id).unwrap().exists() {
                return id;
            }
        }
    }

    pub fn add_one<T>(&self, object: &T) -> Result<ObjectId, DbError>
    where
        T: Serialize,
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        let id = self.gen_object_id();
        let path = self.object_path(&id)?;
        let file = fs::File::create(path)?;
        let writer = io::BufWriter::new(file);
        serde_json::to_writer(writer, &object)?;
        lock.unlock()?;
        Ok(id)
    }

    pub fn delete_one(&self, id: &ObjectId) -> Result<(), DbError> {
        let mut lock = FileLock::exclusive(&self.root)?;
        let path = self.object_path(id)?;
        fs::remove_file(path)?;
        lock.unlock()?;
        Ok(())
    }

    pub fn replace_one<T>(&self, id: &ObjectId, object: &T) -> Result<(), DbError>
    where
        T: Serialize,
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        let path = self.object_path(id)?;
        let file = fs::File::create(path)?;
        let writer = io::BufWriter::new(file);
        serde_json::to_writer(writer, &object)?;
        lock.unlock()?;
        Ok(())
    }

    pub fn update_many<T, F, U>(&self, f: F, u: U) -> Result<(), DbError>
    where
        for<'de> T: Deserialize<'de> + Serialize,
        F: Fn(&ObjectId, &T) -> bool,
        U: Fn(&ObjectId, &T) -> T,
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .expect("failed to convert file name to string");
            let id = ObjectId::from_str(&name)?;
            let path = entry.path();
            let file = fs::File::open(&path)?;
            let reader = io::BufReader::new(file);
            let val = serde_json::from_reader(reader)?;
            if f(&id, &val) {
                let val = u(&id, &val);
                let file = fs::File::create(&path)?;
                let writer = io::BufWriter::new(file);
                serde_json::to_writer(writer, &val)?;
            }
        }
        lock.unlock()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Db {
    root: PathBuf,
}

impl Db {
    pub fn open(root: &Path) -> Result<Db, DbError> {
        if !root.exists() {
            fs::create_dir_all(root)?;
        }
        Ok(Db {
            root: root.to_path_buf(),
        })
    }

    pub fn collection(&self, name: &str) -> Result<Collection, DbError> {
        let path = self.root.join(name);
        if !path.exists() {
            fs::create_dir(&path)?;
        }
        Ok(Collection { root: path })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn test_insert_and_get() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        let conn = db.collection("abc").unwrap();
        let id = conn.add_one(&123).unwrap();
        let val: u32 = conn.get_one(&id).unwrap();
        assert_eq!(val, 123);
    }
}

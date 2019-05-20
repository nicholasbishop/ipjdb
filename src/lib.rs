pub mod error;
mod lock;

use error::DbError;
use lock::FileLock;
use rand::seq::SliceRandom;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const ID_SIZE: usize = 16;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Id([u8; ID_SIZE]);

impl Id {
    fn from_str(s: &str) -> Result<Id, DbError> {
        let b = s.as_bytes();
        if b.len() == ID_SIZE {
            let mut arr: [u8; ID_SIZE] = Default::default();
            arr.copy_from_slice(b);
            Ok(Id(arr))
        } else {
            Err(DbError::InvalidId)
        }
    }

    fn to_str(&self) -> Result<&str, DbError> {
        std::str::from_utf8(&self.0).map_err(|_| DbError::InvalidId)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Item<T> {
    pub id: Id,
    pub data: T,
}

impl<T> Item<T> {
    pub fn new(id: Id, data: T) -> Item<T> {
        Item { id, data }
    }
}

#[derive(Debug)]
pub struct Collection {
    root: PathBuf,
}

impl Collection {
    fn item_path(&self, id: &Id) -> Result<PathBuf, DbError> {
        Ok(self.root.join(id.to_str()?))
    }

    pub fn get_all<T>(&self) -> Result<Vec<Item<T>>, DbError>
    where
        for<'de> T: Deserialize<'de>,
    {
        self.find_many(|_| true)
    }

    pub fn find_many<T, F>(&self, f: F) -> Result<Vec<Item<T>>, DbError>
    where
        for<'de> T: Deserialize<'de>,
        F: Fn(&Item<T>) -> bool,
    {
        let mut lock = FileLock::shared(&self.root)?;
        let mut result = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .expect("failed to convert file name to string");
            let id = Id::from_str(&name)?;
            let path = entry.path();
            let file = fs::File::open(path)?;
            let reader = io::BufReader::new(file);
            if let Ok(val) = serde_json::from_reader(reader) {
                let item = Item::new(id, val);
                if f(&item) {
                    result.push(item);
                }
            }
        }
        lock.unlock()?;
        Ok(result)
    }

    pub fn get_one<T>(&self, id: &Id) -> Result<Item<T>, DbError>
    where
        for<'de> T: Deserialize<'de>,
    {
        let mut lock = FileLock::shared(&self.root)?;
        let path = self.item_path(id)?;
        let file = fs::File::open(path)?;
        let reader = io::BufReader::new(file);
        let val = serde_json::from_reader(reader)?;
        lock.unlock()?;
        Ok(Item::new(id.clone(), val))
    }

    // Precondition: an exclusive lock must be taken before calling
    // this function
    fn gen_id(&self) -> Id {
        loop {
            let chars = b"0123456789abcdef";
            let mut rng = thread_rng();
            let mut arr: [u8; ID_SIZE] = Default::default();
            for index in 0..arr.len() {
                arr[index] = *chars.choose(&mut rng).unwrap();
            }
            let id = Id(arr);
            // Check if the ID is already in use
            if !self.item_path(&id).unwrap().exists() {
                return id;
            }
        }
    }

    pub fn insert_one<T>(&self, data: &T) -> Result<Id, DbError>
    where
        T: Serialize,
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        let id = self.gen_id();
        let path = self.item_path(&id)?;
        let file = fs::File::create(path)?;
        let writer = io::BufWriter::new(file);
        serde_json::to_writer(writer, &data)?;
        lock.unlock()?;
        Ok(id)
    }

    pub fn delete_one(&self, id: &Id) -> Result<(), DbError> {
        let mut lock = FileLock::exclusive(&self.root)?;
        let path = self.item_path(id)?;
        fs::remove_file(path)?;
        lock.unlock()?;
        Ok(())
    }

    pub fn replace_one<T>(&self, item: &Item<T>) -> Result<(), DbError>
    where
        T: Serialize,
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        let path = self.item_path(&item.id)?;
        let file = fs::File::create(path)?;
        let writer = io::BufWriter::new(file);
        serde_json::to_writer(writer, &item.data)?;
        lock.unlock()?;
        Ok(())
    }

    pub fn update_by_id<T, U>(&self, id: &Id, u: U) -> Result<(), DbError>
    where
        for<'de> T: Deserialize<'de> + Serialize,
        U: Fn(&Item<T>) -> T,
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        let path = self.item_path(id)?;
        let file = fs::File::open(&path)?;
        let reader = io::BufReader::new(file);
        let val = serde_json::from_reader(reader)?;
        let val = u(&Item::new(id.clone(), val));
        let file = fs::File::create(&path)?;
        let writer = io::BufWriter::new(file);
        serde_json::to_writer(writer, &val)?;
        lock.unlock()?;
        Ok(())
    }

    pub fn update_many<T, F, U>(&self, f: F, u: U) -> Result<(), DbError>
    where
        for<'de> T: Deserialize<'de> + Serialize,
        F: Fn(&Item<T>) -> bool,
        U: Fn(&Item<T>) -> T,
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .expect("failed to convert file name to string");
            let id = Id::from_str(&name)?;
            let path = entry.path();
            let file = fs::File::open(&path)?;
            let reader = io::BufReader::new(file);
            let val = serde_json::from_reader(reader)?;
            let item = Item::new(id.clone(), val);
            if f(&item) {
                let val = u(&item);
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
        let id = conn.insert_one(&123).unwrap();
        let val: Item<u32> = conn.get_one(&id).unwrap();
        assert_eq!(val.data, 123);
    }
}

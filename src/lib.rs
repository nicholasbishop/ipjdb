mod error;
mod id;
mod lock;

pub use error::Error;
pub use id::Id;
use lock::FileLock;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// JSON data with its unique ID
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Item<T> {
    pub id: Id,
    #[serde(flatten)]
    pub data: T,
}

impl<T> Item<T> {
    pub fn new(id: Id, data: T) -> Item<T> {
        Item { id, data }
    }
}

/// Group of items in the database
#[derive(Clone, Debug)]
pub struct Collection {
    root: PathBuf,
}

impl Collection {
    fn item_path(&self, id: &Id) -> Result<PathBuf, Error> {
        Ok(self.root.join(id.to_str()?))
    }

    /// Get all the items in the collection
    pub fn get_all<T>(&self) -> Result<Vec<Item<T>>, Error>
    where
        for<'de> T: Deserialize<'de>,
    {
        self.find_many(|_| true)
    }

    /// Get a subset of the items in the collection
    ///
    /// Items are filtered by the function `f`, which is passed an
    /// `Item` and should return `true` to include that `Item` in the
    /// results, or `false` to exclude it from the results.
    pub fn find_many<T, F>(&self, f: F) -> Result<Vec<Item<T>>, Error>
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
            let id = name.parse::<Id>()?;
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

    /// Get one item by its ID
    pub fn get_one<T>(&self, id: &Id) -> Result<Item<T>, Error>
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
            let id = Id::random();
            // Check if the ID is already in use
            if !self.item_path(&id).unwrap().exists() {
                return id;
            }
        }
    }

    /// Insert one item into the collection
    ///
    /// A unique ID will be generated for the item and returned.
    pub fn insert_one<T>(&self, data: &T) -> Result<Id, Error>
    where
        T: Serialize,
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        let id = self.gen_id();
        let path = self.item_path(&id)?;
        let file = fs::File::create(path)?;
        let writer = io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &data)?;
        lock.unlock()?;
        Ok(id)
    }

    /// Delete one item from the collection
    pub fn delete_one(&self, id: &Id) -> Result<(), Error> {
        let mut lock = FileLock::exclusive(&self.root)?;
        let path = self.item_path(id)?;
        fs::remove_file(path)?;
        lock.unlock()?;
        Ok(())
    }

    /// Overwrite one item in the collection
    pub fn replace_one<T>(&self, item: &Item<T>) -> Result<(), Error>
    where
        T: Serialize,
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        let path = self.item_path(&item.id)?;
        let file = fs::File::create(path)?;
        let writer = io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &item.data)?;
        lock.unlock()?;
        Ok(())
    }

    /// Find an item by its ID and update it
    ///
    /// If the item is found, the function `u` will be called with
    /// that item. The function can modify the data as needed, and the
    /// new item will be written to the collection. Note that the ID
    /// cannot be modified.
    pub fn update_by_id<T, U>(&self, id: &Id, u: U) -> Result<(), Error>
    where
        for<'de> T: Deserialize<'de> + Serialize,
        U: Fn(&mut Item<T>),
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        let path = self.item_path(id)?;
        let file = fs::File::open(&path)?;
        let reader = io::BufReader::new(file);
        let val = serde_json::from_reader(reader)?;
        let mut item = Item::new(id.clone(), val);
        u(&mut item);
        let file = fs::File::create(&path)?;
        let writer = io::BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &item.data)?;
        lock.unlock()?;
        Ok(())
    }

    /// Update a subset of the items in the collection
    ///
    /// For each item in the collection the function `f` is called
    /// with that item. The function should return `true` to update
    /// the item or `false` to leave it unmodified. For each item
    /// where `f` returned `true`, the function `u` is called to
    /// update the item. The function can modify the data as needed,
    /// and the new item will be written to the collection. Note that
    /// the ID cannot be modified.
    pub fn update_many<T, F, U>(&self, f: F, u: U) -> Result<(), Error>
    where
        for<'de> T: Deserialize<'de> + Serialize,
        F: Fn(&Item<T>) -> bool,
        U: Fn(&mut Item<T>),
    {
        let mut lock = FileLock::exclusive(&self.root)?;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .expect("failed to convert file name to string");
            let id = name.parse::<Id>()?;
            let path = entry.path();
            let file = fs::File::open(&path)?;
            let reader = io::BufReader::new(file);
            let val = serde_json::from_reader(reader)?;
            let mut item = Item::new(id.clone(), val);
            if f(&item) {
                u(&mut item);
                let file = fs::File::create(&path)?;
                let writer = io::BufWriter::new(file);
                serde_json::to_writer_pretty(writer, &item.data)?;
            }
        }
        lock.unlock()?;
        Ok(())
    }
}

/// Database handle
#[derive(Clone, Debug)]
pub struct Db {
    root: PathBuf,
}

impl Db {
    /// Open or create a database
    pub fn open(root: &Path) -> Result<Db, Error> {
        if !root.exists() {
            fs::create_dir_all(root)?;
        }
        Ok(Db {
            root: root.to_path_buf(),
        })
    }

    /// Open or create a collection in the database
    pub fn collection(&self, name: &str) -> Result<Collection, Error> {
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

    #[test]
    fn test_update_by_id() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path()).unwrap();
        let conn = db.collection("abc").unwrap();
        let id = conn.insert_one(&123).unwrap();
        conn.update_by_id(&id, |item| {
            item.data = 456;
        })
        .unwrap();
        let val: Item<u32> = conn.get_one(&id).unwrap();
        assert_eq!(val.data, 456);
    }
}

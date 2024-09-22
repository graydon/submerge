#![allow(dead_code)]

pub struct Database {
    db: redb::Database
}
/*
impl Database {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            db: redb::Database::create(path)
        }
    }

    pub fn get(&self, path: Path) -> Result<Record, Error> {
        let key = path.to_string();
        let val = self.db.get(&key).ok_or_else(|| err("Key not found"))?;
        let record = Record::from_str(&val)?;
        Ok(record)
    }

    pub fn put(&mut self, path: Path, record: Record) -> Result<(), Error> {
        let key = path.to_string();
        let val = record.to_string();
        self.db.put(&key, &val);
        Ok(())
    }

    pub fn abort(&mut self, path: Path) -> Result<(), Error> {
        let key = path.to_string();
        self.db.delete(&key);
        Ok(())
    }
}
    */
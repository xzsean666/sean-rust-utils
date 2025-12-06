use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use serde::{Serialize, de::DeserializeOwned};
use sled::{Batch, Db, Tree};
use std::{
    fs::{self, File},
    io::{self, Cursor},
    path::{Path, PathBuf},
    string::FromUtf8Error,
    time::{SystemTime, UNIX_EPOCH},
};
use tar::{Archive, Builder};
use thiserror::Error;
use walkdir::WalkDir;

/// Simple wrapper around sled providing typed get/put/delete on a named tree.
#[derive(Clone)]
pub struct KVDB {
    db: Db,
    tree: Tree,
    path: PathBuf,
}

const BACKUP_PREFIX: &str = "kvdb-backup-";

impl KVDB {
    /// Open (or create) a sled database at `db_path` and use the named `tree`.
    pub fn new<P: AsRef<Path>>(db_path: P, tree: &str) -> Result<Self, KVError> {
        let path = db_path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| KVError::CreateDir(parent.to_path_buf(), err))?;
        }

        let db = sled::open(path).map_err(|err| KVError::OpenDb(path.to_path_buf(), err))?;
        let tree_handle = db
            .open_tree(tree)
            .map_err(|err| KVError::OpenTree(tree.to_string(), err))?;

        Ok(Self {
            db,
            tree: tree_handle,
            path: path.to_path_buf(),
        })
    }

    /// Open another tree within the same underlying database, sharing the DB handle.
    pub fn with_tree(&self, tree: &str) -> Result<Self, KVError> {
        let tree_handle = self
            .db
            .open_tree(tree)
            .map_err(|err| KVError::OpenTree(tree.to_string(), err))?;
        Ok(Self {
            db: self.db.clone(),
            tree: tree_handle,
            path: self.path.clone(),
        })
    }

    /// Insert or replace a value under `key`.
    pub fn put<V: Serialize>(&self, key: &str, value: &V) -> Result<(), KVError> {
        let data =
            bincode::serialize(value).map_err(|err| KVError::Serialize(key.to_string(), err))?;

        self.tree
            .insert(key.as_bytes(), data)
            .map_err(|err| KVError::Write(key.to_string(), err))?;
        self.tree.flush().map_err(KVError::Flush)?;
        Ok(())
    }

    /// Fetch and deserialize a value stored at `key`.
    pub fn get<V: DeserializeOwned>(&self, key: &str) -> Result<Option<V>, KVError> {
        let bytes = match self
            .tree
            .get(key.as_bytes())
            .map_err(|err| KVError::Read(key.to_string(), err))?
        {
            Some(data) => data,
            None => return Ok(None),
        };

        let value = bincode::deserialize(&bytes)
            .map_err(|err| KVError::Deserialize(key.to_string(), err))?;
        Ok(Some(value))
    }

    /// Remove a value. Returns `true` if the key existed.
    pub fn delete(&self, key: &str) -> Result<bool, KVError> {
        let existed = self
            .tree
            .remove(key.as_bytes())
            .map_err(|err| KVError::Write(key.to_string(), err))?
            .is_some();
        self.tree.flush().map_err(KVError::Flush)?;
        Ok(existed)
    }

    /// Remove all keys from the current tree.
    pub fn clear(&self) -> Result<(), KVError> {
        let tree_name = tree_name(&self.tree);
        self.tree
            .clear()
            .map_err(|err| KVError::Clear(tree_name.clone(), err))?;
        self.tree.flush().map_err(KVError::Flush)?;
        Ok(())
    }

    /// Check whether the key exists.
    pub fn contains(&self, key: &str) -> Result<bool, KVError> {
        self.tree
            .contains_key(key.as_bytes())
            .map_err(|err| KVError::Read(key.to_string(), err))
    }

    /// Fetch all key/value pairs that start with the given prefix.
    pub fn get_with_prefix<V: DeserializeOwned>(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, V)>, KVError> {
        let mut items = Vec::new();
        for result in self.tree.scan_prefix(prefix.as_bytes()) {
            let (raw_key, raw_value) =
                result.map_err(|err| KVError::Read(prefix.to_string(), err))?;
            let key = String::from_utf8(raw_key.to_vec())
                .map_err(|err| KVError::KeyUtf8(raw_key.to_vec(), err))?;
            let value = bincode::deserialize(&raw_value)
                .map_err(|err| KVError::Deserialize(key.clone(), err))?;
            items.push((key, value));
        }
        Ok(items)
    }

    /// Fetch all key/value pairs with optional offset and limit (lexicographic order).
    pub fn get_all<V: DeserializeOwned>(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Result<Vec<(String, V)>, KVError> {
        let skip = offset.unwrap_or(0);
        let take = limit.unwrap_or(usize::MAX);

        let mut items = Vec::new();
        for result in self.tree.iter().skip(skip).take(take) {
            let (raw_key, raw_value) =
                result.map_err(|err| KVError::Read("<iter>".to_string(), err))?;
            let key = String::from_utf8(raw_key.to_vec())
                .map_err(|err| KVError::KeyUtf8(raw_key.to_vec(), err))?;
            let value = bincode::deserialize(&raw_value)
                .map_err(|err| KVError::Deserialize(key.clone(), err))?;
            items.push((key, value));
        }
        Ok(items)
    }

    /// Insert many key/value pairs using a single batch for better throughput.
    pub fn put_many<K, V, I>(&self, items: I) -> Result<(), KVError>
    where
        K: AsRef<str>,
        V: Serialize,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut batch = Batch::default();
        for (key, value) in items.into_iter() {
            let key_str = key.as_ref().to_string();
            let data = bincode::serialize(&value)
                .map_err(|err| KVError::Serialize(key_str.clone(), err))?;
            batch.insert(key_str.as_bytes(), data);
        }
        self.tree
            .apply_batch(batch)
            .map_err(|err| KVError::Write("<batch>".to_string(), err))?;
        self.tree.flush().map_err(KVError::Flush)?;
        Ok(())
    }

    /// Create a compressed backup (.tar.gz) as in-memory bytes.
    pub fn create_backup_bytes(&self) -> Result<Vec<u8>, KVError> {
        self.db.flush().map_err(KVError::Flush)?;

        let mut buffer = Vec::new();
        {
            let enc = GzEncoder::new(&mut buffer, Compression::default());
            let mut tar = Builder::new(enc);

            for entry in WalkDir::new(&self.path) {
                let entry = entry.map_err(|err| KVError::BackupWalk(self.path.clone(), err))?;
                let path = entry.path();
                let rel = match path.strip_prefix(&self.path) {
                    Ok(r) if r.as_os_str().is_empty() => continue,
                    Ok(r) => r.to_path_buf(),
                    Err(_) => continue,
                };

                if entry.file_type().is_dir() {
                    tar.append_dir(rel, path)
                        .map_err(|err| KVError::BackupArchive(path.to_path_buf(), err))?;
                } else {
                    tar.append_path_with_name(path, rel)
                        .map_err(|err| KVError::BackupArchive(path.to_path_buf(), err))?;
                }
            }

            tar.finish().map_err(KVError::BackupFinish)?;
            let enc = tar.into_inner().map_err(KVError::BackupFinish)?;
            enc.finish().map_err(KVError::BackupFinishBuffer)?;
        }

        Ok(buffer)
    }

    /// Create a compressed backup (.tar.gz) of the current database directory into `backup_dir`.
    pub fn create_backup<P: AsRef<Path>>(&self, backup_dir: P) -> Result<PathBuf, KVError> {
        let backup_dir = backup_dir.as_ref();
        fs::create_dir_all(backup_dir)
            .map_err(|err| KVError::CreateDir(backup_dir.to_path_buf(), err))?;

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let db_tag = sanitize_for_backup(&self.path);
        let backup_path = backup_dir.join(format!("{BACKUP_PREFIX}{db_tag}-{ts}.tar.gz"));

        let bytes = self.create_backup_bytes()?;
        fs::write(&backup_path, bytes)
            .map_err(|err| KVError::BackupIo(backup_path.clone(), err))?;

        Ok(backup_path)
    }

    /// Backup to a directory and keep at most `keep` newest backups (0 = keep all).
    pub fn backup_to_path<P: AsRef<Path>>(
        &self,
        backup_dir: P,
        keep: usize,
    ) -> Result<PathBuf, KVError> {
        let backup_dir_ref = backup_dir.as_ref();
        let db_tag = sanitize_for_backup(&self.path);
        let prefix = format!("{BACKUP_PREFIX}{db_tag}-");

        let latest = self.create_backup(backup_dir_ref)?;

        if keep > 0 {
            let mut backups: Vec<(PathBuf, u128)> = fs::read_dir(backup_dir_ref)
                .map_err(|err| KVError::ListDir(backup_dir_ref.to_path_buf(), err))?
                .filter_map(|entry| {
                    let entry = entry.ok()?;
                    let path = entry.path();
                    if !path.is_file() {
                        return None;
                    }
                    let name = path.file_name()?.to_string_lossy();
                    if !name.starts_with(&prefix) || !name.ends_with(".tar.gz") {
                        return None;
                    }
                    let name_no_ext = name.trim_end_matches(".tar.gz");
                    let stamp_str = name_no_ext
                        .rsplit_once('-')
                        .map(|(_, ts)| ts)
                        .unwrap_or("0");
                    let stamp = stamp_str.parse::<u128>().unwrap_or(0);
                    Some((path, stamp))
                })
                .collect();

            backups.sort_by_key(|(_, ts)| *ts);

            while backups.len() > keep {
                let (old_path, _) = backups.remove(0);
                fs::remove_file(&old_path)
                    .map_err(|err| KVError::Retention(old_path.clone(), err))?;
            }
        }

        Ok(latest)
    }

    /// Restore a database directory from a backup file (.tar.gz) to `target_db_path`.
    pub fn restore_from<P: AsRef<Path>, Q: AsRef<Path>>(
        backup_file: P,
        target_db_path: Q,
    ) -> Result<(), KVError> {
        let backup = backup_file.as_ref();
        let target = target_db_path.as_ref();

        if target.exists() {
            fs::remove_dir_all(target)
                .map_err(|err| KVError::RestoreIo(target.to_path_buf(), err))?;
        }
        fs::create_dir_all(target).map_err(|err| KVError::CreateDir(target.to_path_buf(), err))?;

        let file =
            File::open(backup).map_err(|err| KVError::RestoreIo(backup.to_path_buf(), err))?;
        let decoder = GzDecoder::new(file);
        let mut archive = Archive::new(decoder);
        archive
            .unpack(target)
            .map_err(|err| KVError::RestoreArchive(backup.to_path_buf(), err))?;
        Ok(())
    }

    /// Restore a database directory from in-memory backup bytes (.tar.gz) to `target_db_path`.
    pub fn restore_from_bytes<P: AsRef<Path>>(
        backup_bytes: &[u8],
        target_db_path: P,
    ) -> Result<(), KVError> {
        let target = target_db_path.as_ref();

        if target.exists() {
            fs::remove_dir_all(target)
                .map_err(|err| KVError::RestoreIo(target.to_path_buf(), err))?;
        }
        fs::create_dir_all(target).map_err(|err| KVError::CreateDir(target.to_path_buf(), err))?;

        let cursor = Cursor::new(backup_bytes);
        let decoder = GzDecoder::new(cursor);
        let mut archive = Archive::new(decoder);
        archive
            .unpack(target)
            .map_err(|err| KVError::RestoreArchive(target.to_path_buf(), err))?;
        Ok(())
    }

    /// Access the underlying sled database handle.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Access the underlying sled tree handle.
    pub fn tree(&self) -> &Tree {
        &self.tree
    }
}

fn tree_name(tree: &Tree) -> String {
    String::from_utf8_lossy(tree.name().as_ref()).into_owned()
}

fn sanitize_for_backup(path: &Path) -> String {
    let mut s = path.to_string_lossy().replace(['\\', '/', ':', ' '], "_");
    if s.is_empty() {
        s.push_str("db");
    }
    s
}

#[derive(Debug, Error)]
pub enum KVError {
    #[error("failed to create db directory {0:?}: {1}")]
    CreateDir(PathBuf, #[source] io::Error),
    #[error("failed to open db at {0:?}: {1}")]
    OpenDb(PathBuf, #[source] sled::Error),
    #[error("failed to open tree {0}: {1}")]
    OpenTree(String, #[source] sled::Error),
    #[error("failed to write key {0}: {1}")]
    Write(String, #[source] sled::Error),
    #[error("failed to read key {0}: {1}")]
    Read(String, #[source] sled::Error),
    #[error("failed to clear tree {0}: {1}")]
    Clear(String, #[source] sled::Error),
    #[error("failed to serialize key {0}: {1}")]
    Serialize(String, #[source] bincode::Error),
    #[error("failed to deserialize key {0}: {1}")]
    Deserialize(String, #[source] bincode::Error),
    #[error("failed to flush data: {0}")]
    Flush(#[source] sled::Error),
    #[error("failed to parse key as utf-8: {0:?}, {1}")]
    KeyUtf8(Vec<u8>, #[source] FromUtf8Error),
    #[error("failed to walk path for backup {0:?}: {1}")]
    BackupWalk(PathBuf, #[source] walkdir::Error),
    #[error("failed to write backup {0:?}: {1}")]
    BackupIo(PathBuf, #[source] io::Error),
    #[error("failed to archive path {0:?}: {1}")]
    BackupArchive(PathBuf, #[source] io::Error),
    #[error("failed to finalize backup: {0}")]
    BackupFinish(#[source] io::Error),
    #[error("failed to finalize backup buffer: {0}")]
    BackupFinishBuffer(#[source] io::Error),
    #[error("failed to list directory {0:?}: {1}")]
    ListDir(PathBuf, #[source] io::Error),
    #[error("failed to remove old backup {0:?}: {1}")]
    Retention(PathBuf, #[source] io::Error),
    #[error("failed to open/restore from {0:?}: {1}")]
    RestoreIo(PathBuf, #[source] io::Error),
    #[error("failed to unpack archive {0:?}: {1}")]
    RestoreArchive(PathBuf, #[source] io::Error),
}

#[cfg(test)]
mod tests {
    use super::KVDB;
    use std::{fs, thread, time::Duration};
    use tempfile::tempdir;

    #[test]
    fn round_trip_values() {
        let dir = tempdir().unwrap();
        let db = KVDB::new(dir.path().join("test.db"), "test").unwrap();

        db.put("number", &42u64).unwrap();
        db.put("text", &"hello".to_string()).unwrap();

        assert_eq!(db.get::<u64>("number").unwrap(), Some(42));
        assert_eq!(db.get::<String>("text").unwrap(), Some("hello".to_string()));
        assert!(db.contains("text").unwrap());

        assert!(db.delete("text").unwrap());
        assert!(!db.contains("text").unwrap());
        assert!(db.get::<String>("text").unwrap().is_none());
    }

    #[test]
    fn fetch_with_prefix() {
        let dir = tempdir().unwrap();
        let db = KVDB::new(dir.path().join("test_prefix.db"), "test").unwrap();

        db.put("user:1", &"alice".to_string()).unwrap();
        db.put("user:2", &"bob".to_string()).unwrap();
        db.put("post:1", &"ignored".to_string()).unwrap();

        let mut results = db.get_with_prefix::<String>("user:").unwrap();
        results.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "user:1");
        assert_eq!(results[0].1, "alice".to_string());
        assert_eq!(results[1].0, "user:2");
        assert_eq!(results[1].1, "bob".to_string());
    }

    #[test]
    fn get_all_with_offset_limit_and_batch_put() {
        let dir = tempdir().unwrap();
        let db = KVDB::new(dir.path().join("test_all.db"), "test").unwrap();

        db.put_many(vec![("a", 1u8), ("b", 2u8), ("c", 3u8), ("d", 4u8)])
            .unwrap();

        let all: Vec<(String, u8)> = db.get_all(None, None).unwrap();
        assert_eq!(all.len(), 4);
        assert_eq!(all[0], ("a".to_string(), 1));
        assert_eq!(all[3], ("d".to_string(), 4));

        let subset: Vec<(String, u8)> = db.get_all(Some(1), Some(2)).unwrap();
        assert_eq!(subset, vec![("b".to_string(), 2), ("c".to_string(), 3)]);
    }

    #[test]
    fn backup_and_restore() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("db");
        let backup_dir = dir.path().join("backups");
        let restore_path = dir.path().join("restore");

        let db = KVDB::new(&db_path, "test").unwrap();
        db.put("k1", &"v1".to_string()).unwrap();

        let backup_file = db.backup_to_path(&backup_dir, 3).unwrap();
        KVDB::restore_from(&backup_file, &restore_path).unwrap();

        let restored = KVDB::new(&restore_path, "test").unwrap();
        assert_eq!(
            restored.get::<String>("k1").unwrap(),
            Some("v1".to_string())
        );

        // Second backup should trigger retention to 1 file.
        thread::sleep(Duration::from_millis(2));
        let latest = db.backup_to_path(&backup_dir, 1).unwrap();
        let files: Vec<_> = fs::read_dir(&backup_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], latest);
    }
}

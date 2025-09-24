use serde::{Deserialize, Serialize};
use serde_json;
use sled::{Db, Result as SledResult};
use std::path::PathBuf;

/// 基于Sled的本地Key-Value数据库
#[derive(Clone)]
pub struct KvDbLocal {
    db: Db,
}

impl KvDbLocal {
    /// 创建新的数据库实例，数据库文件将创建在可执行文件目录下
    pub fn new(db_name: &str) -> SledResult<Self> {
        let db_path = Self::get_db_path(db_name)?;
        let db = sled::open(db_path)?;
        Ok(KvDbLocal { db })
    }

    /// 使用指定路径创建数据库实例
    pub fn with_path(db_path: PathBuf) -> SledResult<Self> {
        let db = sled::open(db_path)?;
        Ok(KvDbLocal { db })
    }

    /// 使用内存数据库创建实例（用于测试）
    pub fn memory() -> SledResult<Self> {
        let db = sled::Config::new().temporary(true).open()?;
        Ok(KvDbLocal { db })
    }

    /// 获取数据库路径（在可执行文件目录下）
    fn get_db_path(db_name: &str) -> std::io::Result<PathBuf> {
        let exe_path = std::env::current_exe()?;
        let exe_dir = exe_path.parent().ok_or(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Cannot get parent directory",
        ))?;
        
        // 创建一个专门的数据库目录来避免与可执行文件名冲突
        let db_base_dir = exe_dir.join("db_local");
        let db_dir = db_base_dir.join(db_name);

        // Ensure the directory exists
        std::fs::create_dir_all(&db_dir)?;
        Ok(db_dir)
    }

    /// 存储键值对
    /// 值将被序列化为JSON格式
    pub fn put<T>(&self, key: &str, value: &T) -> Result<(), Box<dyn std::error::Error>>
    where
        T: Serialize,
    {
        let json_value = serde_json::to_vec(value)?;
        self.db.insert(key.as_bytes(), json_value)?;
        self.db.flush()?;
        Ok(())
    }

    /// 获取值
    /// 返回反序列化后的值，如果键不存在则返回None
    pub fn get<T>(&self, key: &str) -> Result<Option<T>, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de>,
    {
        if let Some(data) = self.db.get(key.as_bytes())? {
            let value: T = serde_json::from_slice(&data)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// 删除键值对
    /// 返回是否成功删除（键是否存在）
    pub fn delete(&self, key: &str) -> SledResult<bool> {
        let result = self.db.remove(key.as_bytes())?;
        self.db.flush()?;
        Ok(result.is_some())
    }

    /// 检查键是否存在
    pub fn has(&self, key: &str) -> SledResult<bool> {
        Ok(self.db.contains_key(key.as_bytes())?)
    }

    /// 获取所有键
    pub fn keys(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let keys: Result<Vec<String>, Box<dyn std::error::Error>> = self
            .db
            .iter()
            .keys()
            .map(|key_result| {
                let key_bytes = key_result?;
                String::from_utf8(key_bytes.to_vec()).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
            })
            .collect();
        keys
    }

    /// 获取数据库中的记录数量
    pub fn count(&self) -> u64 {
        self.db.len() as u64
    }

    /// 清空所有数据
    pub fn clear(&self) -> SledResult<()> {
        self.db.clear()?;
        self.db.flush()?;
        Ok(())
    }

    /// 添加键值对（如果键已存在则返回错误）
    pub fn add<T>(&self, key: &str, value: &T) -> Result<(), Box<dyn std::error::Error>>
    where
        T: Serialize,
    {
        if self.has(key)? {
            return Err(format!("Key '{}' already exists", key).into());
        }
        self.put(key, value)
    }

    /// 获取所有键值对
    pub fn get_all<T>(&self) -> Result<Vec<(String, T)>, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut results = Vec::new();
        for item in self.db.iter() {
            let (key_bytes, value_bytes) = item?;
            let key = String::from_utf8(key_bytes.to_vec())?;
            let value: T = serde_json::from_slice(&value_bytes)?;
            results.push((key, value));
        }
        Ok(results)
    }

    /// 批量存储键值对
    pub fn put_many<T>(&self, entries: &[(&str, &T)]) -> Result<(), Box<dyn std::error::Error>>
    where
        T: Serialize,
    {
        for (key, value) in entries {
            let json_value = serde_json::to_vec(value)?;
            self.db.insert(key.as_bytes(), json_value)?;
        }
        self.db.flush()?;
        Ok(())
    }

    /// 批量删除键
    pub fn delete_many(&self, keys: &[&str]) -> SledResult<usize> {
        let mut deleted_count = 0;
        for key in keys {
            if self.db.remove(key.as_bytes())?.is_some() {
                deleted_count += 1;
            }
        }
        self.db.flush()?;
        Ok(deleted_count)
    }

    /// 合并JSON对象（仅适用于JSON值）
    pub fn merge<T>(&self, key: &str, new_value: &T) -> Result<(), Box<dyn std::error::Error>>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Some(existing_value) = self.get::<serde_json::Value>(key)? {
            if let Ok(new_json) = serde_json::to_value(new_value) {
                if let (serde_json::Value::Object(mut existing_obj), serde_json::Value::Object(new_obj)) = (existing_value, new_json) {
                    // 合并对象
                    for (k, v) in new_obj {
                        existing_obj.insert(k, v);
                    }
                    self.put(key, &existing_obj)?;
                } else {
                    // 如果不是对象，直接替换
                    self.put(key, new_value)?;
                }
            } else {
                self.put(key, new_value)?;
            }
        } else {
            // 键不存在，直接存储新值
            self.put(key, new_value)?;
        }
        Ok(())
    }

    /// 关闭数据库连接
    pub fn close(self) -> SledResult<()> {
        self.db.flush()?;
        drop(self.db);
        Ok(())
    }
}

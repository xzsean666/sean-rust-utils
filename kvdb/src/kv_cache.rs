use crate::kvdb::{KVDB, KVError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::convert::Infallible;
use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Simple cache wrapper around `KVDB` with optional TTL and key prefixing.
#[derive(Clone)]
pub struct KVCache {
    db: KVDB,
    default_ttl: Option<Duration>,
    prefix: String,
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error(transparent)]
    Kv(#[from] KVError),
    #[error("compute function failed: {0}")]
    Compute(#[source] Box<dyn Error + Send + Sync>),
}

#[derive(Serialize, Deserialize)]
struct CacheEntry<V> {
    value: V,
    expires_at_ms: Option<u128>,
}

impl<V> CacheEntry<V> {
    fn is_expired(&self, now_ms: u128) -> bool {
        matches!(self.expires_at_ms, Some(exp) if now_ms >= exp)
    }
}

impl KVCache {
    /// Create a cache wrapper over an existing `KVDB`.
    pub fn new(db: KVDB) -> Self {
        Self {
            db,
            default_ttl: None,
            prefix: String::new(),
        }
    }

    /// Set a default TTL used when a call does not provide one.
    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    /// Prefix all keys. A trailing `:` is added when missing.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        if !prefix.is_empty() && !prefix.ends_with(':') {
            prefix.push(':');
        }
        self.prefix = prefix;
        self
    }

    /// Cached call with an infallible computation closure.
    pub fn get_or_insert_with<K, V, F>(
        &self,
        key: K,
        ttl: Option<Duration>,
        compute: F,
    ) -> Result<V, CacheError>
    where
        K: AsRef<str>,
        V: Serialize + DeserializeOwned,
        F: FnOnce() -> V,
    {
        self.get_or_try_insert_with::<_, _, _, Infallible>(key, ttl, || Ok(compute()))
    }

    /// Cached call that can fail. Returns cached value when present and valid,
    /// otherwise computes, stores, and returns a fresh value.
    pub fn get_or_try_insert_with<K, V, F, E>(
        &self,
        key: K,
        ttl: Option<Duration>,
        compute: F,
    ) -> Result<V, CacheError>
    where
        K: AsRef<str>,
        V: Serialize + DeserializeOwned,
        F: FnOnce() -> Result<V, E>,
        E: Error + Send + Sync + 'static,
    {
        let combined_key = self.build_key(key.as_ref());
        let now_ms = now_millis();
        let ttl = ttl.or(self.default_ttl);

        if let Some(entry) = self.db.get::<CacheEntry<V>>(&combined_key)? {
            if !entry.is_expired(now_ms) {
                return Ok(entry.value);
            }
            // drop expired entries to prevent unbounded growth
            let _ = self.db.delete(&combined_key);
        }

        let value = compute().map_err(|err| CacheError::Compute(Box::new(err)))?;
        let expires_at_ms = ttl.map(|d| now_ms.saturating_add(d.as_millis() as u128));
        let entry = CacheEntry {
            value,
            expires_at_ms,
        };

        self.db.put(&combined_key, &entry)?;
        Ok(entry.value)
    }

    fn build_key(&self, raw_key: &str) -> String {
        let mut key = if self.prefix.is_empty() {
            raw_key.to_string()
        } else {
            format!("{}{}", self.prefix, raw_key)
        };

        // Avoid overly long keys; sled allows longer, but this mirrors the TS helper.
        if key.len() > 255 {
            key.truncate(255);
        }
        key
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn uses_cached_value_until_ttl_expires() {
        let dir = tempdir().unwrap();
        let base = KVDB::new(dir.path().join("cache.db"), "cache").unwrap();
        let cache = KVCache::new(base).with_default_ttl(Duration::from_millis(100));

        let calls = Arc::new(AtomicUsize::new(0));
        let first = cache
            .get_or_insert_with("answer", None, {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    42u32
                }
            })
            .unwrap();
        assert_eq!(first, 42);

        let second = cache
            .get_or_insert_with("answer", None, {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    7u32
                }
            })
            .unwrap();
        assert_eq!(second, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        sleep(Duration::from_millis(120));
        let third = cache
            .get_or_insert_with("answer", None, {
                let calls = Arc::clone(&calls);
                move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    7u32
                }
            })
            .unwrap();
        assert_eq!(third, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn propagates_compute_errors() {
        let dir = tempdir().unwrap();
        let base = KVDB::new(dir.path().join("cache_err.db"), "cache").unwrap();
        let cache = KVCache::new(base);

        let err = cache
            .get_or_try_insert_with::<_, u8, _, std::io::Error>("fail", None, || {
                Err(std::io::Error::new(std::io::ErrorKind::Other, "boom"))
            })
            .unwrap_err();

        match err {
            CacheError::Compute(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

use std::time::{Duration, Instant};

use super::{Store, StoreError};
use super::types::{Entry, Value};

impl Store {
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        let expires_at = ttl.map(|d| Instant::now() + d);
        let mut inner = self.inner.lock().unwrap();

        inner.map.insert(
            key,
            Entry {
                value: Value::String(value),
                expires_at,
            },
        );
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let mut inner = self.inner.lock().unwrap();

        let expired = match inner.map.get(key) {
            Some(entry) => match entry.expires_at {
                Some(deadline) => Instant::now() >= deadline,
                None => false,
            },
            None => return Ok(None),
        };

        if expired {
            inner.map.remove(key);
            return Ok(None);
        }

        match &inner.map.get(key).unwrap().value {
            Value::String(bytes) => Ok(Some(bytes.clone())),
            Value::List(_) | Value::Stream(_) => Err(StoreError::WrongType),
        }
    }
}
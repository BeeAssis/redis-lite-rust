use std::collections::VecDeque;

use super::{Store, StoreError};
use super::types::{Entry, Value};

impl Store {
    pub fn rpush(&self, key: Vec<u8>, value: Vec<Vec<u8>>) -> Result<usize, StoreError> {
        let mut inner = self.inner.lock().unwrap();

        let new_len = match inner.map.get_mut(&key) {
            Some(entry) => match &mut entry.value {
                Value::List(list) => {
                    list.extend(value);
                    list.len()
                }
                _ => return Err(StoreError::WrongType),
            },
            None => {
                let mut list = VecDeque::new();
                list.extend(value);
                let len = list.len();
                inner.map.insert(
                    key.clone(),
                    Entry {
                        value: Value::List(list),
                        expires_at: None,
                    },
                );
                len
            }
        };

        Self::wake_waiters(&mut inner, &key);
        Ok(new_len)
    }

    pub fn lpush(&self, key: Vec<u8>, value: Vec<Vec<u8>>) -> Result<usize, StoreError> {
        let mut inner = self.inner.lock().unwrap();

        let new_len = match inner.map.get_mut(&key) {
            Some(entry) => match &mut entry.value {
                Value::List(list) => {
                    for v in value {
                        list.push_front(v);
                    }
                    list.len()
                }
                _ => return Err(StoreError::WrongType),
            },
            None => {
                let mut list = VecDeque::new();
                for v in value {
                    list.push_front(v);
                }
                let len = list.len();
                inner.map.insert(
                    key.clone(),
                    Entry {
                        value: Value::List(list),
                        expires_at: None,
                    },
                );
                len
            }
        };

        Self::wake_waiters(&mut inner, &key);
        Ok(new_len)
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>, StoreError> {
        let inner = self.inner.lock().unwrap();

        match inner.map.get(key) {
            Some(entry) => match &entry.value {
                Value::List(list) => {
                    let len = list.len() as i64;
                    if len == 0 {
                        return Ok(Vec::new());
                    }

                    let mut start = start;
                    let mut stop = stop;

                    if start < 0 { start += len; }
                    if stop < 0  { stop  += len; }
                    if start < 0 { start = 0; }
                    if stop >= len { stop = len - 1; }
                    if stop < start { return Ok(Vec::new()); }

                    let start_usize = start as usize;
                    let count_usize = (stop - start + 1) as usize;

                    let result: Vec<Vec<u8>> = list
                        .iter()
                        .skip(start_usize)
                        .take(count_usize)
                        .cloned()
                        .collect();

                    Ok(result)
                }
                _ => Err(StoreError::WrongType),
            },
            None => Ok(Vec::new()),
        }
    }

    pub fn llen(&self, key: &[u8]) -> Result<usize, StoreError> {
        let inner = self.inner.lock().unwrap();

        match inner.map.get(key) {
            Some(entry) => match &entry.value {
                Value::List(list) => Ok(list.len()),
                _ => Err(StoreError::WrongType),
            },
            None => Ok(0),
        }
    }

    pub fn lpop(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let mut inner = self.inner.lock().unwrap();

        match inner.map.get_mut(key) {
            Some(entry) => match &mut entry.value {
                Value::List(list) => Ok(list.pop_front()),
                _ => Err(StoreError::WrongType),
            },
            None => Ok(None),
        }
    }

    pub fn lpop_count(&self, key: &[u8], count: usize) -> Result<Vec<Vec<u8>>, StoreError> {
        let mut inner = self.inner.lock().unwrap();

        match inner.map.get_mut(key) {
            Some(entry) => match &mut entry.value {
                Value::List(list) => {
                    let mut result = Vec::new();
                    for _ in 0..count {
                        match list.pop_front() {
                            Some(v) => result.push(v),
                            None => break,
                        }
                    }
                    Ok(result)
                }
                _ => Err(StoreError::WrongType),
            },
            None => Ok(Vec::new()),
        }
    }
}
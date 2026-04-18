use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};


struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

pub struct Store {
    inner: Mutex<HashMap<Vec<u8>,Entry>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            // Creates table that can only be accessed by one thread at a time
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl:Option<Duration>) {

        let expires_at = ttl.map(|d| Instant::now() + d);

        // .lock() asks for access of protected data
        let mut map = self.inner.lock().unwrap();
        map.insert(
            key,
            Entry{
                value,
                expires_at
            });
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut map = self.inner.lock().unwrap();

        let expired = match map.get(key){
            Some(entry) => match entry.expires_at{
                Some(deadline) => Instant::now() >= deadline,
                None => false,
            },
            None => return None
        };

        if expired {
            map.remove(key);
            return None;
        }

        map.get(key).map(|entry| entry.value.clone())
    }
}
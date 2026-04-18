use std::collections::HashMap;
use std::sync::Mutex;

pub struct Store {
    inner: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut map = self.inner.lock().unwrap();
        map.insert(key, value);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let map = self.inner.lock().unwrap();
        map.get(key).cloned()
    }
}
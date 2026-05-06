use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::mpsc::Sender;

mod types;
mod string_ops;
mod list_ops;
mod stream_ops;
mod blocking;

pub use types::{BlpopResult, EntryId, StoreError, XaddId};
pub(crate) use types::{Entry, StreamEntry, Value};

pub(crate) struct Inner {
    pub map: HashMap<Vec<u8>, Entry>,
    pub waiters: HashMap<Vec<u8>, VecDeque<Sender<Vec<u8>>>>,
}

pub struct Store {
    pub(crate) inner: Mutex<Inner>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                waiters: HashMap::new(),
            }),
        }
    }

    pub fn type_of(&self, key: &[u8]) -> &'static str {
        let mut inner = self.inner.lock().unwrap();

        let expired = match inner.map.get(key) {
            Some(entry) => match entry.expires_at {
                Some(deadline) => std::time::Instant::now() >= deadline,
                None => false,
            },
            None => return "none",
        };

        if expired {
            inner.map.remove(key);
            return "none";
        }

        match &inner.map.get(key).unwrap().value {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Stream(_) => "stream",
        }
    }
}
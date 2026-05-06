use std::collections::VecDeque;
use std::sync::mpsc::channel;

use super::{Inner, Store, StoreError};
use super::types::{BlpopResult, Value};

impl Store {
    pub fn blpop(&self, key: Vec<u8>) -> Result<BlpopResult, StoreError> {
        let mut inner = self.inner.lock().unwrap();

        match inner.map.get_mut(&key) {
            Some(entry) => match &mut entry.value {
                Value::List(list) => {
                    if let Some(elem) = list.pop_front() {
                        return Ok(BlpopResult::Popped(elem));
                    }
                }
                _ => return Err(StoreError::WrongType),
            },
            None => {}
        }

        let (tx, rx) = channel();
        inner
            .waiters
            .entry(key)
            .or_insert_with(VecDeque::new)
            .push_back(tx);

        Ok(BlpopResult::Waiting(rx))
    }

    /// Wake any BLPOP waiters on this key by handing them the next list element.
    /// Caller must already hold the lock on `inner`.
    pub(crate) fn wake_waiters(inner: &mut Inner, key: &[u8]) {
        loop {
            let waiter_queue = match inner.waiters.get_mut(key) {
                Some(q) => q,
                None => break,
            };

            let tx = match waiter_queue.pop_front() {
                Some(tx) => tx,
                None => break,
            };

            let list = match inner.map.get_mut(key) {
                Some(entry) => match &mut entry.value {
                    Value::List(list) => list,
                    _ => break,
                },
                None => break,
            };

            let elem = match list.pop_front() {
                Some(e) => e,
                None => {
                    inner.waiters.get_mut(key).unwrap().push_front(tx);
                    break;
                }
            };

            let _ = tx.send(elem);
        }
    }
}
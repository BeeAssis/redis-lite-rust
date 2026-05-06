use std::time::{SystemTime, UNIX_EPOCH};

use super::{Store, StoreError};
use super::types::{Entry, EntryId, StreamEntry, Value, XaddId};

impl Store {
    pub fn xadd(
        &self,
        key: Vec<u8>,
        req: XaddId,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<EntryId, StoreError> {
        let mut inner = self.inner.lock().unwrap();

        let last_id = match inner.map.get(&key) {
            Some(entry) => match &entry.value {
                Value::Stream(stream_vec) => stream_vec.last().map(|e| e.id),
                _ => return Err(StoreError::WrongType),
            },
            None => None,
        };

        let new_id = resolve_entry_id(req, last_id);

        if new_id.ms == 0 && new_id.seq == 0 {
            return Err(StoreError::InvalidStreamId(
                "ERR The ID specified in XADD must be greater than 0-0".to_string(),
            ));
        }

        if let Some(last) = last_id {
            if new_id <= last {
                return Err(StoreError::InvalidStreamId(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                ));
            }
        }

        let new_entry = StreamEntry { id: new_id, fields };

        match inner.map.get_mut(&key) {
            Some(entry) => match &mut entry.value {
                Value::Stream(stream_vec) => {
                    stream_vec.push(new_entry);
                    Ok(new_id)
                }
                _ => Err(StoreError::WrongType),
            },
            None => {
                inner.map.insert(
                    key,
                    Entry {
                        value: Value::Stream(vec![new_entry]),
                        expires_at: None,
                    },
                );
                Ok(new_id)
            }
        }
    }
}

fn resolve_entry_id(req: XaddId, last_id: Option<EntryId>) -> EntryId {
    match req {
        XaddId::Explicit(id) => id,
        XaddId::PartialAuto(ms) => {
            let seq = match last_id {
                Some(last) if last.ms == ms => last.seq + 1,
                _ => if ms == 0 { 1 } else { 0 },
            };
            EntryId { ms, seq }
        }
        XaddId::FullAuto => {
            let mut current_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let seq = match last_id {
                Some(last) => {
                    if current_ms < last.ms {
                        current_ms = last.ms;
                    }
                    if current_ms == last.ms { last.seq + 1 } else { 0 }
                }
                None => 0,
            };
            EntryId { ms: current_ms, seq }
        }
    }
}
use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::time::Instant;

pub(crate) enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Stream(Vec<StreamEntry>),
}

pub(crate) struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

pub(crate) struct StreamEntry {
    pub id: EntryId,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct EntryId {
    pub ms: u64,
    pub seq: u64,
}

pub enum XaddId {
    Explicit(EntryId),
    PartialAuto(u64),
    FullAuto,
}

#[derive(Debug)]
pub enum StoreError {
    WrongType,
    InvalidStreamId(String),
}

pub enum BlpopResult {
    Popped(Vec<u8>),
    Waiting(Receiver<Vec<u8>>),
}
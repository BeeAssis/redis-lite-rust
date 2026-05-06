mod args;
mod string;
mod list;
mod stream;
mod generic;

use crate::protocol::RespType;
use crate::protocol::serializer::serialize_resp;
use crate::storage::{Store, StoreError};

pub fn dispatch(out: &mut Vec<u8>, resp: &RespType, store: &Store) {
    let elements = match resp {
        RespType::Array(elements) => elements,
        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR expected array of bulk strings".to_string()),
            );
            return;
        }
    };

    let command = match elements.first() {
        Some(RespType::BulkString(Some(bytes))) => bytes.as_slice(),
        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR missing or malformed command".to_string()),
            );
            return;
        }
    };

    let args = &elements[1..];

    if command.eq_ignore_ascii_case(b"PING") {
        string::handle_ping(out, args);
    } else if command.eq_ignore_ascii_case(b"ECHO") {
        string::handle_echo(out, args);
    } else if command.eq_ignore_ascii_case(b"SET") {
        string::handle_set(out, args, store);
    } else if command.eq_ignore_ascii_case(b"GET") {
        string::handle_get(out, args, store);
    } else if command.eq_ignore_ascii_case(b"RPUSH") {
        list::handle_rpush(out, args, store);
    } else if command.eq_ignore_ascii_case(b"LPUSH") {
        list::handle_lpush(out, args, store);
    } else if command.eq_ignore_ascii_case(b"LRANGE") {
        list::handle_lrange(out, args, store);
    } else if command.eq_ignore_ascii_case(b"LLEN") {
        list::handle_llen(out, args, store);
    } else if command.eq_ignore_ascii_case(b"LPOP") {
        list::handle_lpop(out, args, store);
    } else if command.eq_ignore_ascii_case(b"BLPOP") {
        list::handle_blpop(out, args, store);
    } else if command.eq_ignore_ascii_case(b"TYPE") {
        generic::handle_type(out, args, store);
    } else if command.eq_ignore_ascii_case(b"XADD") {
        stream::handle_xadd(out, args, store);
    } else {
        let msg = format!("ERR unknown command '{}'", String::from_utf8_lossy(command));
        serialize_resp(out, &RespType::Error(msg));
    }
}

/// Shared helper: convert a StoreError into a RESP error reply.
pub(crate) fn serialize_store_error(out: &mut Vec<u8>, error: StoreError) {
    match error {
        StoreError::WrongType => {
            serialize_resp(
                out,
                &RespType::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            );
        }
        StoreError::InvalidStreamId(msg) => {
            serialize_resp(out, &RespType::Error(msg));
        }
    }
}
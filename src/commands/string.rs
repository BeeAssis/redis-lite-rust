use std::time::Duration;

use crate::protocol::RespType;
use crate::protocol::serializer::serialize_resp;
use crate::storage::Store;

use super::args::{expect_bulk_string, parse_u64};
use super::serialize_store_error;

pub fn handle_ping(out: &mut Vec<u8>, args: &[RespType]) {
    match args {
        [] => serialize_resp(out, &RespType::SimpleString("PONG".to_string())),
        [RespType::BulkString(Some(bytes))] => {
            serialize_resp(out, &RespType::BulkString(Some(bytes.clone())));
        }
        _ => serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'ping'".to_string()),
        ),
    }
}

pub fn handle_echo(out: &mut Vec<u8>, args: &[RespType]) {
    match args {
        [RespType::BulkString(Some(bytes))] => {
            serialize_resp(out, &RespType::BulkString(Some(bytes.clone())));
        }
        _ => serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'echo'".to_string()),
        ),
    }
}

pub fn handle_set(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    match args {
        [k, v] => {
            let (Some(key), Some(value)) = (expect_bulk_string(k), expect_bulk_string(v)) else {
                serialize_resp(out, &RespType::Error("ERR syntax error".to_string()));
                return;
            };
            store.set(key.to_vec(), value.to_vec(), None);
            serialize_resp(out, &RespType::SimpleString("OK".to_string()));
        }
        [k, v, opt, ms] if matches!(expect_bulk_string(opt), Some(b) if b.eq_ignore_ascii_case(b"PX")) => {
            let (Some(key), Some(value)) = (expect_bulk_string(k), expect_bulk_string(v)) else {
                serialize_resp(out, &RespType::Error("ERR syntax error".to_string()));
                return;
            };
            let Some(ms) = parse_u64(ms) else {
                serialize_resp(out, &RespType::Error("ERR invalid PX milliseconds".to_string()));
                return;
            };
            store.set(key.to_vec(), value.to_vec(), Some(Duration::from_millis(ms)));
            serialize_resp(out, &RespType::SimpleString("OK".to_string()));
        }
        _ => serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'set'".to_string()),
        ),
    }
}

pub fn handle_get(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    let [arg] = args else {
        serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'get'".to_string()),
        );
        return;
    };

    let Some(key) = expect_bulk_string(arg) else {
        serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
        return;
    };

    match store.get(key) {
        Ok(Some(bytes)) => serialize_resp(out, &RespType::BulkString(Some(bytes))),
        Ok(None) => serialize_resp(out, &RespType::BulkString(None)),
        Err(e) => serialize_store_error(out, e),
    }
}
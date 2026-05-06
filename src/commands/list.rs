use crate::protocol::RespType;
use crate::protocol::serializer::serialize_resp;
use crate::storage::{BlpopResult, Store};

use super::args::{expect_bulk_string, parse_f64, parse_i64};
use super::serialize_store_error;

pub fn handle_rpush(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    if args.len() < 2 {
        serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'rpush'".to_string()),
        );
        return;
    }

    let Some(key) = expect_bulk_string(&args[0]) else {
        serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
        return;
    };

    let mut elements = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        match expect_bulk_string(arg) {
            Some(bytes) => elements.push(bytes.to_vec()),
            None => {
                serialize_resp(out, &RespType::Error("ERR invalid element".to_string()));
                return;
            }
        }
    }

    match store.rpush(key.to_vec(), elements) {
        Ok(len) => serialize_resp(out, &RespType::Integer(len as i64)),
        Err(e) => serialize_store_error(out, e),
    }
}

pub fn handle_lpush(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    if args.len() < 2 {
        serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'lpush'".to_string()),
        );
        return;
    }

    let Some(key) = expect_bulk_string(&args[0]) else {
        serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
        return;
    };

    let mut elements = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        match expect_bulk_string(arg) {
            Some(bytes) => elements.push(bytes.to_vec()),
            None => {
                serialize_resp(out, &RespType::Error("ERR invalid element".to_string()));
                return;
            }
        }
    }

    match store.lpush(key.to_vec(), elements) {
        Ok(len) => serialize_resp(out, &RespType::Integer(len as i64)),
        Err(e) => serialize_store_error(out, e),
    }
}

pub fn handle_lrange(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    let [k, s, e] = args else {
        serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'lrange'".to_string()),
        );
        return;
    };

    let Some(key) = expect_bulk_string(k) else {
        serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
        return;
    };

    let (Some(start), Some(stop)) = (parse_i64(s), parse_i64(e)) else {
        serialize_resp(
            out,
            &RespType::Error("ERR value is not an integer or out of range".to_string()),
        );
        return;
    };

    match store.lrange(key, start, stop) {
        Ok(elements) => {
            let resp: Vec<RespType> = elements
                .into_iter()
                .map(|b| RespType::BulkString(Some(b)))
                .collect();
            serialize_resp(out, &RespType::Array(resp));
        }
        Err(e) => serialize_store_error(out, e),
    }
}

pub fn handle_llen(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    let [arg] = args else {
        serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'llen'".to_string()),
        );
        return;
    };

    let Some(key) = expect_bulk_string(arg) else {
        serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
        return;
    };

    match store.llen(key) {
        Ok(len) => serialize_resp(out, &RespType::Integer(len as i64)),
        Err(e) => serialize_store_error(out, e),
    }
}

pub fn handle_lpop(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    match args {
        [arg] => {
            let Some(key) = expect_bulk_string(arg) else {
                serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
                return;
            };
            match store.lpop(key) {
                Ok(Some(bytes)) => serialize_resp(out, &RespType::BulkString(Some(bytes))),
                Ok(None) => serialize_resp(out, &RespType::BulkString(None)),
                Err(e) => serialize_store_error(out, e),
            }
        }
        [k, c] => {
            let Some(key) = expect_bulk_string(k) else {
                serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
                return;
            };
            let Some(count) = parse_i64(c) else {
                serialize_resp(
                    out,
                    &RespType::Error("ERR value is not an integer or out of range".to_string()),
                );
                return;
            };
            if count < 0 {
                serialize_resp(
                    out,
                    &RespType::Error("ERR value is not an integer or out of range".to_string()),
                );
                return;
            }
            match store.lpop_count(key, count as usize) {
                Ok(elements) => {
                    let resp: Vec<RespType> = elements
                        .into_iter()
                        .map(|b| RespType::BulkString(Some(b)))
                        .collect();
                    serialize_resp(out, &RespType::Array(resp));
                }
                Err(e) => serialize_store_error(out, e),
            }
        }
        _ => serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'lpop'".to_string()),
        ),
    }
}

pub fn handle_blpop(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    let [k, t] = args else {
        serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'blpop'".to_string()),
        );
        return;
    };

    let Some(key) = expect_bulk_string(k) else {
        serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
        return;
    };

    let Some(timeout_secs) = parse_f64(t).filter(|n| *n >= 0.0) else {
        serialize_resp(
            out,
            &RespType::Error("ERR timeout is not float or out of range".to_string()),
        );
        return;
    };

    let elem = match store.blpop(key.to_vec()) {
        Ok(BlpopResult::Popped(elem)) => elem,
        Ok(BlpopResult::Waiting(rx)) => {
            if timeout_secs == 0.0 {
                match rx.recv() {
                    Ok(elem) => elem,
                    Err(_) => {
                        out.extend_from_slice(b"*-1\r\n");
                        return;
                    }
                }
            } else {
                let dur = std::time::Duration::from_secs_f64(timeout_secs);
                match rx.recv_timeout(dur) {
                    Ok(elem) => elem,
                    Err(_) => {
                        out.extend_from_slice(b"*-1\r\n");
                        return;
                    }
                }
            }
        }
        Err(e) => {
            serialize_store_error(out, e);
            return;
        }
    };

    let response = RespType::Array(vec![
        RespType::BulkString(Some(key.to_vec())),
        RespType::BulkString(Some(elem)),
    ]);
    serialize_resp(out, &response);
}
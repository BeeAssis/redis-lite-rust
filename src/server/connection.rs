use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::protocol::parser::{resp_protocol_parser, ParserError};
use crate::protocol::serializer::serialize_resp;
use crate::protocol::RespType;

use std::sync::Arc;
use crate::storage::{Store, StoreError, BlpopResult, XaddId, EntryId};
use std::time::Duration;

pub fn handle_connection(mut stream: TcpStream, store: Arc<Store>) {
    let mut incoming_bytes: Vec<u8> = Vec::new();
    let mut response_buffer: Vec<u8> = Vec::new();

    loop {
        let mut temp_buffer = [0; 1024];

        let num_bytes_read = match stream.read(&mut temp_buffer) {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };

        incoming_bytes.extend_from_slice(&temp_buffer[..num_bytes_read]);

        loop {
            match resp_protocol_parser(&incoming_bytes, 0) {
                Ok((resp, consumed)) => {
                    debug_assert!(consumed > 0, "parser returned Ok with zero bytes consumed");

                    response_buffer.clear();
                    build_response(&mut response_buffer, &resp, &store);

                    if stream.write_all(&response_buffer).is_err() {
                        return;
                    }

                    if stream.flush().is_err() {
                        return;
                    }

                    incoming_bytes.drain(..consumed);
                    continue;
                }

                Err(ParserError::IncompleteInput) => {
                    break;
                }

                Err(_) => {
                    return;
                }
            }
        }
    }
}

fn build_response(out: &mut Vec<u8>, resp: &RespType, store: &Store) {
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

    if command.eq_ignore_ascii_case(b"PING") {
        handle_ping(out, &elements[1..]);
    } else if command.eq_ignore_ascii_case(b"ECHO") {
        handle_echo(out, &elements[1..]);
    } else if command.eq_ignore_ascii_case(b"SET") {
        handle_set(out, &elements[1..], store);
    } else if command.eq_ignore_ascii_case(b"GET") {
        handle_get(out, &elements[1..], store);
    } else if command.eq_ignore_ascii_case(b"RPUSH") {
        handle_rpush(out, &elements[1..], store);
    } else if command.eq_ignore_ascii_case(b"LRANGE") {
        handle_lrange(out, &elements[1..], store);
    } else if command.eq_ignore_ascii_case(b"LPUSH") {
        handle_lpush(out, &elements[1..], store);
    } else if command.eq_ignore_ascii_case(b"LLEN") {
        handle_llen(out, &elements[1..], store);
    } else if command.eq_ignore_ascii_case(b"LPOP") {
        handle_lpop(out, &elements[1..], store);
    } else if command.eq_ignore_ascii_case(b"BLPOP") {
        handle_blpop(out, &elements[1..], store);
    } else if command.eq_ignore_ascii_case(b"TYPE") {
        handle_type(out, &elements[1..], store);
    } else if command.eq_ignore_ascii_case(b"XADD") {
        handle_xadd(out, &elements[1..], store);
    } else {
        let msg = format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(command)
        );
        serialize_resp(out, &RespType::Error(msg));
    }
}

fn serialize_store_error(out: &mut Vec<u8>, error: StoreError) {
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

fn handle_ping(out: &mut Vec<u8>, args: &[RespType]) {
    match args {
        [] => {
            serialize_resp(out, &RespType::SimpleString("PONG".to_string()));
        }

        [RespType::BulkString(Some(bytes))] => {
            serialize_resp(out, &RespType::BulkString(Some(bytes.clone())));
        }

        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR wrong number of arguments for 'ping'".to_string()),
            );
        }
    }
}

fn handle_echo(out: &mut Vec<u8>, args: &[RespType]) {
    match args {
        [RespType::BulkString(Some(bytes))] => {
            serialize_resp(out, &RespType::BulkString(Some(bytes.clone())));
        }

        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR wrong number of arguments for 'echo'".to_string()),
            );
        }
    }
}

fn handle_set(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    match args {
        [RespType::BulkString(Some(key)), RespType::BulkString(Some(value))] => {
            store.set(key.clone(), value.clone(), None);
            serialize_resp(out, &RespType::SimpleString("OK".to_string()));
        }

        [
            RespType::BulkString(Some(key)),
            RespType::BulkString(Some(value)),
            RespType::BulkString(Some(option)),
            RespType::BulkString(Some(ms_bytes)),
        ] if option.eq_ignore_ascii_case(b"PX") => {
            let ms_str = match std::str::from_utf8(ms_bytes) {
                Ok(s) => s,
                Err(_) => {
                    serialize_resp(
                        out,
                        &RespType::Error("ERR invalid PX milliseconds".to_string()),
                    );
                    return;
                }
            };

            let ms = match ms_str.parse::<u64>() {
                Ok(n) => n,
                Err(_) => {
                    serialize_resp(
                        out,
                        &RespType::Error("ERR invalid PX milliseconds".to_string()),
                    );
                    return;
                }
            };

            store.set(
                key.clone(),
                value.clone(),
                Some(Duration::from_millis(ms)),
            );

            serialize_resp(out, &RespType::SimpleString("OK".to_string()));
        }

        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR wrong number of arguments for 'set'".to_string()),
            );
        }
    }
}

fn handle_get(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    match args {
        [RespType::BulkString(Some(key))] => {
            match store.get(key) {
                Ok(Some(bytes)) => {
                    serialize_resp(out, &RespType::BulkString(Some(bytes)));
                }

                Ok(None) => {
                    serialize_resp(out, &RespType::BulkString(None));
                }

                Err(e) => {
                    serialize_store_error(out, e);
                }
            }
        }

        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR wrong number of arguments for 'get'".to_string()),
            );
        }
    }
}

fn handle_rpush(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    if args.len() < 2 {
        serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'rpush'".to_string()),
        );
        return;
    }

    let key = match &args[0] {
        RespType::BulkString(Some(bytes)) => bytes.clone(),
        _ => {
            serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
            return;
        }
    };

    let mut elements: Vec<Vec<u8>> = Vec::new();

    for arg in &args[1..] {
        match arg {
            RespType::BulkString(Some(bytes)) => elements.push(bytes.clone()),
            _ => {
                serialize_resp(out, &RespType::Error("ERR invalid element".to_string()));
                return;
            }
        }
    }

    match store.rpush(key, elements) {
        Ok(len) => {
            serialize_resp(out, &RespType::Integer(len as i64));
        }

        Err(e) => {
            serialize_store_error(out, e);
        }
    }
}

fn handle_lrange(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    match args {
        [
            RespType::BulkString(Some(key)),
            RespType::BulkString(Some(start_bytes)),
            RespType::BulkString(Some(stop_bytes)),
        ] => {
            let start: i64 = match std::str::from_utf8(start_bytes) {
                Ok(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        serialize_resp(
                            out,
                            &RespType::Error("ERR value is not an integer or out of range".to_string()),
                        );
                        return;
                    }
                },

                Err(_) => {
                    serialize_resp(
                        out,
                        &RespType::Error("ERR value is not an integer or out of range".to_string()),
                    );
                    return;
                }
            };

            let stop: i64 = match std::str::from_utf8(stop_bytes) {
                Ok(s) => match s.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        serialize_resp(
                            out,
                            &RespType::Error("ERR value is not an integer or out of range".to_string()),
                        );
                        return;
                    }
                },

                Err(_) => {
                    serialize_resp(
                        out,
                        &RespType::Error("ERR value is not an integer or out of range".to_string()),
                    );
                    return;
                }
            };

            match store.lrange(key, start, stop) {
                Ok(elements) => {
                    let resp_elements: Vec<RespType> = elements
                        .into_iter()
                        .map(|bytes| RespType::BulkString(Some(bytes)))
                        .collect();

                    serialize_resp(out, &RespType::Array(resp_elements));
                }

                Err(e) => {
                    serialize_store_error(out, e);
                }
            }
        }

        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR wrong number of arguments for 'lrange'".to_string()),
            );
        }
    }
}

fn handle_lpush(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    if args.len() < 2 {
        serialize_resp(
            out,
            &RespType::Error("ERR wrong number of arguments for 'rpush'".to_string()),
        );
        return;
    }

    let key = match &args[0] {
        RespType::BulkString(Some(bytes)) => bytes.clone(),
        _ => {
            serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
            return;
        }
    };

    let mut elements: Vec<Vec<u8>> = Vec::new();

    for arg in &args[1..] {
        match arg {
            RespType::BulkString(Some(bytes)) => elements.push(bytes.clone()),
            _ => {
                serialize_resp(out, &RespType::Error("ERR invalid element".to_string()));
                return;
            }
        }
    }

    match store.lpush(key, elements) {
        Ok(len) => {
            serialize_resp(out, &RespType::Integer(len as i64));
        }

        Err(e) => {
            serialize_store_error(out, e);
        }
    }
}

fn handle_llen(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    match args {
        [RespType::BulkString(Some(key))] => {
            match store.llen(key) {
                Ok(len) => {
                    serialize_resp(out, &RespType::Integer(len as i64));
                }

                Err(e) => {
                    serialize_store_error(out, e);
                }
            }
        }

        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR wrong number of arguments for 'llen'".to_string()),
            );
        }
    }
}

fn handle_lpop(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    match args {
        [RespType::BulkString(Some(key))] => {
            match store.lpop(key) {
                Ok(Some(bytes)) => {
                    serialize_resp(out, &RespType::BulkString(Some(bytes)));
                }

                Ok(None) => {
                    serialize_resp(out, &RespType::BulkString(None));
                }

                Err(e) => {
                    serialize_store_error(out, e);
                }
            }
        }

        [RespType::BulkString(Some(key)), RespType::BulkString(Some(count_bytes))] => {
            let count_str = match std::str::from_utf8(count_bytes) {
                Ok(s) => s,
                Err(_) => {
                    serialize_resp(
                        out,
                        &RespType::Error("ERR value is not an integer or out of range".to_string()),
                    );
                    return;
                }
            };

            let count_i64 = match count_str.parse::<i64>() {
                Ok(n) => n,
                Err(_) => {
                    serialize_resp(
                        out,
                        &RespType::Error("ERR value is not an integer or out of range".to_string()),
                    );
                    return;
                }
            };

            if count_i64 < 0 {
                serialize_resp(
                    out,
                    &RespType::Error("ERR value is not an integer or out of range".to_string()),
                );
                return;
            }

            let count = count_i64 as usize;

            match store.lpop_count(key, count) {
                Ok(elements) => {
                    let resp_elements: Vec<RespType> = elements
                        .into_iter()
                        .map(|bytes| RespType::BulkString(Some(bytes)))
                        .collect();

                    serialize_resp(out, &RespType::Array(resp_elements));
                }

                Err(e) => {
                    serialize_store_error(out, e);
                }
            }
        }

        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR wrong number of arguments for 'lpop'".to_string()),
            );
        }
    }
}

fn handle_blpop(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    let (key, timeout_bytes) = match args {
        [RespType::BulkString(Some(k)), RespType::BulkString(Some(t))] => (k, t),
        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR wrong number of arguments for 'blpop'".to_string()),
            );
            return;
        }
    };

    let timeout_str = match std::str::from_utf8(timeout_bytes) {
        Ok(s) => s,
        Err(_) => {
            serialize_resp(out, &RespType::Error("ERR invalid timeout".to_string()));
            return;
        }
    };

    let timeout_secs = match timeout_str.parse::<f64>() {
        Ok(n) if n >= 0.0 => n,
        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR timeout is not float or out of range".to_string()),
            );
            return;
        }
    };

    let elem = match store.blpop(key.clone()) {
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
        RespType::BulkString(Some(key.clone())),
        RespType::BulkString(Some(elem)),
    ]);

    serialize_resp(out, &response);
}

fn handle_type(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    match args {
        [RespType::BulkString(Some(key))] => {
            let type_string = store.type_of(key);

            let response = RespType::SimpleString(type_string.to_string());

            serialize_resp(out, &response);
        }

        _ => {
            let error = RespType::Error("ERR wrong number of arguments for 'type' command".to_string());
            serialize_resp(out, &error);
        }
    }
}

fn handle_xadd(out: &mut Vec<u8>, args: &[RespType], store: &Store) {
    if args.len() < 4 || args.len() % 2 != 0 {
        let error = RespType::Error("ERR wrong number of arguments for 'xadd' command".to_string());
        serialize_resp(out, &error);
        return;
    }

    let key = match &args[0] {
        RespType::BulkString(Some(bytes)) => bytes.clone(),
        _ => {
            serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
            return;
        }
    };

    let id_str = match &args[1] {
        RespType::BulkString(Some(bytes)) => match std::str::from_utf8(bytes) {
            Ok(s) => s,
            Err(_) => {
                serialize_resp(out, &RespType::Error("ERR invalid stream ID".to_string()));
                return;
            }
        },

        _ => {
            serialize_resp(out, &RespType::Error("ERR invalid stream ID".to_string()));
            return;
        }
    };

    let xadd_req = match parse_xadd_id(id_str) {
        Ok(req) => req,
        Err(msg) => {
            serialize_resp(out, &RespType::Error(msg));
            return;
        }
    };

    let mut fields = Vec::new();

    for chunk in args[2..].chunks(2) {
        match (&chunk[0], &chunk[1]) {
            (RespType::BulkString(Some(k)), RespType::BulkString(Some(v))) => {
                fields.push((k.clone(), v.clone()));
            }

            _ => {
                serialize_resp(out, &RespType::Error("ERR invalid field/value pair".to_string()));
                return;
            }
        }
    }

    match store.xadd(key, xadd_req, fields) {
        Ok(final_id) => {
            let response_str = format!("{}-{}", final_id.ms, final_id.seq);
            let response = RespType::BulkString(Some(response_str.into_bytes()));
            serialize_resp(out, &response);
        }

        Err(e) => {
            serialize_store_error(out, e);
        }
    }
}

fn parse_xadd_id(id_str: &str) -> Result<XaddId, String> {
    if id_str == "*" {
        return Ok(XaddId::FullAuto);
    }

    if let Some(ms_part) = id_str.strip_suffix("-*") {
        let ms = ms_part
            .parse::<u64>()
            .map_err(|_| "ERR invalid stream ID".to_string())?;

        return Ok(XaddId::PartialAuto(ms));
    }

    let mut parts = id_str.split('-');

    let ms = parts
        .next()
        .ok_or_else(|| "ERR invalid stream ID".to_string())?
        .parse::<u64>()
        .map_err(|_| "ERR invalid stream ID".to_string())?;

    let seq = parts
        .next()
        .ok_or_else(|| "ERR invalid stream ID".to_string())?
        .parse::<u64>()
        .map_err(|_| "ERR invalid stream ID".to_string())?;

    if parts.next().is_some() {
        return Err("ERR invalid stream ID".to_string());
    }

    Ok(XaddId::Explicit(EntryId { ms, seq }))
}

pub fn run_server() {
    let listener = match TcpListener::bind("127.0.0.1:6379") {
        Ok(l) => l,

        Err(e) => {
            eprintln!("failed to bind 127.0.0.1:6379: {}", e);
            std::process::exit(1);
        }
    };

    let store = Arc::new(Store::new());

    println!("Listening on 127.0.0.1:6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store = Arc::clone(&store);
                thread::spawn(move || handle_connection(stream, store));
            }

            Err(e) => {
                println!("connection error: {}", e);
            }
        }
    }
}
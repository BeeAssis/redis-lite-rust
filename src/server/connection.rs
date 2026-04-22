use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::protocol::parser::{resp_protocol_parser, ParserError};
use crate::protocol::serializer::serialize_resp;
use crate::protocol::RespType;

use std::sync::Arc;
use crate::storage::{Store, StoreError};
use std::time::Duration;



pub fn handle_connection(mut stream: TcpStream, store:Arc<Store>) {
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
    }else if command.eq_ignore_ascii_case(b"SET"){
        handle_set(out,&elements[1..],store);
    } else if command.eq_ignore_ascii_case(b"GET") {
        handle_get(out, &elements[1..], store);
    }else if command.eq_ignore_ascii_case(b"RPUSH") {
        handle_rpush(out, &elements[1..], store);
    }else {
        let msg = format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(command)
        );
        serialize_resp(out, &RespType::Error(msg));
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
                Err(StoreError::WrongType) => {
                    serialize_resp(out, &RespType::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".to_string()
                    ));
                }
            }
        }
        _ => {
            serialize_resp(
                out,
                &RespType::Error("ERR wrong number of arguments for 'get'".to_string())
            );
        }
    }
}


fn handle_rpush(out: &mut Vec<u8>, args:&[RespType], store:&Store){


    if args.len() < 2 {
        serialize_resp(out, &RespType::Error(
            "ERR wrong number of arguments for 'rpush'".to_string()
        ));

        return;
    }

    let key = match &args[0]{
        RespType::BulkString(Some(bytes)) => bytes.clone(),
        _ => {
            serialize_resp(out, &RespType::Error("ERR invalid key".to_string()));
            return;
        }
    };

    let mut elements: Vec<Vec<u8>> = Vec::new();
    for arg in &args[1..]{
        match arg {
            RespType::BulkString(Some(bytes)) => elements.push(bytes.clone()),
            _ => {
                serialize_resp(out, &RespType::Error("ERR invalid element".to_string()));
                return;
            }
        }
    }

    match store.rpush(key, elements) {
        Ok(len) =>{
            serialize_resp(out,&RespType::Integer(len as i64));
        }
        Err(StoreError::WrongType) =>{
            serialize_resp(out,&RespType::Error(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string()
            ));
        }

        }

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
                thread::spawn(move || handle_connection(stream,store));
            }
            Err(e) => {
                println!("connection error: {}", e);
            }
        }
    }
}






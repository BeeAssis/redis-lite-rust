use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::protocol::parser::{resp_protocol_parser, ParserError};
use crate::protocol::serializer::serialize_resp;
use crate::protocol::RespType;

pub fn handle_connection(mut stream: TcpStream) {
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
                    build_response(&mut response_buffer, &resp);

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

fn build_response(out: &mut Vec<u8>, resp: &RespType) {
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
    } else {
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

pub fn run_server() {
  let listener = match TcpListener::bind("127.0.0.1:6379") {
    Ok(l) => l,
    Err(e) => {
        eprintln!("failed to bind 127.0.0.1:6379: {}", e);
        std::process::exit(1);
        }
    };
    println!("Listening on 127.0.0.1:6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || handle_connection(stream));
            }
            Err(e) => {
                println!("connection error: {}", e);
            }
        }
    }
}




// use std::str;
// use crate::protocol::RespType;

// fn append_len(buf: &mut Vec<u8>, n: usize) {
//     buf.extend_from_slice(n.to_string().as_bytes());
// }

// fn append_crlf(buf: &mut Vec<u8>) {
//     buf.extend_from_slice(b"\r\n");
// }



// fn serialize_bulk_string(out: &mut Vec<u8>, data: Option<&[u8]>) {
//     match data {
//         Some(bytes) => {
//             out.push(b'$');
//             append_len(out, bytes.len());
//             append_crlf(out);
//             out.extend_from_slice(bytes);
//             append_crlf(out);
//         }
//         None => out.extend_from_slice(b"$-1\r\n"),
//     }
// }

    
// pub fn serialize_resp(out: &mut Vec<u8>, resp: &RespType) {
//     match resp {
//         RespType::SimpleString(s) => todo!(),
//         RespType::Error(e) => todo!(),
//         RespType::Integer(i) => todo!(),
//         RespType::BulkString(opt) => {
//             serialize_bulk_string(out, opt.as_deref());
//         }
//         RespType::Array(elements) => todo!(),
//     }
// }

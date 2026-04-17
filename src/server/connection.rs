use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::protocol::parser::{resp_protocol_parser, ParserError};

pub fn handle_connection(mut stream: TcpStream) {
    let mut incoming_bytes: Vec<u8> = Vec::new();

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
                Ok((_resp, consumed)) => {
                    if stream.write_all(b"+PONG\r\n").is_err() {
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

pub fn run_server() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
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
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::commands::dispatch;
use crate::protocol::parser::{resp_protocol_parser, ParserError};
use crate::storage::Store;

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
                    dispatch(&mut response_buffer, &resp, &store);

                    if stream.write_all(&response_buffer).is_err() {
                        return;
                    }
                    if stream.flush().is_err() {
                        return;
                    }

                    incoming_bytes.drain(..consumed);
                }
                Err(ParserError::IncompleteInput) => break,
                Err(_) => return,
            }
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
                thread::spawn(move || handle_connection(stream, store));
            }
            Err(e) => {
                println!("connection error: {}", e);
            }
        }
    }
}
mod protocol;
mod server;
mod storage;
mod commands;

fn main() {
    server::run_server();
}
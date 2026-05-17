# redis-lite-rust

A partial Redis server implementation in Rust, built as a learning project
through CodeCrafters' "Build Your Own Redis" challenge. Uses only the Rust
standard library — no external dependencies.

## Status

Completed through the stream-write stage. Stages beyond XADD (XRANGE, XREAD,
transactions, replication, RDB persistence) are not implemented.

## Implemented commands

- **String / connection:** `PING`, `ECHO`, `SET` (with `PX` expiry), `GET`
- **Lists:** `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LPOP` (with optional count),
  `BLPOP` (channel-based blocking)
- **Streams:** `XADD` (explicit, partial-auto, full-auto IDs, with monotonic
  ID validation)
- **Generic:** `TYPE`

## Architecture

- `protocol/` — RESP parser and serializer, from scratch using only std
- `server/` — TCP listener, thread-per-connection handler, partial-read
  buffering
- `commands/` — command dispatch and per-type handlers
- `storage/` — in-memory store behind a single `Mutex<Inner>`, with lazy
  expiration and a waiter queue for BLPOP

`BLPOP` is the most interesting piece. A blocked client receives a
`Receiver<Vec<u8>>`; subsequent `RPUSH`/`LPUSH` calls drain the waiter queue
and hand elements directly to parked senders via channel `send`.

## Run

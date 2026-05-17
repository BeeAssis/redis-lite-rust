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

```bash
cargo run --release
```

Server listens on `127.0.0.1:6379`. Test with `redis-cli`:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 SET foo bar PX 60000
redis-cli -p 6379 GET foo

redis-cli -p 6379 RPUSH mylist a b c
redis-cli -p 6379 LRANGE mylist 0 -1
redis-cli -p 6379 BLPOP mylist 5

redis-cli -p 6379 XADD mystream '*' field1 value1
redis-cli -p 6379 TYPE mystream
```

## Acknowledgments

Built through the [CodeCrafters](https://codecrafters.io) "Build Your Own
Redis" Rust track, which provides staged test specifications and an
architectural progression. All code in this repository was written in Rust
against those tests. AI tools (Claude, Cursor) were used during development
for concept explanation, code suggestions, and debugging.

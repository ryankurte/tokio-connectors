# (TCP|UDP|UNIX|OTHER) Servers and Clients for Tokio

This crate provides a bunch of servers and clients for communication between TCP, (UDP), Unix, (and in future, more) sockets and socket-like things, because I hope to never again need to write connection management logic.

Each connector also supports a set of common `Codec`s for encoding and decoding outgoing and incoming messages respectively.

## Status

TODO

## Features

**Connectors:**
- [x] Unix
  - [x] Server
  - [x] Client
- [x] TCP
  - [x] Server
  - [x] Client
- [ ] UDP
  - [ ] Server
  - [ ] Client

**Codecs:**
- [x] JSON
- [x] serde/postcard+COBS
- [ ] protobuf?
- [ ] rkyv?
- [ ] wincode?


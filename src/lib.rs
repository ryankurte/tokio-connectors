//! A helper package for communicating (with|between) processes using tokio/async clients and servers.
//!
//!

// TODO: the TCP and UNIX server/client implementations share a lot of code... should be refactored
// to be generic over a transport somehow (but it all works fine for now)...

pub mod tcp;

pub mod unix;

pub mod codecs;

pub mod error;

#[cfg(test)]
pub mod helpers;

mod handle;

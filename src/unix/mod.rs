use tokio::{
    io::AsyncWriteExt,
    net::unix::{OwnedReadHalf, OwnedWriteHalf},
};

mod client;
pub use client::UnixClient;

mod server;
pub use server::UnixServer;

use crate::handle::{Readable, Writeable};

#[cfg(test)]
mod test;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnixSocketId(u64);

impl std::fmt::Display for UnixSocketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Readable for OwnedReadHalf {
    async fn readable_internal(&mut self) -> tokio::io::Result<()> {
        OwnedReadHalf::readable(self).await
    }

    fn try_read_buf_internal(&mut self, buf: &mut Vec<u8>) -> tokio::io::Result<usize> {
        OwnedReadHalf::try_read_buf(self, buf)
    }
}

impl Writeable for OwnedWriteHalf {
    async fn write_all_internal(&mut self, buf: &[u8]) -> tokio::io::Result<()> {
        OwnedWriteHalf::write_all(self, buf).await
    }
}

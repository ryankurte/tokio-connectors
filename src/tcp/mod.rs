use tokio::{
    io::AsyncWriteExt,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

mod client;
pub use client::TcpClient;

mod server;
pub use server::TcpServer;

#[cfg(test)]
mod test;

use crate::handle::{Readable, Writeable};

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

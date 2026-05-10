//! A TCP client connector for arbitrary serialisable messages, using postcard + COBS for framing and serialization.

use std::{
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tokio::{
    net::{TcpStream, ToSocketAddrs},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use crate::{codecs::Codec, error::Error, handle::Handle};

/// A TCP client connector for sending and receiving arbitrary serialisable messages to/from a TCP server.
///
/// Uses the provided [Codec] for framing and (de)serialization of messages.
pub struct TcpClient<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> {
    local_addr: SocketAddr,
    out_tx: UnboundedSender<OUT>,
    in_rx: UnboundedReceiver<(IN, SocketAddr)>,
    handle: Handle<C, OUT, IN, SocketAddr>,
    _c: PhantomData<C>,
}

impl<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> Unpin for TcpClient<C, OUT, IN> {}

impl<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> TcpClient<C, OUT, IN> {
    /// Connect a TCP client to the given address
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        // Bind the TCP stream to the given address
        let stream = TcpStream::connect(addr).await?;
        let local_addr = stream.local_addr()?;

        // Setup a channel for incoming messages
        let (in_tx, in_rx) = tokio::sync::mpsc::unbounded_channel();

        // Setup a channel for outgoing messages
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();

        let addr = stream.peer_addr()?;
        let (stream_rx, stream_tx) = stream.into_split();

        // Setup internal tasks for handling the TCP stream
        let handle = Handle::new(addr, stream_rx, stream_tx, out_rx, in_tx).await?;

        // Create a new TcpClient from the connected stream
        Ok(Self {
            local_addr,
            handle,
            out_tx,
            in_rx,
            _c: PhantomData,
        })
    }

    /// Fetch the local address of the TCP client connection
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Send an outgoing message
    // TODO: this should fail if the target client is not currently connected...
    pub async fn send(&mut self, msg: OUT) -> Result<(), Error> {
        self.out_tx.send(msg).map_err(|_e| Error::Send)?;
        Ok(())
    }

    /// Close the client connection and tear down internal tasks
    pub fn close(self) {
        // Send exit signal to internal handle
        let _ = self.handle.close();
    }
}

/// Poll for the next incoming message
impl<C: Codec<OUT, IN>, OUT: Send + Unpin + 'static, IN: Send + Unpin + 'static> Stream
    for TcpClient<C, OUT, IN>
{
    type Item = IN;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.as_mut().in_rx.poll_recv(cx) {
            Poll::Ready(Some((msg, _addr))) => Poll::Ready(Some(msg)),
            Poll::Ready(None) => Poll::Ready(None), // Channel closed
            Poll::Pending => Poll::Pending,
        }
    }
}

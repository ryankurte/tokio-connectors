//! A Unix client connector for arbitrary serialisable messages

use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tokio::{
    net::UnixStream,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use super::UnixSocketId;
use crate::{codecs::Codec, error::Error, handle::Handle};

/// A Unix client connector for sending and receiving arbitrary serialisable messages to/from a Unix server.
///
/// Uses the provided [Codec] for framing and (de)serialization of messages.
pub struct UnixClient<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> {
    local_path: PathBuf,
    out_tx: UnboundedSender<OUT>,
    in_rx: UnboundedReceiver<(IN, UnixSocketId)>,
    handle: Handle<C, OUT, IN, UnixSocketId>,
    _c: PhantomData<C>,
}

impl<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> Unpin for UnixClient<C, OUT, IN> {}

impl<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> UnixClient<C, OUT, IN> {
    /// Connect a Unix client to the given path
    pub async fn connect(path: &Path) -> Result<Self, Error> {
        // Bind the Unix stream to the given path
        let stream = UnixStream::connect(path).await?;

        // Setup a channel for incoming messages
        let (in_tx, in_rx) = tokio::sync::mpsc::unbounded_channel();

        // Setup a channel for outgoing messages
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();

        // Setup internal tasks for handling the Unix stream
        let (stream_rx, stream_tx) = stream.into_split();
        let handle = Handle::new(UnixSocketId(0), stream_rx, stream_tx, out_rx, in_tx).await?;

        // Create a new TcpClient from the connected stream
        Ok(Self {
            local_path: path.to_path_buf(),
            handle,
            out_tx,
            in_rx,
            _c: PhantomData,
        })
    }

    /// Fetch the local path of the Unix client connection
    pub fn local_path(&self) -> &Path {
        &self.local_path
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
    for UnixClient<C, OUT, IN>
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

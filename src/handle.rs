use std::marker::PhantomData;

use tokio::{
    select,
    sync::{
        broadcast::Sender as BroadcastSender,
        mpsc::{UnboundedReceiver, UnboundedSender},
    },
    task::JoinHandle,
};
use tracing::{debug, error, warn};

use crate::{codecs::Codec, error::Error};

/// Generic [Readable] trait over different (split) transport types
pub(crate) trait Readable: Send + Sync + 'static {
    // Wait for the underlying stream to be readable
    fn readable_internal(&mut self) -> impl Future<Output = Result<(), std::io::Error>> + Send;

    // Try to read data from the underlying stream into the provided buffer
    fn try_read_buf_internal(&mut self, buf: &mut Vec<u8>) -> Result<usize, std::io::Error>;
}

/// Generic [Writeable] trait over different (split) transport types
pub(crate) trait Writeable: Send + Sync + 'static {
    // Write the provided buffer to the underlying stream, ensuring all data is sent
    fn write_all_internal(
        &mut self,
        buf: &[u8],
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send;
}

/// A handle for an active connection, used for both the client and server implementations.
///
pub(crate) struct Handle<
    C: Codec<OUT, IN>,
    OUT: Send + 'static,
    IN: Send + 'static,
    ADDR: Clone + Sync + Send + 'static,
> {
    addr: ADDR,

    exit_tx: BroadcastSender<()>,

    _rx_handle: JoinHandle<()>,
    _tx_handle: JoinHandle<()>,

    _c: PhantomData<C>,
    _out: PhantomData<OUT>,
    _in: PhantomData<IN>,
}

impl<
    C: Codec<OUT, IN>,
    OUT: Send + 'static,
    IN: Send + 'static,
    ADDR: Clone + Sync + Send + 'static,
> Handle<C, OUT, IN, ADDR>
{
    /// Create tasks to read from and write to an existing TCP stream and channels
    pub(crate) async fn new(
        addr: ADDR,
        mut reader: impl Readable + Unpin + Send + 'static,
        mut writer: impl Writeable + Unpin + Send + 'static,
        mut out_rx: UnboundedReceiver<OUT>,
        in_tx: UnboundedSender<(IN, ADDR)>,
    ) -> Result<Self, Error> {
        // Setup the exit channel
        let (exit_tx, _exit_rx) = tokio::sync::broadcast::channel::<()>(1);

        // Setup a task to handle reading from the stream
        let rx_exit_tx = exit_tx.clone();
        let addr_ = addr.clone();
        let _rx_handle = tokio::task::spawn(async move {
            let mut accumulator = Vec::new();
            let mut exit_rx = rx_exit_tx.subscribe();

            loop {
                select! {
                    // Poll for incoming data from the stream and accumulate it into the buffer
                    r = reader.readable_internal() => match r {
                        Ok(_) => {
                            // Read new data from the stream into a temporary buffer
                            let mut buff = Vec::with_capacity(1024);
                            let n = match reader.try_read_buf_internal(&mut buff) {
                                Ok(n) => n,
                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    // No more data to read
                                    continue;
                                }
                                Err(e) => {
                                    error!("Failed to read from stream: {:?}", e);
                                    rx_exit_tx.send(()).ok();
                                    break;
                                }
                            };

                            // Append the new data to the accumulator buffer for decoding
                            accumulator.extend_from_slice(&buff[..n]);

                            // Try to parse complete messages from the codec
                            'decode: loop {
                                 match C::try_decode(&mut accumulator) {
                                    Ok(Some(cmd)) => {
                                        // Successfully parsed a complete message, forward it to the server
                                        _ = in_tx.send((cmd, addr_.clone()));
                                    }
                                    Ok(None) => {
                                        // Not enough data yet, wait for more
                                        break 'decode;
                                    }
                                    Err(e) => {
                                        error!("Failed to decode message: {:?}", e);
                                        rx_exit_tx.send(()).ok();
                                        break 'decode;
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            error!("Failed to read from stream: {:?}", e);
                            rx_exit_tx.send(()).ok();
                            break;
                        }
                    },
                    // Handle exit signal
                    _ = exit_rx.recv() => {
                        debug!("TCP client reader exiting");
                        break;
                    }
                }
            }

            drop(reader);
        });

        // Setup a task to handle writing events to the stream
        let tx_exit_tx = exit_tx.clone();
        let _tx_handle = tokio::task::spawn(async move {
            let mut exit_rx = tx_exit_tx.subscribe();
            loop {
                select! {
                    e = out_rx.recv() => match e {
                        Some(event) => {
                            // Serialize the event with the codec
                            let data = match C::encode(&event) {
                                Ok(data) => data,
                                Err(e) => {
                                    error!("Failed to serialize event: {:?}", e);
                                    continue;
                                }
                            };
                            // Write the event data to the stream
                            if let Err(e) = writer.write_all_internal(&data).await {
                                error!("Failed to write to stream: {:?}", e);
                                tx_exit_tx.send(()).ok();
                                break;
                            }
                        },
                        None => {
                            warn!("Failed to receive event, channel closed");
                            tx_exit_tx.send(()).ok();
                            break;
                        }
                    },
                    _ = exit_rx.recv() => {
                        debug!("TCP client writer exiting");
                        break;
                    }
                }
            }

            drop(writer);
        });

        Ok(Self {
            addr,
            _rx_handle,
            _tx_handle,
            exit_tx,
            _c: PhantomData,
            _in: PhantomData,
            _out: PhantomData,
        })
    }

    /// Fetch the target address of the TCP connection
    pub fn addr(&self) -> ADDR {
        self.addr.clone()
    }

    /// Register a callback to be called when the connection is closed
    pub fn on_closed<F: FnOnce() + Send + 'static>(&self, callback: F) {
        let mut exit_rx = self.exit_tx.subscribe();

        tokio::task::spawn(async move {
            debug!("Registering on_closed callback");
            let _ = exit_rx.recv().await;
            callback();
        });
    }

    /// Exit the internal tasks and close the TCP connection
    pub fn close(self) -> Result<(), Error> {
        let _ = self.exit_tx.send(());
        Ok(())
    }
}

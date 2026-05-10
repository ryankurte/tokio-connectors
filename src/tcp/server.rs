//! A TCP listener server for arbitrary serialisable messages, using postcard + COBS for framing and serialization.

use std::{
    collections::HashMap,
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    select,
    sync::{
        broadcast::{self, Sender as BroadcastSender},
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    task::JoinHandle,
};
use tracing::{debug, warn};

use crate::{codecs::Codec, error::Error, handle::Handle};

/// A TCP server implementation for managing multiple client connections and routing messages between them.
///
/// Uses the provided [Codec] for framing and (de)serialization of messages.
pub struct TcpServer<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> {
    local_addr: SocketAddr,
    out_tx: UnboundedSender<(OUT, SocketAddr)>,
    in_rx: UnboundedReceiver<(IN, SocketAddr)>,
    exit_tx: BroadcastSender<()>,

    _listener_handle: JoinHandle<()>,
    _router_handle: JoinHandle<()>,
    _c: PhantomData<C>,
}

impl<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> Unpin for TcpServer<C, OUT, IN> {}

enum TcpConnection<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> {
    Connected {
        out_tx: UnboundedSender<OUT>,
        handle: Handle<C, OUT, IN, SocketAddr>,
    },
    Disconnected {
        addr: SocketAddr,
    },
}

impl<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> TcpServer<C, OUT, IN> {
    /// Bind a new TCP server to the given address and start accepting connections
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        debug!("TCP server listening on {}", local_addr);

        // Setup a channel to allow graceful shutdown of the listener and router tasks
        let (exit_tx, _) = broadcast::channel(1);

        // Create a channel for receiving incoming messages
        let (in_tx, in_rx) = unbounded_channel();

        // Create a channel for sending outgoing messages
        let (out_tx, mut out_rx) = unbounded_channel();

        // Create a channel for forwarding connection events from the listener to the router
        let (conn_tx, mut conn_rx) = unbounded_channel();

        // Spawn a task to accept incoming connections
        let exit_tx_ = exit_tx.clone();
        let _listener_handle = tokio::task::spawn(async move {
            let mut exit_rx = exit_tx_.subscribe();
            loop {
                select! {
                    // Handle incoming connections
                    // TODO: exit loop on .accept() error?
                    Ok((socket, addr)) = listener.accept() => {
                        match Self::handle_connection(socket, addr.clone(), in_tx.clone()).await {
                            Ok((out_tx, handle)) => {
                                // Bind a callback to handle connection closed events
                                let conn_tx_ = conn_tx.clone();
                                let addr_ = addr.clone();
                                handle.on_closed(move || {
                                    conn_tx_.send(TcpConnection::Disconnected { addr: addr_ }).ok();
                                });

                                // Forward the new connection to the router
                                conn_tx.send(TcpConnection::Connected { out_tx, handle }).unwrap_or_else(|e| {
                                    warn!("Failed to forward connection from {addr} to router {e:?}");
                                });
                            }
                            Err(e) => {
                                warn!("Failed to handle connection from {addr}: {e:?}");
                            }
                        }
                    }
                    // Handle shutdown signal
                    _ = exit_rx.recv() => {
                        break;
                    }
                }
            }

            debug!("Shutting down TCP server listener");
        });

        // Spawn a task to route outgoing messages to the appropriate clients
        let mut exit_rx = exit_tx.subscribe();
        let _router_handle = tokio::task::spawn(async move {
            let mut clients: HashMap<
                SocketAddr,
                (UnboundedSender<OUT>, Handle<C, OUT, IN, SocketAddr>),
            > = HashMap::new();

            loop {
                select! {
                    // Route outgoing messages to the appropriate clients
                    Some((msg, target)) = out_rx.recv() => {
                        // Find a matching client for the target address
                        if let Some((out_tx, _handle)) = clients.get(&target) {
                            if let Err(e) = out_tx.send(msg) {
                                warn!("Failed to send message to {target}: {e:?}");

                                // Remove and shutdown the client if the channel is closed
                                if let Some((_out_rx, handle)) = clients.remove(&target) {
                                    let _ = handle.close();
                                }
                            }
                        } else {
                            warn!("No client found for target {target}");
                        }
                    },
                    // Handle new connections
                    // TODO: we should probably propagate disconnect events from the client back
                    // out to the router so we can proactively remove them?
                    Some(evt) = conn_rx.recv() => match evt {
                        TcpConnection::Connected { out_tx, handle } => {
                            debug!("Client connected: {}", handle.addr());
                            clients.insert(handle.addr(), (out_tx, handle));
                        }
                        TcpConnection::Disconnected { addr } => {
                            debug!("Client disconnected: {addr}");
                            if let Some((_out_tx, handle)) = clients.remove(&addr) {
                                let _ = handle.close();
                            }
                        }
                    },
                    // Handle shutdown signal
                    _ = exit_rx.recv() => {
                        break;
                    }
                }
            }

            debug!("Shutting down TCP server router");

            for (_addr, (_out_tx, handle)) in clients.drain() {
                let _ = handle.close();
            }
        });

        Ok(Self {
            local_addr,
            out_tx,
            in_rx,
            exit_tx,
            _listener_handle,
            _router_handle,
            _c: PhantomData,
        })
    }

    /// Fetch the local address of the TCP server listener
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Send a message to a specific client
    // TODO: this should fail if the target client is not currently connected...
    pub async fn send(&mut self, msg: OUT, target: SocketAddr) -> Result<(), Error> {
        self.out_tx.send((msg, target)).map_err(|_e| Error::Send)?;
        Ok(())
    }

    /// Shutdown the TCP server and all active connections
    pub async fn shutdown(&self) {
        let _ = self.exit_tx.send(());
    }

    async fn handle_connection(
        socket: TcpStream,
        addr: SocketAddr,
        in_rx: UnboundedSender<(IN, SocketAddr)>,
    ) -> Result<(UnboundedSender<OUT>, Handle<C, OUT, IN, SocketAddr>), Error> {
        debug!("New connection from {}", addr);

        // Setup a TCP handler for the new connection
        let (out_tx, out_rx) = unbounded_channel();
        let (sock_rx, sock_tx) = socket.into_split();
        let handle = Handle::new(addr, sock_rx, sock_tx, out_rx, in_rx).await?;

        Ok((out_tx, handle))
    }
}

impl<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> Stream for TcpServer<C, OUT, IN> {
    type Item = (IN, SocketAddr);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().in_rx.poll_recv(cx)
    }
}

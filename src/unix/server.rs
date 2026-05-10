use std::{
    collections::HashMap,
    marker::PhantomData,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tokio::{
    net::{UnixListener, UnixStream},
    select,
    sync::{
        broadcast::Sender as BroadcastSender,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    task::JoinHandle,
};
use tracing::{debug, warn};

use crate::{codecs::Codec, error::Error, handle::Handle, unix::UnixSocketId};

pub struct UnixServer<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> {
    local_addr: PathBuf,
    out_tx: UnboundedSender<(OUT, UnixSocketId)>,
    in_rx: UnboundedReceiver<(IN, UnixSocketId)>,
    exit_tx: BroadcastSender<()>,

    _listener_handle: JoinHandle<()>,
    _router_handle: JoinHandle<()>,
    _c: PhantomData<C>,
}

enum UnixConnection<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> {
    Connected {
        out_tx: UnboundedSender<OUT>,
        handle: Handle<C, OUT, IN, UnixSocketId>,
    },
    Disconnected {
        id: UnixSocketId,
    },
}

impl<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> Unpin for UnixServer<C, OUT, IN> {}

impl<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> UnixServer<C, OUT, IN> {
    /// Create a new unix socket server with the provided path
    pub async fn bind(path: &Path) -> Result<Self, Error> {
        // Pre-clear socket file
        std::fs::remove_file(&path).ok();

        // Connect socket listener
        let listener = UnixListener::bind(&path)?;

        debug!("Unix server listening on {}", path.display());

        // Setup a channel to allow graceful shutdown of the listener and router tasks
        let (exit_tx, _) = tokio::sync::broadcast::channel::<()>(1);

        // Create a channel for receiving incoming messages
        let (in_tx, in_rx) = unbounded_channel();

        // Create a channel for sending outgoing messages
        let (out_tx, mut out_rx) = unbounded_channel();

        // Create a channel for forwarding connection events from the listener to the router
        let (conn_tx, mut conn_rx) = unbounded_channel();

        let mut socket_id_count = 0u64;

        // Spawn a task to accept incoming connections
        let exit_tx_ = exit_tx.clone();
        let _listener_handle = tokio::task::spawn(async move {
            let mut exit_rx = exit_tx_.subscribe();
            loop {
                select! {
                    // Handle incoming connections
                    // TODO: exit loop on .accept() error?
                    Ok((socket, _addr)) = listener.accept() => {
                        let socket_id = UnixSocketId(socket_id_count);
                        socket_id_count += 1;

                        match Self::handle_connection(socket_id, socket, in_tx.clone()).await {
                            Ok((out_tx, handle)) => {
                                // Bind a callback to handle connection closed events
                                let conn_tx_ = conn_tx.clone();
                                handle.on_closed(move || {
                                    conn_tx_.send(UnixConnection::Disconnected { id: socket_id }).ok();
                                });

                                // Forward the new connection to the router
                                conn_tx.send(UnixConnection::Connected { out_tx, handle }).unwrap_or_else(|e| {
                                    warn!("Failed to forward connection from {} to router {e:?}", socket_id);
                                });
                            }
                            Err(e) => {
                                warn!("Failed to handle connection from {}: {e:?}", socket_id);
                            }
                        }
                    }
                    // Handle shutdown signal
                    _ = exit_rx.recv() => {
                        break;
                    }
                }
            }

            debug!("Shutting down Unix server listener");
        });

        // Spawn a task to route outgoing messages to the appropriate clients
        let mut exit_rx = exit_tx.subscribe();
        let _router_handle = tokio::task::spawn(async move {
            let mut clients: HashMap<
                UnixSocketId,
                (UnboundedSender<OUT>, Handle<C, OUT, IN, UnixSocketId>),
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
                        UnixConnection::Connected { out_tx, handle } => {
                            debug!("Client connected: {}", handle.addr());
                            clients.insert(handle.addr(), (out_tx, handle));
                        }
                        UnixConnection::Disconnected { id } => {
                            debug!("Client disconnected: {}", id);
                            if let Some((_out_tx, handle)) = clients.remove(&id) {
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

            debug!("Shutting down Unix server router");

            for (_addr, (_out_tx, handle)) in clients.drain() {
                let _ = handle.close();
            }
        });

        Ok(Self {
            local_addr: path.to_path_buf(),
            out_tx,
            in_rx,
            exit_tx,
            _listener_handle,
            _router_handle,
            _c: PhantomData,
        })
    }

    /// Fetch the local path of the unix socket
    pub fn local_path(&self) -> &Path {
        &self.local_addr
    }

    /// Send a message to a specific client
    // TODO: this should fail if the target client is not currently connected...
    pub async fn send(&mut self, msg: OUT, target: UnixSocketId) -> Result<(), Error> {
        self.out_tx.send((msg, target)).map_err(|_e| Error::Send)?;
        Ok(())
    }

    /// Shutdown the Unix server and all active connections
    pub async fn shutdown(&self) {
        let _ = self.exit_tx.send(());
    }

    async fn handle_connection(
        id: UnixSocketId,
        socket: UnixStream,
        in_rx: UnboundedSender<(IN, UnixSocketId)>,
    ) -> Result<(UnboundedSender<OUT>, Handle<C, OUT, IN, UnixSocketId>), Error> {
        debug!("New connection from {}", id);

        // Setup a Unix handler for the new connection
        let (out_tx, out_rx) = unbounded_channel();
        let (stream_rx, stream_tx) = socket.into_split();
        let handle = Handle::new(id, stream_rx, stream_tx, out_rx, in_rx).await?;

        Ok((out_tx, handle))
    }
}

impl<C: Codec<OUT, IN>, OUT: Send + 'static, IN: Send + 'static> Stream for UnixServer<C, OUT, IN> {
    type Item = (IN, UnixSocketId);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().in_rx.poll_recv(cx)
    }
}

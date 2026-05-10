use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tempdir::TempDir;
use tokio::time::{Duration, timeout};
use tracing::{info, level_filters::LevelFilter};

use super::*;
use crate::{
    codecs::{Codec, Json, PostcardCobs},
    helpers::setup_fmt_subscriber,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct MockRequest {
    request: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct MockStatus {
    status: String,
}

async fn unix_server_client<
    CC: Codec<MockStatus, MockRequest>,
    RC: Codec<MockRequest, MockStatus>,
>() {
    setup_fmt_subscriber(LevelFilter::TRACE);

    // Generate a temporary file path for the Unix socket
    let socket_path = TempDir::new("unix-test")
        .expect("Failed to create temp dir")
        .into_path()
        .join("test.sock");

    // Setup the server
    let mut server = UnixServer::<RC, MockRequest, MockStatus>::bind(&socket_path)
        .await
        .expect("Failed to bind Unix server");

    // Setup the client
    let mut client = UnixClient::<CC, MockStatus, MockRequest>::connect(&socket_path)
        .await
        .expect("Failed to connect Unix client");

    // Send a message from the client to the server
    let msg = MockStatus {
        status: "Hello from client".to_string(),
    };
    client
        .send(msg.clone())
        .await
        .expect("Failed to send message from client");

    // Receive a message from the server to the client
    let recv = timeout(Duration::from_secs(3), server.next())
        .await
        .expect("Timed out waiting for message from server")
        .expect("Failed to receive message from server");
    assert_eq!(recv.0, msg, "Received message mismatch");

    // Copy the client id from the server receive, since the client doesn't know its own id
    let client_id = recv.1;

    // Send a message from the server to the client
    let server_msg = MockRequest {
        request: "Hello from server".to_string(),
    };
    server
        .send(server_msg.clone(), client_id)
        .await
        .expect("Failed to send message from server");

    // Receive a message from the client to the server
    let recv = timeout(Duration::from_secs(3), client.next())
        .await
        .expect("Timed out waiting for message from client")
        .expect("Failed to receive message from client");
    assert_eq!(recv, server_msg, "Received message mismatch");

    // Shudown the client
    info!("Closing Unix client connection");
    client.close();

    // TODO: check that the server detects the client disconnection

    // Shutdown the server
    info!("Shutting down Unix server");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unix_server_client_json() {
    unix_server_client::<Json, Json>().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unix_server_client_postcard() {
    unix_server_client::<PostcardCobs, PostcardCobs>().await;
}

use futures::StreamExt;
use serde::{Deserialize, Serialize};
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

async fn tcp_server_client<
    CC: Codec<MockStatus, MockRequest>,
    RC: Codec<MockRequest, MockStatus>,
>() {
    setup_fmt_subscriber(LevelFilter::TRACE);

    // Setup the server
    let mut server = TcpServer::<RC, MockRequest, MockStatus>::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind TCP server");
    let server_addr = server.local_addr();

    // Setup the client
    let mut client = TcpClient::<CC, MockStatus, MockRequest>::connect(server_addr)
        .await
        .expect("Failed to connect TCP client");
    let client_addr = client.local_addr();

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
    assert_eq!(recv.1, client_addr, "Received message target mismatch");

    // Send a message from the server to the client
    let server_msg = MockRequest {
        request: "Hello from server".to_string(),
    };
    server
        .send(server_msg.clone(), client_addr)
        .await
        .expect("Failed to send message from server");

    // Receive a message from the client to the server
    let recv = timeout(Duration::from_secs(3), client.next())
        .await
        .expect("Timed out waiting for message from client")
        .expect("Failed to receive message from client");
    assert_eq!(recv, server_msg, "Received message mismatch");

    // Shudown the client
    info!("Closing TCP client connection");
    client.close();

    // TODO: check that the server detects the client disconnection

    // Shutdown the server
    info!("Shutting down TCP server");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_server_client_json() {
    tcp_server_client::<Json, Json>().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_server_client_postcard() {
    tcp_server_client::<PostcardCobs, PostcardCobs>().await;
}

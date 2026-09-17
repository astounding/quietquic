// SPDX-License-Identifier: 0BSD
//! Cancellation and stream-ownership contracts over the shared endpoint driver.

mod common;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use quietquic::config::{ClientConfigFile, ClientEntry, ServerSecrets};
use quietquic::{Endpoint, EndpointConfig};
use tokio::time::{sleep, timeout};

const STEP: Duration = Duration::from_secs(10);
const PSK: &str = "00000000000000000000000000000000000000000000000000000000000000ce";

fn credential() -> ClientEntry {
    let config: ServerSecrets = toml::from_str(&format!(
        "listen='127.0.0.1:1'\n[[clients]]\nclient_id='cancel'\npsk='{PSK}'\n"
    ))
    .unwrap();
    config.clients.into_iter().next().unwrap()
}

fn client(server: SocketAddr) -> ClientConfigFile {
    toml::from_str(&format!(
        "client_id='cancel'\npsk='{PSK}'\nserver='{server}'\n"
    ))
    .unwrap()
}

#[tokio::test]
async fn canceled_read_to_end_preserves_consumed_prefix() {
    let server = Endpoint::bind(
        common::bind_addr(),
        EndpointConfig::accept(vec![credential()]),
    )
    .await
    .unwrap();
    let client_endpoint = Endpoint::bind(common::sender_bind_addr(), EndpointConfig::dial())
        .await
        .unwrap();
    let (client_conn, server_conn) = tokio::join!(
        async {
            timeout(STEP, client_endpoint.connect(client(server.local_addr())))
                .await
                .unwrap()
                .unwrap()
        },
        async {
            timeout(STEP, server.accept())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        },
    );

    let (mut send, _) = timeout(STEP, client_conn.open_bi()).await.unwrap().unwrap();
    send.write_all(b"prefix-").await.unwrap();
    let (_, mut recv) = timeout(STEP, server_conn.accept_bi())
        .await
        .unwrap()
        .unwrap();
    let canceled = timeout(Duration::from_millis(100), recv.read_to_end(64)).await;
    assert!(
        canceled.is_err(),
        "collector should still be waiting for FIN"
    );

    // Give the driver a service pass in which it observes the dropped waiter
    // and moves the already consumed prefix back to stream-owned storage.
    sleep(Duration::from_millis(20)).await;
    send.write_all(b"suffix").await.unwrap();
    send.finish().await.unwrap();

    let bytes = timeout(STEP, recv.read_to_end(64)).await.unwrap().unwrap();
    assert_eq!(bytes, b"prefix-suffix");
}

#[tokio::test]
async fn completed_fin_before_caller_poll_is_reclaimed() {
    let server = Endpoint::bind(
        common::bind_addr(),
        EndpointConfig::accept(vec![credential()]),
    )
    .await
    .unwrap();
    let client_endpoint = Endpoint::bind(common::sender_bind_addr(), EndpointConfig::dial())
        .await
        .unwrap();
    let (client_conn, server_conn) = tokio::join!(
        async {
            timeout(STEP, client_endpoint.connect(client(server.local_addr())))
                .await
                .unwrap()
                .unwrap()
        },
        async {
            timeout(STEP, server.accept())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        },
    );
    let (mut send, _) = client_conn.open_bi().await.unwrap();
    send.write_all(b"complete-before-poll").await.unwrap();
    let (_, mut recv) = server_conn.accept_bi().await.unwrap();

    let mut collector = Box::pin(recv.read_to_end(128));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        Pin::new(&mut collector).poll(&mut cx),
        Poll::Pending
    ));
    send.finish().await.unwrap();
    sleep(Duration::from_millis(100)).await;
    drop(collector);

    let bytes = timeout(STEP, recv.read_to_end(128)).await.unwrap().unwrap();
    assert_eq!(bytes, b"complete-before-poll");
}

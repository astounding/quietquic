// SPDX-License-Identifier: 0BSD
//! Real-UDP stream drop and reset lifecycle contracts.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use quietquic::config::{ClientConfigFile, ClientEntry, ServerSecrets};
use quietquic::conn::{
    ConnError, ResetOutcome, AUTO_CODE_START, AUTO_RESET_DROPPED_SEND, AUTO_STOP_DROPPED_RECV,
};
use quietquic::{Endpoint, EndpointConfig};
use tokio::time::timeout;

const STEP: Duration = Duration::from_secs(10);
const PSK: &str = "00000000000000000000000000000000000000000000000000000000000000d4";

fn credential() -> ClientEntry {
    let config: ServerSecrets = toml::from_str(&format!(
        "listen='127.0.0.1:1'\n[[clients]]\nclient_id='drop'\npsk='{PSK}'\n"
    ))
    .unwrap();
    config.clients.into_iter().next().unwrap()
}

fn client(server: SocketAddr) -> ClientConfigFile {
    toml::from_str(&format!(
        "client_id='drop'\npsk='{PSK}'\nserver='{server}'\n"
    ))
    .unwrap()
}

async fn pair() -> (
    Endpoint,
    Endpoint,
    quietquic::conn::Connection,
    quietquic::conn::Connection,
) {
    let server = Endpoint::bind(
        common::bind_addr(),
        EndpointConfig::accept(vec![credential()]),
    )
    .await
    .unwrap();
    let dialer = Endpoint::bind(common::sender_bind_addr(), EndpointConfig::dial())
        .await
        .unwrap();
    let (outgoing, incoming) = tokio::join!(
        timeout(STEP, dialer.connect(client(server.local_addr()))),
        timeout(STEP, server.accept()),
    );
    (
        dialer,
        server,
        outgoing.unwrap().unwrap(),
        incoming.unwrap().unwrap().unwrap(),
    )
}

#[tokio::test]
async fn unfinished_send_drop_resets_peer_and_opposite_direction_survives() {
    let (_dialer, _server, client_conn, server_conn) = pair().await;
    let (mut client_send, mut client_recv) = client_conn.open_bi().await.unwrap();
    client_send.write_all(b"prefix before reset").await.unwrap();
    let (mut server_send, mut server_recv) = timeout(STEP, server_conn.accept_bi())
        .await
        .unwrap()
        .unwrap();

    drop(client_send);

    let observe_reset = async {
        let error = server_recv
            .read_to_end(1024)
            .await
            .expect_err("unfinished send drop must reset the peer receive half");
        assert_eq!(error.prefix, b"prefix before reset");
        assert_eq!(
            error.error,
            ConnError::Reset {
                code: AUTO_RESET_DROPPED_SEND
            }
        );
        server_recv
    };
    let send_reverse = async {
        server_send.write_all(b"reverse still works").await.unwrap();
        server_send.finish_and_wait().await.unwrap();
        server_send
    };
    let read_reverse = async {
        let bytes = client_recv.read_to_end(1024).await.unwrap();
        (client_recv, bytes)
    };
    let (reset, reverse_send, reverse_recv) = tokio::join!(
        timeout(STEP, observe_reset),
        timeout(STEP, send_reverse),
        timeout(STEP, read_reverse),
    );
    let _server_recv = reset.expect("peer reset observation timed out");
    let _server_send = reverse_send.expect("reverse send timed out");
    let (_client_recv, bytes) = reverse_recv.expect("reverse read timed out");
    assert_eq!(bytes, b"reverse still works");
}

#[tokio::test]
async fn unfinished_recv_drop_stops_peer_and_opposite_direction_survives() {
    let (_dialer, _server, client_conn, server_conn) = pair().await;
    let (mut client_send, mut client_recv) = client_conn.open_bi().await.unwrap();
    client_send.write_all(b"make stream visible").await.unwrap();
    let (mut server_send, server_recv) = timeout(STEP, server_conn.accept_bi())
        .await
        .unwrap()
        .unwrap();

    drop(server_recv);

    let observe_stop = async {
        match client_send.finish().await {
            Err(ConnError::Stopped { code }) => assert_eq!(code, AUTO_STOP_DROPPED_RECV),
            Ok(()) => {
                let error = client_send
                    .wait_finished()
                    .await
                    .expect_err("dropped receive half must stop the peer sender");
                assert_eq!(
                    error,
                    ConnError::Stopped {
                        code: AUTO_STOP_DROPPED_RECV
                    }
                );
            }
            Err(error) => panic!("unexpected send terminal error: {error:?}"),
        }
        client_send
    };
    let send_reverse = async {
        server_send.write_all(b"reverse after stop").await.unwrap();
        server_send.finish_and_wait().await.unwrap();
        server_send
    };
    let read_reverse = async {
        let bytes = client_recv.read_to_end(1024).await.unwrap();
        (client_recv, bytes)
    };
    let (stopped, reverse_send, reverse_recv) = tokio::join!(
        timeout(STEP, observe_stop),
        timeout(STEP, send_reverse),
        timeout(STEP, read_reverse),
    );
    let _client_send = stopped.expect("peer stop observation timed out");
    let _server_send = reverse_send.expect("reverse send timed out");
    let (_client_recv, bytes) = reverse_recv.expect("reverse read timed out");
    assert_eq!(bytes, b"reverse after stop");
}

#[tokio::test]
async fn dropping_successfully_finished_send_preserves_fin() {
    let (_dialer, _server, client_conn, server_conn) = pair().await;
    let (mut client_send, mut client_recv) = client_conn.open_bi().await.unwrap();
    client_send.write_all(b"finished payload").await.unwrap();
    client_send.finish().await.unwrap();
    drop(client_send);

    let receive_finished = async {
        let (mut server_send, mut server_recv) = server_conn.accept_bi().await.unwrap();
        let bytes = server_recv.read_to_end(1024).await.unwrap();
        server_send.write_all(b"receipt").await.unwrap();
        server_send.finish_and_wait().await.unwrap();
        (server_send, server_recv, bytes)
    };
    let receive_receipt = async {
        let bytes = client_recv.read_to_end(1024).await.unwrap();
        (client_recv, bytes)
    };
    let (server_result, client_result) = tokio::join!(
        timeout(STEP, receive_finished),
        timeout(STEP, receive_receipt),
    );
    let (_server_send, _server_recv, payload) =
        server_result.expect("peer did not receive preserved FIN");
    let (_client_recv, receipt) = client_result.expect("receipt read timed out");
    assert_eq!(payload, b"finished payload");
    assert_eq!(receipt, b"receipt");
}

#[tokio::test]
async fn explicit_reset_is_idempotent_and_reserved_codes_are_rejected() {
    let (_dialer, _server, client_conn, server_conn) = pair().await;
    let (mut send, _client_recv) = client_conn.open_bi().await.unwrap();
    send.write_all(b"reset me").await.unwrap();
    let (_server_send, mut server_recv) = timeout(STEP, server_conn.accept_bi())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(send.reset(17).await.unwrap(), ResetOutcome::ResetRequested);
    assert_eq!(
        send.reset(29).await.unwrap(),
        ResetOutcome::AlreadyReset { code: 17 }
    );
    let peer_error = server_recv.read_to_end(1024).await.unwrap_err();
    assert_eq!(peer_error.error, ConnError::Reset { code: 17 });

    let (mut reserved, _recv) = client_conn.open_bi().await.unwrap();
    assert_eq!(
        reserved.reset(AUTO_CODE_START).await.unwrap_err(),
        ConnError::InvalidErrorCode {
            code: AUTO_CODE_START
        }
    );
}

#[tokio::test]
async fn acknowledged_fin_remains_observable_after_connection_cleanup() {
    let (_dialer, _server, client_conn, server_conn) = pair().await;
    let (mut send, client_recv) = client_conn.open_bi().await.unwrap();
    send.write_all(b"durable FIN fact").await.unwrap();

    let peer = async {
        let (server_send, mut recv) = server_conn.accept_bi().await.unwrap();
        let bytes = recv.read_to_end(1024).await.unwrap();
        (server_send, recv, bytes)
    };
    let (finished, received) =
        tokio::join!(timeout(STEP, send.finish_and_wait()), timeout(STEP, peer),);
    finished
        .expect("initial FIN wait timed out")
        .expect("initial FIN wait failed");
    let (server_send, server_recv, bytes) = received.expect("peer read timed out");
    assert_eq!(bytes, b"durable FIN fact");

    client_conn.close(0, b"finished").await.unwrap();
    timeout(STEP, client_conn.closed())
        .await
        .expect("connection cleanup timed out");

    timeout(STEP, send.wait_finished())
        .await
        .expect("late FIN observation timed out")
        .expect("late FIN observation lost acknowledgement");
    assert_eq!(
        timeout(STEP, send.reset(23))
            .await
            .expect("late reset observation timed out")
            .expect("late reset observation failed"),
        ResetOutcome::AlreadyAcknowledged
    );
    drop((send, client_recv, server_send, server_recv));
}

// SPDX-License-Identifier: 0BSD

use std::time::Duration;

use quietquic::client::Client;
use quietquic::config::{ClientConfigFile, ServerSecrets};
use quietquic::conn::{ConnError, Connection, ConnectionError, AUTO_CODE_START};
use quietquic::server::Server;

mod common;

struct Connected {
    client: Connection,
    server: Connection,
    _server: Server,
}

fn server_secrets(suffix: u8) -> (ServerSecrets, String) {
    let psk_hex = format!("{suffix:064x}");
    let secrets: ServerSecrets = toml::from_str(&format!(
        "listen = \"{}\"\n[[clients]]\nclient_id=\"a\"\npsk=\"{psk_hex}\"\n",
        common::bind_addr_string()
    ))
    .expect("server secrets");
    (secrets, psk_hex)
}

async fn connected_pair(suffix: u8) -> Connected {
    let (secrets, psk_hex) = server_secrets(suffix);
    let server = Server::bind(secrets).await.expect("bind server");
    let addr = server.local_addr();
    let cfg: ClientConfigFile = toml::from_str(&format!(
        "client_id=\"a\"\npsk=\"{psk_hex}\"\nserver=\"{addr}\"\n"
    ))
    .expect("client config");

    let (server_conn, client) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(server.accept(), Client::connect(cfg))
    })
    .await
    .expect("handshake timeout");
    let server_conn = server_conn.expect("accept");
    let client = client.expect("connect");
    Connected {
        client,
        server: server_conn,
        _server: server,
    }
}

#[tokio::test]
async fn finish_and_wait_completes_after_the_peer_receives_the_stream() {
    let pair = connected_pair(0x31).await;
    let client = pair.client;
    let server = pair.server;
    let _server_driver = pair._server;

    let server_task = tokio::spawn(async move {
        let (_send, mut recv) = server.accept_bi().await.expect("accept stream");
        recv.read_to_end(1024).await.expect("read request")
    });

    let (mut send, _recv) = client.open_bi().await.expect("open stream");
    send.write_all(b"request").await.expect("write request");
    send.finish_and_wait().await.expect("finish acked");

    let got = tokio::time::timeout(Duration::from_secs(10), server_task)
        .await
        .expect("server read timeout")
        .expect("server task");
    assert_eq!(got, b"request");
}

#[tokio::test]
async fn split_finish_then_later_wait_finished_uses_the_recorded_fact() {
    let pair = connected_pair(0x32).await;
    let client = pair.client;
    let server = pair.server;
    let _server_driver = pair._server;

    let server_task = tokio::spawn(async move {
        let (_send, mut recv) = server.accept_bi().await.expect("accept stream");
        recv.read_to_end(1024).await.expect("read request")
    });

    let (mut send, _recv) = client.open_bi().await.expect("open stream");
    send.write_all(b"request").await.expect("write request");
    send.finish().await.expect("finish");

    let got = tokio::time::timeout(Duration::from_secs(10), server_task)
        .await
        .expect("server read timeout")
        .expect("server task");
    assert_eq!(got, b"request");

    send.wait_finished()
        .await
        .expect("ack fact should be retained for a later waiter");
}

#[tokio::test]
async fn wait_finished_before_finish_errors_immediately() {
    let pair = connected_pair(0x33).await;
    let client = pair.client;
    let _server = pair.server;
    let _server_driver = pair._server;

    let (mut send, _recv) = client.open_bi().await.expect("open stream");
    let err = send
        .wait_finished()
        .await
        .expect_err("wait before finish must not park forever");
    assert_eq!(err, ConnError::ClosedStream);
}

#[tokio::test]
async fn peer_stop_is_the_terminal_wait_finished_result() {
    let pair = connected_pair(0x34).await;
    let client = pair.client;
    let server = pair.server;
    let _server_driver = pair._server;

    let server_task = tokio::spawn(async move {
        let (_send, mut recv) = server.accept_bi().await.expect("accept stream");
        recv.stop(77).await.expect("stop receive half");
        // Keep application owners alive until the peer observes STOP_SENDING.
        // Last-owner connection close is allowed to supersede pending frames.
        (server, _send, recv)
    });

    let (mut send, _recv) = client.open_bi().await.expect("open stream");
    send.write_all(b"stop-me").await.expect("write request");
    let _server_owners = tokio::time::timeout(Duration::from_secs(10), server_task)
        .await
        .expect("server stop timeout")
        .expect("server task");

    if let Err(err) = send.finish().await {
        assert_eq!(err, ConnError::Stopped { code: 77 });
        return;
    }
    let err = send
        .wait_finished()
        .await
        .expect_err("peer stop is terminal");
    assert_eq!(err, ConnError::Stopped { code: 77 });
}

#[tokio::test]
async fn close_rejects_out_of_range_application_error_codes() {
    let pair = connected_pair(0x35).await;
    let client = pair.client;
    let _server = pair.server;
    let _server_driver = pair._server;

    let err = client
        .close(1 << 62, b"too large")
        .await
        .expect_err("QUIC application codes are varints");
    assert_eq!(err, ConnError::InvalidErrorCode { code: 1 << 62 });

    let err = client
        .close(AUTO_CODE_START, b"reserved")
        .await
        .expect_err("automatic cleanup codes are reserved");
    assert_eq!(
        err,
        ConnError::InvalidErrorCode {
            code: AUTO_CODE_START
        }
    );
}

#[tokio::test]
async fn closed_reports_the_same_peer_application_close_to_every_clone() {
    let pair = connected_pair(0x36).await;
    let client = pair.client;
    let server = pair.server;
    let _server_driver = pair._server;
    let server_clone = server.clone();

    let before = tokio::spawn(async move { server.closed().await });
    client.close(123, b"done").await.expect("close");
    let first = tokio::time::timeout(Duration::from_secs(10), before)
        .await
        .expect("closed timeout")
        .expect("closed task");
    let second = tokio::time::timeout(Duration::from_secs(10), server_clone.closed())
        .await
        .expect("closed clone timeout");

    let expected = ConnectionError::ApplicationClosed {
        code: 123,
        reason: b"done".to_vec(),
    };
    assert_eq!(first, expected);
    assert_eq!(second, expected);
}

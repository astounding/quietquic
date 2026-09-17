// SPDX-License-Identifier: 0BSD
//! Regression guard for driver reaping of LOCALLY-initiated connection closes.
//!
//! quinn-proto 0.11's `Connection::close()` only arms the connection's close
//! timer; when that timer fires the connection moves to `Drained` WITHOUT ever
//! setting quinn-proto's internal `error` field. Consequently `Connection::poll()`
//! never yields `Event::ConnectionLost` for a *locally*-initiated close (only a
//! *remotely*-initiated close — receiving a CONNECTION_CLOSE frame, a transport
//! error, or a reset — sets that field).
//!
//! Before the fix, this meant a server-side `Connection::close()` left the
//! connection's driver-side state in `Driver::connections`/`Driver::surfaced`
//! forever, its `ConnectionHandle` never freed in our bookkeeping even though
//! quinn-proto's own endpoint slab had already reclaimed it (via
//! `EndpointEventInner::Drained`). Eventually quinn-proto reused a
//! `ConnectionHandle` that still had a stale `surfaced` entry, and the collision
//! wedged `Server::accept` forever — reproducibly around ~32 connect/accept/close
//! cycles against one long-lived `Server`.
//!
//! The fix reaps a connection once `state.conn.is_drained()` (the terminal state
//! reached by BOTH the remote path — already caught by `progress.lost` — and the
//! local-close path) in addition to `progress.lost`, in the core's servicing pass
//! (`quietquic_proto::endpoint::Endpoint`), which both the server (`src/server.rs`)
//! and client (`src/client.rs`) drivers now pump.
//!
//! This test drives one long-lived `Server` through enough server-side-close
//! cycles to exceed the historical ~32-cycle collision point, wrapping every step
//! in `tokio::time::timeout` so a reaping regression fails as a bounded panic
//! rather than hanging the whole test binary.

use quietquic::client::Client;
use quietquic::config::{ClientConfigFile, ServerSecrets};
use quietquic::server::Server;
use std::time::Duration;
use tokio::time::timeout;

mod common;

/// A single connect/stream/accept/close cycle must never hang, and this must hold
/// across enough cycles that quinn-proto reuses a `ConnectionHandle` freed by a
/// prior locally-initiated close. 48 > the historical ~32-cycle collision point.
const CYCLES: usize = 48;
const STEP: Duration = Duration::from_secs(10);
const PAYLOAD: &[u8] = b"lifecycle-ping";

#[tokio::test]
async fn server_side_close_is_reaped_across_many_cycles() {
    let psk_hex = "0000000000000000000000000000000000000000000000000000000000000009";

    let secrets: ServerSecrets = toml::from_str(&format!(
        "listen = \"{}\"\n[[clients]]\nclient_id=\"a\"\npsk=\"{psk_hex}\"\n",
        common::bind_addr_string()
    ))
    .unwrap();

    // ONE long-lived server, reused across every cycle — this is what exposed the
    // handle-reuse collision.
    let server = Server::bind(secrets).await.unwrap();
    let addr = server.local_addr();

    for cycle in 0..CYCLES {
        // Fresh client per cycle. The task connects, sends a stream, and returns
        // the connection so the main task can keep it alive until the exchange
        // completes, then close it too (exercising the client-side reaping path).
        let cfg: ClientConfigFile = toml::from_str(&format!(
            "client_id=\"a\"\npsk=\"{psk_hex}\"\nserver=\"{addr}\"\n"
        ))
        .unwrap();
        let client_task = tokio::spawn(async move {
            let conn = Client::connect(cfg).await.expect("client connect");
            let (mut send, _recv) = conn.open_bi().await.expect("client open_bi");
            send.write_all(PAYLOAD).await.expect("client write_all");
            send.finish().await.expect("client finish");
            conn
        });

        // The load-bearing assertion: accept must not hang on ANY cycle. Before
        // the `is_drained()` reaping fix, a reused handle collided with a stale
        // `surfaced` entry and this wedged forever around cycle ~32.
        let server_conn = match timeout(STEP, server.accept()).await {
            Ok(opt) => opt.expect("server.accept() returned None"),
            Err(_) => panic!(
                "Server::accept() hung on cycle {cycle} — locally-closed connections \
                 are not being reaped (is_drained() reaping regression)"
            ),
        };

        let (_server_send, mut server_recv) = timeout(STEP, server_conn.accept_bi())
            .await
            .unwrap_or_else(|_| panic!("accept_bi hung on cycle {cycle}"))
            .expect("server accept_bi");
        let got = timeout(STEP, server_recv.read_to_end(1024))
            .await
            .unwrap_or_else(|_| panic!("read_to_end hung on cycle {cycle}"))
            .expect("server read_to_end");
        assert_eq!(got, PAYLOAD, "server received the payload on cycle {cycle}");

        let client_conn = timeout(STEP, client_task)
            .await
            .unwrap_or_else(|_| panic!("client task hung on cycle {cycle}"))
            .expect("client task did not panic");

        // The previously-broken path: the SERVER side locally closes the
        // connection. Its handle must be reaped so the next cycle's accept works.
        timeout(STEP, server_conn.close(0, b""))
            .await
            .unwrap_or_else(|_| panic!("server close hung on cycle {cycle}"))
            .expect("server close");

        // Also close the client side locally, exercising the mirrored
        // `ClientDriver` reaping fix (a self-closed client driver must not wedge).
        timeout(STEP, client_conn.close(0, b""))
            .await
            .unwrap_or_else(|_| panic!("client close hung on cycle {cycle}"))
            .expect("client close");

        // Both connections drop here; the next iteration reuses the same server.
    }
}

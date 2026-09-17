// SPDX-License-Identifier: 0BSD
//! Non-blocking stream semantics of the sans-IO core, proved with **no sockets,
//! no runtime, and no threads**.
//!
//! The unit under test is [`quietquic_proto::conn::ConnState`] — the per-stream
//! plumbing that replaces `src/conn.rs`'s command-channel layer. The load-bearing
//! property is that *nothing here can park*: a read with no data buffered must
//! report [`ReadOutcome::Blocked`] and return, because the core has no way to
//! wait. (In the tokio wrapper that `Blocked` is what a task parks on; in a
//! hand-rolled `select()` loop it is what sends the caller back around.)
//!
//! # Why the pair here is plain QUIC, not cloaked QUIC
//!
//! `ConnState` is transport-agnostic: it wraps a `quinn_proto::Connection` and
//! knows nothing about selectors, PSKs, or the pre-filter. The cloaking layer is
//! covered by `core_silence.rs` against the real [`quietquic_proto::endpoint::Endpoint`].
//! Building the pair from stock `quinn_proto::Endpoint`s therefore exercises
//! exactly the code this test is about, and — importantly — does **not** require
//! `Endpoint::new_client`, which is a later task's deliverable. When the core
//! grows a client endpoint, this helper can be re-pointed at it without changing
//! a single assertion.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use quinn_proto::crypto::rustls::QuicClientConfig;
use quinn_proto::{
    ClientConfig as TransportClientConfig, ConnectionHandle, DatagramEvent, Endpoint,
    EndpointConfig, ServerConfig as TransportServerConfig, StreamId,
};

use quietquic_proto::conn::{ConnState, ResetOutcome};
use quietquic_proto::crypto::{reset_key, token_key, SelfSigned};
use quietquic_proto::outcome::{ReadOutcome, WriteOutcome};

/// The TLS name presented in the ClientHello. Verification is skipped (the PSK
/// authenticates in production; here nothing does), so any stable name works.
const SERVER_NAME: &str = "localhost";

/// Upper bound on datagram-shuttling passes. A loopback handshake settles in a
/// handful; this only exists so a regression fails fast instead of spinning.
const MAX_PASSES: usize = 256;

// ---------------------------------------------------------------------------
// In-memory endpoint pair
// ---------------------------------------------------------------------------

/// Two endpoints wired to each other through `Vec<u8>` queues instead of UDP.
struct Pair {
    client_ep: Endpoint,
    server_ep: Endpoint,
    client_addr: SocketAddr,
    server_addr: SocketAddr,
    client_h: ConnectionHandle,
    client: ConnState,
    server_h: Option<ConnectionHandle>,
    server: Option<ConnState>,
    to_server: VecDeque<Vec<u8>>,
    to_client: VecDeque<Vec<u8>>,
}

impl Pair {
    /// The server's `ConnState`, once the handshake has produced one.
    fn server(&mut self) -> &mut ConnState {
        self.server.as_mut().expect("server connection exists")
    }

    /// Shuttle datagrams between the two endpoints until neither has anything
    /// left to say.
    fn drive(&mut self) {
        for _ in 0..MAX_PASSES {
            let now = Instant::now();
            let mut moved = false;

            for dg in pump(&mut self.client_ep, self.client_h, &mut self.client, now) {
                self.to_server.push_back(dg);
                moved = true;
            }
            if let (Some(h), Some(state)) = (self.server_h, self.server.as_mut()) {
                for dg in pump(&mut self.server_ep, h, state, now) {
                    self.to_client.push_back(dg);
                    moved = true;
                }
            }

            while let Some(dg) = self.to_server.pop_front() {
                self.deliver_to_server(&dg);
                moved = true;
            }
            while let Some(dg) = self.to_client.pop_front() {
                self.deliver_to_client(&dg);
                moved = true;
            }

            if !moved {
                return;
            }
        }
        panic!("in-memory pair did not quiesce within {MAX_PASSES} passes");
    }

    fn deliver_to_server(&mut self, data: &[u8]) {
        let now = Instant::now();
        let mut resp = Vec::new();
        let event = self.server_ep.handle(
            now,
            self.client_addr,
            None,
            None,
            BytesMut::from(data),
            &mut resp,
        );
        if !resp.is_empty() {
            self.to_client.push_back(resp);
        }
        match event {
            Some(DatagramEvent::NewConnection(incoming)) => {
                let mut buf = Vec::new();
                let (ch, conn) = self
                    .server_ep
                    .accept(incoming, now, &mut buf, None)
                    .expect("server accepts the incoming connection");
                if !buf.is_empty() {
                    self.to_client.push_back(buf);
                }
                self.server_h = Some(ch);
                self.server = Some(ConnState::new(conn));
            }
            Some(DatagramEvent::ConnectionEvent(ch, cev)) if Some(ch) == self.server_h => {
                if let Some(state) = self.server.as_mut() {
                    state.conn_mut().handle_event(cev);
                }
            }
            _ => {}
        }
    }

    fn deliver_to_client(&mut self, data: &[u8]) {
        let now = Instant::now();
        let mut resp = Vec::new();
        let event = self.client_ep.handle(
            now,
            self.server_addr,
            None,
            None,
            BytesMut::from(data),
            &mut resp,
        );
        if !resp.is_empty() {
            self.to_server.push_back(resp);
        }
        if let Some(DatagramEvent::ConnectionEvent(ch, cev)) = event {
            if ch == self.client_h {
                self.client.conn_mut().handle_event(cev);
            }
        }
    }
}

/// Route one connection's endpoint events, service its streams, and collect
/// whatever it wants to send. This is the whole of a driver, minus the socket.
fn pump(
    ep: &mut Endpoint,
    h: ConnectionHandle,
    state: &mut ConnState,
    now: Instant,
) -> Vec<Vec<u8>> {
    while let Some(ev) = state.conn_mut().poll_endpoint_events() {
        if let Some(cev) = ep.handle_event(h, ev) {
            state.conn_mut().handle_event(cev);
        }
    }
    // The SOLE `conn.poll()` caller, exactly as in the real drivers.
    let _ = state.service_streams(usize::MAX);

    let mut out = Vec::new();
    let mut buf = Vec::new();
    while state.conn_mut().poll_transmit(now, 1, &mut buf).is_some() {
        if buf.is_empty() {
            break;
        }
        out.push(std::mem::take(&mut buf));
    }
    out
}

/// Build a fully connected pair of [`ConnState`]s over in-memory datagrams.
fn connected_pair() -> Pair {
    let client_addr: SocketAddr = "127.0.0.1:41001".parse().unwrap();
    let server_addr: SocketAddr = "127.0.0.1:41002".parse().unwrap();

    let identity = SelfSigned::generate().expect("self-signed identity");
    let server_config = Arc::new(TransportServerConfig::new(
        identity.quic_server_config().expect("quic server config"),
        token_key(),
    ));
    let server_ep = Endpoint::new(
        Arc::new(EndpointConfig::new(Arc::new(reset_key()))),
        Some(server_config),
        true,
        None,
    );

    let mut client_ep = Endpoint::new(
        Arc::new(EndpointConfig::new(Arc::new(reset_key()))),
        None,
        true,
        None,
    );
    let client_config = TransportClientConfig::new(Arc::new(build_quic_client()));
    let (client_h, conn) = client_ep
        .connect(Instant::now(), client_config, server_addr, SERVER_NAME)
        .expect("client connect");

    let mut pair = Pair {
        client_ep,
        server_ep,
        client_addr,
        server_addr,
        client_h,
        client: ConnState::new(conn),
        server_h: None,
        server: None,
        to_server: VecDeque::new(),
        to_client: VecDeque::new(),
    };
    pair.drive();

    assert!(
        !pair.client.conn_mut().is_handshaking(),
        "client finished the handshake"
    );
    assert!(
        pair.server
            .as_mut()
            .is_some_and(|s| !s.conn_mut().is_handshaking()),
        "server finished the handshake"
    );
    pair
}

/// Stock rustls TLS 1.3 with certificate verification skipped. This mirrors the
/// production posture (`src/client.rs`), where the PSK — not the certificate —
/// authenticates the peer.
fn build_quic_client() -> QuicClientConfig {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let rustls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("tls 1.3")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    QuicClientConfig::try_from(rustls_config).expect("quic client config")
}

#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA256,
            RSA_PKCS1_SHA384,
            RSA_PKCS1_SHA512,
            ECDSA_NISTP256_SHA256,
            ECDSA_NISTP384_SHA384,
            ED25519,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
        ]
    }
}

/// Write `data` on `id` from the client, looping over `Blocked` the way a caller
/// driving the core by hand would, and return once everything is accepted.
fn write_all(pair: &mut Pair, id: StreamId, data: &[u8]) {
    let mut sent = 0;
    for _ in 0..MAX_PASSES {
        if sent == data.len() {
            return;
        }
        match pair.client.stream_write(id, &data[sent..]).expect("write") {
            WriteOutcome::Wrote(n) => sent += n,
            WriteOutcome::Blocked => pair.drive(),
        }
    }
    panic!("write_all never completed");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// THE core invariant: with nothing buffered, a read reports `Blocked` and
/// returns. It must not be an error, and — since the core cannot park — it must
/// not hang. The opener's own receive half of a fresh bidirectional stream has
/// no data by construction.
#[test]
fn read_on_an_idle_stream_reports_blocked_not_an_error() {
    let mut pair = connected_pair();
    let id = pair.client.open_bi().expect("open_bi");

    let mut buf = [0u8; 16];
    assert_eq!(
        pair.client.stream_read(id, &mut buf).expect("read"),
        ReadOutcome::Blocked,
        "an idle stream must report Blocked, not an error"
    );
}

#[test]
fn reset_is_idempotent_and_retains_original_code() {
    let mut pair = connected_pair();
    let id = pair.client.open_bi().expect("open_bi");
    assert_eq!(
        pair.client.stream_reset(id, 17).unwrap(),
        ResetOutcome::ResetRequested
    );
    assert_eq!(
        pair.client.stream_reset(id, 29).unwrap(),
        ResetOutcome::AlreadyReset { code: 17 }
    );
}

/// A stream that has delivered all its data but has NOT been finished still
/// reports `Blocked` — `Blocked` and `Finished` are distinct answers, and
/// conflating them would truncate a peer mid-message.
#[test]
fn read_after_draining_available_data_reports_blocked_until_fin() {
    let mut pair = connected_pair();
    let id = pair.client.open_bi().expect("open_bi");
    write_all(&mut pair, id, b"hello");
    pair.drive();

    let accepted = pair.server().accept_bi().expect("accept_bi");
    assert_eq!(
        accepted,
        Some(id),
        "server sees the stream the client opened"
    );

    let mut buf = [0u8; 64];
    assert_eq!(
        pair.server().stream_read(id, &mut buf).expect("read"),
        ReadOutcome::Read(5)
    );
    assert_eq!(&buf[..5], b"hello");

    // No FIN yet: the peer may still send more.
    assert_eq!(
        pair.server().stream_read(id, &mut buf).expect("read"),
        ReadOutcome::Blocked,
        "a drained-but-unfinished stream must report Blocked, not Finished"
    );
}

/// After the peer finishes the stream and all data has been consumed, a read
/// reports `Finished` — and keeps reporting it, so a caller that reads once more
/// gets a stable answer rather than an error.
#[test]
fn read_after_fin_reports_finished() {
    let mut pair = connected_pair();
    let id = pair.client.open_bi().expect("open_bi");
    write_all(&mut pair, id, b"hello");
    pair.client.stream_finish(id).expect("finish");
    pair.drive();

    assert_eq!(pair.server().accept_bi().expect("accept_bi"), Some(id));

    let mut buf = [0u8; 64];
    assert_eq!(
        pair.server().stream_read(id, &mut buf).expect("read"),
        ReadOutcome::Read(5)
    );
    assert_eq!(&buf[..5], b"hello");
    assert_eq!(
        pair.server().stream_read(id, &mut buf).expect("read"),
        ReadOutcome::Finished,
        "the peer finished the stream"
    );
    assert_eq!(
        pair.server().stream_read(id, &mut buf).expect("read"),
        ReadOutcome::Finished,
        "Finished is idempotent, not a one-shot that then errors"
    );
}

/// A caller's buffer bounds the read: a short buffer yields `Read(buf.len())`
/// and the remainder stays queued for the next call. Nothing is dropped.
#[test]
fn a_short_buffer_reads_incrementally_without_losing_bytes() {
    let mut pair = connected_pair();
    let id = pair.client.open_bi().expect("open_bi");
    write_all(&mut pair, id, b"abcdefgh");
    pair.client.stream_finish(id).expect("finish");
    pair.drive();

    assert_eq!(pair.server().accept_bi().expect("accept_bi"), Some(id));

    let mut got = Vec::new();
    let mut buf = [0u8; 3];
    for _ in 0..MAX_PASSES {
        match pair.server().stream_read(id, &mut buf).expect("read") {
            ReadOutcome::Read(n) => got.extend_from_slice(&buf[..n]),
            ReadOutcome::Blocked => pair.drive(),
            ReadOutcome::Finished => break,
        }
    }
    assert_eq!(
        got, b"abcdefgh",
        "every byte survived the incremental reads"
    );
}

/// `accept_bi` is non-blocking: with no peer-opened stream pending it answers
/// `None` immediately rather than waiting for one.
#[test]
fn accept_bi_with_nothing_pending_returns_none() {
    let mut pair = connected_pair();
    assert_eq!(
        pair.server().accept_bi().expect("accept_bi"),
        None,
        "no stream has been opened yet"
    );
}

/// A locally-initiated close drives the connection to `Drained` once its close
/// timer fires — and `is_drained()` is how a driver learns that, because
/// quinn-proto never sets its internal error field for a self-close and so never
/// yields `ConnectionLost` from `poll()`. This is the signal both drivers reap
/// on; see `tests/connection_lifecycle.rs` for the failure it prevents.
#[test]
fn locally_closed_connection_reaches_is_drained_without_a_lost_event() {
    let mut pair = connected_pair();
    assert!(
        !pair.client.is_drained(),
        "a live connection is not drained"
    );

    let now = Instant::now();
    pair.client
        .conn_mut()
        .close(now, quinn_proto::VarInt::from_u32(0), bytes::Bytes::new());
    pair.drive();

    // The close timer is what completes the transition; fire it directly rather
    // than sleeping.
    let mut saw_lost = false;
    for _ in 0..MAX_PASSES {
        if pair.client.is_drained() {
            break;
        }
        let deadline = pair.client.conn_mut().poll_timeout();
        match deadline {
            Some(at) => pair.client.conn_mut().handle_timeout(at),
            None => break,
        }
        saw_lost |= pair.client.service_streams(usize::MAX).lost.is_some();
    }

    assert!(
        pair.client.is_drained(),
        "a locally-closed connection must reach is_drained()"
    );
    assert!(
        !saw_lost,
        "quinn-proto does NOT report ConnectionLost for a self-close — \
         is_drained() is the only signal, which is exactly why the drivers \
         check it in addition to progress.lost"
    );
}

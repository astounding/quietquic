// SPDX-License-Identifier: 0BSD
//! Task 11: end-to-end cloaking / silence integration suite.
//!
//! This is the headline proof that a real, socket-bound [`Server`] is invisible
//! to unauthorized peers and works normally for authorized ones. Every silence
//! case here sends REAL UDP bytes at a REAL bound server and then asserts the
//! sender's socket receives nothing within a bounded timeout — a timeout
//! elapsing is the pass condition for a silence case, never its absence of
//! effort. The happy-path case is the mirror: a real [`Client`] with the
//! correct PSK must connect and echo data end-to-end.
//!
//! Cases (see individual `#[tokio::test]`s below):
//!  1. `junk_scan_is_silent` — 100 random/varied-length UDP payloads, each
//!     from a distinct source socket so the bulk actually reach the DCID
//!     pre-filter rather than being eaten by the per-source rate limiter.
//!  2. `stock_quic_initial_is_silent` — a stock-QUIC-shaped long header with a
//!     random (non-selector) DCID.
//!  3. `replay_is_silent` — a valid-PSK, fresh selector DCID sent twice; the
//!     second (replayed) delivery must be silently dropped.
//!  4. `stale_freshness_is_silent` — a selector DCID built with a freshness
//!     far outside the acceptance window.
//!  5. `wrong_psk_is_silent` — an authorized-shaped DCID keyed with the WRONG
//!     PSK, both as a raw crafted datagram AND via a real `Client::connect`
//!     configured with the wrong PSK (must return `Err`/time out).
//!  6. `happy_path_connects_and_echoes` — correct-PSK `Client::connect`
//!     succeeds, opens a stream, and echoes a payload end-to-end.

mod common;

use std::time::Duration;

use aws_lc_rs::rand::SecureRandom;
use quietquic::client::Client;
use quietquic::config::{ClientConfigFile, ServerSecrets};
use quietquic::freshness::now_minutes;
use quietquic::selector::{build_dcid, DCID_LEN};
use quietquic::server::Server;
use tokio::net::UdpSocket;
use tokio::time::timeout;

/// Silence-case recv timeout. Short enough to keep the suite fast, long enough
/// not to flake on a loopback round-trip (there is none in the silence cases —
/// this is purely "did anything arrive at all").
const SILENCE_TIMEOUT: Duration = Duration::from_millis(400);

/// Happy-path timeout: a real handshake + stream echo, given more headroom.
const HAPPY_TIMEOUT: Duration = Duration::from_secs(10);

/// The PSK matching [`secrets_one_client`] (0x11 repeated).
const TEST_PSK: [u8; 32] = [0x11; 32];

/// A `ServerSecrets` with one authorized client, bound to an ephemeral loopback
/// port, built via TOML (the only public constructor for `ServerSecrets`).
fn secrets_one_client() -> ServerSecrets {
    toml::from_str(&format!(
        "listen = \"{}\"\n[[clients]]\nclient_id=\"authorized\"\npsk=\"1111111111111111111111111111111111111111111111111111111111111111\"\n",
        common::bind_addr_string()
    ))
    .expect("valid server secrets")
}

/// Build a synthetic QUIC long-header datagram carrying `dcid`, padded to look
/// like an Initial. This is *not* a valid Initial (it will not decrypt under
/// any keys), but it is exactly the shape a scanner armed with the selector
/// would send: the point of these tests is that the pre-filter never lets such
/// a datagram reach `Endpoint::handle` unless the selector matches AND is
/// fresh AND has not been seen before.
fn long_header_with_dcid(dcid: &[u8]) -> Vec<u8> {
    let mut dg = vec![0xc0, 0x00, 0x00, 0x00, 0x01, dcid.len() as u8];
    dg.extend_from_slice(dcid);
    dg.push(0); // scid len
    dg.extend_from_slice(&[0u8; 1200]); // pad to Initial-ish size
    dg
}

/// Fill `len` bytes of non-trivial randomness via the crate's existing RNG
/// dependency (`aws-lc-rs`), so junk payloads are not just zeros/patterns that
/// might accidentally line up with a code path.
fn random_bytes(len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    aws_lc_rs::rand::SystemRandom::new()
        .fill(&mut buf)
        .expect("system rng");
    buf
}

/// Send every payload in `datagrams` from a fresh sender socket to a fresh
/// server, then assert BOTH that the server never yields a connection AND
/// that the sender socket receives zero bytes back — the full silence
/// contract, not just "no connection". Returns the server (still bound) in
/// case a caller wants to make further assertions.
async fn assert_silent(datagrams: &[Vec<u8>]) {
    let server = Server::bind(secrets_one_client())
        .await
        .expect("bind server");
    let server_addr = server.local_addr();

    let sender = UdpSocket::bind(common::sender_bind_addr())
        .await
        .expect("bind sender");
    for dg in datagrams {
        sender
            .send_to(dg, server_addr)
            .await
            .expect("send datagram to server (non-vacuous: bytes actually hit the wire)");
    }

    // The server must never yield a connection for any of this traffic.
    let accepted = timeout(SILENCE_TIMEOUT, server.accept()).await;
    assert!(
        accepted.is_err(),
        "server yielded a connection for unauthorized/replayed/stale/wrong-psk input \
         (should have timed out waiting on accept())"
    );

    // The sender must receive absolutely nothing back.
    let mut buf = [0u8; 2048];
    let reply = timeout(SILENCE_TIMEOUT, sender.recv_from(&mut buf)).await;
    assert!(
        reply.is_err(),
        "server emitted bytes to an unauthorized sender (silence violated): {:?}",
        reply.map(|r| r.map(|(n, from)| format!("{n} bytes from {from}")))
    );
}

// ---------------------------------------------------------------------------
// 1. Junk-scan silence.
// ---------------------------------------------------------------------------

/// Send 100 random UDP payloads of varied lengths — the traffic pattern of a
/// scanner or fuzzer with no knowledge of the protocol at all — and assert the
/// server never responds and never yields a connection.
///
/// Each datagram is sent from a DISTINCT source socket (a fresh loopback port
/// per packet). This matters for coverage: the per-source rate limiter allows
/// a burst of only 32 packets per source IP-and-port, so if all 100 were sent
/// from one socket the per-source bucket would silently drop ~70 of them BEFORE
/// the DCID pre-filter (`peek_dcid` / selector match) ever ran — the test would
/// still pass, but on rate-limit drops rather than on the pre-filter it claims
/// to exercise. Spreading the junk across 100 distinct source addresses keeps
/// each per-source bucket well under its burst, so the bulk of these datagrams
/// genuinely reach `peek_dcid` and are dropped there for failing the selector
/// match — which is the code path this case is meant to prove silent. (The
/// global bucket, burst 2048, has ample headroom for 100 packets.)
#[tokio::test]
async fn junk_scan_is_silent() {
    let server = Server::bind(secrets_one_client())
        .await
        .expect("bind server");
    let server_addr = server.local_addr();

    // One fresh sender socket per datagram → one distinct source address per
    // datagram → per-source buckets stay well under their burst limit, so the
    // packets reach the DCID pre-filter rather than being eaten by per-source
    // rate limiting.
    let mut senders = Vec::with_capacity(100);
    for i in 0..100usize {
        let len = 1 + (i * 37) % 1400; // spread lengths across [1, 1400]
        let junk = random_bytes(len);
        let sender = UdpSocket::bind(common::sender_bind_addr())
            .await
            .expect("bind junk sender");
        sender
            .send_to(&junk, server_addr)
            .await
            .expect("send junk datagram from a distinct source socket");
        senders.push(sender);
    }

    // The server must never yield a connection for any of this junk.
    let accepted = timeout(SILENCE_TIMEOUT, server.accept()).await;
    assert!(
        accepted.is_err(),
        "server yielded a connection for junk-scan input (should have timed out)"
    );

    // No sender socket may receive anything back — full silence across every
    // distinct source. Poll all sockets concurrently under a single shared
    // silence budget (rather than paying the timeout once per socket serially):
    // if the server were going to reply to any of them it would have done so
    // well within SILENCE_TIMEOUT of the send.
    let recv_any = async {
        let mut set = tokio::task::JoinSet::new();
        for (i, sender) in senders.into_iter().enumerate() {
            set.spawn(async move {
                let mut buf = [0u8; 2048];
                let (n, from) = sender.recv_from(&mut buf).await.expect("recv");
                (i, n, from)
            });
        }
        set.join_next().await
    };
    let reply = timeout(SILENCE_TIMEOUT, recv_any).await;
    assert!(
        reply.is_err(),
        "server emitted bytes to a junk sender (silence violated): {:?}",
        reply
            .map(|r| r
                .map(|j| j.map(|(i, n, from)| format!("{n} bytes to sender #{i} from {from}"))))
    );
}

// ---------------------------------------------------------------------------
// 2. Stock-QUIC silence.
// ---------------------------------------------------------------------------

/// A raw datagram shaped exactly like a standard QUIC v1 long-header Initial
/// (form bit, version, DCID-length-prefixed DCID, SCID length, padding) but
/// with a RANDOM DCID — i.e. what a stock `quinn`/`quiche`/scanner client
/// would send with no knowledge of the PSK selector scheme. Since the DCID is
/// not `build_dcid(psk, nonce, freshness)` for any authorized client, the
/// pre-filter's selector match fails and the datagram is dropped before
/// `Endpoint::handle` ever sees it — so the server cannot even emit the
/// Version-Negotiation / stateless-reset bytes a stock QUIC server would.
#[tokio::test]
async fn stock_quic_initial_is_silent() {
    let random_dcid = random_bytes(DCID_LEN);
    let stock_initial = long_header_with_dcid(&random_dcid);
    assert_silent(&[stock_initial]).await;
}

// ---------------------------------------------------------------------------
// 3. Replay silence.
// ---------------------------------------------------------------------------

/// Approach: rather than proxy a real `Client`'s socket (which would require
/// intercepting UDP mid-flight — not something a plain tokio socket API
/// supports without a MITM relay), this crafts a datagram with a VALID
/// selector DCID (correct PSK, fresh timestamp) exactly as the brief's
/// fallback describes: "craft a datagram with a valid selector DCID + record
/// it via the server once, then resend and assert the replay path is
/// silent." The datagram itself is a synthetic long header (like the other
/// crafted cases) rather than a real decryptable Initial, but that is
/// irrelevant to what this test proves: the anti-replay guard's
/// `check_and_record` runs in the pre-filter, BEFORE the datagram ever
/// reaches quinn-proto (see the core's `Endpoint::handle_datagram`), so a synthetic
/// payload exercises exactly the same replay-detection code path a real
/// Initial would. The first send records the (nonce, freshness) pair in the
/// server's per-client `ReplayGuard`; the second send of the byte-identical
/// datagram from a FRESH socket (a different source, proving this is not just
/// per-socket dedup) must be dropped by the replay guard.
#[tokio::test]
async fn replay_is_silent() {
    let server = Server::bind(secrets_one_client())
        .await
        .expect("bind server");
    let server_addr = server.local_addr();

    let nonce = {
        let mut n = [0u8; 8];
        n.copy_from_slice(&random_bytes(8));
        n
    };
    let dcid = build_dcid(&TEST_PSK, nonce, now_minutes());
    let initial = long_header_with_dcid(&dcid);

    // First delivery: a fresh (nonce, freshness) pair, correct PSK, fresh
    // timestamp — it passes freshness and the replay guard and gets RECORDED.
    // (It still won't produce a connection because it is not a real
    // decryptable Initial, but recording happens in the pre-filter, ahead of
    // crypto — see the core's `Endpoint::handle_datagram` ordering.) We don't assert silence
    // here; the point of this send is purely to populate the replay guard, as
    // the brief's fallback approach describes.
    let first_sender = UdpSocket::bind(common::sender_bind_addr())
        .await
        .expect("bind first sender");
    first_sender
        .send_to(&initial, server_addr)
        .await
        .expect("send original datagram to server");
    // Give the driver a moment to process and record the (nonce, freshness).
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second delivery: the BYTE-IDENTICAL datagram, replayed from a FRESH
    // socket (different source address), proving the replay guard keys on
    // (nonce, freshness) and not on the sender. This must be silently
    // dropped: same nonce + same freshness has already been recorded.
    let replay_sender = UdpSocket::bind(common::sender_bind_addr())
        .await
        .expect("bind replay sender");
    replay_sender
        .send_to(&initial, server_addr)
        .await
        .expect("send replayed datagram to server");

    let accepted = timeout(SILENCE_TIMEOUT, server.accept()).await;
    assert!(
        accepted.is_err(),
        "server yielded a connection for a replayed datagram (should have timed out)"
    );

    let mut buf = [0u8; 2048];
    let reply = timeout(SILENCE_TIMEOUT, replay_sender.recv_from(&mut buf)).await;
    assert!(
        reply.is_err(),
        "server emitted bytes in response to a replayed datagram (silence violated): {:?}",
        reply.map(|r| r.map(|(n, _)| n))
    );
}

// ---------------------------------------------------------------------------
// 4. Stale freshness silence.
// ---------------------------------------------------------------------------

/// A selector DCID built with the correct PSK but a freshness value 10
/// minutes outside "now" — well beyond `WINDOW_MINUTES` (2) — so `is_fresh`
/// rejects it in the pre-filter before the replay guard is ever consulted.
#[tokio::test]
async fn stale_freshness_is_silent() {
    let nonce = {
        let mut n = [0u8; 8];
        n.copy_from_slice(&random_bytes(8));
        n
    };
    let stale_freshness = now_minutes().wrapping_sub(10);
    let dcid = build_dcid(&TEST_PSK, nonce, stale_freshness);
    assert_silent(&[long_header_with_dcid(&dcid)]).await;
}

// ---------------------------------------------------------------------------
// 5. Wrong-PSK silence (crafted datagram + real Client::connect).
// ---------------------------------------------------------------------------

/// An authorized-shaped DCID (right length, fresh timestamp, well-formed
/// long header) but with its selector computed from a DIFFERENT PSK than the
/// server's configured client. `selector_matches` must fail for every
/// configured client, so the pre-filter drops it.
#[tokio::test]
async fn wrong_psk_crafted_datagram_is_silent() {
    let wrong_psk = [0x22u8; 32];
    let nonce = {
        let mut n = [0u8; 8];
        n.copy_from_slice(&random_bytes(8));
        n
    };
    let dcid = build_dcid(&wrong_psk, nonce, now_minutes());
    assert_silent(&[long_header_with_dcid(&dcid)]).await;
}

/// The real-client mirror of the above (folds in a Task 8 review follow-up: a
/// negative admission case driven through the actual `Client` API, not just a
/// hand-crafted datagram). A real `Client::connect` configured with the WRONG
/// PSK against the real server must never complete a handshake: the server's
/// pre-filter drops every Initial the client sends (selector mismatch), so
/// the client's own bounded internal timeout must eventually fire and
/// `connect` must return `Err`.
#[tokio::test]
async fn wrong_psk_client_connect_fails() {
    let server = Server::bind(secrets_one_client())
        .await
        .expect("bind server");
    let addr = server.local_addr();

    // Drain any accept in the background so the server keeps driving (not
    // required for this to work, since accept() would simply never fire, but
    // keeps the server task shape consistent with the other tests and proves
    // nothing sneaks through into accept() either).
    let accept_task = tokio::spawn(async move { server.accept().await });

    let cfg: ClientConfigFile = toml::from_str(&format!(
        "client_id=\"authorized\"\npsk=\"2222222222222222222222222222222222222222222222222222222222222222\"\nserver=\"{addr}\"\n"
    ))
    .expect("valid client config with wrong psk");

    // The client has its own bounded internal connect timeout (10s default);
    // wrap it in a slightly more generous outer bound so a genuine hang fails
    // the test loudly rather than stalling the suite.
    let result = timeout(Duration::from_secs(15), Client::connect(cfg)).await;

    match result {
        // Client's own internal timeout fired first: also a pass — either way
        // the wrong-PSK client never connects.
        Err(_elapsed) => {}
        Ok(Err(_client_error)) => {}
        Ok(Ok(_)) => panic!("a client with the WRONG psk must never complete a handshake"),
    }

    // And the server must never have surfaced a connection for it either.
    let accepted = timeout(SILENCE_TIMEOUT, accept_task).await;
    assert!(
        accepted.is_err(),
        "server must not accept a connection from a wrong-PSK client"
    );
}

// ---------------------------------------------------------------------------
// 6. Happy path: correct PSK connects and echoes end-to-end.
// ---------------------------------------------------------------------------

/// The positive control for every silence case above: with the CORRECT PSK,
/// `Client::connect` must succeed, and a bidirectional stream opened on the
/// resulting connection must echo a payload end-to-end through the real
/// server. Without this test, "the server is silent" could vacuously be
/// achieved by a server that is silent to EVERYONE, including its own
/// authorized clients.
#[tokio::test]
async fn happy_path_connects_and_echoes() {
    let server = Server::bind(secrets_one_client())
        .await
        .expect("bind server");
    let addr = server.local_addr();

    let server_task = tokio::spawn(async move {
        let conn = server
            .accept()
            .await
            .expect("server should accept the authorized peer");
        let (mut send, mut recv) = conn
            .accept_bi()
            .await
            .expect("server should accept a bidirectional stream");
        let got = recv
            .read_to_end(1024)
            .await
            .expect("server should read the stream to end");
        send.write_all(&got)
            .await
            .expect("server should write the echo");
        send.finish().await.expect("server should finish the echo");
        got
    });

    let cfg: ClientConfigFile = toml::from_str(&format!(
        "client_id=\"authorized\"\npsk=\"1111111111111111111111111111111111111111111111111111111111111111\"\nserver=\"{addr}\"\n"
    ))
    .expect("valid client config with correct psk");

    let conn = timeout(HAPPY_TIMEOUT, Client::connect(cfg))
        .await
        .expect("client connect should not time out")
        .expect("authorized client should complete the handshake");
    assert_eq!(
        conn.remote_address(),
        addr,
        "client is connected to the real server"
    );

    const PAYLOAD: &[u8] = b"quietquic-cloaking-suite-happy-path";
    let (mut send, mut recv) = conn.open_bi().await.expect("client should open a stream");
    send.write_all(PAYLOAD)
        .await
        .expect("client should write the payload");
    send.finish().await.expect("client should finish its send");

    let echo = timeout(HAPPY_TIMEOUT, recv.read_to_end(1024))
        .await
        .expect("client read should not time out")
        .expect("client should read the echo");
    assert_eq!(
        &echo, PAYLOAD,
        "client must receive the echoed payload unchanged"
    );

    let server_got = timeout(HAPPY_TIMEOUT, server_task)
        .await
        .expect("server task should not time out")
        .expect("server task should not panic");
    assert_eq!(
        &server_got, PAYLOAD,
        "server must have received the exact payload"
    );
}

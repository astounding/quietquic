// SPDX-License-Identifier: 0BSD
//! Server pre-filter integration + `peek_dcid` unit tests.

mod common;

use quietquic::transport::peek_dcid;

#[test]
fn peek_dcid_extracts_from_long_header() {
    // minimal long header: form/fixed bits, version, dcid len=4, dcid bytes
    let mut pkt = vec![0xc0]; // long header
    pkt.extend_from_slice(&0x0000_0001u32.to_be_bytes()); // version 1
    pkt.push(4); // dcid len
    pkt.extend_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd]); // dcid
    pkt.push(0); // scid len
    assert_eq!(peek_dcid(&pkt), Some(&[0xaa, 0xbb, 0xcc, 0xdd][..]));
}

#[test]
fn peek_dcid_rejects_short_or_short_header() {
    assert_eq!(peek_dcid(&[0x40]), None); // short header (form bit clear)
    assert_eq!(peek_dcid(&[0xc0, 0x00]), None); // truncated
}

// ---------------------------------------------------------------------------
// Silence integration test: junk UDP must be dropped silently — the server
// yields no connection AND writes zero bytes back to the sender.
// ---------------------------------------------------------------------------

use std::time::Duration;

use quietquic::config::ServerSecrets;
use quietquic::selector::{build_dcid, DCID_LEN};
use quietquic::server::Server;
use tokio::net::UdpSocket;
use tokio::time::timeout;

/// A `ServerSecrets` with one client, bound to an ephemeral port, built via TOML
/// (the only public constructor for `ServerSecrets`).
fn secrets_one_client() -> ServerSecrets {
    toml::from_str(&format!(
        "listen = \"{}\"\n[[clients]]\nclient_id=\"test\"\npsk=\"1111111111111111111111111111111111111111111111111111111111111111\"\n",
        common::bind_addr_string()
    ))
    .expect("valid server secrets")
}

/// The PSK matching `secrets_one_client` (0x11 * 32).
const TEST_PSK: [u8; 32] = [0x11; 32];

/// Build a synthetic QUIC long-header datagram carrying `dcid`, padded to look
/// like an Initial. This is *not* a valid Initial (it won't decrypt), but it is
/// exactly what a scanner armed with the selector would send; the point is the
/// pre-filter never lets it reach `Endpoint::handle` unless the selector matches.
fn long_header_with_dcid(dcid: &[u8]) -> Vec<u8> {
    let mut dg = vec![0xc0, 0x00, 0x00, 0x00, 0x01, dcid.len() as u8];
    dg.extend_from_slice(dcid);
    dg.push(0); // scid len
    dg.extend_from_slice(&[0u8; 1200]); // pad to Initial-ish size
    dg
}

/// Send `payloads` at the server from a fresh client socket and assert the
/// server (a) never yields a connection and (b) sends zero bytes back.
async fn assert_silent(payloads: &[Vec<u8>]) {
    let server = Server::bind(secrets_one_client())
        .await
        .expect("bind server");
    let server_addr = server.local_addr();

    let client = UdpSocket::bind(common::sender_bind_addr())
        .await
        .expect("bind client");
    for p in payloads {
        client.send_to(p, server_addr).await.expect("send junk");
    }

    // (a) The server must not yield a connection.
    let accepted = timeout(Duration::from_millis(400), server.accept()).await;
    assert!(
        accepted.is_err(),
        "server yielded a connection for unauthenticated junk (should have timed out)"
    );

    // (b) The sender's socket must receive no reply.
    let mut buf = [0u8; 2048];
    let reply = timeout(Duration::from_millis(400), client.recv_from(&mut buf)).await;
    assert!(
        reply.is_err(),
        "server emitted bytes to an unauthenticated sender (silence violated): {:?}",
        reply.map(|r| r.map(|(n, _)| n))
    );
}

#[tokio::test]
async fn junk_udp_is_silently_dropped() {
    let junk: Vec<Vec<u8>> = vec![
        vec![0x00],                                      // 1 byte
        vec![0xff; 64],                                  // short random
        (0..1200u32).map(|i| (i % 251) as u8).collect(), // large random
        // Stock-QUIC-shaped long header with a random (non-selector) DCID.
        long_header_with_dcid(&[0x5a; DCID_LEN]),
        // Short-header-shaped packet with unknown CID.
        {
            let mut v = vec![0x40];
            v.extend_from_slice(&[0x99; 8]);
            v.extend_from_slice(&[0u8; 100]);
            v
        },
    ];
    assert_silent(&junk).await;
}

#[tokio::test]
async fn wrong_psk_selector_dcid_is_silently_dropped() {
    // A well-formed selector DCID, but built from the WRONG PSK: the selector
    // will not match, so the pre-filter drops it before the endpoint.
    let wrong_psk = [0x22u8; 32];
    let dcid = build_dcid(&wrong_psk, [0xab; 8], now_minute());
    assert_silent(&[long_header_with_dcid(&dcid)]).await;
}

#[tokio::test]
async fn stale_selector_dcid_is_silently_dropped() {
    // Correct PSK selector, but a freshness far outside the window → dropped by
    // `is_fresh` before the replay guard.
    let dcid = build_dcid(&TEST_PSK, [0xcd; 8], now_minute().wrapping_sub(1000));
    assert_silent(&[long_header_with_dcid(&dcid)]).await;
}

fn now_minute() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 60) as u32
}

// ---------------------------------------------------------------------------
// Task 10: rate-limit flood test. A flood of junk must (a) still yield zero
// server replies (the rate-limit drop is exactly as silent as every other
// pre-filter drop) and (b) not starve a legitimate connection attempt shortly
// afterward — the server must remain live under flood.
// ---------------------------------------------------------------------------

use quietquic::client::Client;
use quietquic::config::ClientConfigFile;

/// Flood the server with thousands of junk datagrams from one source, then
/// prove: (a) the flood itself gets zero replies (rate-limited drops are
/// silent, same as every other pre-filter rejection), and (b) a legitimate,
/// correctly-keyed client can still connect promptly afterward — i.e. the
/// global token bucket refills fast enough that a real connection attempt is
/// not starved by an earlier flood.
///
/// This does not attempt to precisely observe "packet N was rejected because
/// the bucket was empty" — quinn-proto gives no hook to distinguish a
/// rate-limit drop from any other silent pre-filter drop from outside the
/// driver, and the whole point of the silence contract is that they are
/// indistinguishable on the wire. Instead this proves the two properties that
/// actually matter operationally: junk floods produce no reply traffic, and
/// the server recovers to serve real clients — which is the liveness
/// guarantee the rate limiter must preserve.
#[tokio::test]
async fn flood_of_junk_is_silent_and_server_stays_live() {
    let psk_hex = "0000000000000000000000000000000000000000000000000000000000000010";
    let secrets: ServerSecrets = toml::from_str(&format!(
        "listen = \"{}\"\n[[clients]]\nclient_id=\"a\"\npsk=\"{psk_hex}\"\n",
        common::bind_addr_string()
    ))
    .unwrap();

    let server = Server::bind(secrets).await.expect("bind server");
    let server_addr = server.local_addr();

    // Flood: thousands of junk long-header datagrams with random (non-selector)
    // DCIDs, all from one attacker socket. None of these should ever reach
    // `Endpoint::handle`, whether they are dropped by the rate limiter or by
    // the selector/freshness checks further down the pre-filter.
    let attacker = UdpSocket::bind(common::sender_bind_addr())
        .await
        .expect("bind attacker");
    const FLOOD_SIZE: usize = 5_000;

    // Regression guard: this test is only meaningful if the flood volume
    // exceeds the global token-bucket burst capacity, so that the rate limiter
    // is actually forced to drop packets (rather than every packet fitting
    // inside a bucket that never saturates). If the default global capacity is
    // ever raised at or above FLOOD_SIZE, this flood would stop exercising the
    // limiter and silently become vacuous — the assertion below catches that.
    // (`DEFAULT_GLOBAL_CAPACITY` is a private constant in `ratelimit.rs`,
    // currently 2048; this mirrors it. If that constant changes, update this
    // bound to keep FLOOD_SIZE strictly greater than it.)
    const DOCUMENTED_GLOBAL_CAPACITY: usize = 2_048;
    const _: () = assert!(
        FLOOD_SIZE > DOCUMENTED_GLOBAL_CAPACITY,
        "flood volume must exceed the global bucket capacity or this test is vacuous"
    );
    for i in 0..FLOOD_SIZE {
        let dcid = [(i % 256) as u8; DCID_LEN];
        attacker
            .send_to(&long_header_with_dcid(&dcid), server_addr)
            .await
            .expect("send junk");
    }

    // (a) Silence: the attacker socket receives nothing back.
    let mut buf = [0u8; 2048];
    let reply = timeout(Duration::from_millis(400), attacker.recv_from(&mut buf)).await;
    assert!(
        reply.is_err(),
        "server emitted bytes to the flood source (silence violated): {:?}",
        reply.map(|r| r.map(|(n, _)| n))
    );

    // Give the token buckets a little time to refill (defaults: global bucket
    // refills at ~512 tokens/sec, per-source at ~8 tokens/sec) so a legitimate
    // connection attempt right after a flood is not spuriously dropped by
    // leftover bucket exhaustion rather than by an actual sustained attack.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // (b) Liveness: a legitimate, correctly-keyed client must still be able to
    // connect promptly. The client uses a fresh socket but the SAME loopback
    // IP as the flood, so this also proves the per-source bucket recovers
    // (not just the global one) — the rate limiter must not permanently lock
    // out a source after a burst.
    let cfg: ClientConfigFile = toml::from_str(&format!(
        "client_id=\"a\"\npsk=\"{psk_hex}\"\nserver=\"{server_addr}\"\n"
    ))
    .unwrap();

    let accept_fut = server.accept();
    let connect_fut = Client::connect(cfg);
    let (accepted, connected) = tokio::join!(
        timeout(Duration::from_secs(10), accept_fut),
        timeout(Duration::from_secs(10), connect_fut),
    );

    accepted
        .expect("server accept must not time out after the flood (liveness under flood)")
        .expect("server must accept the legitimate peer after the flood");
    connected
        .expect("client connect must not time out after the flood")
        .expect("legitimate client must complete the handshake after the flood");
}

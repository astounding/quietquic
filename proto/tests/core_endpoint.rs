// SPDX-License-Identifier: 0BSD
//! End-to-end exercise of the completed sans-IO [`Endpoint`], through the REAL
//! cloaking path, with **no sockets, no runtime, and no threads**.
//!
//! `core_streams.rs` deliberately drives a *stock* quinn-proto pair, because its
//! unit under test (`ConnState`) is transport-agnostic. This file is the other
//! half: every datagram here passes through
//! [`quietquic_proto::endpoint::Endpoint::handle_datagram`], so the blinded
//! selector DCID, the PSK-rekeyed Initial, the freshness/replay gates and the
//! rate limiter are all genuinely in the loop. If cloaking breaks, these fail.
//!
//! [`Endpoint`]: quietquic_proto::endpoint::Endpoint

use quinn_proto::{Side, TransportConfig, VarInt};
use std::time::{Duration, Instant};

use quietquic_proto::config::{
    ClientConfigFile, EndpointConfig, ServerSecrets, TransportSettings, MAX_ENDPOINT_DURATION,
};
use quietquic_proto::conn::AUTO_CODE_START;
use quietquic_proto::endpoint::Endpoint;
use quietquic_proto::freshness::now_minutes;
use quietquic_proto::outcome::{CloseConnectionError, ConnectionError, DatagramOutcome, Event};
use quietquic_proto::testing::{connected_pair, Pair};

/// Bound on timer-firing passes when reaping a closed connection. A close timer
/// settles in a couple; this only exists so a regression fails fast.
const MAX_REAP_PASSES: usize = 64;

/// Did this side observe a `Connected` event for the given handle?
fn saw_connected(pair: &Pair, side: Side) -> bool {
    pair.events(side)
        .iter()
        .any(|e| matches!(e, Event::Connected(_)))
}

/// A cloaked handshake — blinded selector DCID, PSK-derived Initial keys, the
/// full server pre-filter — completes entirely in memory, and BOTH sides surface
/// `Event::Connected` through `poll_event`.
///
/// This is the first test in the tree that proves `new_client` and `new_server`
/// interoperate: `core_silence.rs` only ever proved what the server *refuses*.
#[test]
fn cloaked_pair_completes_handshake_and_both_sides_see_connected() {
    let pair = connected_pair();

    assert!(
        saw_connected(&pair, Side::Client),
        "the client must surface Event::Connected; got {:?}",
        pair.events(Side::Client)
    );
    assert!(
        saw_connected(&pair, Side::Server),
        "the server must surface Event::Connected; got {:?}",
        pair.events(Side::Server)
    );
}

/// A stream opened by the cloaked client is accepted and read back by the
/// server, byte for byte. This is the payload path the whole crate exists for.
#[test]
fn client_stream_is_accepted_and_read_by_the_server() {
    let mut pair = connected_pair();

    let id = pair.open_bi(Side::Client);
    pair.write_all(Side::Client, id, b"ping");
    pair.conn(Side::Client).stream_finish(id).expect("finish");
    pair.drive();

    let accepted = pair.accept_bi(Side::Server);
    assert_eq!(
        accepted, id,
        "the server accepts the stream the client opened"
    );

    let got = pair.pump_until_read(Side::Server, id);
    assert_eq!(&got, b"ping", "every byte survived the cloaked round trip");
}

/// Both directions, so the server's send path is exercised too.
#[test]
fn a_stream_echoes_in_both_directions() {
    let mut pair = connected_pair();

    let id = pair.open_bi(Side::Client);
    pair.write_all(Side::Client, id, b"echo-me");
    pair.conn(Side::Client).stream_finish(id).expect("finish");
    pair.drive();

    assert_eq!(pair.accept_bi(Side::Server), id);
    let got = pair.pump_until_read(Side::Server, id);
    assert_eq!(&got, b"echo-me");

    pair.write_all(Side::Server, id, &got);
    pair.conn(Side::Server).stream_finish(id).expect("finish");
    pair.drive();

    let back = pair.pump_until_read(Side::Client, id);
    assert_eq!(&back, b"echo-me", "the server's echo reaches the client");
}

/// THE `is_drained()` reaping guard, at the core level.
///
/// quinn-proto's `Connection::close()` only arms the close timer; the connection
/// reaches `Drained` WITHOUT ever setting the internal error field, so `poll()`
/// never yields `ConnectionLost` for a self-close. An endpoint that reaped only
/// on `progress.lost` would keep a locally-closed connection in its maps forever
/// — until quinn-proto reused the freed `ConnectionHandle` and the collision
/// wedged accept (~32 cycles; see `tests/connection_lifecycle.rs`).
///
/// So: close the CLIENT locally, fire its timers, and require that the endpoint
/// both drops the connection from its bookkeeping AND emits `ConnectionLost`.
#[test]
fn a_locally_closed_connection_is_reaped_and_reports_connection_lost() {
    let mut pair = connected_pair();
    let ch = pair.client_ch();
    let now = pair.now();

    assert!(
        pair.client().conn_mut(ch).is_some(),
        "the connection is live before the close"
    );

    pair.conn(Side::Client)
        .conn_mut()
        .close(now, VarInt::from_u32(0), bytes::Bytes::new());
    pair.drive();

    // Fire the close timer. Nothing else can complete the transition.
    let mut reaped = false;
    for _ in 0..MAX_REAP_PASSES {
        if pair.client().conn_mut(ch).is_none() {
            reaped = true;
            break;
        }
        pair.fire_timers();
        pair.drive();
    }

    assert!(
        reaped,
        "a locally-closed connection MUST be reaped via is_drained() — quinn-proto \
         never reports ConnectionLost for a self-close, so reaping on progress.lost \
         alone leaks the handle until a reused one wedges accept"
    );
    assert!(
        pair.events(Side::Client).iter().any(|event| {
            matches!(
                event,
                Event::ConnectionLost {
                    conn,
                    reason: quietquic_proto::outcome::ConnectionError::LocallyClosed
                } if *conn == ch
            )
        }),
        "the reap must surface ConnectionLost so the caller knows the handle is dead; \
         got {:?}",
        pair.events(Side::Client)
    );
}

/// CID attribution, end to end: the server minted and recorded this connection's
/// CIDs during the cloaked handshake, and losing the connection must remove
/// exactly those CIDs from the routing set.
///
/// This is what `admit()`'s spike stub (`pending_cids.clear()`) could not do:
/// without `cids_by_conn`, the CIDs were unattributable and leaked forever.
#[test]
fn a_lost_connections_cids_are_pruned_from_the_routing_set() {
    let mut pair = connected_pair();

    assert!(
        pair.server().issued_cid_count() > 0,
        "the server must have recorded the live connection's CIDs"
    );

    // A local close on the client sends CONNECTION_CLOSE, so the SERVER observes
    // a remote loss and reaps promptly.
    let now = pair.now();
    pair.conn(Side::Client)
        .conn_mut()
        .close(now, VarInt::from_u32(0), bytes::Bytes::new());
    pair.drive();

    let mut pruned = false;
    for _ in 0..MAX_REAP_PASSES {
        if pair.server().issued_cid_count() == 0 {
            pruned = true;
            break;
        }
        pair.fire_timers();
        pair.drive();
    }

    assert!(
        pruned,
        "a lost connection's CIDs must be pruned from the routing set; still {} left",
        pair.server().issued_cid_count()
    );
}

fn configs() -> (ServerSecrets, ClientConfigFile) {
    let server = toml::from_str(
        "listen=\"127.0.0.1:4433\"\n[[clients]]\nclient_id=\"testing\"\npsk=\"0000000000000000000000000000000000000000000000000000000000000001\"\n",
    ).unwrap();
    let client = toml::from_str(
        "client_id=\"testing\"\npsk=\"0000000000000000000000000000000000000000000000000000000000000001\"\nserver=\"127.0.0.1:4433\"\n",
    ).unwrap();
    (server, client)
}

fn connect_to_server(
    server: &mut Endpoint,
    client_cfg: ClientConfigFile,
    client_addr: std::net::SocketAddr,
    now: Instant,
) -> (Endpoint, quietquic_proto::outcome::ConnectionHandle) {
    let server_addr = client_cfg.server;
    let (mut client, client_ch) = Endpoint::new_client(now, now_minutes(), client_cfg).unwrap();
    let mut server_ch = None;
    let mut client_connected = false;
    let mut server_connected = false;
    for _ in 0..128 {
        while let Some(tx) = client.poll_transmit(now) {
            if let DatagramOutcome::Accepted(ch) =
                server.handle_datagram(now, client_addr, &tx.contents)
            {
                server_ch = Some(ch);
            }
        }
        while let Some(tx) = server.poll_transmit(now) {
            if tx.destination == client_addr {
                client.handle_datagram(now, server_addr, &tx.contents);
            }
        }
        while let Some(event) = client.poll_event() {
            client_connected |= matches!(event, Event::Connected(ch) if ch == client_ch);
        }
        while let Some(event) = server.poll_event() {
            server_connected |= matches!(event, Event::Connected(_));
        }
        if client_connected && server_connected {
            return (client, server_ch.unwrap());
        }
    }
    panic!("in-memory handshake did not complete")
}

#[test]
fn pending_admission_is_bounded_and_mark_accepted_releases_the_slot() {
    let now = Instant::now();
    let (secrets, client_cfg) = configs();
    let mut cfg = EndpointConfig::accept(secrets.clients);
    cfg.max_pending_incoming = 1;
    let mut server = Endpoint::new(cfg).unwrap();
    let (mut first, _) = Endpoint::new_client(now, now_minutes(), client_cfg.clone()).unwrap();
    let (mut second, _) = Endpoint::new_client(now, now_minutes(), client_cfg).unwrap();
    let first_packet = first.poll_transmit(now).unwrap().contents;
    let second_packet = second.poll_transmit(now).unwrap().contents;

    let first_handle =
        match server.handle_datagram(now, "127.0.0.1:5001".parse().unwrap(), &first_packet) {
            DatagramOutcome::Accepted(ch) => ch,
            other => panic!("first connection was not admitted: {other:?}"),
        };
    assert_eq!(server.pending_incoming_count(), 1);
    assert_eq!(
        server.handle_datagram(now, "127.0.0.1:5002".parse().unwrap(), &second_packet),
        DatagramOutcome::Dropped,
        "a full admission table rejects before allocating state"
    );
    assert_eq!(server.connections().count(), 1);

    server.mark_accepted(first_handle).unwrap();
    assert_eq!(server.pending_incoming_count(), 0);
    assert!(
        matches!(
            server.handle_datagram(now, "127.0.0.1:5002".parse().unwrap(), &second_packet),
            DatagramOutcome::Accepted(_)
        ),
        "the rejected Initial remains admissible after a slot is released"
    );
}

#[test]
fn admission_bound_combines_completed_and_in_progress_connections() {
    let now = Instant::now();
    let (secrets, client_cfg) = configs();
    let mut cfg = EndpointConfig::accept(secrets.clients);
    cfg.max_pending_incoming = 2;
    let mut server = Endpoint::new(cfg).unwrap();

    let (_completed_client, completed) = connect_to_server(
        &mut server,
        client_cfg.clone(),
        "127.0.0.1:5301".parse().unwrap(),
        now,
    );
    let (mut handshaking_client, _) =
        Endpoint::new_client(now, now_minutes(), client_cfg.clone()).unwrap();
    let handshaking_initial = handshaking_client.poll_transmit(now).unwrap().contents;
    assert!(matches!(
        server.handle_datagram(now, "127.0.0.1:5302".parse().unwrap(), &handshaking_initial),
        DatagramOutcome::Accepted(_)
    ));
    assert_eq!(server.pending_incoming_count(), 2);
    while server.poll_transmit(now).is_some() {}

    let (mut rejected_client, _) = Endpoint::new_client(now, now_minutes(), client_cfg).unwrap();
    let rejected_initial = rejected_client.poll_transmit(now).unwrap().contents;
    let connection_count = server.connections().count();
    let cid_count = server.issued_cid_count();
    assert_eq!(
        server.handle_datagram(now, "127.0.0.1:5303".parse().unwrap(), &rejected_initial),
        DatagramOutcome::Dropped
    );
    assert_eq!(server.connections().count(), connection_count);
    assert_eq!(server.issued_cid_count(), cid_count);
    assert!(
        server.poll_transmit(now).is_none(),
        "full admission is silent"
    );

    server.mark_accepted(completed).unwrap();
    assert_eq!(server.pending_incoming_count(), 1);
    assert!(matches!(
        server.handle_datagram(now, "127.0.0.1:5303".parse().unwrap(), &rejected_initial),
        DatagramOutcome::Accepted(_)
    ));
    assert_eq!(server.pending_incoming_count(), 2);
}

#[test]
fn pausing_admission_preserves_an_already_admitted_connection() {
    let mut pair = connected_pair();
    let server_ch = pair.server_ch();
    pair.server().pause_admission();
    let id = pair.open_bi(Side::Client);
    pair.write_all(Side::Client, id, b"still-live");
    pair.conn(Side::Client).stream_finish(id).unwrap();
    pair.drive();
    assert_eq!(pair.accept_bi(Side::Server), id);
    assert_eq!(pair.pump_until_read(Side::Server, id), b"still-live");
    assert!(pair.server().conn_mut(server_ch).is_some());
}

#[test]
fn close_pending_incoming_spares_connections_after_handoff() {
    let mut pair = connected_pair();
    let ch = pair.server_ch();
    assert_eq!(pair.server().pending_incoming_count(), 1);
    pair.server().mark_accepted(ch).unwrap();
    let now = pair.now();
    pair.server().close_pending_incoming(now);
    assert!(pair.server().conn_mut(ch).is_some());

    let mut pending = connected_pair();
    let pending_ch = pending.server_ch();
    let now = pending.now();
    pending.server().close_pending_incoming(now);
    assert!(
        pending.server().conn_mut(pending_ch).is_none(),
        "local close is visible immediately"
    );
    assert!(
        !pending.server().is_idle(),
        "transport cleanup still retains the endpoint"
    );
    for _ in 0..MAX_REAP_PASSES {
        if pending.server().is_idle() {
            break;
        }
        pending.fire_timers();
    }
    assert!(
        pending.server().is_idle(),
        "Quinn state eventually reaches Drained"
    );
}

#[test]
fn force_remove_drains_transport_and_prunes_all_bookkeeping() {
    let mut pair = connected_pair();
    let ch = pair.server_ch();
    let now = pair.now();
    pair.server()
        .force_remove(now, ch, ConnectionError::TimedOut)
        .unwrap();
    assert!(pair.server().conn_mut(ch).is_none());
    assert_eq!(pair.server().issued_cid_count(), 0);
    assert!(matches!(
        pair.server().poll_event(),
        Some(Event::ConnectionLost { conn, reason: ConnectionError::TimedOut }) if conn == ch
    ));
}

#[test]
fn application_connection_close_cannot_claim_reserved_cleanup_codes() {
    let mut pair = connected_pair();
    let ch = pair.client_ch();
    let now = pair.now();

    assert_eq!(
        pair.client().close_connection(
            now,
            ch,
            VarInt::from_u64(AUTO_CODE_START).expect("reserved code is a QUIC varint"),
            Vec::new(),
        ),
        Err(CloseConnectionError::ReservedCode {
            code: AUTO_CODE_START
        }),
        "the automatic-cleanup range is reserved to QuietQUIC",
    );
    assert!(
        pair.client().conn_mut(ch).is_some(),
        "rejecting an application code must leave the connection untouched"
    );

    pair.client()
        .close_connection(
            now,
            ch,
            VarInt::from_u64(AUTO_CODE_START - 1).expect("application code is a QUIC varint"),
            Vec::new(),
        )
        .expect("the highest application-owned code remains valid");
    assert!(pair.client().conn_mut(ch).is_none());
    assert_eq!(
        pair.client()
            .close_connection(now, ch, VarInt::from_u32(0), Vec::new()),
        Err(CloseConnectionError::UnknownConnection),
        "a stale handle remains distinguishable from an invalid close code"
    );
}

#[test]
fn repeated_connection_lifecycle_returns_all_endpoint_counters_to_baseline() {
    const CYCLES: u16 = 32;
    let now = Instant::now();
    let (secrets, client_cfg) = configs();
    let mut server = Endpoint::new(EndpointConfig::accept(secrets.clients)).unwrap();

    for cycle in 0..CYCLES {
        let client_addr = format!("127.0.0.1:{}", 10_000 + cycle).parse().unwrap();
        let (_client, server_ch) =
            connect_to_server(&mut server, client_cfg.clone(), client_addr, now);
        server.mark_accepted(server_ch).unwrap();
        assert_eq!(server.pending_incoming_count(), 0, "cycle {cycle}");
        assert!(!server.is_idle(), "cycle {cycle} must create live state");
        assert!(
            server.issued_cid_count() > 0,
            "cycle {cycle} must issue CIDs"
        );

        server
            .force_remove(now, server_ch, ConnectionError::TimedOut)
            .unwrap();
        assert!(server.is_idle(), "cycle {cycle} leaked connection state");
        assert_eq!(
            server.cleanup_handles().count(),
            0,
            "cycle {cycle} leaked cleanup state"
        );
        assert_eq!(
            server.pending_incoming_count(),
            0,
            "cycle {cycle} leaked admission state"
        );
        assert_eq!(
            server.issued_cid_count(),
            0,
            "cycle {cycle} leaked CID attribution"
        );
        assert!(matches!(
            server.poll_event(),
            Some(Event::ConnectionLost {
                conn,
                reason: ConnectionError::TimedOut
            }) if conn == server_ch
        ));
        assert!(
            server.poll_event().is_none(),
            "cycle {cycle} leaked queued events"
        );
    }
}

#[test]
fn transport_defaults_are_snapshots_and_zero_timeout_override_is_rejected() {
    let now = Instant::now();
    let (_, client_cfg) = configs();
    let mut endpoint = Endpoint::new(EndpointConfig::dial()).unwrap();
    let original = endpoint.config().transport.clone();
    let replacement = TransportSettings::new(TransportConfig::default());
    endpoint.set_transport_defaults(replacement.clone());
    assert!(std::ptr::eq(
        endpoint.config().transport.as_quinn(),
        replacement.as_quinn()
    ));
    assert!(!std::ptr::eq(original.as_quinn(), replacement.as_quinn()));
    assert!(endpoint
        .connect(
            now,
            now_minutes(),
            client_cfg,
            Some(original),
            Some(Duration::ZERO)
        )
        .is_err());
}

#[test]
fn updated_transport_defaults_affect_only_future_incoming_connections() {
    let now = Instant::now();
    let (secrets, client_cfg) = configs();
    let mut first_transport = TransportConfig::default();
    first_transport.max_concurrent_bidi_streams(VarInt::from_u32(1));
    let mut endpoint_config = EndpointConfig::accept(secrets.clients);
    endpoint_config.transport = TransportSettings::new(first_transport);
    let mut server = Endpoint::new(endpoint_config).unwrap();

    let (mut first, _) = connect_to_server(
        &mut server,
        client_cfg.clone(),
        "127.0.0.1:5101".parse().unwrap(),
        now,
    );
    let first_ch = first.connections().next().unwrap();
    assert!(first.conn_mut(first_ch).unwrap().open_bi().is_ok());
    assert!(first.conn_mut(first_ch).unwrap().open_bi().is_err());

    let mut second_transport = TransportConfig::default();
    second_transport.max_concurrent_bidi_streams(VarInt::from_u32(2));
    server.set_transport_defaults(TransportSettings::new(second_transport));
    let (mut second, _) = connect_to_server(
        &mut server,
        client_cfg,
        "127.0.0.1:5102".parse().unwrap(),
        now,
    );
    let second_ch = second.connections().next().unwrap();
    assert!(second.conn_mut(second_ch).unwrap().open_bi().is_ok());
    assert!(second.conn_mut(second_ch).unwrap().open_bi().is_ok());
    assert!(second.conn_mut(second_ch).unwrap().open_bi().is_err());

    assert!(first.conn_mut(first_ch).unwrap().open_bi().is_err());
}

#[test]
fn unfinished_incoming_handshake_times_out_and_releases_admission_slot() {
    let now = Instant::now();
    let (secrets, client_cfg) = configs();
    let mut config = EndpointConfig::accept(secrets.clients);
    config.incoming_handshake_timeout = Duration::from_millis(5);
    let mut server = Endpoint::new(config).unwrap();
    let (mut client, _) = Endpoint::new_client(now, now_minutes(), client_cfg).unwrap();
    let initial = client.poll_transmit(now).unwrap();
    assert!(matches!(
        server.handle_datagram(now, "127.0.0.1:5201".parse().unwrap(), &initial.contents),
        DatagramOutcome::Accepted(_)
    ));
    assert_eq!(server.pending_incoming_count(), 1);

    server.handle_timeout(now + Duration::from_millis(5));
    assert_eq!(server.pending_incoming_count(), 0);
    assert_eq!(server.connections().count(), 0);
    assert_eq!(server.cleanup_handles().count(), 1);
}

#[test]
fn completed_unaccepted_connection_has_no_acceptance_deadline() {
    let mut pair = connected_pair();
    let ch = pair.server_ch();
    assert_eq!(pair.server().pending_incoming_count(), 1);
    let later = pair.now() + Duration::from_secs(11);
    pair.server().handle_timeout(later);
    assert!(pair.server().conn_mut(ch).is_some());
    assert_eq!(pair.server().pending_incoming_count(), 1);
}

#[test]
fn oversized_attempt_deadline_is_rejected_before_allocation() {
    let now = Instant::now();
    let (_, client_cfg) = configs();
    let mut endpoint = Endpoint::new(EndpointConfig::dial()).unwrap();
    assert!(endpoint
        .connect(now, now_minutes(), client_cfg, None, Some(Duration::MAX))
        .is_err());
    assert!(endpoint.is_idle());
    assert_eq!(endpoint.connections().count(), 0);
    assert_eq!(endpoint.issued_cid_count(), 0);
}

#[test]
fn outgoing_override_times_out_without_harming_healthy_sibling() {
    let now = Instant::now();
    let (secrets, healthy_cfg) = configs();
    let server_addr = healthy_cfg.server;
    let client_addr = "127.0.0.1:5401".parse().unwrap();
    let mut server = Endpoint::new(EndpointConfig::accept(secrets.clients)).unwrap();
    let mut client_config = EndpointConfig::dial();
    client_config.outgoing_handshake_timeout = Duration::from_secs(30);
    let mut client = Endpoint::new(client_config).unwrap();
    let healthy = client
        .connect(now, now_minutes(), healthy_cfg.clone(), None, None)
        .unwrap();
    let mut doomed_cfg = healthy_cfg;
    doomed_cfg.server = "127.0.0.1:5999".parse().unwrap();
    let doomed = client
        .connect(
            now,
            now_minutes(),
            doomed_cfg,
            None,
            Some(Duration::from_millis(5)),
        )
        .unwrap();

    let mut healthy_connected = false;
    for _ in 0..128 {
        while let Some(tx) = client.poll_transmit(now) {
            if tx.destination == server_addr {
                server.handle_datagram(now, client_addr, &tx.contents);
            }
        }
        while let Some(tx) = server.poll_transmit(now) {
            client.handle_datagram(now, server_addr, &tx.contents);
        }
        while let Some(event) = client.poll_event() {
            healthy_connected |= matches!(event, Event::Connected(ch) if ch == healthy);
        }
        if healthy_connected {
            break;
        }
    }
    assert!(
        healthy_connected,
        "healthy sibling completes before timeout"
    );

    client.handle_timeout(now + Duration::from_millis(5));
    assert!(client.conn_mut(healthy).is_some());
    assert!(client.conn_mut(doomed).is_none());
    assert!(matches!(
        client.poll_event(),
        Some(Event::ConnectionLost {
            conn,
            reason: ConnectionError::TimedOut
        }) if conn == doomed
    ));
}

#[test]
fn maximum_finite_outgoing_override_is_accepted() {
    let now = Instant::now();
    let (_, client_cfg) = configs();
    let mut endpoint = Endpoint::new(EndpointConfig::dial()).unwrap();
    assert!(endpoint
        .connect(
            now,
            now_minutes(),
            client_cfg,
            None,
            Some(MAX_ENDPOINT_DURATION),
        )
        .is_ok());
}

#[test]
fn transfer_recovers_from_deterministic_loss_and_reordering() {
    let mut pair = connected_pair();
    let id = pair.open_bi(Side::Client);
    let payload = vec![0x5a; 32 * 1024];
    pair.write_all(Side::Client, id, &payload);
    pair.conn(Side::Client).stream_finish(id).unwrap();
    assert!(
        pair.drop_next_client_datagram(),
        "the application write queued a datagram"
    );

    for _ in 0..MAX_REAP_PASSES {
        pair.fire_timers();
        pair.drive_reordered();
        if pair.conn(Side::Server).accept_bi().unwrap().is_some() {
            break;
        }
    }
    let received = pair.pump_until_read(Side::Server, id);
    assert_eq!(received, payload, "loss recovery preserves every byte");

    let sibling = pair.open_bi(Side::Client);
    pair.write_all(Side::Client, sibling, b"sibling-survived");
    pair.conn(Side::Client).stream_finish(sibling).unwrap();
    pair.drive_reordered();
    assert_eq!(pair.accept_bi(Side::Server), sibling);
    assert_eq!(
        pair.pump_until_read(Side::Server, sibling),
        b"sibling-survived"
    );
}

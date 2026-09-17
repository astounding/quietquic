// SPDX-License-Identifier: 0BSD
//! In-memory scaffolding for driving a **cloaked** client/server pair with no
//! sockets, no runtime, and no threads.
//!
//! [`Pair`] wires two real [`Endpoint`]s to each other through `VecDeque<Vec<u8>>`
//! queues instead of UDP: [`Endpoint::poll_transmit`] output on one side becomes
//! [`Endpoint::handle_datagram`] input on the other, and
//! [`Endpoint::poll_event`] is drained into per-side buffers a test can inspect.
//! Every datagram therefore travels the genuine path — blinded selector DCID,
//! PSK-derived Initial keys, freshness and replay gates, rate limiter, CID
//! routing — so a handshake here proves the same thing a loopback handshake
//! would, minus the flakiness.
//!
//! # Why this is public
//!
//! It is not gated behind a `testing` feature, for two reasons. First, a feature
//! only this crate's own integration tests could turn on would need a
//! self-referential dev-dependency to work — real complexity for no benefit.
//! Second, the audience is wider than this crate: quietquic's whole point is
//! that an embedder can drive the core from a hand-rolled event loop, and an
//! embedder who does that needs a way to test their loop against a live peer
//! without opening a socket. [`Pair`] is that.
//!
//! Nothing here is on any production path, and nothing in the core depends on
//! it. It is also the only place in this crate that reads a clock on behalf of a
//! caller: [`connected_pair`] seeds the monotonic epoch with `Instant::now()`
//! ([`connected_pair_at`] lets a caller supply it instead), and [`Pair::new_at`]
//! passes [`crate::freshness::now_minutes()`] to
//! [`Endpoint::new_client`] as the selector's freshness minute. Both are
//! legitimate precisely because a test harness *is* the caller that owns the
//! clocks. The freshness minute has to be the real one here, because the
//! server's pre-filter checks it against the real wall clock.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Instant;

use quinn_proto::{Side, StreamId};

use crate::config::{ClientConfigFile, ServerSecrets};
use crate::conn::ConnState;
use crate::endpoint::Endpoint;
use crate::freshness::now_minutes;
use crate::outcome::{ConnectionHandle, DatagramOutcome, Event, ReadOutcome, WriteOutcome};

/// The PSK both sides of a [`Pair`] share. A fixed test value; it never leaves
/// this process.
pub const TESTING_PSK_HEX: &str =
    "00000000000000000000000000000000000000000000000000000000000000ab";

/// The client's apparent source address. Never bound — the pair moves bytes by
/// hand — but the endpoints route on it, so it has to be a real, distinct
/// address.
const CLIENT_ADDR: &str = "127.0.0.1:41001";
/// The server's address, as the client dials it.
const SERVER_ADDR: &str = "127.0.0.1:41002";

/// Upper bound on shuttling passes. A handshake settles in a handful; this only
/// exists so a regression fails fast instead of spinning forever.
const MAX_PASSES: usize = 256;

/// Read buffer size for [`Pair::pump_until_read`].
const READ_CHUNK: usize = 4096;

/// Two cloaked [`Endpoint`]s connected to each other through in-memory queues.
pub struct Pair {
    client: Endpoint,
    server: Endpoint,
    client_ch: ConnectionHandle,
    server_ch: Option<ConnectionHandle>,
    client_addr: SocketAddr,
    server_addr: SocketAddr,
    to_server: VecDeque<Vec<u8>>,
    to_client: VecDeque<Vec<u8>>,
    client_events: Vec<Event>,
    server_events: Vec<Event>,
    now: Instant,
}

/// Build a pair and drive it until the cloaked handshake settles.
///
/// Uses `Instant::now()` as the epoch of the pair's virtual clock; see
/// [`connected_pair_at`] to supply your own.
pub fn connected_pair() -> Pair {
    connected_pair_at(Instant::now())
}

/// [`connected_pair`] with a caller-supplied clock epoch.
pub fn connected_pair_at(epoch: Instant) -> Pair {
    let mut pair = Pair::new_at(epoch);
    pair.drive();
    assert!(
        pair.server_ch.is_some(),
        "the server never admitted the cloaked client — the pre-filter rejected an \
         authorized dial, or the handshake never started"
    );
    pair
}

impl Pair {
    /// Discard one pending client datagram, for deterministic loss tests.
    pub fn drop_next_client_datagram(&mut self) -> bool {
        self.client.poll_transmit(self.now).is_some()
    }

    /// Drive like [`Pair::drive`], but deliver each batch in reverse order.
    pub fn drive_reordered(&mut self) {
        for _ in 0..MAX_PASSES {
            let mut moved = false;
            while let Some(t) = self.client.poll_transmit(self.now) {
                self.to_server.push_back(t.contents);
                moved = true;
            }
            while let Some(t) = self.server.poll_transmit(self.now) {
                self.to_client.push_back(t.contents);
                moved = true;
            }
            while let Some(dg) = self.to_server.pop_back() {
                if let DatagramOutcome::Accepted(ch) =
                    self.server.handle_datagram(self.now, self.client_addr, &dg)
                {
                    self.server_ch = Some(ch);
                }
                moved = true;
            }
            while let Some(dg) = self.to_client.pop_back() {
                self.client.handle_datagram(self.now, self.server_addr, &dg);
                moved = true;
            }
            self.collect_events();
            if !moved {
                return;
            }
        }
        panic!("the reordered pair did not quiesce within {MAX_PASSES} passes");
    }

    /// Build an un-driven pair: the client has dialed, but nothing has moved.
    ///
    /// Use this to observe the handshake step by step; [`connected_pair`] is the
    /// shortcut for tests that only care about the connected state.
    pub fn new_at(epoch: Instant) -> Self {
        let client_addr: SocketAddr = CLIENT_ADDR.parse().expect("client addr");
        let server_addr: SocketAddr = SERVER_ADDR.parse().expect("server addr");

        let secrets: ServerSecrets = toml::from_str(&format!(
            "listen = \"{server_addr}\"\n\
             [[clients]]\n\
             client_id = \"testing\"\n\
             psk = \"{TESTING_PSK_HEX}\"\n"
        ))
        .expect("parse server secrets");
        let server = Endpoint::new_server(secrets).expect("server endpoint");

        let cfg: ClientConfigFile = toml::from_str(&format!(
            "client_id = \"testing\"\n\
             psk = \"{TESTING_PSK_HEX}\"\n\
             server = \"{server_addr}\"\n"
        ))
        .expect("parse client config");
        // The core never reads a clock, so the harness supplies both: `epoch` is
        // the monotonic instant, `now_minutes()` the wall-clock minute the
        // selector is stamped with. It must be the *real* minute — the server's
        // pre-filter freshness gate compares it against `SystemTime::now()`.
        let (client, client_ch) =
            Endpoint::new_client(epoch, now_minutes(), cfg).expect("client endpoint");

        Self {
            client,
            server,
            client_ch,
            server_ch: None,
            client_addr,
            server_addr,
            to_server: VecDeque::new(),
            to_client: VecDeque::new(),
            client_events: Vec::new(),
            server_events: Vec::new(),
            now: epoch,
        }
    }

    /// The pair's current virtual instant. Fixed unless [`Pair::fire_timers`]
    /// advances it, so a test never races a real clock.
    pub fn now(&self) -> Instant {
        self.now
    }

    /// The client endpoint.
    pub fn client(&mut self) -> &mut Endpoint {
        &mut self.client
    }

    /// The server endpoint.
    pub fn server(&mut self) -> &mut Endpoint {
        &mut self.server
    }

    /// The client's connection handle.
    pub fn client_ch(&self) -> ConnectionHandle {
        self.client_ch
    }

    /// The server's connection handle. Panics before the server has admitted the
    /// client.
    pub fn server_ch(&self) -> ConnectionHandle {
        self.server_ch
            .expect("the server has admitted a connection")
    }

    /// The named side's connection state. Panics if it has been reaped.
    pub fn conn(&mut self, side: Side) -> &mut ConnState {
        match side {
            Side::Client => {
                let ch = self.client_ch;
                self.client.conn_mut(ch).expect("live client connection")
            }
            Side::Server => {
                let ch = self.server_ch();
                self.server.conn_mut(ch).expect("live server connection")
            }
        }
    }

    /// Every [`Event`] the named side has surfaced so far, in order.
    pub fn events(&self, side: Side) -> &[Event] {
        match side {
            Side::Client => &self.client_events,
            Side::Server => &self.server_events,
        }
    }

    /// Take and clear the named side's events.
    pub fn take_events(&mut self, side: Side) -> Vec<Event> {
        match side {
            Side::Client => std::mem::take(&mut self.client_events),
            Side::Server => std::mem::take(&mut self.server_events),
        }
    }

    /// Shuttle datagrams (and drain events) until neither side has anything left
    /// to say. This is the pump: `poll_transmit` on one endpoint becomes
    /// `handle_datagram` on the other.
    pub fn drive(&mut self) {
        for _ in 0..MAX_PASSES {
            let mut moved = false;

            while let Some(t) = self.client.poll_transmit(self.now) {
                assert_eq!(
                    t.destination, self.server_addr,
                    "the client only ever transmits to the server it dialed"
                );
                self.to_server.push_back(t.contents);
                moved = true;
            }
            while let Some(t) = self.server.poll_transmit(self.now) {
                assert_eq!(
                    t.destination, self.client_addr,
                    "the server only ever transmits to the peer that reached it"
                );
                self.to_client.push_back(t.contents);
                moved = true;
            }

            while let Some(dg) = self.to_server.pop_front() {
                if let DatagramOutcome::Accepted(ch) =
                    self.server.handle_datagram(self.now, self.client_addr, &dg)
                {
                    self.server_ch = Some(ch);
                }
                moved = true;
            }
            while let Some(dg) = self.to_client.pop_front() {
                self.client.handle_datagram(self.now, self.server_addr, &dg);
                moved = true;
            }

            self.collect_events();
            if !moved {
                return;
            }
        }
        panic!("the in-memory cloaked pair did not quiesce within {MAX_PASSES} passes");
    }

    /// Advance the virtual clock to the earliest pending deadline on either side
    /// and fire it. A no-op when neither side has a timer.
    ///
    /// This is how a test completes a transition that only a timer can complete —
    /// most importantly the close timer that drives a locally-closed connection
    /// to `Drained`.
    pub fn fire_timers(&mut self) {
        let earliest = [self.client.next_timeout(), self.server.next_timeout()]
            .into_iter()
            .flatten()
            .min();
        let Some(at) = earliest else {
            return;
        };
        self.now = self.now.max(at);
        self.client.handle_timeout(self.now);
        self.server.handle_timeout(self.now);
        self.collect_events();
    }

    /// Open a bidirectional stream on the named side.
    pub fn open_bi(&mut self, side: Side) -> StreamId {
        self.conn(side).open_bi().expect("open_bi")
    }

    /// Drive until the named side has a peer-opened stream to accept, then take
    /// it. Panics if none arrives.
    pub fn accept_bi(&mut self, side: Side) -> StreamId {
        for _ in 0..MAX_PASSES {
            if let Some(id) = self.conn(side).accept_bi().expect("accept_bi") {
                return id;
            }
            self.drive();
        }
        panic!("no stream was accepted within {MAX_PASSES} passes");
    }

    /// Write every byte of `data` to `id`, driving the pair whenever flow control
    /// blocks — the loop a caller driving the core by hand would write.
    pub fn write_all(&mut self, side: Side, id: StreamId, data: &[u8]) {
        let mut sent = 0;
        for _ in 0..MAX_PASSES {
            if sent == data.len() {
                return;
            }
            match self.conn(side).stream_write(id, &data[sent..]) {
                Ok(WriteOutcome::Wrote(n)) => sent += n,
                Ok(WriteOutcome::Blocked) => self.drive(),
                Err(e) => panic!("stream_write failed: {e}"),
            }
        }
        panic!("write_all did not complete within {MAX_PASSES} passes");
    }

    /// Read `id` on the named side until the peer finishes it, driving the pair
    /// on every [`ReadOutcome::Blocked`], and return everything that arrived.
    ///
    /// This is the counterpart of the tokio wrapper's `read_to_end`, which the
    /// core deliberately does not offer: waiting is the caller's job, so the
    /// loop lives here in the test harness rather than in the API.
    pub fn pump_until_read(&mut self, side: Side, id: StreamId) -> Vec<u8> {
        let mut got = Vec::new();
        let mut buf = vec![0u8; READ_CHUNK];
        for _ in 0..MAX_PASSES {
            match self.conn(side).stream_read(id, &mut buf) {
                Ok(ReadOutcome::Read(n)) => got.extend_from_slice(&buf[..n]),
                Ok(ReadOutcome::Blocked) => self.drive(),
                Ok(ReadOutcome::Finished) => return got,
                Err(e) => panic!("stream_read failed: {e}"),
            }
        }
        panic!("pump_until_read never reached end-of-stream within {MAX_PASSES} passes");
    }

    fn collect_events(&mut self) {
        while let Some(e) = self.client.poll_event() {
            self.client_events.push(e);
        }
        while let Some(e) = self.server.poll_event() {
            self.server_events.push(e);
        }
    }
}

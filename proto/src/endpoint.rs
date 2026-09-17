// SPDX-License-Identifier: 0BSD
//! The sans-IO endpoint: a cloaked QUIC state machine the caller drives.
//!
//! Nothing here performs I/O, spawns, blocks, or reads the monotonic clock. The
//! caller owns the socket and passes `now` explicitly, which is what makes
//! quietquic embeddable in a hand-rolled event loop (see
//! `examples/poll_loop.rs`).
//!
//! # The silence invariant
//!
//! **A datagram that fails the cloaking pre-filter queues nothing to send.**
//!
//! [`Endpoint::handle_datagram`] runs the full pre-filter — rate limiter,
//! long-header parse, exact selector length, PSK selector match, freshness,
//! anti-replay — *before* the datagram is allowed anywhere near quinn-proto. A
//! packet that fails any of those gates returns from `handle_datagram` without
//! ever reaching quinn-proto, so nothing can have been queued: a caller
//! who faithfully drains [`Endpoint::poll_transmit`] still emits zero bytes to
//! an unauthorized peer. Invisibility is therefore a property of this API, not
//! of the caller's control flow — a C-style embedder cannot reply by mistake.
//!
//! The invariant is about pre-filter *rejection*, not about the
//! [`DatagramOutcome::Dropped`] return value. `Dropped` is the wider category
//! "this datagram produced no connection handle for you", and a datagram that
//! *passed* the pre-filter can legitimately be `Dropped` while still queuing an
//! endpoint-level response — the reachable case is a peer that proved PSK
//! possession but asked for a QUIC version we do not speak, which earns a
//! Version Negotiation packet. That peer is authorized, so answering it does not
//! break silence. See [`DatagramOutcome::Dropped`].
//!
//! # The driving contract
//!
//! One pass of a caller's loop looks like this, and **the order matters**:
//!
//! ```text
//! // 1. feed every inbound datagram
//! ep.handle_datagram(now, from, &pkt);
//! // 2. service the timer deadline, if it is due
//! if ep.next_timeout().is_some_and(|t| t <= now) { ep.handle_timeout(now) }
//! // 3. drain ALL events, doing the caller's stream work as they arrive
//! while let Some(e) = ep.poll_event() { react(e) /* stream_read / stream_write / ... */ }
//! // 4. drain ALL transmits — LAST, after every stream operation of this pass
//! while let Some(t) = ep.poll_transmit(now) { send_to(&t.contents, t.destination) }
//! // 5. only now is the deadline meaningful
//! sleep_until(ep.next_timeout());
//! ```
//!
//! Two obligations, both load-bearing:
//!
//! * **[`Endpoint::poll_transmit`] must be drained to `None`, and it must come
//!   after the caller's stream work.** It is the only method that services
//!   connections lazily, so a caller who stops early — or who drains transmits
//!   *before* calling `stream_read`/`stream_write` — leaves stream bytes and
//!   flow-control credit unsent. Draining first is the classic sans-IO stall:
//!   the credit a `stream_read` released never reaches the peer, the peer stays
//!   blocked, nothing arrives to wake the loop, and the connection hangs until
//!   the idle timeout.
//! * **[`Endpoint::next_timeout`] is read last.** It is refreshed by every
//!   servicing pass, so it only reflects the current pass once `poll_transmit`
//!   has returned `None`.
//!
//! The first obligation is also enforced rather than merely documented: any
//! caller-side stream operation that can produce something to send marks its
//! connection *dirty*, and while any connection is dirty `next_timeout()`
//! returns an already-elapsed deadline. A caller that gets the order wrong and
//! sleeps on that value wakes immediately instead of sleeping through the stall.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use quinn_proto::{
    ClientConfig as TransportClientConfig, ConnectionHandle as QuinnConnectionHandle, ConnectionId,
    DatagramEvent, EndpointConfig as QuinnEndpointConfig, ServerConfig as TransportServerConfig,
    VarInt,
};

use crate::config::{
    ClientConfigFile, ConfigError, EndpointConfig, Psk, ServerSecrets, TransportSettings,
};
use crate::conn::ConnState;
use crate::crypto::{
    quic_client_config, random_bytes, reset_key, token_key, RecordingCidGenerator, SelfSigned,
};
use crate::freshness::{is_fresh, now_minutes, WINDOW_MINUTES};
use crate::initial_keys::{PskClientConfig, PskServerConfig};
use crate::outcome::{
    ConnectionError, ConnectionHandle, ConnectionHandleError, DatagramOutcome, Event, Transmit,
};
use crate::ratelimit::RateLimiter;
use crate::replay::ReplayGuard;
use crate::selector::{build_dcid, parse_dcid, selector_matches, DcidParts, DCID_LEN};
use crate::transport::peek_dcid;

/// Length of the connection IDs this endpoint issues for its own connections.
pub const LOCAL_CID_LEN: usize = 8;

/// The TLS server name a client presents in its ClientHello. It authenticates
/// nothing (the PSK does, and certificate verification is skipped), so any
/// stable name works; `localhost` matches the server's self-signed identity.
const SERVER_NAME: &str = "localhost";

/// Which side of the protocol an [`Endpoint`] plays.
///
/// The distinction is load-bearing in exactly two places, both in
/// [`Endpoint::handle_datagram`]/[`Endpoint::feed`]: only a server runs the
/// cloaking pre-filter (a client has no PSK *set* to select from — it dials one
/// known server), and only a server may emit an endpoint-level response.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    Server,
    Client,
}

/// One authorized client's PSK and its PSK-rekeyed transport config.
struct ClientCrypto {
    client_id: String,
    psk: Psk,
    server_config: Arc<TransportServerConfig>,
}

/// Build one PSK-rekeyed transport `ServerConfig` per authorized client.
fn build_clients(
    entries: &[crate::config::ClientEntry],
    transport: &TransportSettings,
) -> Result<Vec<ClientCrypto>, ConfigError> {
    let mut client_ids = HashSet::new();
    let mut psks = HashSet::new();
    for entry in entries {
        if entry.client_id.trim().is_empty() {
            return Err(ConfigError::Invalid("client_id must not be empty".into()));
        }
        if !client_ids.insert(entry.client_id.clone()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate client_id {:?}",
                entry.client_id
            )));
        }
        if !psks.insert(*entry.psk.as_bytes()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate PSK makes client identity ambiguous (client_id {:?})",
                entry.client_id
            )));
        }
    }

    // A single self-signed identity is fine: the PSK — not the TLS certificate —
    // authenticates the peer (the client skips cert verification). One identity
    // shared across PSKs keeps setup trivial.
    let identity = SelfSigned::generate()
        .map_err(|e| ConfigError::Io(std::io::Error::other(format!("tls identity: {e}"))))?;
    let quic_server = identity
        .quic_server_config()
        .map_err(|e| ConfigError::Io(std::io::Error::other(format!("quic server config: {e}"))))?;

    let token_key = token_key();

    let mut clients = Vec::with_capacity(entries.len());
    for entry in entries {
        let psk = *entry.psk.as_bytes();
        let crypto = Arc::new(PskServerConfig::new(quic_server.clone(), psk));
        let mut server_config = TransportServerConfig::new(crypto, token_key.clone());
        server_config.transport_config(transport.arc());
        let server_config = Arc::new(server_config);
        clients.push(ClientCrypto {
            client_id: entry.client_id.clone(),
            psk: entry.psk.clone(),
            server_config,
        });
    }
    Ok(clients)
}

/// An `EndpointConfig` whose CID generator records every CID it mints.
///
/// Two consumers, one generator: `issued` is THE routing table that lets
/// post-handshake packets bypass the pre-filter, and `pending` preserves mint
/// order so a freshly minted CID can be attributed to the connection being
/// serviced (see [`Endpoint::drain_pending_cids`]). quinn-proto builds one
/// shared generator per endpoint and exposes no CID→connection accessor, so
/// this is the only way to know who owns a CID.
fn recording_endpoint_config(
    issued: &Arc<Mutex<HashSet<ConnectionId>>>,
    pending: &Arc<Mutex<Vec<ConnectionId>>>,
) -> QuinnEndpointConfig {
    let mut endpoint_config = QuinnEndpointConfig::new(Arc::new(reset_key()));
    let recorder = issued.clone();
    let pending = pending.clone();
    endpoint_config.cid_generator(move || {
        Box::new(RecordingCidGenerator::new(
            LOCAL_CID_LEN,
            recorder.clone(),
            pending.clone(),
        ))
    });
    endpoint_config
}

/// A cloaked QUIC endpoint, driven entirely by the caller.
pub struct Endpoint {
    inner: quinn_proto::Endpoint,
    role: Role,
    config: EndpointConfig,
    admission_paused: bool,
    pending_incoming: HashMap<ConnectionHandle, Instant>,
    handshake_deadlines: HashMap<ConnectionHandle, Instant>,
    loss_overrides: HashMap<ConnectionHandle, ConnectionError>,
    cleanup_deadlines: HashMap<ConnectionHandle, Instant>,
    terminal_connections: HashMap<ConnectionHandle, ConnectionError>,
    service_cursor: usize,
    needs_service: bool,
    terminal: Option<ConnectionError>,
    /// Authorized clients. Empty for [`Role::Client`].
    clients: Vec<ClientCrypto>,
    /// Per-client anti-replay guards (index-aligned with `clients`).
    replay: Vec<ReplayGuard>,
    /// Bounds the CPU cost of the unauthenticated pre-filter path: consulted
    /// BEFORE any selector/MAC work, so a flood is dropped at near-zero cost.
    rate_limiter: RateLimiter,
    /// Every CID this endpoint has issued, for routing post-handshake packets
    /// without re-running the pre-filter. A CID leaves this set in exactly one
    /// place — [`Endpoint::prune_connection_cids`], strictly after its owning
    /// connection has been reaped — so a live CID can never stop routing.
    issued_cids: Arc<Mutex<HashSet<ConnectionId>>>,
    /// Mint order, so a freshly issued CID can be attributed to the connection
    /// being serviced. Drained immediately after every endpoint call that can
    /// mint (`connect`, `accept`, per-handle `handle_event`).
    pending_cids: Arc<Mutex<Vec<ConnectionId>>>,
    /// CIDs in `issued_cids` grouped by owner, so a lost connection's — and only
    /// a lost connection's — CIDs can be pruned. Never touched for a live one.
    cids_by_conn: HashMap<ConnectionHandle, Vec<ConnectionId>>,
    /// Live connections, each wrapped in the [`ConnState`] that owns its
    /// per-stream bookkeeping.
    connections: HashMap<ConnectionHandle, ConnState>,
    /// Authenticated server-side identity, derived from the unique PSK entry.
    client_ids: HashMap<ConnectionHandle, String>,
    /// Current generation-safe handle for each live Quinn slab handle.
    by_quinn: HashMap<QuinnConnectionHandle, ConnectionHandle>,
    /// Monotonic source for public handle generations. Zero is never issued.
    next_generation: u64,
    /// Datagrams the caller should send, drained by [`Endpoint::poll_transmit`].
    outbound: VecDeque<Transmit>,
    /// Things the caller should react to, drained by [`Endpoint::poll_event`].
    events: VecDeque<Event>,
    /// Earliest connection deadline, recomputed by every servicing pass. Cached
    /// because quinn-proto's `Connection::poll_timeout` takes `&mut self` even
    /// though it only reads `timers.next_timeout()`, and [`Endpoint::next_timeout`]
    /// is a `&self` query.
    next_timeout: Option<Instant>,
    /// The `now` of the most recent servicing pass (or of the dial, for a
    /// client). Used as the "already elapsed" deadline [`Endpoint::next_timeout`]
    /// reports while a connection is dirty: the caller's clock is monotonic and
    /// non-decreasing, so an instant it already handed us can never be in the
    /// future.
    last_service: Option<Instant>,
}

impl Endpoint {
    /// Build an endpoint without initiating a connection.
    pub fn new(config: EndpointConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        let clients = build_clients(&config.credentials, &config.transport)?;
        let issued_cids = Arc::new(Mutex::new(HashSet::new()));
        let pending_cids = Arc::new(Mutex::new(Vec::new()));
        let endpoint_config = recording_endpoint_config(&issued_cids, &pending_cids);
        let inner = quinn_proto::Endpoint::new(Arc::new(endpoint_config), None, true, None);
        let replay = clients
            .iter()
            .map(|_| ReplayGuard::new(WINDOW_MINUTES))
            .collect();
        let role = if config.capability.can_accept() {
            Role::Server
        } else {
            Role::Client
        };
        Ok(Self {
            inner,
            role,
            config,
            admission_paused: false,
            pending_incoming: HashMap::new(),
            handshake_deadlines: HashMap::new(),
            loss_overrides: HashMap::new(),
            service_cursor: 0,
            cleanup_deadlines: HashMap::new(),
            terminal_connections: HashMap::new(),
            needs_service: false,
            terminal: None,
            clients,
            replay,
            rate_limiter: RateLimiter::new(),
            issued_cids,
            pending_cids,
            cids_by_conn: HashMap::new(),
            connections: HashMap::new(),
            client_ids: HashMap::new(),
            by_quinn: HashMap::new(),
            next_generation: 1,
            outbound: VecDeque::new(),
            events: VecDeque::new(),
            next_timeout: None,
            last_service: None,
        })
    }

    /// Start another outgoing connection on this endpoint.
    pub fn connect(
        &mut self,
        now: Instant,
        freshness_minute: u32,
        cfg: ClientConfigFile,
        transport_override: Option<TransportSettings>,
        handshake_timeout_override: Option<Duration>,
    ) -> Result<ConnectionHandle, ConfigError> {
        if !self.config.capability.can_dial() {
            return Err(ConfigError::Invalid(
                "endpoint does not have dial capability".into(),
            ));
        }
        if self.terminal.is_some() {
            return Err(ConfigError::Invalid("endpoint is closed".into()));
        }
        let timeout = handshake_timeout_override.unwrap_or(self.config.outgoing_handshake_timeout);
        if timeout.is_zero() {
            return Err(ConfigError::Invalid(
                "outgoing handshake timeout must be finite and positive".into(),
            ));
        }
        if timeout > crate::config::MAX_ENDPOINT_DURATION {
            return Err(ConfigError::Invalid(
                "outgoing handshake timeout exceeds the 24 hour maximum".into(),
            ));
        }
        let handshake_deadline = deadline(now, timeout)?;
        let psk = *cfg.psk.as_bytes();
        let dcid = build_dcid(&psk, random_bytes::<8>(), freshness_minute);
        let crypto = quic_client_config()
            .map_err(|e| ConfigError::Io(std::io::Error::other(format!("client crypto: {e}"))))?;
        let mut config = TransportClientConfig::new(Arc::new(PskClientConfig::new(crypto, psk)));
        config.transport_config(
            transport_override
                .unwrap_or_else(|| self.config.transport.clone())
                .arc(),
        );
        config.initial_dst_cid_provider(Arc::new(move || ConnectionId::new(&dcid)));
        let (quinn_ch, conn) = match self.inner.connect(now, config, cfg.server, SERVER_NAME) {
            Ok(connection) => connection,
            Err(error) => {
                self.sweep_orphan_cids();
                return Err(ConfigError::Io(std::io::Error::other(format!(
                    "connect: {error}"
                ))));
            }
        };
        let ch = self.allocate_handle(quinn_ch);
        self.drain_pending_cids(ch);
        self.connections.insert(ch, ConnState::new(conn));
        self.handshake_deadlines.insert(ch, handshake_deadline);
        self.last_service = Some(now);
        self.refresh_next_timeout();
        Ok(ch)
    }

    /// Build a server endpoint from parsed secrets.
    pub fn new_server(secrets: ServerSecrets) -> Result<Self, ConfigError> {
        Self::new(EndpointConfig::accept(secrets.clients))
    }

    /// Build a client endpoint and start dialing the server named in `cfg`.
    ///
    /// Returns the endpoint plus the handle of the one connection it owns. The
    /// handshake has NOT completed yet: the caller drives it by pumping
    /// [`Endpoint::poll_transmit`] / [`Endpoint::handle_datagram`] until
    /// [`Event::Connected`] arrives.
    ///
    /// Two seams make the dial *cloaked* rather than stock QUIC, and both are
    /// installed here:
    ///
    /// * **Initial DCID** — instead of a random first-flight DCID, it is
    ///   `build_dcid(psk, nonce, freshness)`, installed via
    ///   [`quinn_proto::ClientConfig::initial_dst_cid_provider`]. The server's
    ///   pre-filter recomputes that selector from its own PSK and admits only a
    ///   match; everything else is dropped in silence.
    /// * **Initial packet keys** — [`PskClientConfig`] overrides *only*
    ///   `initial_keys`, deriving them from the PSK instead of the published
    ///   version salt, so an observer cannot unseal the ClientHello.
    ///
    /// Everything after the Initial is ordinary QUIC v1.
    ///
    /// # `now` and `freshness_minute`
    ///
    /// **The core never reads a clock — neither of them.** Both of the clocks a
    /// cloaked dial needs are supplied by the caller:
    ///
    /// * `now` is the current *monotonic* instant, because
    ///   `quinn_proto::Endpoint::connect` seeds the connection's timers from it,
    ///   exactly as for [`Endpoint::handle_datagram`].
    /// * `freshness_minute` is the current *wall-clock* minute — Unix seconds
    ///   divided by 60 — which goes into the selector DCID and which the
    ///   server's pre-filter re-derives and checks against its own clock (see
    ///   [`crate::freshness`]). Production callers pass
    ///   [`crate::freshness::now_minutes()`]; a test passes whatever it likes,
    ///   which is what makes a stale-selector case reproducible rather than a
    ///   race against the wall clock, and what lets a C embedder own both
    ///   clocks.
    pub fn new_client(
        now: Instant,
        freshness_minute: u32,
        cfg: ClientConfigFile,
    ) -> Result<(Self, ConnectionHandle), ConfigError> {
        let mut endpoint = Self::new(EndpointConfig::dial())?;
        let ch = endpoint.connect(now, freshness_minute, cfg, None, None)?;
        Ok((endpoint, ch))
    }

    /// Feed one inbound datagram.
    ///
    /// A packet that **fails the cloaking pre-filter** returns
    /// [`DatagramOutcome::Dropped`] having queued nothing to send, and having
    /// touched no connection state. That is the silence invariant, and it is why
    /// every early return below happens before `feed`.
    ///
    /// A packet that *passes* the pre-filter may still be `Dropped` — see
    /// [`DatagramOutcome::Dropped`] — and such a packet can queue an
    /// endpoint-level response, because its sender has already proved PSK
    /// possession.
    pub fn handle_datagram(
        &mut self,
        now: Instant,
        from: SocketAddr,
        data: &[u8],
    ) -> DatagramOutcome {
        if self.terminal.is_some() {
            return DatagramOutcome::Dropped;
        }
        let mut admitted_client = None;
        if self.role == Role::Server && !self.is_active_dcid(data) {
            if self.admission_paused
                || !self.config.capability.can_accept()
                || self.pending_incoming.len() >= self.config.max_pending_incoming
            {
                return DatagramOutcome::Dropped;
            }
            // NEW-CONNECTION ATTEMPT: run the silence pre-filter.
            //
            // The rate limiter is consulted BEFORE any selector/MAC work, so a
            // flood of junk — from one source or sprayed across many spoofed
            // sources — is dropped at near-zero cost. UDP source IPs are
            // spoofable, so the GLOBAL bucket is the real backstop. Only
            // would-be-new connections reach here; already-authenticated
            // post-handshake traffic is never throttled.
            if !self.rate_limiter.check(from.ip(), now) {
                return DatagramOutcome::Dropped;
            }
            let Some(client_idx) = self.prefilter(data) else {
                return DatagramOutcome::Dropped;
            };
            // Install the matching PSK's server config so `handle_first_packet`
            // derives the correct PSK Initial keys.
            self.inner
                .set_server_config(Some(self.clients[client_idx].server_config.clone()));
            admitted_client = Some(client_idx);
        }

        let outcome = self.feed(now, from, data, admitted_client);
        // The datagram was admitted, so whatever it unblocked — a handshake
        // step, stream data, a lifecycle transition — is turned into queued
        // transmits and events right here, before the caller polls either.
        self.service_connections(now);
        outcome
    }

    /// Take the next datagram the caller should send, if any.
    ///
    /// Services every live connection when the outbound queue runs dry, which is
    /// what turns caller-side stream work (`conn_mut(ch).stream_write(..)`) into
    /// bytes on the wire. A caller MUST loop this until it returns `None` each
    /// pass; skipping it stalls connections silently.
    pub fn poll_transmit(&mut self, now: Instant) -> Option<Transmit> {
        if let Some(t) = self.outbound.pop_front() {
            return Some(t);
        }
        self.service_connections(now);
        self.outbound.pop_front()
    }

    /// Take the next thing the caller should react to, if any.
    ///
    /// Events are produced by [`Endpoint::handle_datagram`],
    /// [`Endpoint::handle_timeout`] and [`Endpoint::poll_transmit`] — this is a
    /// pure drain, which is why it needs no `now`.
    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// The earliest instant at which [`Endpoint::handle_timeout`] must be called,
    /// or `None` if no connection has a pending timer.
    ///
    /// Recomputed at the end of every servicing pass, so read it *after* draining
    /// [`Endpoint::poll_transmit`] to `None` — that is the point at which every
    /// connection's timers reflect all the work of the current pass.
    ///
    /// # The dirty guarantee
    ///
    /// If any caller-side stream operation that can produce something to send —
    /// [`ConnState::stream_read`] (which releases flow-control credit),
    /// [`ConnState::stream_write`], [`ConnState::stream_finish`],
    /// [`ConnState::stream_reset`], [`ConnState::stream_stop`] — has run since
    /// the last servicing pass, this returns a deadline that has **already
    /// elapsed**. A caller that gets the loop order wrong and sleeps here is
    /// therefore forced straight back around the loop instead of sleeping
    /// through a stall: the credit or the bytes reach the peer on the next
    /// `poll_transmit` drain rather than at the idle timeout.
    ///
    /// The flag is cleared by the servicing pass that flushes the work, so the
    /// elapsed deadline is reported at most until the next
    /// `poll_transmit`-to-`None`.
    pub fn next_timeout(&self) -> Option<Instant> {
        if self.needs_service || self.connections.values().any(ConnState::is_dirty) {
            // `last_service` is an instant the caller itself handed us on an
            // earlier pass, so on a monotonic clock it is necessarily <= "now".
            // It is always `Some` whenever a connection exists (set by the dial
            // and by every servicing pass); the `or` is belt and braces.
            return self.last_service.or(self.next_timeout);
        }
        self.next_timeout
    }

    /// Earliest transport/handshake/cleanup timer, excluding deferred local work.
    ///
    /// A socket driver whose own send queue is full can use this deadline to
    /// avoid spinning on [`Endpoint::next_timeout`]'s immediate work wakeup
    /// while still servicing protocol timers.
    pub fn next_protocol_timeout(&self) -> Option<Instant> {
        self.next_timeout
    }

    /// Advance every connection whose timer has expired, then service.
    pub fn handle_timeout(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .handshake_deadlines
            .iter()
            .filter_map(|(ch, at)| (*at <= now).then_some(*ch))
            .collect();
        for ch in expired {
            self.loss_overrides.insert(ch, ConnectionError::TimedOut);
            self.handshake_deadlines.remove(&ch);
            let _ = self.close_connection(now, ch, VarInt::from_u32(0), Vec::new());
        }
        let cleanup_expired: Vec<_> = self
            .cleanup_deadlines
            .iter()
            .filter_map(|(ch, at)| (*at <= now).then_some(*ch))
            .collect();
        for ch in cleanup_expired {
            let reason = self
                .terminal_connections
                .get(&ch)
                .cloned()
                .or_else(|| self.loss_overrides.get(&ch).cloned())
                .unwrap_or(ConnectionError::LocallyClosed);
            let _ = self.force_remove(now, ch, reason);
        }
        for state in self.connections.values_mut() {
            if state.conn_mut().poll_timeout().is_some_and(|t| t <= now) {
                state.conn_mut().handle_timeout(now);
            }
        }
        self.service_connections(now);
    }

    /// Borrow a live connection for stream work, or `None` if `ch` is not (or is
    /// no longer) a connection of this endpoint.
    ///
    /// A `None` here after an [`Event::ConnectionLost`] is not an error: the
    /// endpoint has reaped the connection and the handle is dead.
    ///
    /// Handles are generation-tagged. After [`Event::ConnectionLost`], a stale
    /// handle always returns `None`, even if Quinn has reused its internal slab
    /// slot for a later connection.
    pub fn conn_mut(&mut self, ch: ConnectionHandle) -> Option<&mut ConnState> {
        if self.terminal_connections.contains_key(&ch) {
            return None;
        }
        self.connections.get_mut(&ch)
    }

    pub fn config(&self) -> &EndpointConfig {
        &self.config
    }

    pub fn set_transport_defaults(&mut self, transport: TransportSettings) {
        for client in &mut self.clients {
            let mut server = (*client.server_config).clone();
            server.transport_config(transport.arc());
            client.server_config = Arc::new(server);
        }
        self.config.transport = transport;
    }

    pub fn pause_admission(&mut self) {
        self.admission_paused = true;
    }

    pub fn resume_admission(&mut self) {
        self.admission_paused = false;
    }

    pub fn admission_paused(&self) -> bool {
        self.admission_paused
    }

    /// Number of inbound handshakes/connections still occupying admission slots.
    pub fn pending_incoming_count(&self) -> usize {
        self.pending_incoming.len()
    }

    pub fn mark_accepted(&mut self, ch: ConnectionHandle) -> Result<(), ConnectionHandleError> {
        if !self.connections.contains_key(&ch) || self.terminal_connections.contains_key(&ch) {
            return Err(ConnectionHandleError);
        }
        self.pending_incoming.remove(&ch);
        Ok(())
    }

    /// Close inbound connections that have not crossed the application handoff point.
    pub fn close_pending_incoming(&mut self, now: Instant) {
        let handles: Vec<_> = self.pending_incoming.keys().copied().collect();
        for ch in handles {
            let _ = self.close_connection(now, ch, VarInt::from_u32(0), Vec::new());
        }
    }

    pub fn connections(&self) -> impl Iterator<Item = ConnectionHandle> + '_ {
        self.connections
            .keys()
            .filter(|ch| !self.terminal_connections.contains_key(ch))
            .copied()
    }

    /// True only after all live and terminal transport state has been drained.
    pub fn is_idle(&self) -> bool {
        self.connections.is_empty()
    }

    /// Internal cleanup work still retaining Quinn endpoint state.
    pub fn cleanup_handles(&self) -> impl Iterator<Item = ConnectionHandle> + '_ {
        self.terminal_connections.keys().copied()
    }

    pub fn endpoint_error(&self) -> Option<&ConnectionError> {
        self.terminal.as_ref()
    }

    /// Begin terminal graceful shutdown of every connection.
    ///
    /// Application-visible loss is immediate. Transport state remains retained
    /// until Quinn drains it or the configured cleanup deadline is forced.
    pub fn close(&mut self, now: Instant) {
        if self.terminal.is_some() {
            return;
        }
        self.terminal = Some(ConnectionError::LocallyClosed);
        let handles: Vec<_> = self.connections.keys().copied().collect();
        for ch in handles {
            let _ = self.close_connection(now, ch, VarInt::from_u32(0), Vec::new());
        }
        self.admission_paused = true;
        self.refresh_next_timeout();
    }

    /// Record a terminal endpoint failure and begin bounded transport cleanup.
    pub fn fail(&mut self, now: Instant, message: impl Into<String>, raw_os_error: Option<i32>) {
        if self.terminal.is_some() {
            return;
        }
        let reason = ConnectionError::EndpointFailed {
            message: message.into(),
            raw_os_error,
        };
        self.terminal = Some(reason.clone());
        let handles: Vec<_> = self.connections.keys().copied().collect();
        for ch in handles {
            if let Some(state) = self.connections.get_mut(&ch) {
                state
                    .conn_mut()
                    .close(now, VarInt::from_u32(0), bytes::Bytes::new());
            }
            self.mark_terminal(ch, reason.clone(), now);
        }
        self.admission_paused = true;
        self.refresh_next_timeout();
    }

    pub fn close_connection(
        &mut self,
        now: Instant,
        ch: ConnectionHandle,
        code: VarInt,
        reason: Vec<u8>,
    ) -> Result<(), ConnectionHandleError> {
        let state = self.connections.get_mut(&ch).ok_or(ConnectionHandleError)?;
        state.conn_mut().close(now, code, reason.into());
        self.handshake_deadlines.remove(&ch);
        let reason = self
            .loss_overrides
            .remove(&ch)
            .unwrap_or(ConnectionError::LocallyClosed);
        self.mark_terminal(ch, reason, now);
        self.service_connections(now);
        Ok(())
    }

    /// Finish transport cleanup for one connection without advancing siblings' clocks.
    pub fn force_remove(
        &mut self,
        now: Instant,
        ch: ConnectionHandle,
        reason: ConnectionError,
    ) -> Result<(), ConnectionHandleError> {
        if !self.connections.contains_key(&ch) {
            return Err(ConnectionHandleError);
        }
        if let Some(state) = self.connections.get_mut(&ch) {
            state
                .conn_mut()
                .close(now, VarInt::from_u32(0), bytes::Bytes::new());
        }
        for _ in 0..64 {
            let Some(state) = self.connections.get_mut(&ch) else {
                break;
            };
            let Some(t) = state.conn_mut().poll_timeout() else {
                break;
            };
            state.conn_mut().handle_timeout(t);
            while let Some(ev) = state.conn_mut().poll_endpoint_events() {
                let cev = self.inner.handle_event(ch.quinn, ev);
                if let Some(cev) = cev {
                    state.conn_mut().handle_event(cev);
                }
            }
            if state.is_drained() {
                break;
            }
        }
        debug_assert!(
            self.connections.get(&ch).is_some_and(ConnState::is_drained),
            "forced cleanup did not drain Quinn state"
        );
        self.remove_connection(ch, reason);
        self.refresh_next_timeout();
        Ok(())
    }

    /// Authenticated server-side client identity for a live connection.
    ///
    /// The identity is the unique configuration entry whose PSK admitted the
    /// connection. Client endpoints return `None`.
    pub fn client_id(&self, ch: ConnectionHandle) -> Option<&str> {
        self.client_ids.get(&ch).map(String::as_str)
    }

    /// How many connection IDs currently route to a live connection.
    ///
    /// Observability for the routing set: it must grow as connections are
    /// admitted and shrink back when they are lost. A count that never returns
    /// to its baseline across many connections is the CID leak this endpoint's
    /// `cids_by_conn` bookkeeping exists to prevent.
    pub fn issued_cid_count(&self) -> usize {
        self.issued_cids.lock().expect("issued_cids poisoned").len()
    }

    // -- internals ---------------------------------------------------------

    /// True iff `data`'s DCID belongs to a connection we created.
    fn is_active_dcid(&self, data: &[u8]) -> bool {
        let Some(dcid) = read_dcid_any(data) else {
            return false;
        };
        let cid = ConnectionId::new(dcid);
        self.issued_cids
            .lock()
            .expect("issued_cids poisoned")
            .contains(&cid)
    }

    /// The full pre-filter. `Some(client_index)` admits; `None` drops silently.
    ///
    /// Order is load-bearing: freshness is checked BEFORE the replay guard so
    /// future-dated nonces cannot accumulate in the replay set.
    fn prefilter(&mut self, data: &[u8]) -> Option<usize> {
        // 1. long-header parse
        let dcid = peek_dcid(data)?;
        // 2. exact selector length
        if dcid.len() != DCID_LEN {
            return None;
        }
        let parts = parse_dcid(dcid)?;
        // 3. freshness (enforces BOTH bounds; before the replay guard)
        let minute = now_minutes();
        if !is_fresh(parts.freshness, minute, WINDOW_MINUTES) {
            return None;
        }
        // 4. select PSK
        let client_idx = self.select_psk(&parts)?;
        // 5. anti-replay (lower-bound enforcement + dedupe)
        if !self.replay[client_idx].check_and_record(parts.nonce, parts.freshness, minute) {
            return None;
        }
        Some(client_idx)
    }

    /// Iterate the client set, returning the index of the PSK whose selector
    /// matches. Each `selector_matches` compare is constant-time.
    fn select_psk(&self, parts: &DcidParts) -> Option<usize> {
        self.clients
            .iter()
            .position(|c| selector_matches(c.psk.as_bytes(), parts))
    }

    /// Hand an admitted datagram to quinn-proto and queue anything it emits.
    fn feed(
        &mut self,
        now: Instant,
        from: SocketAddr,
        data: &[u8],
        admitted_client: Option<usize>,
    ) -> DatagramOutcome {
        let mut resp = Vec::new();
        let event = self
            .inner
            .handle(now, from, None, None, BytesMut::from(data), &mut resp);

        // An endpoint-level response is queued ONLY for a server, where it can
        // only ever target an admitted/active peer because junk never reaches
        // this point. A CLIENT endpoint has no such guarantee — it feeds every
        // datagram it receives — and quinn-proto's `handle` will happily produce
        // a stateless reset for an unrecognized short-header packet. Answering
        // one would turn the client into a fingerprinting oracle: an off-path
        // prober could confirm "something QUIC-ish lives here" by eliciting a
        // reply. A client has nothing legitimate to say at the endpoint level,
        // so it says nothing.
        if !resp.is_empty()
            && self.role == Role::Server
            && self.outbound.len() < self.config.max_outbound_datagrams
        {
            self.outbound.push_back(Transmit {
                destination: from,
                contents: resp,
            });
        }

        let outcome = match event {
            Some(DatagramEvent::NewConnection(incoming)) => {
                // Unreachable for a client (quinn-proto only mints this with a
                // server config installed), but accepting would be nonsense, so
                // say so structurally rather than relying on that.
                match self.role {
                    Role::Server => {
                        let Some(client_idx) = admitted_client else {
                            return DatagramOutcome::Dropped;
                        };
                        self.admit(now, incoming, client_idx)
                    }
                    Role::Client => DatagramOutcome::Dropped,
                }
            }
            Some(DatagramEvent::ConnectionEvent(quinn_ch, cev)) => {
                match self.by_quinn.get(&quinn_ch).copied() {
                    Some(ch) => {
                        if let Some(state) = self.connections.get_mut(&ch) {
                            state.conn_mut().handle_event(cev);
                        }
                        DatagramOutcome::Accepted(ch)
                    }
                    None => DatagramOutcome::Dropped,
                }
            }
            // Some `Response`s DO mint a CID: quinn-proto's `initial_close`
            // calls the CID generator for the close packet's source CID. No
            // connection exists to attribute it to, which is exactly what the
            // sweep below is for.
            Some(DatagramEvent::Response(_)) | None => DatagramOutcome::Dropped,
        };

        // THE choke point. Every endpoint call that can mint a CID for this
        // datagram has now happened — `inner.handle` above, and `inner.accept`
        // inside `admit` — and `admit`'s success path has already attributed its
        // CIDs to the connection it created. Anything still pending therefore
        // belongs to no connection at all, so sweep it before anyone can
        // mis-attribute it. Doing this HERE rather than at the end of
        // `handle_datagram` matters: `service_connections` drains the pending
        // queue on behalf of whichever connection it is servicing, so an orphan
        // left in the queue would be handed to an unrelated connection.
        self.sweep_orphan_cids();
        outcome
    }

    /// Accept an admitted incoming connection and register it.
    fn admit(
        &mut self,
        now: Instant,
        incoming: quinn_proto::Incoming,
        client_idx: usize,
    ) -> DatagramOutcome {
        let Ok(handshake_deadline) = deadline(now, self.config.incoming_handshake_timeout) else {
            return DatagramOutcome::Dropped;
        };
        let mut buf = Vec::new();
        match self.inner.accept(incoming, now, &mut buf, None) {
            Ok((quinn_ch, conn)) => {
                let ch = self.allocate_handle(quinn_ch);
                if !buf.is_empty() && self.outbound.len() < self.config.max_outbound_datagrams {
                    self.outbound.push_back(Transmit {
                        destination: conn.remote_address(),
                        contents: buf,
                    });
                }
                // `accept` minted this connection's initial CID(s) via the
                // recorder; attribute them to `ch` for later pruning.
                self.drain_pending_cids(ch);
                self.connections.insert(ch, ConnState::new(conn));
                self.pending_incoming.insert(ch, handshake_deadline);
                self.handshake_deadlines.insert(ch, handshake_deadline);
                self.client_ids
                    .insert(ch, self.clients[client_idx].client_id.clone());
                DatagramOutcome::Accepted(ch)
            }
            Err(_) => {
                // `accept` may produce an `initial_close` for an
                // authenticated-but-refused peer (e.g. CID exhaustion). We
                // deliberately DROP it: emitting zero bytes keeps silence
                // strict, and the peer simply times out.
                //
                // It also leaves CIDs behind. quinn-proto mints `loc_cid`
                // BEFORE the `handle_first_packet` whose failure produces this
                // error, and `initial_close` mints a second one — both recorded
                // by our generator, neither belonging to any connection. They
                // are collected by the `sweep_orphan_cids` at the end of `feed`,
                // which is why there is no drain call here: a drain needs a
                // handle, and there is no connection to name.
                DatagramOutcome::Dropped
            }
        }
    }

    /// Wrap a newly-created Quinn slab handle in a never-reused public
    /// generation and register the live routing relation.
    fn allocate_handle(&mut self, quinn: QuinnConnectionHandle) -> ConnectionHandle {
        let generation = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("connection generation exhausted");
        let handle = ConnectionHandle::new(quinn, generation);
        let previous = self.by_quinn.insert(quinn, handle);
        debug_assert!(previous.is_none(), "live Quinn handle unexpectedly reused");
        handle
    }

    /// Pump every connection: route endpoint events, service streams into
    /// [`Event`]s, collect outbound datagrams, and reap connections that are
    /// gone. This is the whole of the old tokio `drive_connections`, minus the
    /// socket and the channels.
    fn service_connections(&mut self, now: Instant) {
        // Remembered as the "already elapsed" deadline `next_timeout` reports
        // for a dirty connection. Recorded up front so it is correct even if
        // this pass reaps everything.
        self.last_service = Some(now);
        self.needs_service = false;

        let mut handles: Vec<ConnectionHandle> = self.connections.keys().copied().collect();
        handles.sort_by_key(|ch| ch.generation());
        if !handles.is_empty() {
            let start = self.service_cursor % handles.len();
            handles.rotate_left(start);
            self.service_cursor = (start + 1) % handles.len();
        }

        // 1. Route connection→endpoint events (which may mint or retire CIDs).
        //    Any CID minted by `handle_event` — a `NeedIdentifiers`-triggered
        //    reissue for this connection — is attributed to `ch` via the pending
        //    queue, immediately, while we still know who asked for it.
        for ch in &handles {
            let mut work = 0;
            for _ in 0..self.config.max_connection_work {
                let Some(ev) = self
                    .connections
                    .get_mut(ch)
                    .and_then(|s| s.conn_mut().poll_endpoint_events())
                else {
                    break;
                };
                work += 1;
                let cev = self.inner.handle_event(ch.quinn, ev);
                self.drain_pending_cids(*ch);
                if let Some(cev) = cev {
                    if let Some(state) = self.connections.get_mut(ch) {
                        state.conn_mut().handle_event(cev);
                    }
                }
            }
            if work == self.config.max_connection_work {
                self.needs_service = true;
            }
        }

        // 2. Service application events and collect transmits. Staged into
        //    locals because `self.connections` is borrowed for the whole loop.
        let mut events: Vec<Event> = Vec::new();
        let mut transmits: Vec<Transmit> = Vec::new();
        let mut observed_lost: Vec<(ConnectionHandle, ConnectionError)> = Vec::new();
        let mut drained: Vec<ConnectionHandle> = Vec::new();
        let mut connected: Vec<ConnectionHandle> = Vec::new();

        for ch in &handles {
            let Some(state) = self.connections.get_mut(ch) else {
                continue;
            };

            // `service_streams` is the SOLE `conn.poll()` caller.
            let progress = state.service_streams(self.config.max_connection_work);
            if progress.more_work {
                self.needs_service = true;
            }
            if progress.connected {
                events.push(Event::Connected(*ch));
                connected.push(*ch);
            }
            for id in progress.opened {
                events.push(Event::StreamOpened {
                    conn: *ch,
                    id,
                    dir: quinn_proto::Dir::Bi,
                });
            }
            for id in progress.readable {
                events.push(Event::StreamReadable { conn: *ch, id });
            }
            for id in progress.writable {
                events.push(Event::StreamWritable { conn: *ch, id });
            }
            for id in progress.fin_acked {
                events.push(Event::StreamFinAcked { conn: *ch, id });
            }
            for (id, error_code) in progress.stopped {
                events.push(Event::StreamStopped {
                    conn: *ch,
                    id,
                    error_code,
                });
            }

            // `progress.lost` alone is NOT enough, and this is the single most
            // easily-lost line in the crate. A REMOTELY-initiated loss (a
            // CONNECTION_CLOSE frame, a transport error, a stateless reset) sets
            // quinn-proto's internal `error` field, so `poll()` yields
            // `Event::ConnectionLost` and `progress.lost` catches it. A
            // LOCALLY-initiated close does not: `Connection::close()` merely arms
            // the close timer, and quinn-proto 0.11's `handle_timeout`/
            // `Timer::Close` arm sets `state = State::Drained` and pushes
            // `EndpointEventInner::Drained` WITHOUT ever touching `self.error` —
            // so `poll()` never reports a self-close as lost. Left unreaped, that
            // connection's state sits here forever even though quinn-proto's own
            // slab has already freed and can reuse its `ConnectionHandle`; the
            // eventual collision between a reused handle and stale bookkeeping
            // wedges accept (reproducibly around ~32 cycles). `is_drained()` is
            // quinn-proto's own signal that the terminal transition completed by
            // EITHER path. Guarded by `tests/connection_lifecycle.rs` and by
            // `a_locally_closed_connection_is_reaped_and_reports_connection_lost`
            // in `proto/tests/core_endpoint.rs`.
            if let Some(reason) = progress.lost {
                observed_lost.push((*ch, reason));
            }
            if state.is_drained() {
                drained.push(*ch);
            }

            // Drain outbound datagrams. One datagram per `poll_transmit`
            // (`max_datagrams = 1`), so `buf` holds exactly one each time.
            let mut buf = Vec::new();
            let mut work = 0;
            for _ in 0..self.config.max_connection_work {
                if self.outbound.len() + transmits.len() >= self.config.max_outbound_datagrams {
                    self.needs_service = true;
                    break;
                }
                let Some(t) = state.conn_mut().poll_transmit(now, 1, &mut buf) else {
                    break;
                };
                work += 1;
                if buf.is_empty() {
                    break;
                }
                transmits.push(Transmit {
                    destination: t.destination,
                    contents: std::mem::take(&mut buf),
                });
            }
            if work == self.config.max_connection_work {
                self.needs_service = true;
            }

            // Everything the caller's stream operations produced — bytes, FIN,
            // RESET_STREAM, STOP_SENDING, and the MAX_STREAM_DATA/MAX_DATA
            // credit a read released — is now in `transmits`. The connection is
            // clean until the caller touches a stream again.
            if work < self.config.max_connection_work {
                state.clear_dirty();
            }
        }

        for ch in connected {
            self.handshake_deadlines.remove(&ch);
        }
        self.events.extend(events);
        self.outbound.extend(transmits);

        for (ch, reason) in observed_lost {
            let reason = self.loss_overrides.remove(&ch).unwrap_or(reason);
            self.mark_terminal(ch, reason, now);
        }

        // 3. Reap only after Quinn has delivered its Drained endpoint event.
        //    connection's CONNECTION_CLOSE is already queued for the caller.
        for ch in drained {
            let reason = self
                .terminal_connections
                .get(&ch)
                .cloned()
                .or_else(|| self.loss_overrides.remove(&ch))
                .unwrap_or(ConnectionError::LocallyClosed);
            self.remove_connection(ch, reason);
        }

        // 4. Backstop. Every mint site above is followed immediately by its own
        //    `drain_pending_cids`, so in normal operation this is a no-op. It
        //    exists so that a pass can NEVER end with an unattributed CID: if a
        //    future quinn-proto mints somewhere we do not drain, the CID is
        //    swept rather than left to leak and to be mis-attributed to whatever
        //    connection the next drain happens to name.
        self.sweep_orphan_cids();

        self.refresh_next_timeout();
    }

    /// Recompute the cached earliest deadline across all live connections.
    fn refresh_next_timeout(&mut self) {
        self.next_timeout = self
            .connections
            .values_mut()
            .filter_map(|s| s.conn_mut().poll_timeout())
            .chain(self.handshake_deadlines.values().copied())
            .chain(self.cleanup_deadlines.values().copied())
            .min();
    }

    fn remove_connection(&mut self, ch: ConnectionHandle, reason: ConnectionError) {
        if self.connections.remove(&ch).is_some() {
            self.by_quinn.remove(&ch.quinn);
            self.client_ids.remove(&ch);
            self.pending_incoming.remove(&ch);
            self.handshake_deadlines.remove(&ch);
            self.loss_overrides.remove(&ch);
            self.cleanup_deadlines.remove(&ch);
            self.prune_connection_cids(ch);
            if self.terminal_connections.remove(&ch).is_none() {
                self.events
                    .push_back(Event::ConnectionLost { conn: ch, reason });
            }
        }
    }

    fn mark_terminal(&mut self, ch: ConnectionHandle, reason: ConnectionError, now: Instant) {
        if !self.connections.contains_key(&ch) {
            return;
        }
        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.terminal_connections.entry(ch)
        {
            entry.insert(reason.clone());
            self.events
                .push_back(Event::ConnectionLost { conn: ch, reason });
        }
        self.pending_incoming.remove(&ch);
        self.handshake_deadlines.remove(&ch);
        let cleanup = now.checked_add(self.config.cleanup_timeout).unwrap_or(now);
        self.cleanup_deadlines.entry(ch).or_insert(cleanup);
    }

    /// Move every CID the recorder minted since the last drain into `ch`'s
    /// owned-CID list.
    ///
    /// Called immediately after each endpoint call that can mint (`connect`,
    /// `accept`, per-handle `handle_event`), so the pending queue only ever
    /// holds CIDs freshly minted for `ch`. Deferring this — or, as the spike
    /// did, merely `clear()`ing the queue — leaves the CID unattributed, which
    /// means it can never be pruned and routes to nothing forever.
    fn drain_pending_cids(&mut self, ch: ConnectionHandle) {
        let drained: Vec<ConnectionId> = {
            let mut pending = self.pending_cids.lock().expect("pending_cids poisoned");
            if pending.is_empty() {
                return;
            }
            std::mem::take(&mut *pending)
        };
        self.cids_by_conn.entry(ch).or_default().extend(drained);
    }

    /// Discard every CID that is still pending, as belonging to no connection.
    ///
    /// A CID reaches the pending queue the instant quinn-proto mints it, and
    /// every mint that belongs to a connection is attributed by the
    /// [`Endpoint::drain_pending_cids`] that immediately follows its minting
    /// call. So a CID still pending when a pass reaches a sweep point was minted
    /// for a connection that never came into existence — `Endpoint::accept`
    /// mints `loc_cid` *before* the `handle_first_packet` whose failure returns
    /// `AcceptError`, and then `initial_close` mints another; the same is true
    /// of the `initial_close` behind a `DatagramEvent::Response`.
    ///
    /// Leaving such a CID alone is doubly wrong: in `issued_cids` it leaks
    /// (unbounded growth, triggerable at will by any PSK holder whose
    /// ClientHello fails) and makes `is_active_dcid` answer true for a
    /// connection that does not exist, letting a packet bypass the pre-filter;
    /// in `pending_cids` the next `drain_pending_cids(ch)` misattributes it to
    /// an unrelated connection `ch`, corrupting `cids_by_conn`.
    ///
    /// This can never take a live connection's CID, because there is no window
    /// in which a live connection's CID is pending and unattributed: `connect`,
    /// `accept`'s success path and `handle_event` each drain in the very next
    /// statement, before any sweep point is reached.
    fn sweep_orphan_cids(&mut self) {
        let orphans =
            std::mem::take(&mut *self.pending_cids.lock().expect("pending_cids poisoned"));
        if orphans.is_empty() {
            return;
        }
        let mut set = self.issued_cids.lock().expect("issued_cids poisoned");
        for cid in orphans {
            set.remove(&cid);
        }
    }

    /// Remove all of a lost connection's CIDs from the routing set. Safe because
    /// the connection has already been reaped, so none of these CIDs can route
    /// to a live connection. Leaks nothing: the per-connection list goes too.
    fn prune_connection_cids(&mut self, ch: ConnectionHandle) {
        if let Some(cids) = self.cids_by_conn.remove(&ch) {
            let mut set = self.issued_cids.lock().expect("issued_cids poisoned");
            for cid in cids {
                set.remove(&cid);
            }
        }
    }
}

/// Read the DCID from either header form.
///
/// Long headers carry an explicit DCID length; short headers do not, so we read
/// exactly [`LOCAL_CID_LEN`] (we only route short-header packets to connections
/// we created, so that is the only length that can match).
fn read_dcid_any(data: &[u8]) -> Option<&[u8]> {
    let first = *data.first()?;
    if first & 0x80 != 0 {
        peek_dcid(data) // long header
    } else {
        data.get(1..1 + LOCAL_CID_LEN) // short header: [flags][dcid(LOCAL_CID_LEN)]...
    }
}

fn deadline(now: Instant, timeout: Duration) -> Result<Instant, ConfigError> {
    now.checked_add(timeout)
        .ok_or_else(|| ConfigError::Invalid("deadline exceeds monotonic clock range".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    // Needed in scope to mint CIDs directly from the recorder.
    use quinn_proto::{ConnectionIdGenerator, Side};

    /// A server endpoint with no authorized clients: enough to exercise the CID
    /// bookkeeping without a handshake.
    fn bare_server() -> Endpoint {
        Endpoint::new(EndpointConfig::dial()).expect("endpoint")
    }

    #[test]
    fn duplicate_client_identity_or_psk_is_rejected() {
        let duplicate_id: ServerSecrets = toml::from_str(
            "listen=\"127.0.0.1:0\"\n\
             [[clients]]\nclient_id=\"same\"\npsk=\"0000000000000000000000000000000000000000000000000000000000000001\"\n\
             [[clients]]\nclient_id=\"same\"\npsk=\"0000000000000000000000000000000000000000000000000000000000000002\"\n",
        )
        .expect("parse");
        assert!(matches!(
            Endpoint::new_server(duplicate_id),
            Err(ConfigError::Invalid(message)) if message.contains("duplicate client_id")
        ));

        let duplicate_psk: ServerSecrets = toml::from_str(
            "listen=\"127.0.0.1:0\"\n\
             [[clients]]\nclient_id=\"one\"\npsk=\"0000000000000000000000000000000000000000000000000000000000000001\"\n\
             [[clients]]\nclient_id=\"two\"\npsk=\"0000000000000000000000000000000000000000000000000000000000000001\"\n",
        )
        .expect("parse");
        assert!(matches!(
            Endpoint::new_server(duplicate_psk),
            Err(ConfigError::Invalid(message)) if message.contains("duplicate PSK")
        ));
    }

    #[test]
    fn admission_pause_is_reversible_but_capability_is_fixed() {
        let mut ep = bare_server();
        assert_eq!(ep.config().capability, crate::config::Capability::Dial);
        assert!(!ep.admission_paused());
        ep.pause_admission();
        assert!(ep.admission_paused());
        ep.resume_admission();
        assert!(!ep.admission_paused());
        assert_eq!(ep.config().capability, crate::config::Capability::Dial);
    }

    #[test]
    fn dial_is_rejected_without_capability() {
        let secrets: ServerSecrets = toml::from_str(
            "listen=\"127.0.0.1:0\"\n[[clients]]\nclient_id=\"a\"\npsk=\"0000000000000000000000000000000000000000000000000000000000000001\"\n",
        )
        .unwrap();
        let client: ClientConfigFile = toml::from_str(
            "client_id=\"a\"\npsk=\"0000000000000000000000000000000000000000000000000000000000000001\"\nserver=\"127.0.0.1:443\"\n",
        )
        .unwrap();
        let mut ep = Endpoint::new_server(secrets).unwrap();
        assert!(matches!(
            ep.connect(Instant::now(), 0, client, None, None),
            Err(ConfigError::Invalid(message)) if message.contains("dial capability")
        ));
    }

    #[test]
    fn transport_default_update_rebuilds_future_incoming_snapshots() {
        let secrets: ServerSecrets = toml::from_str(
            "listen=\"127.0.0.1:0\"\n[[clients]]\nclient_id=\"a\"\npsk=\"0000000000000000000000000000000000000000000000000000000000000001\"\n",
        )
        .unwrap();
        let mut endpoint = Endpoint::new_server(secrets).unwrap();
        let before = endpoint.clients[0].server_config.clone();
        endpoint.set_transport_defaults(TransportSettings::new(
            quinn_proto::TransportConfig::default(),
        ));
        assert!(!Arc::ptr_eq(&before, &endpoint.clients[0].server_config));
    }

    #[test]
    fn generations_make_reused_quinn_slots_distinct() {
        let mut ep = bare_server();
        let raw = QuinnConnectionHandle(7);
        let old = ep.allocate_handle(raw);
        ep.by_quinn.remove(&raw);
        let new = ep.allocate_handle(raw);
        assert_ne!(old, new);
        assert!(new.generation() > old.generation());
        assert_eq!(ep.by_quinn.get(&raw), Some(&new));
    }

    /// Simulate the endpoint minting CIDs for a connection by running a recorder
    /// wired to the same shared state the real generator uses. Callers then call
    /// `drain_pending_cids` exactly as the endpoint does after `accept` /
    /// `handle_event`.
    fn mint_for(ep: &Endpoint, count: usize) -> Vec<ConnectionId> {
        let mut generator = RecordingCidGenerator::new(
            LOCAL_CID_LEN,
            ep.issued_cids.clone(),
            ep.pending_cids.clone(),
        );
        (0..count).map(|_| generator.generate_cid()).collect()
    }

    #[test]
    fn recorder_populates_routing_set_and_pending_queue() {
        let ep = bare_server();
        let cids = mint_for(&ep, 3);

        let set = ep.issued_cids.lock().unwrap();
        for cid in &cids {
            assert!(set.contains(cid), "a minted CID must route");
        }
        drop(set);
        assert_eq!(
            *ep.pending_cids.lock().unwrap(),
            cids,
            "and be queued for attribution, in mint order"
        );
    }

    #[test]
    fn drain_attributes_minted_cids_to_the_connection_being_serviced() {
        let mut ep = bare_server();
        let live = ConnectionHandle::new(QuinnConnectionHandle(1), 1);
        let doomed = ConnectionHandle::new(QuinnConnectionHandle(2), 2);

        let live_cids = mint_for(&ep, 2);
        ep.drain_pending_cids(live);
        let doomed_cids = mint_for(&ep, 3);
        ep.drain_pending_cids(doomed);

        assert!(
            ep.pending_cids.lock().unwrap().is_empty(),
            "the pending queue is fully drained after attribution"
        );
        assert_eq!(ep.cids_by_conn[&live], live_cids);
        assert_eq!(ep.cids_by_conn[&doomed], doomed_cids);
        assert_eq!(ep.issued_cid_count(), 5);
    }

    #[test]
    fn prune_removes_only_the_lost_connections_cids() {
        let mut ep = bare_server();
        let live = ConnectionHandle::new(QuinnConnectionHandle(1), 1);
        let doomed = ConnectionHandle::new(QuinnConnectionHandle(2), 2);

        let live_cids = mint_for(&ep, 2);
        ep.drain_pending_cids(live);
        let doomed_cids = mint_for(&ep, 3);
        ep.drain_pending_cids(doomed);

        ep.prune_connection_cids(doomed);

        let set = ep.issued_cids.lock().unwrap();
        for cid in &live_cids {
            assert!(set.contains(cid), "a live connection's CID is never pruned");
        }
        for cid in &doomed_cids {
            assert!(!set.contains(cid), "a lost connection's CID is pruned");
        }
        assert_eq!(set.len(), 2);
        drop(set);
        assert!(
            !ep.cids_by_conn.contains_key(&doomed),
            "the per-connection list goes too — no leak"
        );
    }

    #[test]
    fn pruning_an_unknown_handle_is_a_noop() {
        let mut ep = bare_server();
        let cids = mint_for(&ep, 2);
        ep.drain_pending_cids(ConnectionHandle::new(QuinnConnectionHandle(1), 1));

        ep.prune_connection_cids(ConnectionHandle::new(QuinnConnectionHandle(99), 99));

        let set = ep.issued_cids.lock().unwrap();
        for cid in &cids {
            assert!(set.contains(cid));
        }
        assert_eq!(set.len(), 2);
    }

    /// An endpoint with no connections has no deadline, and asking for one must
    /// not invent work.
    #[test]
    fn an_idle_endpoint_has_no_timeout_and_nothing_to_send() {
        let mut ep = bare_server();
        let now = Instant::now();
        assert_eq!(ep.next_timeout(), None);
        assert!(ep.poll_transmit(now).is_none());
        assert!(ep.poll_event().is_none());
        assert_eq!(ep.next_timeout(), None);
    }

    // -- orphaned-CID sweep -------------------------------------------------

    /// The sweep's contract in isolation: it discards exactly the CIDs that no
    /// `drain_pending_cids` claimed, and cannot touch one that was claimed.
    ///
    /// The orphans here stand in for what `quinn_proto::Endpoint::accept` leaves
    /// behind when it fails: `loc_cid`, minted before the `handle_first_packet`
    /// that errors, plus the `initial_close` source CID minted on the way out.
    #[test]
    fn sweep_discards_orphans_and_leaves_attributed_cids_alone() {
        let mut ep = bare_server();
        let live = ConnectionHandle::new(QuinnConnectionHandle(1), 1);

        let live_cids = mint_for(&ep, 2);
        ep.drain_pending_cids(live);

        // A failed accept: two CIDs minted, no connection, no drain.
        let orphans = mint_for(&ep, 2);
        assert_eq!(ep.issued_cid_count(), 4, "all four were recorded");

        ep.sweep_orphan_cids();

        let set = ep.issued_cids.lock().unwrap();
        for cid in &orphans {
            assert!(
                !set.contains(cid),
                "an unattributed CID must not survive the pass: in `issued_cids` it \
                 leaks and makes `is_active_dcid` true for a connection that does \
                 not exist"
            );
        }
        for cid in &live_cids {
            assert!(
                set.contains(cid),
                "the sweep must never steal a CID a drain already attributed"
            );
        }
        assert_eq!(set.len(), 2, "back to the live connection's baseline");
        drop(set);

        assert!(
            ep.pending_cids.lock().unwrap().is_empty(),
            "and the pending queue is empty, so the NEXT drain cannot misattribute"
        );
        assert_eq!(
            ep.cids_by_conn[&live], live_cids,
            "the live connection's attribution is untouched"
        );
    }

    /// The guard that matters: after a servicing pass, `issued_cid_count()` is
    /// back to the live connection's baseline — and that connection still works.
    ///
    /// Orphans are injected into the recorder's shared state exactly as a failed
    /// `Endpoint::accept` would leave them (recorded in `issued_cids`, still at
    /// the head of `pending_cids`, attributed to nobody), because forcing a real
    /// `handle_first_packet` failure would mean hand-forging a PSK-sealed
    /// Initial that decrypts and *then* violates the transport. What is being
    /// guarded is the endpoint's response to that state, and this reproduces it
    /// faithfully.
    #[test]
    fn a_servicing_pass_returns_issued_cids_to_baseline_after_orphans_appear() {
        let mut pair = crate::testing::connected_pair();
        let now = pair.now();

        let baseline = pair.server().issued_cid_count();
        assert!(baseline > 0, "the live connection has CIDs");
        let live_cids: HashSet<ConnectionId> = pair
            .server()
            .issued_cids
            .lock()
            .expect("issued_cids poisoned")
            .clone();

        let orphans = mint_for(pair.server(), 2);
        assert_eq!(pair.server().issued_cid_count(), baseline + 2);

        // One ordinary servicing pass — the caller draining transmits to `None`.
        while pair.server().poll_transmit(now).is_some() {}

        assert_eq!(
            pair.server().issued_cid_count(),
            baseline,
            "a pass must not end with CIDs belonging to no connection; unswept, \
             every failed accept grows this set forever"
        );
        let set = pair
            .server()
            .issued_cids
            .lock()
            .expect("issued_cids poisoned")
            .clone();
        for cid in &orphans {
            assert!(!set.contains(cid), "the orphans are gone");
        }
        for cid in &live_cids {
            assert!(
                set.contains(cid),
                "the live connection's CIDs are NOT collateral — they must still route"
            );
        }
        assert!(
            pair.server()
                .pending_cids
                .lock()
                .expect("pending_cids poisoned")
                .is_empty(),
            "and nothing is left for the next drain to misattribute"
        );

        // The strongest statement that no live CID was stolen: the connection
        // still carries a stream end to end.
        let id = pair.open_bi(Side::Client);
        pair.write_all(Side::Client, id, b"still-routing");
        pair.conn(Side::Client).stream_finish(id).expect("finish");
        pair.drive();
        assert_eq!(pair.accept_bi(Side::Server), id);
        assert_eq!(&pair.pump_until_read(Side::Server, id), b"still-routing");
    }

    // -- what `Dropped` does and does not promise ---------------------------

    /// A server whose only authorized client is [`VN_PSK_HEX`].
    fn psk_server() -> (Endpoint, [u8; 32]) {
        let secrets: ServerSecrets = toml::from_str(&format!(
            "listen = \"127.0.0.1:0\"\n[[clients]]\nclient_id = \"a\"\npsk = \"{VN_PSK_HEX}\"\n"
        ))
        .expect("parse secrets");
        let psk = *secrets.clients[0].psk.as_bytes();
        (Endpoint::new_server(secrets).expect("server endpoint"), psk)
    }

    const VN_PSK_HEX: &str = "000000000000000000000000000000000000000000000000000000000000002a";

    /// `DatagramOutcome::Dropped` does NOT mean "nothing was queued", and the
    /// silence invariant does not claim it does.
    ///
    /// The pre-filter is QUIC-version-agnostic by construction — it reads the
    /// DCID out of the long header and never looks at the version — so a peer
    /// holding a valid PSK can present a well-formed selector with a version we
    /// do not speak. quinn-proto answers with Version Negotiation: bytes queued,
    /// no connection created, outcome `Dropped`.
    ///
    /// This is not a silence violation (that peer proved PSK possession before a
    /// single byte was queued) and the fix is not to suppress the reply; it is
    /// that the invariant is about **pre-filter rejection**, which is what
    /// `core_silence.rs` actually tests. This test exists so the documented
    /// carve-out cannot quietly become false in either direction.
    #[test]
    fn an_authorized_peer_on_an_unsupported_version_is_dropped_but_answered() {
        let (mut ep, psk) = psk_server();
        let now = Instant::now();
        let peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        let dcid = build_dcid(&psk, [0x11; 8], now_minutes());
        let mut pkt = vec![0xc0];
        pkt.extend_from_slice(&0xdead_beefu32.to_be_bytes()); // not a version we speak
        pkt.push(DCID_LEN as u8);
        pkt.extend_from_slice(&dcid);
        pkt.push(0); // zero-length SCID
        pkt.extend_from_slice(&[0u8; 1200]);

        assert_eq!(
            ep.handle_datagram(now, peer, &pkt),
            DatagramOutcome::Dropped,
            "no connection was created, so the outcome is Dropped"
        );
        assert!(
            ep.poll_transmit(now).is_some(),
            "...and yet bytes WERE queued (Version Negotiation). `Dropped` promises \
             only 'no connection for you'; the silence invariant is about a datagram \
             that FAILS THE PRE-FILTER, and this one passed it."
        );
        assert_eq!(
            ep.issued_cid_count(),
            0,
            "the response path must leave no CID behind either"
        );
    }
}

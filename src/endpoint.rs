//! Socket-owning, shared QUIC endpoints.
//!
//! One endpoint can dial and accept multiple connections on one UDP socket.
//! Application handles retain existing connections independently of the endpoint
//! handle. Explicit shutdown always overrides that retention.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch, Notify};

use crate::config::ClientConfigFile;
use crate::conn::{Cleanup, Cmd, CmdSender, Connection, Parked, Tagged, TaggedCleanup};
use quietquic_proto::endpoint::Endpoint as Core;
use quietquic_proto::freshness::now_minutes;
use quietquic_proto::outcome::{ConnectionError, ConnectionHandle, Event};
use quinn_proto::VarInt;

pub use quietquic_proto::config::{Capability, EndpointConfig, TransportSettings};

const COMMAND_CAPACITY: usize = 256;
const OUTGOING_CAPACITY: usize = 128;
const WORK_BUDGET: usize = 64;
const SOCKET_RETRY_DELAY: Duration = Duration::from_millis(10);

/// A local endpoint operation or socket failure. No secret configuration is included.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EndpointError {
    #[error("invalid endpoint configuration: {0}")]
    InvalidConfiguration(String),
    #[error("endpoint capability is disabled")]
    CapabilityDisabled,
    #[error("local connection-attempt capacity is exhausted")]
    Capacity,
    #[error("endpoint is closed")]
    Closed,
    #[error("UDP socket failed: {message}")]
    Socket {
        message: String,
        raw_os_error: Option<i32>,
    },
    #[error("connection failed: {0}")]
    Connection(ConnectionError),
}

impl EndpointError {
    fn io(error: io::Error) -> Self {
        Self::Socket {
            message: error.to_string(),
            raw_os_error: error.raw_os_error(),
        }
    }
}

/// Persistent reason the endpoint stopped operating; cleanup may still be running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointTermination {
    Closed,
    Failed(EndpointError),
}

/// Overrides for one outgoing connection. Existing connections are unaffected.
#[derive(Clone, Default)]
pub struct ConnectOptions {
    pub transport: Option<TransportSettings>,
    pub handshake_timeout: Option<Duration>,
}

struct State {
    incoming: VecDeque<Connection>,
    waiters: VecDeque<(u64, Waker)>,
    next_waiter: u64,
    outgoing: VecDeque<Arc<Attempt>>,
    in_flight: usize,
    accepted: Vec<ConnectionHandle>,
    pause: bool,
    transport_update: Option<TransportSettings>,
    transport: TransportSettings,
    close: Option<(VarInt, Vec<u8>)>,
}

struct Shared {
    state: Mutex<State>,
    wake: Arc<Notify>,
    endpoint_gone: AtomicBool,
    termination: watch::Sender<Option<EndpointTermination>>,
    closed: watch::Sender<bool>,
    local: SocketAddr,
    capability: Capability,
    only_v6: bool,
}

impl Shared {
    fn wake_acceptors(&self) {
        let wakers: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .waiters
            .iter()
            .map(|(_, w)| w.clone())
            .collect();
        for waker in wakers {
            waker.wake();
        }
    }

    fn terminate(&self, reason: EndpointTermination) {
        self.termination.send_if_modified(|slot| {
            if slot.is_none() {
                *slot = Some(reason);
                true
            } else {
                false
            }
        });
        self.wake_acceptors();
    }
}

struct EndpointLease(Arc<Shared>);
impl Drop for EndpointLease {
    fn drop(&mut self) {
        self.0.endpoint_gone.store(true, Ordering::Release);
        self.0.wake.notify_one();
    }
}

/// An application handle to one bound socket and its shared connection driver.
#[derive(Clone)]
pub struct Endpoint {
    shared: Arc<Shared>,
    _lease: Arc<EndpointLease>,
}

impl Endpoint {
    /// Bind exactly this address. Use an explicit IPv6 socket for dual-stack policy.
    pub async fn bind(address: SocketAddr, config: EndpointConfig) -> Result<Self, EndpointError> {
        config
            .validate()
            .map_err(|e| EndpointError::InvalidConfiguration(e.to_string()))?;
        let socket = UdpSocket::bind(address).await.map_err(EndpointError::io)?;
        Self::from_tokio_socket(socket, config)
    }

    /// Take exclusive ownership of an already-bound UDP socket.
    ///
    /// Must be called inside a Tokio runtime. Do not retain a competing reader.
    pub fn from_socket(
        socket: std::net::UdpSocket,
        config: EndpointConfig,
    ) -> Result<Self, EndpointError> {
        socket.set_nonblocking(true).map_err(EndpointError::io)?;
        let socket = UdpSocket::from_std(socket).map_err(EndpointError::io)?;
        Self::from_tokio_socket(socket, config)
    }

    fn from_tokio_socket(socket: UdpSocket, config: EndpointConfig) -> Result<Self, EndpointError> {
        let (endpoint, driver) = Self::build(socket, config)?;
        tokio::spawn(driver.run());
        Ok(endpoint)
    }

    fn build(socket: UdpSocket, config: EndpointConfig) -> Result<(Self, Driver), EndpointError> {
        let local = socket.local_addr().map_err(EndpointError::io)?;
        let only_v6 = local.is_ipv6()
            && socket2::SockRef::from(&socket)
                .only_v6()
                .map_err(EndpointError::io)?;
        let core = Core::new(config.clone())
            .map_err(|e| EndpointError::InvalidConfiguration(e.to_string()))?;
        let (termination, _) = watch::channel(None);
        let (closed, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                incoming: VecDeque::new(),
                waiters: VecDeque::new(),
                next_waiter: 0,
                outgoing: VecDeque::new(),
                in_flight: 0,
                accepted: Vec::new(),
                pause: false,
                transport_update: None,
                transport: config.transport.clone(),
                close: None,
            }),
            wake: Arc::new(Notify::new()),
            endpoint_gone: AtomicBool::new(false),
            termination,
            closed,
            local,
            capability: config.capability,
            only_v6,
        });
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (cleanup, cleanup_rx) = mpsc::unbounded_channel();
        let driver = Driver {
            core,
            socket,
            shared: shared.clone(),
            commands,
            command_rx,
            cleanup,
            cleanup_rx,
            parked: HashMap::new(),
            attempts: HashMap::new(),
            shutdown: None,
            cleanup_timeout: config.cleanup_timeout,
            endpoint_gone_seen: false,
            pending_transmits: VecDeque::new(),
            send_retry_at: None,
            recv_retry_at: None,
        };
        Ok((
            Self {
                _lease: Arc::new(EndpointLease(shared.clone())),
                shared,
            },
            driver,
        ))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.shared.local
    }

    /// Begin a cancellation-owned connection attempt on this socket.
    pub fn connect(&self, config: ClientConfigFile) -> Connecting {
        self.connect_with(config, ConnectOptions::default())
    }

    pub fn connect_with(
        &self,
        mut config: ClientConfigFile,
        mut options: ConnectOptions,
    ) -> Connecting {
        let mut state = self.shared.state.lock().unwrap();
        let error = if !self.shared.capability.can_dial() {
            Some(EndpointError::CapabilityDisabled)
        } else if let Some(reason) = self.shared.termination.borrow().clone() {
            Some(match reason {
                EndpointTermination::Closed => EndpointError::Closed,
                EndpointTermination::Failed(error) => error,
            })
        } else if state.close.is_some() {
            Some(EndpointError::Closed)
        } else if state.in_flight >= OUTGOING_CAPACITY {
            Some(EndpointError::Capacity)
        } else if (self.shared.local.is_ipv4() && config.server.is_ipv6())
            || (self.shared.only_v6 && config.server.is_ipv4())
        {
            Some(EndpointError::InvalidConfiguration(
                "remote address is incompatible with endpoint socket family".into(),
            ))
        } else if config.bind.is_some_and(|bind| {
            bind.ip() != self.shared.local.ip()
                || (bind.port() != 0 && bind.port() != self.shared.local.port())
        }) {
            Some(EndpointError::InvalidConfiguration(
                "connection bind must match the endpoint socket".into(),
            ))
        } else {
            None
        };
        if self.shared.local.is_ipv6() {
            if let SocketAddr::V4(remote) = config.server {
                config.server = SocketAddr::new(remote.ip().to_ipv6_mapped().into(), remote.port());
            }
        }
        if options.transport.is_none() {
            options.transport = Some(state.transport.clone());
        }
        let attempt = Arc::new(Attempt {
            state: Mutex::new(AttemptState {
                request: if error.is_none() {
                    Some((config, options))
                } else {
                    None
                },
                result: error.map(Err),
                waker: None,
                handle: None,
            }),
            canceled: AtomicBool::new(false),
            shared: self.shared.clone(),
            counted: AtomicBool::new(false),
        });
        // Count only successfully admitted local requests, including completed
        // attempts whose Connection has not yet been handed to the application.
        let queued = attempt.state.lock().unwrap().request.is_some();
        attempt.counted.store(queued, Ordering::Release);
        if queued {
            state.in_flight += 1;
            state.outgoing.push_back(attempt.clone());
        }
        drop(state);
        self.shared.wake.notify_one();
        Connecting {
            attempt,
            completed: false,
        }
    }

    /// Cancellation-safe FIFO acceptance. Pausing does not complete this wait.
    pub fn accept(&self) -> Accept<'_> {
        Accept {
            endpoint: self,
            ticket: None,
        }
    }

    pub fn pause_admission(&self) -> Result<(), EndpointError> {
        self.set_pause(true)
    }
    pub fn resume_admission(&self) -> Result<(), EndpointError> {
        self.set_pause(false)
    }

    fn set_pause(&self, pause: bool) -> Result<(), EndpointError> {
        if !self.shared.capability.can_accept() {
            return Err(EndpointError::CapabilityDisabled);
        }
        let mut state = self.shared.state.lock().unwrap();
        if let Some(reason) = self.shared.termination.borrow().clone() {
            return Err(match reason {
                EndpointTermination::Closed => EndpointError::Closed,
                EndpointTermination::Failed(error) => error,
            });
        }
        if state.close.is_some() {
            return Err(EndpointError::Closed);
        }
        state.pause = pause;
        drop(state);
        self.shared.wake.notify_one();
        Ok(())
    }

    pub fn set_transport_defaults(
        &self,
        transport: TransportSettings,
    ) -> Result<(), EndpointError> {
        let mut state = self.shared.state.lock().unwrap();
        if let Some(reason) = self.shared.termination.borrow().clone() {
            return Err(match reason {
                EndpointTermination::Closed => EndpointError::Closed,
                EndpointTermination::Failed(error) => error,
            });
        }
        if state.close.is_some() {
            return Err(EndpointError::Closed);
        }
        state.transport = transport.clone();
        state.transport_update = Some(transport);
        drop(state);
        self.shared.wake.notify_one();
        Ok(())
    }

    /// Initiate terminal shutdown. Repeated calls retain the first request.
    pub fn close(&self, code: u64, reason: &[u8]) -> Result<(), EndpointError> {
        if code >= crate::conn::AUTO_CODE_START {
            return Err(EndpointError::InvalidConfiguration(
                "close code is reserved for automatic cleanup".into(),
            ));
        }
        let code = VarInt::from_u64(code).map_err(|_| {
            EndpointError::InvalidConfiguration("close code exceeds QUIC varint".into())
        })?;
        let mut state = self.shared.state.lock().unwrap();
        if state.close.is_none() && self.shared.termination.borrow().is_none() {
            state.close = Some((code, reason.to_vec()));
        }
        drop(state);
        self.shared.terminate(EndpointTermination::Closed);
        self.shared.wake.notify_one();
        Ok(())
    }

    /// Observe the persistent terminal cause; this does not initiate shutdown.
    pub async fn terminated(&self) -> EndpointTermination {
        let mut rx = self.shared.termination.subscribe();
        loop {
            if let Some(reason) = rx.borrow().clone() {
                return reason;
            }
            if rx.changed().await.is_err() {
                return EndpointTermination::Closed;
            }
        }
    }

    /// Wait until shutdown cleanup is finished and the socket has been released.
    pub async fn wait_closed(&self) {
        let mut rx = self.shared.closed.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
}

/// A future that owns an outgoing attempt, including the result until handoff.
pub struct Connecting {
    attempt: Arc<Attempt>,
    completed: bool,
}
struct Attempt {
    state: Mutex<AttemptState>,
    canceled: AtomicBool,
    counted: AtomicBool,
    shared: Arc<Shared>,
}
struct AttemptState {
    request: Option<(ClientConfigFile, ConnectOptions)>,
    result: Option<Result<Connection, EndpointError>>,
    waker: Option<Waker>,
    handle: Option<ConnectionHandle>,
}
impl Attempt {
    fn finish(&self, result: Result<Connection, EndpointError>) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            if self.canceled.load(Ordering::Acquire) {
                return;
            }
            state.result = Some(result);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    fn uncount(&self) {
        if self.counted.swap(false, Ordering::AcqRel) {
            let mut state = self.shared.state.lock().unwrap();
            state.in_flight -= 1;
            // Cancellation can happen repeatedly without yielding to the
            // driver. Release the queued request as well as its capacity slot.
            state
                .outgoing
                .retain(|queued| !std::ptr::eq(queued.as_ref(), self));
        }
    }
}
impl Future for Connecting {
    type Output = Result<Connection, EndpointError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = {
            let mut state = self.attempt.state.lock().unwrap();
            match state.result.take() {
                Some(result) => Some(result),
                None => {
                    state.waker = Some(cx.waker().clone());
                    None
                }
            }
        };
        if let Some(result) = result {
            self.completed = true;
            self.attempt.uncount();
            self.attempt.shared.wake.notify_one();
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}
impl Drop for Connecting {
    fn drop(&mut self) {
        if !self.completed {
            self.attempt.canceled.store(true, Ordering::Release);
            self.attempt.state.lock().unwrap().result.take();
            self.attempt.uncount();
            self.attempt.shared.wake.notify_one();
        }
    }
}

/// A cancellation-safe incoming connection waiter.
pub struct Accept<'a> {
    endpoint: &'a Endpoint,
    ticket: Option<u64>,
}
impl Future for Accept<'_> {
    type Output = Result<Option<Connection>, EndpointError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.endpoint.shared.capability.can_accept() {
            return Poll::Ready(Err(EndpointError::CapabilityDisabled));
        }
        let shared = self.endpoint.shared.clone();
        let mut state = shared.state.lock().unwrap();
        if let Some(reason) = shared.termination.borrow().clone() {
            return Poll::Ready(match reason {
                EndpointTermination::Closed => Ok(None),
                EndpointTermination::Failed(e) => Err(e),
            });
        }
        let ticket = match self.ticket {
            Some(ticket) => ticket,
            None => {
                let ticket = state.next_waiter;
                state.next_waiter = state
                    .next_waiter
                    .checked_add(1)
                    .expect("accept ticket exhaustion");
                state.waiters.push_back((ticket, cx.waker().clone()));
                self.ticket = Some(ticket);
                ticket
            }
        };
        if let Some((_, waker)) = state.waiters.iter_mut().find(|(id, _)| *id == ticket) {
            waker.clone_from(cx.waker());
        }
        if state.waiters.front().is_some_and(|(id, _)| *id == ticket) {
            // Closure is published before the driver takes this queue lock to
            // reap an incoming item. Do not hand off a known-dead connection
            // in that interval. Closure after this check is a normal race.
            state
                .incoming
                .retain(|connection| connection.terminal_reason().is_none());
            if let Some(connection) = state.incoming.pop_front() {
                state.waiters.pop_front();
                state.accepted.push(connection.handle());
                self.ticket = None;
                let next = state.waiters.front().map(|(_, w)| w.clone());
                drop(state);
                if let Some(w) = next {
                    w.wake();
                }
                shared.wake.notify_one();
                return Poll::Ready(Ok(Some(connection)));
            }
        }
        Poll::Pending
    }
}
impl Drop for Accept<'_> {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket {
            let next = {
                let mut state = self.endpoint.shared.state.lock().unwrap();
                state.waiters.retain(|(id, _)| *id != ticket);
                state.waiters.front().map(|(_, w)| w.clone())
            };
            if let Some(w) = next {
                w.wake();
            }
        }
    }
}

struct Driver {
    core: Core,
    socket: UdpSocket,
    shared: Arc<Shared>,
    commands: mpsc::Sender<Tagged>,
    command_rx: mpsc::Receiver<Tagged>,
    cleanup: mpsc::UnboundedSender<TaggedCleanup>,
    cleanup_rx: mpsc::UnboundedReceiver<TaggedCleanup>,
    parked: HashMap<ConnectionHandle, Parked>,
    attempts: HashMap<ConnectionHandle, Arc<Attempt>>,
    shutdown: Option<Instant>,
    cleanup_timeout: Duration,
    endpoint_gone_seen: bool,
    pending_transmits: VecDeque<quietquic_proto::outcome::Transmit>,
    send_retry_at: Option<Instant>,
    recv_retry_at: Option<Instant>,
}

impl Driver {
    async fn run(mut self) {
        let mut buffer = vec![0; 65_535];
        loop {
            if self.recv_retry_at.is_some_and(|at| at <= Instant::now()) {
                self.recv_retry_at = None;
            }
            self.control();
            self.pump();
            if let Err(error) = self.flush() {
                self.fail_socket(error);
            }
            if self.finished() {
                break;
            }
            // Pending datagrams after flush mean socket backpressure. Wait
            // for writable readiness or real protocol timers, not an immediate
            // work reminder for output we cannot currently send.
            let next = if self.pending_transmits.is_empty() {
                self.core.next_timeout()
            } else {
                self.core.next_protocol_timeout()
            };
            let deadline = next
                .into_iter()
                .chain(self.shutdown)
                .chain(self.send_retry_at)
                .chain(self.recv_retry_at)
                .min()
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
            let wake = self.shared.wake.clone();
            tokio::select! {
                _ = wake.notified() => {},
                value = self.cleanup_rx.recv() => { if let Some(value) = value { self.clean(value); } },
                value = self.command_rx.recv() => { if let Some(value) = value { self.command(value); } },
                result = self.socket.recv_from(&mut buffer), if self.recv_retry_at.is_none() => self.received(result, &buffer),
                result = self.socket.writable(), if !self.pending_transmits.is_empty() && self.send_retry_at.is_none() => {
                    if let Err(error) = result { self.fail_socket(error); }
                },
                _ = tokio::time::sleep_until(deadline.into()) => {
                    self.core.handle_timeout(Instant::now());
                },
            }
        }
        let shared = self.shared.clone();
        drop(self); // Release socket before publishing completion.
        shared.closed.send_replace(true);
    }

    fn received(&mut self, result: io::Result<(usize, SocketAddr)>, buffer: &[u8]) {
        match result {
            Ok((n, from)) => {
                self.control();
                // Serialize admission with synchronous pause/default updates
                // from other runtime workers.
                let shared = self.shared.clone();
                let mut state = shared.state.lock().unwrap();
                if state.pause
                    || state.close.is_some()
                    || self.endpoint_gone_seen
                    || self.shutdown.is_some()
                {
                    self.core.pause_admission();
                } else {
                    self.core.resume_admission();
                }
                if let Some(settings) = state.transport_update.take() {
                    self.core.set_transport_defaults(settings);
                }
                self.core
                    .handle_datagram(Instant::now(), from, &buffer[..n]);
            }
            Err(error) if transient(&error) => {
                self.recv_retry_at = Some(Instant::now() + SOCKET_RETRY_DELAY);
            }
            Err(error) => self.fail_socket(error),
        }
    }

    fn control(&mut self) {
        let (pause, defaults, accepted, requests, close, orphaned) = {
            let mut state = self.shared.state.lock().unwrap();
            let orphaned =
                if self.shared.endpoint_gone.load(Ordering::Acquire) && !self.endpoint_gone_seen {
                    self.endpoint_gone_seen = true;
                    state
                        .incoming
                        .drain(..)
                        .map(|c| c.handle())
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
            (
                state.pause,
                state.transport_update.take(),
                std::mem::take(&mut state.accepted),
                state.outgoing.drain(..).collect::<Vec<_>>(),
                state.close.take(),
                orphaned,
            )
        };
        if pause || self.endpoint_gone_seen || self.shutdown.is_some() {
            self.core.pause_admission();
        } else {
            self.core.resume_admission();
        }
        if let Some(defaults) = defaults {
            self.core.set_transport_defaults(defaults);
        }
        for ch in accepted {
            let _ = self.core.mark_accepted(ch);
        }
        if self.endpoint_gone_seen {
            // The core tracks pending inbound handshakes as well as ready ones.
            self.core.close_pending_incoming(Instant::now());
        }
        for ch in orphaned {
            self.close_connection(ch);
        }
        if let Some((code, reason)) = close {
            if self.shutdown.is_none() {
                self.shutdown = Some(Instant::now() + self.cleanup_timeout);
                self.shared.terminate(EndpointTermination::Closed);
                let handles: Vec<_> = self.core.connections().collect();
                for ch in handles {
                    let _ = self
                        .core
                        .close_connection(Instant::now(), ch, code, reason.clone());
                    if let Some(p) = self.parked.get_mut(&ch) {
                        p.mark_closed(ConnectionError::LocallyClosed);
                        p.fail_all();
                    }
                    if let Some(attempt) = self.attempts.remove(&ch) {
                        attempt.finish(Err(EndpointError::Closed));
                    }
                }
                self.shared.state.lock().unwrap().incoming.clear();
            }
        }
        for attempt in requests {
            if attempt.canceled.load(Ordering::Acquire) {
                continue;
            }
            if self.shutdown.is_some() {
                attempt.finish(Err(EndpointError::Closed));
                continue;
            }
            let request = attempt.state.lock().unwrap().request.take();
            if let Some((config, options)) = request {
                match self.core.connect(
                    Instant::now(),
                    now_minutes(),
                    config,
                    options.transport,
                    options.handshake_timeout,
                ) {
                    Ok(ch) => {
                        attempt.state.lock().unwrap().handle = Some(ch);
                        self.attempts.insert(ch, attempt);
                    }
                    Err(error) => {
                        attempt.finish(Err(EndpointError::InvalidConfiguration(error.to_string())))
                    }
                }
            }
        }
        let canceled: Vec<_> = self
            .attempts
            .iter()
            .filter(|(_, a)| a.canceled.load(Ordering::Acquire))
            .map(|(&ch, _)| ch)
            .collect();
        for ch in canceled {
            self.attempts.remove(&ch);
            self.close_connection(ch);
        }
        for _ in 0..WORK_BUDGET {
            match self.cleanup_rx.try_recv() {
                Ok(value) => self.clean(value),
                Err(_) => break,
            }
        }
        for _ in 0..WORK_BUDGET {
            match self.command_rx.try_recv() {
                Ok(value) => self.command(value),
                Err(_) => break,
            }
        }
    }

    fn command(&mut self, value: Tagged) {
        if let Cmd::Close { code, reason } = value.cmd {
            let _ = self
                .core
                .close_connection(Instant::now(), value.handle, code, reason);
            return;
        }
        if let (Some(parked), Some(core)) = (
            self.parked.get_mut(&value.handle),
            self.core.conn_mut(value.handle),
        ) {
            parked.apply_cmd(core, value.cmd, Instant::now());
        }
    }

    fn clean(&mut self, value: TaggedCleanup) {
        match value.cleanup {
            Cleanup::OwnerDropped => self.close_connection(value.handle),
            cleanup => {
                if let (Some(parked), Some(core)) = (
                    self.parked.get_mut(&value.handle),
                    self.core.conn_mut(value.handle),
                ) {
                    parked.apply_cleanup(core, cleanup);
                }
            }
        }
    }

    fn close_connection(&mut self, ch: ConnectionHandle) {
        let _ = self
            .core
            .close_connection(Instant::now(), ch, VarInt::from_u32(0), Vec::new());
    }

    fn pump(&mut self) {
        for (&ch, parked) in &mut self.parked {
            if let Some(core) = self.core.conn_mut(ch) {
                parked.maintain(core);
                if parked.needs_service() {
                    self.shared.wake.notify_one();
                }
            }
        }
        for index in 0..WORK_BUDGET {
            let Some(event) = self.core.poll_event() else {
                break;
            };
            self.event(event);
            if index + 1 == WORK_BUDGET {
                self.shared.wake.notify_one();
            }
        }
        for _ in 0..WORK_BUDGET {
            if self.pending_transmits.len() >= WORK_BUDGET {
                break;
            }
            let Some(tx) = self.core.poll_transmit(Instant::now()) else {
                break;
            };
            self.pending_transmits.push_back(tx);
            if self.pending_transmits.len() == WORK_BUDGET {
                self.shared.wake.notify_one();
            }
        }
        // Transmit servicing can itself produce events.
        for index in 0..WORK_BUDGET {
            let Some(event) = self.core.poll_event() else {
                break;
            };
            self.event(event);
            if index + 1 == WORK_BUDGET {
                self.shared.wake.notify_one();
            }
        }
    }

    fn event(&mut self, event: Event) {
        match event {
            Event::Connected(ch) => {
                let id = self.core.client_id(ch).map(str::to_owned);
                let Some(core) = self.core.conn_mut(ch) else {
                    return;
                };
                let remote = core.conn().remote_address();
                let (closed_tx, closed_rx) = watch::channel(None);
                self.parked.insert(ch, Parked::new(closed_tx));
                let cmds = CmdSender::new(
                    ch,
                    self.commands.clone(),
                    self.cleanup.clone(),
                    closed_rx.clone(),
                    self.shared.wake.clone(),
                );
                let connection = Connection::new(ch, remote, id, cmds, closed_rx);
                if let Some(attempt) = self.attempts.remove(&ch) {
                    if attempt.canceled.load(Ordering::Acquire) {
                        self.close_connection(ch);
                    } else {
                        attempt.finish(Ok(connection));
                    }
                } else if self.endpoint_gone_seen || self.shutdown.is_some() {
                    self.close_connection(ch);
                } else {
                    self.shared
                        .state
                        .lock()
                        .unwrap()
                        .incoming
                        .push_back(connection);
                    self.shared.wake_acceptors();
                }
            }
            Event::ConnectionLost { conn, reason } => {
                if let Some(attempt) = self.attempts.remove(&conn) {
                    attempt.finish(Err(EndpointError::Connection(reason.clone())));
                }
                if let Some(mut parked) = self.parked.remove(&conn) {
                    parked.mark_closed(reason);
                    parked.fail_all();
                }
                self.shared
                    .state
                    .lock()
                    .unwrap()
                    .incoming
                    .retain(|c| c.handle() != conn);
            }
            Event::StreamOpened { conn, .. } => {
                if let (Some(p), Some(c)) = (self.parked.get_mut(&conn), self.core.conn_mut(conn)) {
                    p.on_stream_opened(c);
                }
            }
            Event::StreamReadable { conn, id } => {
                if let (Some(p), Some(c)) = (self.parked.get_mut(&conn), self.core.conn_mut(conn)) {
                    p.on_readable(c, id);
                }
            }
            Event::StreamWritable { conn, id } => {
                if let (Some(p), Some(c)) = (self.parked.get_mut(&conn), self.core.conn_mut(conn)) {
                    p.on_writable(c, id);
                }
            }
            Event::StreamFinAcked { conn, id } => {
                if let Some(p) = self.parked.get_mut(&conn) {
                    p.on_fin_acked(id);
                }
            }
            Event::StreamStopped {
                conn,
                id,
                error_code,
            } => {
                if let (Some(p), Some(c)) = (self.parked.get_mut(&conn), self.core.conn_mut(conn)) {
                    p.on_stopped(c, id, error_code);
                }
            }
            _ => {}
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        flush_datagrams(
            &mut self.pending_transmits,
            &mut self.send_retry_at,
            Instant::now(),
            |tx| self.socket.try_send_to(&tx.contents, tx.destination),
        )
    }

    fn fail_socket(&mut self, error: io::Error) {
        let raw_os_error = error.raw_os_error();
        let message = error.to_string();
        let reason = ConnectionError::EndpointFailed {
            message: message.clone(),
            raw_os_error,
        };
        let error = EndpointError::io(error);
        self.shared
            .terminate(EndpointTermination::Failed(error.clone()));
        let pending: Vec<_> = self
            .shared
            .state
            .lock()
            .unwrap()
            .outgoing
            .drain(..)
            .collect();
        for attempt in pending {
            attempt.finish(Err(error.clone()));
        }
        self.shutdown = Some(Instant::now());
        for (_, attempt) in self.attempts.drain() {
            attempt.finish(Err(error.clone()));
        }
        for parked in self.parked.values_mut() {
            parked.mark_closed(reason.clone());
            parked.fail_all();
        }
        self.core.fail(Instant::now(), message, raw_os_error);
        self.pending_transmits.clear();
        self.pump();
    }

    fn finished(&mut self) -> bool {
        if self
            .shutdown
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.core.close(Instant::now());
            let handles: Vec<_> = self
                .core
                .connections()
                .chain(self.core.cleanup_handles())
                .collect();
            for handle in handles {
                let _ =
                    self.core
                        .force_remove(Instant::now(), handle, ConnectionError::LocallyClosed);
            }
            self.pump();
            return true;
        }
        let empty = self.core.is_idle() && self.attempts.is_empty();
        if empty
            && (self.shutdown.is_some()
                || (self.endpoint_gone_seen && self.shared.state.lock().unwrap().in_flight == 0))
        {
            self.shared.terminate(EndpointTermination::Closed);
            return true;
        }
        false
    }
}

fn transient(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

fn flush_datagrams(
    pending: &mut VecDeque<quietquic_proto::outcome::Transmit>,
    retry_at: &mut Option<Instant>,
    now: Instant,
    mut send: impl FnMut(&quietquic_proto::outcome::Transmit) -> io::Result<usize>,
) -> io::Result<()> {
    if retry_at.is_some_and(|at| at > now) {
        return Ok(());
    }
    *retry_at = None;
    for _ in 0..WORK_BUDGET {
        let Some(tx) = pending.front() else { break };
        match send(tx) {
            Ok(n) if n == tx.contents.len() => {
                pending.pop_front();
            }
            Ok(_) => return Err(io::Error::other("partial UDP datagram send")),
            Err(error) if transient(&error) => {
                *retry_at = Some(now + SOCKET_RETRY_DELAY);
                break;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quinn_proto::{Dir, Side, StreamId};
    include!("endpoint_audit_stream_tests.rs");

    #[test]
    fn transient_send_errors_back_off_until_retry_deadline() {
        let now = Instant::now();
        let mut queue = VecDeque::from([quietquic_proto::outcome::Transmit {
            destination: "127.0.0.1:9".parse().unwrap(),
            contents: b"packet".to_vec(),
        }]);
        let mut retry_at = None;
        let mut calls = 0;
        flush_datagrams(&mut queue, &mut retry_at, now, |_| {
            calls += 1;
            Err(io::Error::new(io::ErrorKind::Interrupted, "injected"))
        })
        .unwrap();
        let first_retry = retry_at.unwrap();
        assert_eq!(calls, 1);

        flush_datagrams(
            &mut queue,
            &mut retry_at,
            first_retry - Duration::from_millis(1),
            |_| {
                calls += 1;
                Ok(6)
            },
        )
        .unwrap();
        assert_eq!(calls, 1, "no send is attempted before retry_at");

        flush_datagrams(&mut queue, &mut retry_at, first_retry, |_| {
            calls += 1;
            Err(io::Error::new(io::ErrorKind::WouldBlock, "injected"))
        })
        .unwrap();
        let second_retry = retry_at.unwrap();
        assert_eq!(calls, 2);
        flush_datagrams(&mut queue, &mut retry_at, second_retry, |tx| {
            calls += 1;
            Ok(tx.contents.len())
        })
        .unwrap();
        assert_eq!(calls, 3);
        assert!(queue.is_empty());
        assert!(retry_at.is_none());

        queue.push_back(quietquic_proto::outcome::Transmit {
            destination: "127.0.0.1:9".parse().unwrap(),
            contents: b"fatal".to_vec(),
        });
        let error = flush_datagrams(&mut queue, &mut retry_at, second_retry, |_| {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "fatal"))
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(queue.len(), 1);
    }

    // No driver tasks run in this harness: each operation is deliberately
    // stopped between driver completion and the application's next poll.
    struct ManualPair {
        a: Driver,
        b: Driver,
    }
    impl ManualPair {
        async fn connected() -> (Self, Endpoint, Endpoint, Connection, Connection) {
            let cfg = client();
            let credential = crate::config::ClientEntry {
                client_id: cfg.client_id.clone(),
                psk: cfg.psk.clone(),
            };
            let (a_endpoint, a) = unstarted().await;
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let (b_endpoint, b) =
                Endpoint::build(socket, EndpointConfig::accept(vec![credential])).unwrap();
            let mut pair = Self { a, b };
            let mut cfg = client();
            cfg.server = b_endpoint.local_addr();
            let a_conn = pair.complete(a_endpoint.connect(cfg)).unwrap();
            let b_conn = pair.complete(b_endpoint.accept()).unwrap().unwrap();
            (pair, a_endpoint, b_endpoint, a_conn, b_conn)
        }

        fn drive(&mut self) {
            for _ in 0..16 {
                self.a.control();
                self.b.control();
                self.a.pump();
                self.b.pump();
                while let Some(tx) = self.a.pending_transmits.pop_front() {
                    self.b
                        .core
                        .handle_datagram(Instant::now(), self.a.shared.local, &tx.contents);
                }
                while let Some(tx) = self.b.pending_transmits.pop_front() {
                    self.a
                        .core
                        .handle_datagram(Instant::now(), self.b.shared.local, &tx.contents);
                }
            }
        }

        fn complete<F: Future>(&mut self, future: F) -> F::Output {
            let mut future = Box::pin(tokio::task::unconstrained(future));
            for _ in 0..32 {
                if let Poll::Ready(value) = future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                {
                    return value;
                }
                self.drive();
            }
            panic!("operation did not finish in controlled driver passes");
        }
    }

    #[tokio::test]
    async fn stream_accept_canceled_after_driver_completion_returns_same_stream() {
        let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        pair.complete(send.write_all(b"ready")).unwrap();
        let mut accept = Box::pin(server.accept_bi());
        assert!(accept
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.drive();
        drop(accept);
        // Retry immediately, without yielding a cleanup grace period.
        let (_send, mut recv) = pair.complete(server.accept_bi()).unwrap();
        assert_eq!(recv.id(), send.id());
        assert_eq!(pair.complete(recv.read(32)).unwrap(), b"ready");
    }

    #[tokio::test]
    async fn open_canceled_after_allocation_reclaims_both_stream_directions() {
        let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
        let mut open = Box::pin(client.open_bi());
        assert!(open
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.a.control();
        let canceled = StreamId::new(Side::Client, Dir::Bi, 0);
        drop(open);
        pair.a.control();
        assert!(pair
            .a
            .core
            .conn_mut(client.handle())
            .unwrap()
            .send_fin(canceled)
            .is_none());

        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        pair.complete(send.write_all(b"healthy after canceled open"))
            .unwrap();
        let (_canceled_send, mut canceled_recv) = pair.complete(server.accept_bi()).unwrap();
        assert_eq!(canceled_recv.id(), canceled);
        assert!(matches!(
            pair.complete(canceled_recv.read(64)),
            Err(crate::conn::ConnError::Reset { code }) if code == crate::conn::AUTO_RESET_CANCELLED_OPEN
        ));
        let (_send, mut recv) = pair.complete(server.accept_bi()).unwrap();
        assert_eq!(recv.id(), send.id());
        assert_eq!(
            pair.complete(recv.read(64)).unwrap(),
            b"healthy after canceled open"
        );
    }

    #[tokio::test]
    async fn concurrent_accept_bi_waiters_receive_streams_fifo() {
        let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
        let mut first_accept = Box::pin(server.accept_bi());
        let mut second_accept = Box::pin(server.accept_bi());
        assert!(first_accept
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert!(second_accept
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.b.control();

        let (mut first_send, _first_recv) = pair.complete(client.open_bi()).unwrap();
        let (mut second_send, _second_recv) = pair.complete(client.open_bi()).unwrap();
        pair.complete(first_send.write_all(b"first")).unwrap();
        pair.complete(second_send.write_all(b"second")).unwrap();
        pair.drive();
        let (_send, first_recv) = pair.complete(first_accept).unwrap();
        let (_send, second_recv) = pair.complete(second_accept).unwrap();
        assert_eq!(first_recv.id(), first_send.id());
        assert_eq!(second_recv.id(), second_send.id());
    }

    #[tokio::test]
    async fn accept_waiting_for_operation_budget_wakes_with_terminal_cause() {
        let (mut pair, _a, _b, _client, server) = ManualPair::connected().await;
        let mut held = Vec::new();
        for _ in 0..256 {
            let mut accept = Box::pin(server.accept_bi());
            assert!(accept
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
            pair.b.control();
            held.push(accept);
        }
        let mut waiting = Box::pin(server.accept_bi());
        assert!(waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        let reason = ConnectionError::EndpointFailed {
            message: "closed while waiting for accept budget".into(),
            raw_os_error: None,
        };
        pair.b
            .parked
            .get_mut(&server.handle())
            .unwrap()
            .mark_closed(reason.clone());
        assert!(matches!(
            pair.complete(waiting),
            Err(crate::conn::ConnError::ConnectionLost { reason: got }) if got == reason
        ));
        drop(held);
    }

    #[tokio::test]
    async fn try_open_bi_never_bypasses_waiter_and_preserves_terminal_cause() {
        let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
        let mut held = Vec::new();
        for _ in 0..32 {
            held.push(pair.complete(client.open_bi()).unwrap());
        }
        let mut waiting = Box::pin(client.open_bi());
        assert!(waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.a.control();
        assert!(matches!(
            pair.complete(client.try_open_bi()).unwrap(),
            crate::conn::TryOpenOutcome::TemporarilyUnavailable
        ));

        // Grant additional credit without servicing the local parked opener
        // yet. Quinn batches MAX_STREAMS updates, so increase beyond its
        // update threshold. A later try-open must not get the first new id.
        pair.b
            .core
            .conn_mut(server.handle())
            .unwrap()
            .conn_mut()
            .set_max_concurrent_streams(Dir::Bi, VarInt::from_u32(64));
        pair.b.pump();
        while let Some(tx) = pair.b.pending_transmits.pop_front() {
            pair.a
                .core
                .handle_datagram(Instant::now(), pair.b.shared.local, &tx.contents);
        }
        let later = pair.complete(client.try_open_bi()).unwrap();
        let (send, recv) = pair.complete(waiting).unwrap();
        assert_eq!(send.id(), StreamId::new(Side::Client, Dir::Bi, 32));
        if let crate::conn::TryOpenOutcome::Opened((later_send, later_recv)) = later {
            assert!(later_send.id() > send.id());
            drop((later_send, later_recv));
        }
        drop((send, recv));
        drop(held);
        pair.drive();

        let reason = ConnectionError::EndpointFailed {
            message: "injected terminal cause".into(),
            raw_os_error: None,
        };
        pair.a
            .parked
            .get_mut(&client.handle())
            .unwrap()
            .mark_closed(reason.clone());
        assert!(matches!(
            pair.complete(client.try_open_bi()),
            Err(crate::conn::ConnError::ConnectionLost { reason: got }) if got == reason
        ));
    }

    #[tokio::test]
    async fn incremental_read_canceled_after_driver_completion_preserves_bytes() {
        let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        pair.complete(send.write_all(b"unclaimed bytes")).unwrap();
        let (_send, mut recv) = pair.complete(server.accept_bi()).unwrap();
        let mut read = Box::pin(recv.read(64));
        assert!(read
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.drive();
        drop(read);
        assert_eq!(pair.complete(recv.read(64)).unwrap(), b"unclaimed bytes");
    }

    #[tokio::test]
    async fn canceled_partial_failure_retains_prefix_after_connection_is_removed() {
        let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        pair.complete(send.write_all(b"recover after close"))
            .unwrap();
        let (_send, mut recv) = pair.complete(server.accept_bi()).unwrap();
        let mut read = Box::pin(recv.read_to_end(64));
        assert!(read
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.drive();
        pair.b
            .core
            .close_connection(
                Instant::now(),
                server.handle(),
                VarInt::from_u32(0),
                Vec::new(),
            )
            .unwrap();
        pair.drive();
        drop(read);
        let error = pair.complete(recv.read_to_end(64)).unwrap_err();
        assert_eq!(error.prefix, b"recover after close");
    }

    #[tokio::test]
    async fn canceled_collector_prefix_survives_a_later_connection_close() {
        let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        pair.complete(send.write_all(b"saved before close"))
            .unwrap();
        let (_send, mut recv) = pair.complete(server.accept_bi()).unwrap();
        let mut read = Box::pin(recv.read_to_end(64));
        assert!(read
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.drive();
        drop(read);
        pair.drive();
        pair.b
            .core
            .close_connection(
                Instant::now(),
                server.handle(),
                VarInt::from_u32(0),
                Vec::new(),
            )
            .unwrap();
        pair.drive();
        let error = pair.complete(recv.read_to_end(64)).unwrap_err();
        assert_eq!(error.prefix, b"saved before close");
    }

    #[tokio::test]
    async fn canceled_complete_collector_obeys_a_smaller_retry_limit() {
        let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        pair.complete(send.write_all(b"12345678")).unwrap();
        pair.complete(send.finish()).unwrap();
        let (_send, mut recv) = pair.complete(server.accept_bi()).unwrap();
        let mut read = Box::pin(recv.read_to_end(64));
        assert!(read
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.drive();
        drop(read);
        let error = pair.complete(recv.read_to_end(4)).unwrap_err();
        assert_eq!(error.prefix, b"1234");
        assert_eq!(
            error.error,
            crate::conn::ConnError::ReadLimitExceeded { limit: 4 }
        );
    }

    #[tokio::test]
    async fn reset_during_collection_returns_already_consumed_prefix() {
        let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        pair.complete(send.write_all(b"collected before reset"))
            .unwrap();
        let (_send, mut recv) = pair.complete(server.accept_bi()).unwrap();
        let mut read = Box::pin(recv.read_to_end(128));
        assert!(read
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.drive();
        pair.complete(send.reset(99)).unwrap();
        let error = pair.complete(read).unwrap_err();
        assert_eq!(error.prefix, b"collected before reset");
        assert_eq!(error.error, crate::conn::ConnError::Reset { code: 99 });
    }

    #[tokio::test]
    async fn stream_drop_cleanup_survives_a_full_ordinary_command_queue() {
        let (mut pair, _a, _b, client, _server) = ManualPair::connected().await;
        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        let id = send.id();
        pair.complete(send.write_all(b"opened")).unwrap();
        let mut waiters = Vec::new();
        for _ in 0..COMMAND_CAPACITY {
            let mut future = Box::pin(tokio::task::unconstrained(client.open_bi()));
            assert!(future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
            waiters.push(future);
        }
        assert_eq!(pair.a.command_rx.len(), COMMAND_CAPACITY);
        drop(send);
        pair.a.control();
        pair.a.pump();
        assert!(
            pair.a
                .core
                .conn_mut(client.handle())
                .unwrap()
                .send_fin(id)
                .is_none(),
            "drop must release send bookkeeping even when ordinary commands are full"
        );
        drop(waiters);
        pair.drive();
    }

    #[tokio::test]
    async fn write_canceled_before_dispatch_preserves_the_send_direction() {
        let (mut pair, _a, _b, client, server) = ManualPair::connected().await;
        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        let mut write = Box::pin(send.write_all(b"canceled"));
        assert!(write
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(write);
        pair.complete(send.write_all(b"kept")).unwrap();
        let (_send, mut recv) = pair.complete(server.accept_bi()).unwrap();
        assert_eq!(pair.complete(recv.read(64)).unwrap(), b"kept");
    }

    #[tokio::test]
    async fn write_canceled_after_driver_completion_resets_sending() {
        let (mut pair, _a, _b, client, _server) = ManualPair::connected().await;
        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        let mut write = Box::pin(send.write_all(b"accepted by transport"));
        assert!(write
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.drive();
        drop(write);
        assert_eq!(
            pair.complete(send.reset(123)).unwrap(),
            crate::conn::ResetOutcome::AlreadyReset {
                code: crate::conn::AUTO_RESET_CANCELLED_WRITE,
            }
        );
    }

    #[tokio::test]
    async fn outgoing_result_abandoned_before_handoff_closes_only_that_connection() {
        let (mut pair, a, b, client, _server) = ManualPair::connected().await;
        let mut config = client_config_for(b.local_addr());
        config.bind = None;
        let attempt = a.connect(config);
        pair.drive();
        assert!(attempt.attempt.state.lock().unwrap().result.is_some());
        assert_eq!(pair.a.core.connections().count(), 2);
        drop(attempt);
        pair.drive();
        assert_eq!(pair.a.core.connections().count(), 1);
        assert!(pair.a.core.conn_mut(client.handle()).is_some());
    }

    #[tokio::test]
    async fn fatal_socket_failure_reaches_active_and_late_connection_operations() {
        let (mut pair, _a, endpoint, client, server) = ManualPair::connected().await;
        let (mut send, _recv) = pair.complete(client.open_bi()).unwrap();
        pair.complete(send.write_all(b"partial before socket failure"))
            .unwrap();
        let (_send, mut recv) = pair.complete(server.accept_bi()).unwrap();
        let mut read = Box::pin(recv.read_to_end(128));
        assert!(read
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        pair.drive();
        pair.b.fail_socket(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected active failure",
        ));
        assert!(pair.b.finished());
        let error = pair.complete(read).unwrap_err();
        assert_eq!(error.prefix, b"partial before socket failure");
        assert!(matches!(
            error.error,
            crate::conn::ConnError::ConnectionLost {
                reason: ConnectionError::EndpointFailed { .. }
            }
        ));
        assert!(matches!(
            pair.complete(server.open_bi()),
            Err(crate::conn::ConnError::ConnectionLost {
                reason: ConnectionError::EndpointFailed { .. }
            })
        ));
        assert!(matches!(
            endpoint.terminated().await,
            EndpointTermination::Failed(_)
        ));
        assert!(pair.b.core.is_idle());
    }

    fn client_config_for(address: SocketAddr) -> ClientConfigFile {
        let mut config = client();
        config.server = address;
        config
    }

    async fn unstarted() -> (Endpoint, Driver) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        Endpoint::build(socket, EndpointConfig::dial()).unwrap()
    }

    fn client() -> ClientConfigFile {
        toml::from_str("client_id='test'\nserver='127.0.0.1:9'\npsk='0000000000000000000000000000000000000000000000000000000000000001'").unwrap()
    }

    #[tokio::test]
    async fn fatal_socket_failure_finishes_queued_attempt_and_persists() {
        let (endpoint, mut driver) = unstarted().await;
        let attempt = endpoint.connect(client());
        driver.fail_socket(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected socket failure",
        ));
        assert!(matches!(attempt.await, Err(EndpointError::Socket { .. })));
        let reason = endpoint.terminated().await;
        assert!(
            matches!(&reason, EndpointTermination::Failed(EndpointError::Socket { message, .. }) if message == "injected socket failure")
        );
        endpoint.close(0, b"late close").unwrap();
        assert_eq!(endpoint.terminated().await, reason);
        assert!(driver.finished());
        assert!(driver.core.is_idle());
    }

    #[tokio::test]
    async fn canceled_attempt_before_dispatch_never_allocates_transport() {
        let (endpoint, mut driver) = unstarted().await;
        let attempt = endpoint.connect(client());
        assert_eq!(endpoint.shared.state.lock().unwrap().in_flight, 1);
        drop(attempt);
        driver.control();
        assert!(driver.core.is_idle());
        assert_eq!(endpoint.shared.state.lock().unwrap().in_flight, 0);
    }

    #[tokio::test]
    async fn repeated_canceled_dials_release_queue_storage_without_driver_service() {
        let (endpoint, mut driver) = unstarted().await;
        for _ in 0..1024 {
            drop(endpoint.connect(client()));
            let state = endpoint.shared.state.lock().unwrap();
            assert_eq!(state.in_flight, 0);
            assert!(state.outgoing.is_empty());
        }
        driver.control();
        assert!(driver.core.is_idle());
    }

    #[tokio::test]
    async fn late_dial_retains_original_socket_failure() {
        let (endpoint, mut driver) = unstarted().await;
        driver.fail_socket(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "persistent failure",
        ));
        for _ in 0..2 {
            assert!(matches!(endpoint.connect(client()).await,
                Err(EndpointError::Socket { message, .. }) if message == "persistent failure"));
        }
    }

    #[tokio::test]
    async fn receive_errors_back_off_or_terminate_without_rebinding() {
        let (endpoint, mut driver) = unstarted().await;
        let local = endpoint.local_addr();
        let now = Instant::now();
        driver.received(Err(io::Error::from(io::ErrorKind::Interrupted)), &[]);
        assert!(driver.recv_retry_at.unwrap() >= now + SOCKET_RETRY_DELAY);
        assert!(endpoint.shared.termination.borrow().is_none());
        let attempt = endpoint.connect(client());
        driver.received(
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "receive failure",
            )),
            &[],
        );
        assert!(
            matches!(attempt.await, Err(EndpointError::Socket { message, .. }) if message == "receive failure")
        );
        assert!(driver.finished());
        assert_eq!(driver.socket.local_addr().unwrap(), local);
        assert!(matches!(
            endpoint.terminated().await,
            EndpointTermination::Failed(_)
        ));
        assert!(std::net::UdpSocket::bind(local).is_err());
        drop(driver);
        assert!(std::net::UdpSocket::bind(local).is_ok());
    }

    #[tokio::test]
    async fn endpoint_accept_fifo_survives_middle_waiter_cancellation() {
        let (mut pair, a, b, _client, _server) = ManualPair::connected().await;
        let mut first = Box::pin(b.accept());
        let mut middle = Box::pin(b.accept());
        let mut last = Box::pin(b.accept());
        for waiter in [&mut first, &mut middle, &mut last] {
            assert!(waiter
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
        }
        drop(middle);
        let _first_client = pair
            .complete(a.connect(client_config_for(b.local_addr())))
            .unwrap();
        let _last_client = pair
            .complete(a.connect(client_config_for(b.local_addr())))
            .unwrap();
        let ids: Vec<_> = b
            .shared
            .state
            .lock()
            .unwrap()
            .incoming
            .iter()
            .map(Connection::handle)
            .collect();
        assert_eq!(ids.len(), 2);
        // Polling the newer waiter first cannot let it bypass the oldest.
        assert!(last
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert_eq!(pair.complete(first).unwrap().unwrap().handle(), ids[0]);
        assert_eq!(pair.complete(last).unwrap().unwrap().handle(), ids[1]);
        assert!(b.shared.state.lock().unwrap().waiters.is_empty());
    }

    #[tokio::test]
    async fn fatal_receive_wakes_all_acceptors_and_persists_for_late_operations() {
        let (mut pair, _a, b, _client, _server) = ManualPair::connected().await;
        let mut waits = Vec::new();
        for _ in 0..3 {
            let mut accept = Box::pin(b.accept());
            assert!(accept
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
            waits.push(accept);
        }
        pair.b.received(
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "receive failure",
            )),
            &[],
        );
        for wait in waits {
            assert!(
                matches!(pair.complete(wait), Err(EndpointError::Socket { message, .. }) if message == "receive failure")
            );
        }
        assert!(b.shared.state.lock().unwrap().waiters.is_empty());
        for _ in 0..2 {
            assert!(
                matches!(pair.complete(b.accept()), Err(EndpointError::Socket { message, .. }) if message == "receive failure")
            );
        }
        assert!(matches!(
            b.pause_admission(),
            Err(EndpointError::Socket { .. })
        ));
        assert!(matches!(
            b.set_transport_defaults(TransportSettings::default()),
            Err(EndpointError::Socket { .. })
        ));
        let reason = pair.complete(b.terminated());
        b.close(0, b"late close").unwrap();
        assert_eq!(pair.complete(b.terminated()), reason);
        assert!(pair.b.finished());
    }

    #[tokio::test]
    async fn canceling_wait_closed_neither_starts_nor_stops_shutdown() {
        let endpoint = Endpoint::bind("127.0.0.1:0".parse().unwrap(), EndpointConfig::dial())
            .await
            .unwrap();
        let local = endpoint.local_addr();
        let mut wait = Box::pin(endpoint.wait_closed());
        assert!(wait
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(wait);
        assert!(endpoint.shared.termination.borrow().is_none());
        assert!(std::net::UdpSocket::bind(local).is_err());
        endpoint.close(0, b"done").unwrap();
        let mut wait = Box::pin(endpoint.wait_closed());
        assert!(wait
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(wait);
        endpoint.close(0, b"repeat").unwrap();
        tokio::time::timeout(Duration::from_secs(2), endpoint.wait_closed())
            .await
            .unwrap();
        assert!(std::net::UdpSocket::bind(local).is_ok());
    }

    #[tokio::test]
    async fn last_connection_owner_drop_closes_only_its_transport() {
        let (mut pair, a, b, client, server) = ManualPair::connected().await;
        let healthy = pair
            .complete(a.connect(client_config_for(b.local_addr())))
            .unwrap();
        let peer = pair.complete(b.accept()).unwrap().unwrap();
        let old = client.handle();
        drop(client);
        pair.drive();
        assert!(pair.a.core.conn_mut(old).is_none());
        assert!(matches!(
            pair.complete(server.closed()),
            ConnectionError::ApplicationClosed { code: 0, .. }
        ));
        let (mut send, _recv) = pair.complete(healthy.open_bi()).unwrap();
        pair.complete(send.write_all(b"surviving owner")).unwrap();
        let (_send, mut recv) = pair.complete(peer.accept_bi()).unwrap();
        assert_eq!(pair.complete(recv.read(32)).unwrap(), b"surviving owner");
    }

    #[tokio::test]
    async fn per_attempt_transport_override_is_advertised_without_changing_defaults() {
        let (mut pair, a, b, _client, server) = ManualPair::connected().await;
        let mut transport = quinn_proto::TransportConfig::default();
        transport.max_concurrent_bidi_streams(VarInt::from_u32(1));
        let _limited = pair
            .complete(a.connect_with(
                client_config_for(b.local_addr()),
                ConnectOptions {
                    transport: Some(TransportSettings::new(transport)),
                    handshake_timeout: None,
                },
            ))
            .unwrap();
        let limited_peer = pair.complete(b.accept()).unwrap().unwrap();
        let _one = pair.complete(limited_peer.open_bi()).unwrap();
        assert!(matches!(
            pair.complete(limited_peer.try_open_bi()).unwrap(),
            crate::conn::TryOpenOutcome::TemporarilyUnavailable
        ));
        let _existing_one = pair.complete(server.open_bi()).unwrap();
        let _existing_two = pair.complete(server.open_bi()).unwrap();
        let _default = pair
            .complete(a.connect(client_config_for(b.local_addr())))
            .unwrap();
        let default_peer = pair.complete(b.accept()).unwrap().unwrap();
        let _default_one = pair.complete(default_peer.open_bi()).unwrap();
        let _default_two = pair.complete(default_peer.open_bi()).unwrap();
    }

    #[tokio::test]
    async fn accept_filters_closed_connection_before_driver_queue_reaping() {
        let (mut pair, a, b, _client, _server) = ManualPair::connected().await;
        let _extra = pair
            .complete(a.connect(client_config_for(b.local_addr())))
            .unwrap();
        pair.drive();
        let handle = b
            .shared
            .state
            .lock()
            .unwrap()
            .incoming
            .front()
            .unwrap()
            .handle();
        pair.b
            .parked
            .get(&handle)
            .unwrap()
            .mark_closed(ConnectionError::LocallyClosed);
        let mut accept = Box::pin(b.accept());
        assert!(accept
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert!(b.shared.state.lock().unwrap().incoming.is_empty());
        drop(accept);
        let _healthy = pair
            .complete(a.connect(client_config_for(b.local_addr())))
            .unwrap();
        let accepted = pair.complete(b.accept()).unwrap().unwrap();
        assert_ne!(accepted.handle(), handle);
        assert_eq!(accepted.remote_address(), a.local_addr());
    }

    #[tokio::test]
    async fn queued_attempt_captures_transport_defaults_at_request() {
        let (endpoint, _driver) = unstarted().await;
        let before = endpoint.shared.state.lock().unwrap().transport.clone();
        let attempt = endpoint.connect(client());
        let replacement = TransportSettings::default();
        endpoint
            .set_transport_defaults(replacement.clone())
            .unwrap();
        let after = endpoint.connect(client());
        for (attempt, expected) in [(&attempt, &before), (&after, &replacement)] {
            let state = attempt.attempt.state.lock().unwrap();
            let (_, options) = state.request.as_ref().unwrap();
            assert!(std::ptr::eq(
                options.transport.as_ref().unwrap().as_quinn(),
                expected.as_quinn()
            ));
        }
    }
}

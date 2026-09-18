// SPDX-License-Identifier: 0BSD
//! Post-handshake connection and stream handles, shared by client and server.
//!
//! In sans-IO `quinn-proto`, the DRIVER owns the `quinn_proto::Connection` and
//! must pump it continuously (poll_transmit → socket, feed inbound datagrams,
//! service timers, drain app events). Application code cannot hold the
//! `Connection` directly without stalling that pump. So [`Connection`] and
//! [`SendStream`] / [`RecvStream`] here are *lightweight handles*: they send
//! commands to the driver over a bounded [`tokio::sync::mpsc`] channel. Owned
//! replies remain reclaimable until the application receives them. The driver applies each command against
//! the owned `quinn_proto::Connection` inside its event loop and routes stream
//! events back.
//!
//! The shared driver in [`crate::endpoint`] uses this plumbing for incoming and
//! outgoing connections. The client and server conveniences use that same driver.
//!
//! # Why the *parking* lives here and not in the core
//!
//! `quietquic_proto` is sans-IO: it cannot wait for anything, so its
//! `ConnState::stream_read` answers `Read(n)` / `Blocked` / `Finished` right
//! now and never completes later. But this crate's public API promises
//! `RecvStream::read_to_end(limit).await` — a call that *does* complete later.
//! The difference between those two shapes is exactly `Parked`: the map of
//! handle operations that have been offered to the core, come back `Blocked`,
//! and are now waiting for the [`quietquic_proto::outcome::Event`] that says
//! "try again". The driver owns one `Parked` per live connection and services
//! it from its event dispatch; the core stays free of channels and runtimes.
//!
//! # Forward-compat seam ([`Connection::quinn_connection`])
//!
//! HTTP/3 (`h3`) and other stream protocols need a handle onto the underlying
//! QUIC connection to open/accept streams and drive protocol frames. Because the
//! driver — not the application — owns the `quinn_proto::Connection`, we cannot
//! hand out a `&quinn_proto::Connection` reference (it lives on another task,
//! mutated behind the command channel). Instead [`Connection::quinn_connection`]
//! returns a [`QuinnHandle`]: the minimal, `Clone`able command surface an
//! a stream protocol can use to open/accept uniquely owned bidirectional halves
//! without touching the cloaking/pre-filter layer. See
//! [`QuinnHandle`] for the shape and the rationale.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use quietquic_proto::conn::{ConnState as CoreConn, SendFin};
use quietquic_proto::outcome::{ConnectionHandle, ReadOutcome, WriteOutcome};
use quinn_proto::{StreamId, VarInt};
use tokio::sync::{mpsc, oneshot, watch, Notify, OwnedSemaphorePermit, Semaphore};

struct HandoffState<T> {
    value: Option<T>,
    canceled: bool,
    claimed: bool,
}
pub(crate) struct HandoffTx<T> {
    state: Arc<Mutex<HandoffState<T>>>,
    notify: Arc<Notify>,
    completed: bool,
}
struct HandoffRx<T> {
    state: Arc<Mutex<HandoffState<T>>>,
    notify: Arc<Notify>,
    reclaim: Option<Box<dyn FnOnce(T) + Send>>,
    cancel: Option<Box<dyn FnOnce() + Send>>,
}

fn handoff<T>(
    reclaim: impl FnOnce(T) + Send + 'static,
    cancel: impl FnOnce() + Send + 'static,
) -> (HandoffTx<T>, HandoffRx<T>) {
    let state = Arc::new(Mutex::new(HandoffState {
        value: None,
        canceled: false,
        claimed: false,
    }));
    let notify = Arc::new(Notify::new());
    (
        HandoffTx {
            state: state.clone(),
            notify: notify.clone(),
            completed: false,
        },
        HandoffRx {
            state,
            notify,
            reclaim: Some(Box::new(reclaim)),
            cancel: Some(Box::new(cancel)),
        },
    )
}

impl<T> HandoffTx<T> {
    fn is_closed(&self) -> bool {
        self.state.lock().unwrap().canceled
    }
    fn send(mut self, value: T) -> Result<(), T> {
        let mut state = self.state.lock().unwrap();
        if state.canceled {
            return Err(value);
        }
        state.value = Some(value);
        drop(state);
        self.completed = true;
        self.notify.notify_one();
        Ok(())
    }
}

impl<T> HandoffRx<T> {
    async fn receive(mut self) -> Option<T> {
        loop {
            let notified = self.notify.notified();
            let outcome = {
                let mut state = self.state.lock().unwrap();
                if let Some(value) = state.value.take() {
                    state.claimed = true;
                    Some(Ok(value))
                } else if state.canceled {
                    Some(Err(()))
                } else {
                    None
                }
            };
            if let Some(Ok(value)) = outcome {
                self.reclaim = None;
                self.cancel = None;
                return Some(value);
            }
            if matches!(outcome, Some(Err(()))) {
                return None;
            }
            notified.await;
        }
    }
}

impl<T> Drop for HandoffTx<T> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut state = self.state.lock().unwrap();
        state.canceled = true;
        drop(state);
        self.notify.notify_one();
    }
}

impl<T> Drop for HandoffRx<T> {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        if state.claimed {
            return;
        }
        state.canceled = true;
        let value = state.value.take();
        drop(state);
        if let (Some(value), Some(reclaim)) = (value, self.reclaim.take()) {
            reclaim(value);
        }
        if let Some(cancel) = self.cancel.take() {
            cancel();
        }
    }
}

/// Errors surfaced by [`Connection`], [`SendStream`], and [`RecvStream`]
/// operations.
///
/// The enum itself lives in the sans-IO core (`quietquic_proto::conn`) so both
/// layers report failures in the same vocabulary — a hand-rolled embedder and a
/// tokio application see one error type, not two that must be translated. It is
/// re-exported here so `quietquic::conn::ConnError` keeps resolving.
pub use quietquic_proto::conn::{
    AutomaticCode, ConnError, ResetOutcome, AUTO_CODE_START, AUTO_RESET_CANCELLED_OPEN,
    AUTO_RESET_CANCELLED_WRITE, AUTO_RESET_DROPPED_SEND, AUTO_STOP_CANCELLED_OPEN,
    AUTO_STOP_DROPPED_RECV, AUTO_STOP_READ_LIMIT,
};
pub use quietquic_proto::outcome::ConnectionError;

/// A command paired with the connection it targets. The driver owns one
/// `mpsc::Receiver<Tagged>` across all its connections and routes each command
/// to the matching [`ConnState`] by `handle`. (The server owns many connections;
/// the client owns one — the same channel shape serves both.)
pub(crate) struct Tagged {
    pub(crate) handle: ConnectionHandle,
    pub(crate) cmd: Cmd,
}

pub(crate) struct TaggedCleanup {
    pub(crate) handle: ConnectionHandle,
    pub(crate) cleanup: Cleanup,
}

#[derive(Debug)]
pub(crate) enum Cleanup {
    ResetSend { id: StreamId, code: u64 },
    DropSend { id: StreamId },
    StopRecv { id: StreamId, code: u64 },
    ReturnAccepted { id: StreamId },
    ReleaseRecv { id: StreamId },
    ReleaseSend { id: StreamId },
    ForgetStream { id: StreamId },
    Wake,
    OwnerDropped,
}

pub(crate) struct OwnerLease {
    handle: ConnectionHandle,
    cleanup: mpsc::UnboundedSender<TaggedCleanup>,
}

impl Drop for OwnerLease {
    fn drop(&mut self) {
        let _ = self.cleanup.send(TaggedCleanup {
            handle: self.handle,
            cleanup: Cleanup::OwnerDropped,
        });
    }
}

/// A [`Cmd`] channel sender pre-bound to one connection's handle, so handles can
/// enqueue commands without knowing the routing key. Cloned freely across a
/// connection's [`Connection`] / [`SendStream`] / [`RecvStream`] /
/// [`QuinnHandle`] handles.
#[derive(Clone)]
pub(crate) struct CmdSender {
    handle: ConnectionHandle,
    tx: mpsc::Sender<Tagged>,
    cleanup: mpsc::UnboundedSender<TaggedCleanup>,
    _owner: Arc<OwnerLease>,
    closed: watch::Receiver<Option<ConnectionError>>,
    write_budget: Arc<Semaphore>,
    open_budget: Arc<Semaphore>,
    accept_budget: Arc<Semaphore>,
    wake: Arc<Notify>,
}

impl CmdSender {
    pub(crate) fn new(
        handle: ConnectionHandle,
        tx: mpsc::Sender<Tagged>,
        cleanup: mpsc::UnboundedSender<TaggedCleanup>,
        closed: watch::Receiver<Option<ConnectionError>>,
        wake: Arc<Notify>,
    ) -> Self {
        let owner = Arc::new(OwnerLease {
            handle,
            cleanup: cleanup.clone(),
        });
        Self {
            handle,
            tx,
            cleanup,
            _owner: owner,
            closed,
            write_budget: Arc::new(Semaphore::new(256 * 1024)),
            open_budget: Arc::new(Semaphore::new(256)),
            accept_budget: Arc::new(Semaphore::new(256)),
            wake,
        }
    }

    async fn send(&self, cmd: Cmd) -> Result<(), ConnError> {
        if let Some(reason) = self.closed.borrow().clone() {
            return Err(ConnError::ConnectionLost { reason });
        }
        let mut closed = self.closed.clone();
        tokio::select! {
            result = self.tx.send(Tagged { handle: self.handle, cmd }) => {
                result.map_err(|_| self.terminal_error().unwrap_or(ConnError::Closed))
            }
            changed = closed.changed() => {
                if changed.is_err() { Err(ConnError::Closed) }
                else { Err(self.terminal_error().unwrap_or(ConnError::Closed)) }
            }
        }
    }

    fn terminal_error(&self) -> Option<ConnError> {
        self.closed
            .borrow()
            .clone()
            .map(|reason| ConnError::ConnectionLost { reason })
    }

    async fn acquire_operation(
        &self,
        budget: Arc<Semaphore>,
    ) -> Result<OwnedSemaphorePermit, ConnError> {
        if let Some(error) = self.terminal_error() {
            return Err(error);
        }
        let mut closed = self.closed.clone();
        tokio::select! {
            permit = budget.acquire_owned() => permit.map_err(|_| ConnError::Closed),
            changed = closed.changed() => {
                if changed.is_err() { Err(ConnError::Closed) }
                else { Err(self.terminal_error().unwrap_or(ConnError::Closed)) }
            }
        }
    }

    async fn acquire_write(&self, bytes: u32) -> Result<OwnedSemaphorePermit, ConnError> {
        if let Some(error) = self.terminal_error() {
            return Err(error);
        }
        let mut closed = self.closed.clone();
        tokio::select! {
            permit = self.write_budget.clone().acquire_many_owned(bytes) => permit.map_err(|_| ConnError::Closed),
            changed = closed.changed() => {
                if changed.is_err() { Err(ConnError::Closed) }
                else { Err(self.terminal_error().unwrap_or(ConnError::Closed)) }
            }
        }
    }

    fn try_send(&self, cmd: Cmd) -> Result<(), mpsc::error::TrySendError<Tagged>> {
        self.tx.try_send(Tagged {
            handle: self.handle,
            cmd,
        })
    }

    fn cleanup(&self, cleanup: Cleanup) {
        if matches!(cleanup, Cleanup::Wake) {
            self.wake.notify_one();
            return;
        }
        let _ = self.cleanup.send(TaggedCleanup {
            handle: self.handle,
            cleanup,
        });
    }
}

/// One command the driver applies against its owned `quinn_proto::Connection`.
///
/// Every variant that produces a result carries a [`oneshot::Sender`] the driver
/// fires once it has serviced the command (immediately for open/finish, or once
/// data/FIN arrives for a read). This keeps the driver in sole control of the
/// `Connection` while letting handles await outcomes.
pub(crate) enum Cmd {
    /// Open a new bidirectional stream; reply with its assigned id.
    OpenBi(HandoffTx<Result<StreamId, ConnError>>),
    TryOpenBi(HandoffTx<Result<TryOpenOutcome<StreamId>, ConnError>>),
    /// Await the next peer-initiated bidirectional stream; reply with its id.
    AcceptBi(HandoffTx<Result<StreamId, ConnError>>),
    /// Append `data` to a send stream. Replies once fully buffered (the driver
    /// handles write-blocking internally and re-tries on `Writable`).
    Write {
        id: StreamId,
        data: Vec<u8>,
        reply: oneshot::Sender<Result<(), ConnError>>,
        started: Arc<AtomicBool>,
        permit: OwnedSemaphorePermit,
    },
    /// Finish (FIN) a send stream.
    Finish {
        id: StreamId,
        reply: oneshot::Sender<Result<(), ConnError>>,
        finished: Arc<AtomicBool>,
        terminal: Arc<Mutex<Option<Result<(), ConnError>>>>,
    },
    /// Wait until a previously finished send stream reaches a terminal fact.
    WaitFinished {
        id: StreamId,
        reply: oneshot::Sender<Result<(), ConnError>>,
        terminal: Arc<Mutex<Option<Result<(), ConnError>>>>,
    },
    /// Reset a send stream locally.
    Reset {
        id: StreamId,
        code: VarInt,
        reply: oneshot::Sender<Result<ResetOutcome, ConnError>>,
    },
    /// Stop a receive stream locally.
    Stop {
        id: StreamId,
        code: VarInt,
        reply: oneshot::Sender<Result<(), ConnError>>,
    },
    /// Read a recv stream to end-of-stream; reply with all bytes once FIN is
    /// observed (or an error if the stream is reset).
    ReadToEnd {
        id: StreamId,
        limit: usize,
        reply: HandoffTx<Result<Vec<u8>, ReadToEndError>>,
        recovery: Arc<Mutex<RecvRecovery>>,
    },
    /// Read up to `max` bytes, completing as soon as any data or FIN is
    /// available. An empty vector means clean end-of-stream.
    Read {
        id: StreamId,
        max: usize,
        reply: HandoffTx<Result<Vec<u8>, ConnError>>,
        recovery: Arc<Mutex<RecvRecovery>>,
    },
    /// Close the connection with an application error code, sending a
    /// CONNECTION_CLOSE frame so the peer (and this side's driver) tear down
    /// promptly rather than waiting out the idle timeout.
    Close {
        code: VarInt,
        reason: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryOpenOutcome<T = (SendStream, RecvStream)> {
    Opened(T),
    TemporarilyUnavailable,
}

#[derive(Debug, thiserror::Error)]
#[error("stream read failed after {prefix_len} bytes: {error}", prefix_len = .prefix.len())]
pub struct ReadToEndError {
    pub prefix: Vec<u8>,
    pub error: ConnError,
}

/// A post-handshake, PSK-authenticated QUIC connection.
///
/// Produced by the server (on `accept`) and the client (once its handshake
/// reaches `Connected`), so both sides surface the same type. A `Connection` is
/// a handle onto a connection the driver owns and pumps. Connection clones,
/// stream halves, and QuinnHandle clones retain it. Dropping the last such
/// application owner initiates bounded connection cleanup; explicit close
/// overrides retained owners.
#[derive(Clone)]
pub struct Connection {
    remote: std::net::SocketAddr,
    handle: ConnectionHandle,
    client_id: Option<String>,
    cmds: CmdSender,
    closed: watch::Receiver<Option<ConnectionError>>,
}

impl Connection {
    /// Build a handle for a connection the driver owns. `cmds` is the driver's
    /// command channel, pre-tagged with this connection's handle.
    pub(crate) fn new(
        handle: ConnectionHandle,
        remote: std::net::SocketAddr,
        client_id: Option<String>,
        cmds: CmdSender,
        closed: watch::Receiver<Option<ConnectionError>>,
    ) -> Self {
        Self {
            remote,
            handle,
            client_id,
            cmds,
            closed,
        }
    }

    /// The remote peer's socket address.
    pub fn remote_address(&self) -> std::net::SocketAddr {
        self.remote
    }

    /// The endpoint-local handle identifying this connection.
    pub fn handle(&self) -> ConnectionHandle {
        self.handle
    }

    /// Return the persistent terminal cause once this connection has stopped.
    ///
    /// The endpoint uses this before handing off a queued incoming connection,
    /// so a connection that died after admission but before application
    /// delivery is skipped rather than exposed as a fresh connection.
    pub(crate) fn terminal_reason(&self) -> Option<ConnectionError> {
        self.closed.borrow().clone()
    }

    #[cfg(test)]
    pub(crate) fn available_write_budget(&self) -> usize {
        self.cmds.write_budget.available_permits()
    }

    #[cfg(test)]
    pub(crate) fn available_open_budget(&self) -> usize {
        self.cmds.open_budget.available_permits()
    }

    /// Authenticated server-side client identity.
    ///
    /// On a connection accepted by [`crate::server::Server`], this is the
    /// unique configured `client_id` whose PSK admitted the peer. Client-side
    /// connections return `None`.
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Open a new bidirectional stream.
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnError> {
        let _permit = self
            .cmds
            .acquire_operation(self.cmds.open_budget.clone())
            .await?;
        let cleanup = self.cmds.clone();
        let wake = self.cmds.clone();
        let (tx, rx) = handoff(
            move |result| {
                if let Ok(id) = result {
                    cleanup.cleanup(Cleanup::ResetSend {
                        id,
                        code: AUTO_RESET_CANCELLED_OPEN,
                    });
                    cleanup.cleanup(Cleanup::StopRecv {
                        id,
                        code: AUTO_STOP_CANCELLED_OPEN,
                    });
                    cleanup.cleanup(Cleanup::ForgetStream { id });
                }
            },
            move || wake.cleanup(Cleanup::Wake),
        );
        self.cmds.send(Cmd::OpenBi(tx)).await?;
        let id = rx.receive().await.ok_or(ConnError::Closed)??;
        Ok(bi_stream(id, self.cmds.clone()))
    }

    pub async fn try_open_bi(&self) -> Result<TryOpenOutcome, ConnError> {
        if let Some(error) = self.cmds.terminal_error() {
            return Err(error);
        }
        let Ok(_permit) = self.cmds.open_budget.clone().try_acquire_owned() else {
            return Ok(TryOpenOutcome::TemporarilyUnavailable);
        };
        let cleanup = self.cmds.clone();
        let wake = self.cmds.clone();
        let (tx, rx) = handoff(
            move |result| {
                if let Ok(TryOpenOutcome::Opened(id)) = result {
                    cleanup.cleanup(Cleanup::ResetSend {
                        id,
                        code: AUTO_RESET_CANCELLED_OPEN,
                    });
                    cleanup.cleanup(Cleanup::StopRecv {
                        id,
                        code: AUTO_STOP_CANCELLED_OPEN,
                    });
                    cleanup.cleanup(Cleanup::ForgetStream { id });
                }
            },
            move || wake.cleanup(Cleanup::Wake),
        );
        if self.cmds.try_send(Cmd::TryOpenBi(tx)).is_err() {
            return Ok(TryOpenOutcome::TemporarilyUnavailable);
        }
        match rx.receive().await.ok_or(ConnError::Closed)?? {
            TryOpenOutcome::Opened(id) => {
                Ok(TryOpenOutcome::Opened(bi_stream(id, self.cmds.clone())))
            }
            TryOpenOutcome::TemporarilyUnavailable => Ok(TryOpenOutcome::TemporarilyUnavailable),
        }
    }

    /// Await the next bidirectional stream the peer opens.
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnError> {
        let _permit = self
            .cmds
            .acquire_operation(self.cmds.accept_budget.clone())
            .await?;
        let cleanup = self.cmds.clone();
        let wake = self.cmds.clone();
        let (tx, rx) = handoff(
            move |result| {
                if let Ok(id) = result {
                    cleanup.cleanup(Cleanup::ReturnAccepted { id });
                }
            },
            move || wake.cleanup(Cleanup::Wake),
        );
        self.cmds.send(Cmd::AcceptBi(tx)).await?;
        let id = rx.receive().await.ok_or(ConnError::Closed)??;
        Ok(bi_stream(id, self.cmds.clone()))
    }

    /// Close the connection, sending a CONNECTION_CLOSE frame so the peer tears
    /// down promptly (rather than waiting out the idle timeout). Best-effort: if
    /// the driver is already gone the connection is effectively closed anyway.
    pub async fn close(&self, code: u64, reason: &[u8]) -> Result<(), ConnError> {
        let code = app_varint(code)?;
        let _ = self
            .cmds
            .send(Cmd::Close {
                code,
                reason: reason.to_vec(),
            })
            .await;
        Ok(())
    }

    /// Wait for the connection to terminate and return the terminal reason.
    pub async fn closed(&self) -> ConnectionError {
        let mut closed = self.closed.clone();
        loop {
            if let Some(reason) = closed.borrow().clone() {
                return reason;
            }
            if closed.changed().await.is_err() {
                return ConnectionError::LocallyClosed;
            }
        }
    }

    /// Return a cloneable handle for opening and accepting uniquely owned
    /// bidirectional stream halves through the shared driver. The underlying
    /// Quinn connection remains driver-owned. See [`QuinnHandle`].
    pub fn quinn_connection(&self) -> QuinnHandle {
        QuinnHandle {
            handle: self.handle,
            cmds: self.cmds.clone(),
        }
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("handle", &self.handle)
            .field("remote", &self.remote)
            .field("client_id", &self.client_id)
            .finish()
    }
}

/// The minimal command surface onto the driver-owned QUIC connection that a
/// higher-level stream protocol (e.g. `h3`) layers on top of. This is the
/// documented seam that keeps HTTP/3 layering decoupled from the cloaking layer:
/// h3 opens/accepts streams and moves bytes through *this*, never through the
/// server/client driver internals.
///
/// It mirrors [`Connection`]'s stream API (which is why both are thin wrappers
/// over the same `Cmd` channel) but is `Clone` and carries the raw
/// [`ConnectionHandle`], the identity h3 keys its per-connection
/// state on.
#[derive(Clone)]
pub struct QuinnHandle {
    handle: ConnectionHandle,
    cmds: CmdSender,
}

impl QuinnHandle {
    /// The endpoint-local handle identifying this connection.
    pub fn handle(&self) -> ConnectionHandle {
        self.handle
    }

    /// Open a new bidirectional stream.
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnError> {
        let _permit = self
            .cmds
            .acquire_operation(self.cmds.open_budget.clone())
            .await?;
        let cleanup = self.cmds.clone();
        let wake = self.cmds.clone();
        let (tx, rx) = handoff(
            move |result| {
                if let Ok(id) = result {
                    cleanup.cleanup(Cleanup::ResetSend {
                        id,
                        code: AUTO_RESET_CANCELLED_OPEN,
                    });
                    cleanup.cleanup(Cleanup::StopRecv {
                        id,
                        code: AUTO_STOP_CANCELLED_OPEN,
                    });
                    cleanup.cleanup(Cleanup::ForgetStream { id });
                }
            },
            move || wake.cleanup(Cleanup::Wake),
        );
        self.cmds.send(Cmd::OpenBi(tx)).await?;
        let id = rx.receive().await.ok_or(ConnError::Closed)??;
        Ok(bi_stream(id, self.cmds.clone()))
    }

    /// Await the next peer-initiated bidirectional stream.
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnError> {
        let _permit = self
            .cmds
            .acquire_operation(self.cmds.accept_budget.clone())
            .await?;
        let cleanup = self.cmds.clone();
        let wake = self.cmds.clone();
        let (tx, rx) = handoff(
            move |result| {
                if let Ok(id) = result {
                    cleanup.cleanup(Cleanup::ReturnAccepted { id });
                }
            },
            move || wake.cleanup(Cleanup::Wake),
        );
        self.cmds.send(Cmd::AcceptBi(tx)).await?;
        let id = rx.receive().await.ok_or(ConnError::Closed)??;
        Ok(bi_stream(id, self.cmds.clone()))
    }
}

/// Receive half of a split bidirectional stream.
pub struct RecvStream {
    id: StreamId,
    cmds: CmdSender,
    done: bool,
    recovery: Arc<Mutex<RecvRecovery>>,
}

#[derive(Default)]
pub(crate) struct RecvRecovery {
    bytes: Vec<u8>,
    error: Option<ConnError>,
    eof: bool,
}

impl RecvStream {
    pub fn id(&self) -> StreamId {
        self.id
    }

    pub async fn read(&mut self, max: usize) -> Result<Vec<u8>, ConnError> {
        if max > 0 {
            let mut recovery = self.recovery.lock().unwrap();
            if !recovery.bytes.is_empty() {
                let split = max.min(recovery.bytes.len());
                return Ok(recovery.bytes.drain(..split).collect());
            }
            if let Some(error) = recovery.error.take() {
                return Err(error);
            }
            if recovery.eof {
                self.done = true;
                return Ok(Vec::new());
            }
        }
        let bytes = read_chunk(&self.cmds, self.id, max, self.recovery.clone()).await?;
        if max > 0 && bytes.is_empty() {
            self.done = true;
        }
        Ok(bytes)
    }

    pub async fn read_to_end(&mut self, limit: usize) -> Result<Vec<u8>, ReadToEndError> {
        {
            let mut recovery = self.recovery.lock().unwrap();
            if recovery.bytes.len() > limit {
                recovery.bytes.truncate(limit);
                self.cmds.cleanup(Cleanup::StopRecv {
                    id: self.id,
                    code: AUTO_STOP_READ_LIMIT,
                });
                recovery.error = Some(ConnError::ReadLimitExceeded { limit });
                return Err(ReadToEndError {
                    prefix: std::mem::take(&mut recovery.bytes),
                    error: recovery.error.take().expect("set above"),
                });
            }
            if let Some(error) = recovery.error.take() {
                return Err(ReadToEndError {
                    prefix: std::mem::take(&mut recovery.bytes),
                    error,
                });
            }
            if recovery.eof {
                self.done = true;
                return Ok(std::mem::take(&mut recovery.bytes));
            }
        }
        let recovery = self.recovery.clone();
        let wake = self.cmds.clone();
        let (tx, rx) = handoff(
            move |result: Result<Vec<u8>, ReadToEndError>| match result {
                Ok(bytes) => {
                    let mut r = recovery.lock().unwrap();
                    r.bytes = bytes;
                    r.eof = true;
                }
                Err(e) => {
                    let mut r = recovery.lock().unwrap();
                    r.bytes = e.prefix;
                    r.error = Some(e.error);
                }
            },
            move || wake.cleanup(Cleanup::Wake),
        );
        if let Err(error) = self
            .cmds
            .send(Cmd::ReadToEnd {
                id: self.id,
                limit,
                reply: tx,
                recovery: self.recovery.clone(),
            })
            .await
        {
            return Err(ReadToEndError {
                prefix: std::mem::take(&mut self.recovery.lock().unwrap().bytes),
                error,
            });
        }
        let result = match rx.receive().await {
            Some(result) => result,
            None => {
                let mut recovery = self.recovery.lock().unwrap();
                return Err(ReadToEndError {
                    prefix: std::mem::take(&mut recovery.bytes),
                    error: recovery.error.take().unwrap_or(ConnError::Closed),
                });
            }
        };
        if result.is_ok() {
            self.done = true;
        }
        result
    }

    pub async fn stop(&mut self, code: u64) -> Result<(), ConnError> {
        let code = app_varint(code)?;
        let (tx, rx) = oneshot::channel();
        self.cmds
            .send(Cmd::Stop {
                id: self.id,
                code,
                reply: tx,
            })
            .await?;
        let result = rx.await.map_err(|_| ConnError::Closed)?;
        if result.is_ok() {
            self.done = true;
        }
        result
    }
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        if self.done {
            self.cmds.cleanup(Cleanup::ReleaseRecv { id: self.id });
        } else {
            self.cmds.cleanup(Cleanup::StopRecv {
                id: self.id,
                code: AUTO_STOP_DROPPED_RECV,
            });
        }
    }
}

/// Send half of a split bidirectional stream.
pub struct SendStream {
    id: StreamId,
    cmds: CmdSender,
    done: bool,
    finished: Arc<AtomicBool>,
    terminal: Arc<Mutex<Option<Result<(), ConnError>>>>,
}

struct WriteCancelGuard {
    id: StreamId,
    cmds: CmdSender,
    started: Arc<AtomicBool>,
    complete: bool,
}

impl Drop for WriteCancelGuard {
    fn drop(&mut self) {
        if !self.complete && self.started.load(Ordering::Acquire) {
            self.cmds.cleanup(Cleanup::ResetSend {
                id: self.id,
                code: AUTO_RESET_CANCELLED_WRITE,
            });
        } else if !self.complete {
            self.cmds.cleanup(Cleanup::Wake);
        }
    }
}

impl SendStream {
    pub fn id(&self) -> StreamId {
        self.id
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), ConnError> {
        const WRITE_CHUNK: usize = 16 * 1024;
        let started = Arc::new(AtomicBool::new(false));
        let mut guard = WriteCancelGuard {
            id: self.id,
            cmds: self.cmds.clone(),
            started: started.clone(),
            complete: false,
        };
        for chunk in buf.chunks(WRITE_CHUNK) {
            let permit = self.cmds.acquire_write(chunk.len() as u32).await?;
            let (tx, rx) = oneshot::channel();
            self.cmds
                .send(Cmd::Write {
                    id: self.id,
                    data: chunk.to_vec(),
                    reply: tx,
                    started: started.clone(),
                    permit,
                })
                .await?;
            rx.await.map_err(|_| ConnError::Closed)??;
        }
        guard.complete = true;
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), ConnError> {
        let (tx, rx) = oneshot::channel();
        self.cmds
            .send(Cmd::Finish {
                id: self.id,
                reply: tx,
                finished: self.finished.clone(),
                terminal: self.terminal.clone(),
            })
            .await?;
        let result = rx.await.map_err(|_| ConnError::Closed)?;
        if result.is_ok() {
            self.done = true;
        }
        result
    }

    pub async fn wait_finished(&mut self) -> Result<(), ConnError> {
        wait_finished(&self.cmds, self.id, self.terminal.clone()).await
    }

    pub async fn finish_and_wait(&mut self) -> Result<(), ConnError> {
        self.finish().await?;
        self.wait_finished().await
    }

    pub async fn reset(&mut self, code: u64) -> Result<ResetOutcome, ConnError> {
        let code = app_varint(code)?;
        let recorded = { self.terminal.lock().unwrap().clone() };
        match recorded {
            Some(Ok(())) => return Ok(ResetOutcome::AlreadyAcknowledged),
            Some(Err(ConnError::Stopped { code })) => {
                return Ok(ResetOutcome::PeerStopped { code });
            }
            // A reset records ClosedStream for FIN waiters, but the core
            // retains the original reset code. Fall through and ask it for the
            // idempotent ResetOutcome instead of losing that stable fact.
            Some(Err(ConnError::ClosedStream)) | None => {}
            Some(Err(error)) => return Err(error),
        }
        let (tx, rx) = oneshot::channel();
        self.cmds
            .send(Cmd::Reset {
                id: self.id,
                code,
                reply: tx,
            })
            .await?;
        let result = rx.await.map_err(|_| ConnError::Closed)?;
        if result.is_ok() {
            self.done = true;
        }
        result
    }
}

impl Drop for SendStream {
    fn drop(&mut self) {
        if !self.done && !self.finished.load(Ordering::Acquire) {
            self.cmds.cleanup(Cleanup::DropSend { id: self.id });
        } else {
            self.cmds.cleanup(Cleanup::ReleaseSend { id: self.id });
        }
    }
}

async fn read_chunk(
    cmds: &CmdSender,
    id: StreamId,
    max: usize,
    recovery: Arc<Mutex<RecvRecovery>>,
) -> Result<Vec<u8>, ConnError> {
    if max == 0 {
        return Ok(Vec::new());
    }
    let wake = cmds.clone();
    let reclaim = recovery.clone();
    let (tx, rx) = handoff(
        move |result| match result {
            Ok(bytes) => reclaim.lock().unwrap().bytes = bytes,
            Err(error) => reclaim.lock().unwrap().error = Some(error),
        },
        move || wake.cleanup(Cleanup::Wake),
    );
    cmds.send(Cmd::Read {
        id,
        max,
        reply: tx,
        recovery,
    })
    .await?;
    rx.receive().await.ok_or(ConnError::Closed)?
}

async fn wait_finished(
    cmds: &CmdSender,
    id: StreamId,
    terminal: Arc<Mutex<Option<Result<(), ConnError>>>>,
) -> Result<(), ConnError> {
    if let Some(result) = terminal.lock().unwrap().clone() {
        return result;
    }
    let (tx, rx) = oneshot::channel();
    cmds.send(Cmd::WaitFinished {
        id,
        reply: tx,
        terminal: terminal.clone(),
    })
    .await?;
    match rx.await {
        Ok(result) => result,
        Err(_) => terminal
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Err(ConnError::Closed)),
    }
}

fn bi_stream(id: StreamId, cmds: CmdSender) -> (SendStream, RecvStream) {
    (
        SendStream {
            id,
            cmds: cmds.clone(),
            done: false,
            finished: Arc::new(AtomicBool::new(false)),
            terminal: Arc::new(Mutex::new(None)),
        },
        RecvStream {
            id,
            cmds,
            done: false,
            recovery: Arc::new(Mutex::new(RecvRecovery::default())),
        },
    )
}

fn varint(code: u64) -> Result<VarInt, ConnError> {
    VarInt::from_u64(code).map_err(|_| ConnError::InvalidErrorCode { code })
}

fn app_varint(code: u64) -> Result<VarInt, ConnError> {
    if code >= AUTO_CODE_START {
        return Err(ConnError::InvalidErrorCode { code });
    }
    varint(code)
}

// ---------------------------------------------------------------------------
// Driver-side per-connection state. Owned by the driver task; not part of the
// public handle API. Holds only the *waiting* — the core owns the protocol.
// ---------------------------------------------------------------------------

/// How much of a stream to ask the core for per [`CoreConn::stream_read`] call.
/// The core copies at most `buf.len()` bytes and never stashes a remainder, so
/// this is purely a batching knob; a `read_to_end` loops until `Blocked` or
/// `Finished` regardless.
const READ_CHUNK: usize = 16 * 1024;
const READ_PASS_BUDGET: usize = 64 * 1024;
const MAX_PARKED_OPS: usize = 256;

/// A write that is blocked on flow control: the remaining bytes, and the reply
/// channel to fire once the whole buffer has been accepted.
struct PendingWrite {
    data: Vec<u8>,
    offset: usize,
    reply: oneshot::Sender<Result<(), ConnError>>,
    started: bool,
    call_started: Arc<AtomicBool>,
    _permit: OwnedSemaphorePermit,
}

/// A read waiting for end-of-stream: the bytes gathered so far and the reply
/// channel to fire once FIN (or a reset) is seen.
struct PendingRead {
    buf: Vec<u8>,
    /// `Some` for a read-to-FIN operation. We read at most one byte beyond this
    /// bound to distinguish an exact-size stream from an oversized one.
    end_limit: Option<usize>,
    max: Option<usize>,
    reply: Option<ReadReply>,
    recovery: Arc<Mutex<RecvRecovery>>,
}

type SendTerminal = Arc<Mutex<Option<Result<(), ConnError>>>>;

enum ReadReply {
    Chunk(HandoffTx<Result<Vec<u8>, ConnError>>),
    ToEnd(HandoffTx<Result<Vec<u8>, ReadToEndError>>),
}

impl ReadReply {
    fn is_closed(&self) -> bool {
        match self {
            Self::Chunk(r) => r.is_closed(),
            Self::ToEnd(r) => r.is_closed(),
        }
    }
    fn send_ok(self, bytes: Vec<u8>) -> Option<Vec<u8>> {
        match self {
            Self::Chunk(r) => r.send(Ok(bytes)).err().and_then(Result::ok),
            Self::ToEnd(r) => r.send(Ok(bytes)).err().and_then(Result::ok),
        }
    }
    fn send_err(self, prefix: Vec<u8>, error: ConnError) -> Option<(Vec<u8>, ConnError)> {
        match self {
            Self::Chunk(r) => match r.send(Err(error)) {
                Err(Err(e)) => Some((prefix, e)),
                _ => None,
            },
            Self::ToEnd(r) => match r.send(Err(ReadToEndError { prefix, error })) {
                Err(Err(e)) => Some((e.prefix, e.error)),
                _ => None,
            },
        }
    }
}

/// One live connection's parked handle operations.
///
/// This is the whole of the tokio layer's per-connection state: everything else
/// — streams, flow control, CIDs, timers, lifecycle — belongs to the core's
/// [`CoreConn`], which the driver borrows via
/// `Endpoint::conn_mut(handle)` and passes into every method here.
///
/// Each map holds operations the core answered with "not right now". They are
/// retried from the driver's event dispatch:
///
/// | parked in           | offered again on                        |
/// |---------------------|-----------------------------------------|
/// | `pending_accepts`   | `Event::StreamOpened`                   |
/// | `blocked_writes`    | `Event::StreamWritable`                 |
/// | `pending_reads`     | `Event::StreamReadable`                 |
///
/// and all three are failed with [`ConnError::Closed`] by [`Parked::fail_all`]
/// when `Event::ConnectionLost` names this connection.
pub(crate) struct Parked {
    needs_service: bool,
    pending_opens: VecDeque<HandoffTx<Result<StreamId, ConnError>>>,
    /// Accept requests waiting for a peer-opened bi stream, FIFO.
    pending_accepts: VecDeque<HandoffTx<Result<StreamId, ConnError>>>,
    /// Writes blocked on flow control, keyed by stream.
    blocked_writes: HashMap<StreamId, PendingWrite>,
    /// Reads awaiting end-of-stream, keyed by stream.
    pending_reads: HashMap<StreamId, PendingRead>,
    retained_reads: HashMap<StreamId, Vec<u8>>,
    retained_read_errors: HashMap<StreamId, ConnError>,
    /// Waiters awaiting this stream's send-half terminal fact.
    fin_waiters: HashMap<StreamId, Vec<oneshot::Sender<Result<(), ConnError>>>>,
    send_terminals: HashMap<StreamId, SendTerminal>,
    closed: watch::Sender<Option<ConnectionError>>,
}

impl Parked {
    pub(crate) fn new(closed: watch::Sender<Option<ConnectionError>>) -> Self {
        Self {
            needs_service: false,
            pending_accepts: VecDeque::new(),
            pending_opens: VecDeque::new(),
            blocked_writes: HashMap::new(),
            pending_reads: HashMap::new(),
            retained_reads: HashMap::new(),
            retained_read_errors: HashMap::new(),
            fin_waiters: HashMap::new(),
            send_terminals: HashMap::new(),
            closed,
        }
    }

    pub(crate) fn mark_closed(&self, reason: ConnectionError) {
        let _ = self.closed.send(Some(reason));
    }

    /// Perform cancellation reclamation and retry FIFO stream openers. Drivers
    /// call this once per bounded connection service pass, even without a QUIC
    /// stream event.
    pub(crate) fn maintain(&mut self, core: &mut CoreConn) {
        self.needs_service = false;
        self.pending_accepts.retain(|reply| !reply.is_closed());
        self.pending_opens.retain(|reply| !reply.is_closed());
        while let Some(reply) = self.pending_opens.pop_front() {
            if reply.is_closed() {
                continue;
            }
            match core.open_bi() {
                Ok(id) => {
                    if reply.send(Ok(id)).is_err() {
                        let _ = core.stream_reset_auto(id, AUTO_RESET_CANCELLED_OPEN);
                        let _ = core.stream_stop_auto(id, AUTO_STOP_CANCELLED_OPEN);
                        core.forget_stream(id);
                    }
                }
                Err(ConnError::ClosedStream) => {
                    self.pending_opens.push_front(reply);
                    break;
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            }
        }
        let canceled: Vec<_> = self
            .blocked_writes
            .iter()
            .filter_map(|(id, p)| p.reply.is_closed().then_some(*id))
            .collect();
        for id in canceled {
            if let Some(p) = self.blocked_writes.remove(&id) {
                if p.started {
                    let _ = core.stream_reset_auto(id, AUTO_RESET_CANCELLED_WRITE);
                }
            }
        }
        let canceled_reads: Vec<_> = self
            .pending_reads
            .iter()
            .filter_map(|(id, p)| {
                p.reply
                    .as_ref()
                    .is_some_and(ReadReply::is_closed)
                    .then_some(*id)
            })
            .collect();
        for id in canceled_reads {
            if let Some(mut p) = self.pending_reads.remove(&id) {
                let mut recovery = p.recovery.lock().unwrap();
                recovery.bytes.append(&mut p.buf);
            }
        }
        self.fin_waiters.retain(|_, waiters| {
            waiters.retain(|reply| !reply.is_closed());
            !waiters.is_empty()
        });
        let reads: Vec<_> = self.pending_reads.keys().copied().collect();
        for id in reads {
            let Some(mut pending) = self.pending_reads.remove(&id) else {
                continue;
            };
            if !self.pump_read(core, id, &mut pending) {
                self.pending_reads.insert(id, pending);
            }
        }
    }

    pub(crate) fn needs_service(&self) -> bool {
        self.needs_service
    }

    pub(crate) fn apply_cleanup(&mut self, core: &mut CoreConn, cleanup: Cleanup) {
        match cleanup {
            Cleanup::ResetSend { id, code } => {
                let outcome = core.stream_reset_auto(id, code);
                if let Some(terminal) = self.send_terminals.get(&id) {
                    let mut slot = terminal.lock().unwrap();
                    if slot.is_none() {
                        *slot = Some(match outcome {
                            Ok(ResetOutcome::AlreadyAcknowledged) => Ok(()),
                            Ok(ResetOutcome::PeerStopped { code }) => {
                                Err(ConnError::Stopped { code })
                            }
                            Ok(_) => Err(ConnError::ClosedStream),
                            Err(error) => Err(error),
                        });
                    }
                }
                self.fail_fin_waiters(id, ConnError::ClosedStream);
                self.fail_blocked_write(id, ConnError::ClosedStream);
            }
            Cleanup::DropSend { id } => {
                let _ = core.stream_reset_auto(id, AUTO_RESET_DROPPED_SEND);
                core.forget_send(id);
                self.fail_fin_waiters(id, ConnError::ClosedStream);
                self.fail_blocked_write(id, ConnError::ClosedStream);
                self.send_terminals.remove(&id);
            }
            Cleanup::StopRecv { id, code } => {
                let _ = core.stream_stop_auto(id, code);
                self.fail_pending_read(id, ConnError::ClosedStream);
                self.retained_reads.remove(&id);
                self.retained_read_errors.remove(&id);
            }
            Cleanup::ReturnAccepted { id } => {
                core.put_back_accepted(id);
                self.on_stream_opened(core);
            }
            Cleanup::ReleaseRecv { id } => core.forget_recv(id),
            Cleanup::ReleaseSend { id } => {
                core.forget_send(id);
                self.send_terminals.remove(&id);
            }
            Cleanup::ForgetStream { id } => core.forget_stream(id),
            Cleanup::Wake => {}
            Cleanup::OwnerDropped => {}
        }
    }

    /// Apply one handle-issued command against `core`, answering immediately
    /// where the core can and parking where it cannot.
    ///
    /// Note what is *absent*: no transmit is sent from here and no timer is
    /// read. Every operation below marks its connection dirty inside the core,
    /// and the driver's pump drains `poll_transmit` after this returns — which
    /// is the documented order (stream work first, transmits last) and what
    /// gets the flow-control credit a read released onto the wire.
    pub(crate) fn apply_cmd(&mut self, core: &mut CoreConn, cmd: Cmd, now: Instant) {
        self.maintain(core);
        match cmd {
            Cmd::OpenBi(reply) => {
                if reply.is_closed() {
                    return;
                }
                if self.pending_opens.iter().any(|r| !r.is_closed()) {
                    if self.pending_opens.len() >= MAX_PARKED_OPS {
                        let _ = reply.send(Err(ConnError::Transport(
                            "connection operation queue full".into(),
                        )));
                    } else {
                        self.pending_opens.push_back(reply);
                    }
                    return;
                }
                match core.open_bi() {
                    Ok(id) => {
                        if reply.send(Ok(id)).is_err() {
                            let _ = core.stream_reset_auto(id, AUTO_RESET_CANCELLED_OPEN);
                            let _ = core.stream_stop_auto(id, AUTO_STOP_CANCELLED_OPEN);
                            core.forget_stream(id);
                        }
                    }
                    Err(ConnError::ClosedStream) => self.pending_opens.push_back(reply),
                    Err(e) => {
                        let _ = reply.send(Err(e));
                    }
                }
            }
            Cmd::TryOpenBi(reply) => {
                if reply.is_closed() {
                    return;
                }
                let answer = if self.pending_opens.iter().any(|r| !r.is_closed()) {
                    Ok(TryOpenOutcome::TemporarilyUnavailable)
                } else {
                    match core.open_bi() {
                        Ok(id) => Ok(TryOpenOutcome::Opened(id)),
                        Err(ConnError::ClosedStream) => Ok(TryOpenOutcome::TemporarilyUnavailable),
                        Err(e) => Err(e),
                    }
                };
                if let Err(Ok(TryOpenOutcome::Opened(id))) = reply.send(answer) {
                    let _ = core.stream_reset_auto(id, AUTO_RESET_CANCELLED_OPEN);
                    let _ = core.stream_stop_auto(id, AUTO_STOP_CANCELLED_OPEN);
                    core.forget_stream(id);
                }
            }
            Cmd::AcceptBi(reply) => match core.accept_bi() {
                Ok(Some(id)) => {
                    if reply.send(Ok(id)).is_err() {
                        core.put_back_accepted(id);
                    }
                }
                // Nothing pending: park until `Event::StreamOpened`.
                Ok(None) => {
                    if self.pending_accepts.len() >= MAX_PARKED_OPS {
                        let _ = reply.send(Err(ConnError::Transport(
                            "connection operation queue full".into(),
                        )));
                    } else {
                        self.pending_accepts.push_back(reply);
                    }
                }
                Err(e) => {
                    let _ = reply.send(Err(e));
                }
            },
            Cmd::Write {
                id,
                data,
                reply,
                started,
                permit,
            } => {
                let mut pending = PendingWrite {
                    data,
                    offset: 0,
                    reply,
                    started: false,
                    call_started: started,
                    _permit: permit,
                };
                if pending.reply.is_closed() {
                    return;
                }
                if !Self::pump_write(core, id, &mut pending) {
                    self.blocked_writes.insert(id, pending);
                }
            }
            Cmd::Finish {
                id,
                reply,
                finished,
                terminal,
            } => {
                self.send_terminals.insert(id, terminal.clone());
                let result = core.stream_finish(id);
                if result.is_ok() {
                    finished.store(true, Ordering::Release);
                } else {
                    let mut slot = terminal.lock().unwrap();
                    if slot.is_none() {
                        *slot = Some(result.clone());
                    }
                }
                let _ = reply.send(result);
            }
            Cmd::WaitFinished {
                id,
                reply,
                terminal,
            } => {
                let recorded = { terminal.lock().unwrap().clone() };
                match recorded.or_else(|| match core.send_fin(id) {
                    Some(SendFin::Acked) => Some(Ok(())),
                    Some(SendFin::Stopped(code)) => Some(Err(ConnError::Stopped { code })),
                    _ => None,
                }) {
                    Some(result) => {
                        *terminal.lock().unwrap() = Some(result.clone());
                        let _ = reply.send(result);
                    }
                    None => match core.send_fin(id) {
                        Some(SendFin::Acked) => {
                            let _ = reply.send(Ok(()));
                        }
                        Some(SendFin::Stopped(code)) => {
                            let _ = reply.send(Err(ConnError::Stopped { code }));
                        }
                        Some(SendFin::Queued) => {
                            self.fin_waiters.entry(id).or_default().push(reply)
                        }
                        Some(SendFin::Reset(_)) => {
                            let _ = reply.send(Err(ConnError::ClosedStream));
                        }
                        Some(_) => {
                            let _ = reply.send(Err(ConnError::ClosedStream));
                        }
                        None => {
                            let _ = reply.send(Err(ConnError::ClosedStream));
                        }
                    },
                }
            }
            Cmd::Reset { id, code, reply } => {
                let result = core.stream_reset(id, code.into_inner());
                if let Ok(outcome) = &result {
                    if let Some(terminal) = self.send_terminals.get(&id) {
                        let mut slot = terminal.lock().unwrap();
                        if slot.is_none() {
                            *slot = Some(match outcome {
                                ResetOutcome::AlreadyAcknowledged => Ok(()),
                                ResetOutcome::PeerStopped { code } => {
                                    Err(ConnError::Stopped { code: *code })
                                }
                                _ => Err(ConnError::ClosedStream),
                            });
                        }
                    }
                    self.fail_fin_waiters(id, ConnError::ClosedStream);
                    self.fail_blocked_write(id, ConnError::ClosedStream);
                }
                let _ = reply.send(result);
            }
            Cmd::Stop { id, code, reply } => {
                let result = core.stream_stop(id, code.into_inner());
                if result.is_ok() {
                    self.fail_pending_read(id, ConnError::ClosedStream);
                }
                let _ = reply.send(result);
            }
            Cmd::ReadToEnd {
                id,
                limit,
                reply,
                recovery,
            } => {
                let mut shared = recovery.lock().unwrap();
                let shared_bytes = std::mem::take(&mut shared.bytes);
                let shared_error = shared.error.take();
                drop(shared);
                if let Some(error) = shared_error {
                    let _ = reply.send(Err(ReadToEndError {
                        prefix: shared_bytes,
                        error,
                    }));
                    return;
                }
                if let Some(error) = self.retained_read_errors.remove(&id) {
                    let prefix = self.retained_reads.remove(&id).unwrap_or_default();
                    let _ = reply.send(Err(ReadToEndError { prefix, error }));
                    return;
                }
                if self
                    .retained_reads
                    .get(&id)
                    .is_some_and(|b| b.len() > limit)
                {
                    let prefix = self.retained_reads.remove(&id).expect("checked above");
                    let _ = core.stream_stop_auto(id, AUTO_STOP_READ_LIMIT);
                    let _ = reply.send(Err(ReadToEndError {
                        prefix,
                        error: ConnError::ReadLimitExceeded { limit },
                    }));
                    return;
                }
                let mut pending = PendingRead {
                    buf: shared_bytes,
                    end_limit: Some(limit),
                    max: None,
                    reply: Some(ReadReply::ToEnd(reply)),
                    recovery,
                };
                if !self.pump_read(core, id, &mut pending) {
                    self.pending_reads.insert(id, pending);
                }
            }
            Cmd::Read {
                id,
                max,
                reply,
                recovery,
            } => {
                if let Some(mut retained) = self.retained_reads.remove(&id) {
                    if !retained.is_empty() {
                        let split = max.min(retained.len());
                        let remainder = retained.split_off(split);
                        if !remainder.is_empty() {
                            self.retained_reads.insert(id, remainder);
                        }
                        if let Err(Ok(bytes)) = reply.send(Ok(retained)) {
                            let mut restored = bytes;
                            if let Some(mut tail) = self.retained_reads.remove(&id) {
                                restored.append(&mut tail);
                            }
                            self.retained_reads.insert(id, restored);
                        }
                        return;
                    }
                }
                if self.retained_reads.get(&id).is_none_or(Vec::is_empty) {
                    if let Some(error) = self.retained_read_errors.remove(&id) {
                        let _ = reply.send(Err(error));
                        return;
                    }
                }
                let mut pending = PendingRead {
                    buf: self.retained_reads.remove(&id).unwrap_or_default(),
                    end_limit: None,
                    max: Some(max.max(1)),
                    reply: Some(ReadReply::Chunk(reply)),
                    recovery,
                };
                if !self.pump_read(core, id, &mut pending) {
                    self.pending_reads.insert(id, pending);
                }
            }
            Cmd::Close { code, reason } => {
                // The core deliberately exposes no `close`: it is a
                // connection-level operation with no non-blocking/blocking
                // distinction, so it goes straight to the owned connection.
                // The CONNECTION_CLOSE frame it queues leaves on the pump's
                // next `poll_transmit` drain, and the close timer it arms is
                // what eventually drives the connection to `Drained` — which
                // the core reaps and reports as `Event::ConnectionLost`.
                core.conn_mut().close(now, code, Bytes::from(reason));
            }
        }
    }

    /// A peer-opened stream arrived: hand queued streams to parked accepts,
    /// FIFO, for as long as both are available.
    ///
    /// The stream id is taken from `core.accept_bi()` rather than from the
    /// event, so the core's own accept queue is consumed in step with the
    /// replies — otherwise the next `accept_bi` would re-issue an id we have
    /// already handed out.
    pub(crate) fn on_stream_opened(&mut self, core: &mut CoreConn) {
        while !self.pending_accepts.is_empty() {
            match core.accept_bi() {
                Ok(Some(id)) => {
                    if let Some(reply) = self.pending_accepts.pop_front() {
                        if reply.send(Ok(id)).is_err() {
                            core.put_back_accepted(id);
                        }
                    }
                }
                Ok(None) => return,
                Err(e) => {
                    if let Some(reply) = self.pending_accepts.pop_front() {
                        let _ = reply.send(Err(e));
                    }
                    return;
                }
            }
        }
    }

    /// `id` has data buffered: resume a parked `read_to_end`, if any.
    pub(crate) fn on_readable(&mut self, core: &mut CoreConn, id: StreamId) {
        let Some(mut pending) = self.pending_reads.remove(&id) else {
            return;
        };
        if !self.pump_read(core, id, &mut pending) {
            self.pending_reads.insert(id, pending);
        }
    }

    /// `id`'s flow control opened: resume a parked `write_all`, if any.
    pub(crate) fn on_writable(&mut self, core: &mut CoreConn, id: StreamId) {
        let Some(mut pending) = self.blocked_writes.remove(&id) else {
            return;
        };
        if !Self::pump_write(core, id, &mut pending) {
            self.blocked_writes.insert(id, pending);
        }
    }

    /// Complete a finish waiter when the peer acknowledges the stream's FIN.
    pub(crate) fn on_fin_acked(&mut self, id: StreamId) {
        if let Some(terminal) = self.send_terminals.get(&id) {
            let mut slot = terminal.lock().unwrap();
            if slot.is_none() {
                *slot = Some(Ok(()));
            }
        }
        if let Some(waiters) = self.fin_waiters.remove(&id) {
            for reply in waiters {
                let _ = reply.send(Ok(()));
            }
        }
    }

    /// Complete waiters and writes when the peer asks us to stop sending.
    pub(crate) fn on_stopped(&mut self, core: &mut CoreConn, id: StreamId, code: u64) {
        if let Some(terminal) = self.send_terminals.get(&id) {
            let mut slot = terminal.lock().unwrap();
            if slot.is_none() {
                *slot = Some(Err(ConnError::Stopped { code }));
            }
        }
        self.fail_fin_waiters(id, ConnError::Stopped { code });
        self.on_writable(core, id);
    }

    /// Fail every parked handle operation with [`ConnError::Closed`].
    ///
    /// Called when the connection is lost, so awaiting handles wake with an
    /// error rather than hanging until their oneshot senders happen to drop.
    pub(crate) fn fail_all(&mut self) {
        let error = self
            .closed
            .borrow()
            .clone()
            .map(|reason| ConnError::ConnectionLost { reason })
            .unwrap_or(ConnError::Closed);
        for reply in self.pending_opens.drain(..) {
            let _ = reply.send(Err(error.clone()));
        }
        for reply in self.pending_accepts.drain(..) {
            let _ = reply.send(Err(error.clone()));
        }
        for (_, pending) in self.blocked_writes.drain() {
            let _ = pending.reply.send(Err(error.clone()));
        }
        for (_, pending) in self.pending_reads.drain() {
            if let Some(reply) = pending.reply {
                if let Some((bytes, cause)) = reply.send_err(pending.buf, error.clone()) {
                    let mut recovery = pending.recovery.lock().unwrap();
                    recovery.bytes = bytes;
                    recovery.error = Some(cause);
                }
            }
        }
        for (_, waiters) in self.fin_waiters.drain() {
            for reply in waiters {
                let _ = reply.send(Err(error.clone()));
            }
        }
        for terminal in self.send_terminals.values() {
            let mut slot = terminal.lock().unwrap();
            if slot.is_none() {
                *slot = Some(Err(error.clone()));
            }
        }
    }

    fn fail_fin_waiters(&mut self, id: StreamId, err: ConnError) {
        if let Some(waiters) = self.fin_waiters.remove(&id) {
            for reply in waiters {
                let _ = reply.send(Err(err.clone()));
            }
        }
    }

    fn fail_blocked_write(&mut self, id: StreamId, err: ConnError) {
        if let Some(pending) = self.blocked_writes.remove(&id) {
            let _ = pending.reply.send(Err(err));
        }
    }

    fn fail_pending_read(&mut self, id: StreamId, err: ConnError) {
        if let Some(pending) = self.pending_reads.remove(&id) {
            if let Some(reply) = pending.reply {
                let _ = reply.send_err(pending.buf, err);
            }
        }
    }

    /// Offer the rest of `pending` to the core, looping while it accepts bytes.
    /// Returns true when the write is complete or has errored (the reply has
    /// been sent); false when it is still blocked and should stay parked.
    fn pump_write(core: &mut CoreConn, id: StreamId, pending: &mut PendingWrite) -> bool {
        loop {
            if pending.reply.is_closed() {
                if pending.started {
                    let _ = core.stream_reset_auto(id, AUTO_RESET_CANCELLED_WRITE);
                }
                return true;
            }
            if pending.offset >= pending.data.len() {
                let reply = replace_reply_ok(&mut pending.reply);
                let _ = reply.send(Ok(()));
                return true;
            }
            match core.stream_write(id, &pending.data[pending.offset..]) {
                // `stream_write` only reports `Wrote(0)` for an empty buffer,
                // which the length check above has already excluded, so this
                // always advances.
                Ok(WriteOutcome::Wrote(n)) => {
                    pending.started = true;
                    pending.call_started.store(true, Ordering::Release);
                    pending.offset += n;
                }
                Ok(WriteOutcome::Blocked) => return false,
                Err(e) => {
                    let reply = replace_reply_ok(&mut pending.reply);
                    let _ = reply.send(Err(e));
                    return true;
                }
            }
        }
    }

    /// Drain everything the core has buffered for `id` into `pending`. Returns
    /// true when end-of-stream (or an error) was reached and the reply has been
    /// sent; false when more data may still arrive and the read stays parked.
    ///
    /// This loop **is** `read_to_end`: the core offers only the incremental
    /// `Read`/`Blocked`/`Finished` answer, and accumulating across `Blocked`s
    /// until `Finished` is what turns it back into the crate's one-shot
    /// `RecvStream::read_to_end` promise.
    fn pump_read(&mut self, core: &mut CoreConn, id: StreamId, pending: &mut PendingRead) -> bool {
        let mut work = 0usize;
        loop {
            if work >= READ_PASS_BUDGET {
                self.needs_service = true;
                return false;
            }
            // Read straight into the tail of the accumulator, so a large
            // transfer costs no per-chunk allocation and no extra copy.
            let filled = pending.buf.len();
            let request = pending
                .max
                .map(|max| max.saturating_sub(filled).min(READ_CHUNK))
                .unwrap_or_else(|| {
                    pending
                        .end_limit
                        .map(|limit| {
                            limit
                                .saturating_add(1)
                                .saturating_sub(filled)
                                .min(READ_CHUNK)
                        })
                        .unwrap_or(READ_CHUNK)
                })
                .max(1);
            pending.buf.resize(filled + request, 0);
            let outcome = core.stream_read(id, &mut pending.buf[filled..]);
            match outcome {
                Ok(ReadOutcome::Read(n)) => {
                    work += n;
                    pending.buf.truncate(filled + n);
                    if pending
                        .end_limit
                        .is_some_and(|limit| pending.buf.len() > limit)
                    {
                        let limit = pending.end_limit.expect("checked above");
                        let mut prefix = std::mem::take(&mut pending.buf);
                        prefix.truncate(limit);
                        let _ = core.stream_stop_auto(id, AUTO_STOP_READ_LIMIT);
                        if let Some((bytes, error)) = pending
                            .reply
                            .take()
                            .expect("pending reply")
                            .send_err(prefix, ConnError::ReadLimitExceeded { limit })
                        {
                            let mut recovery = pending.recovery.lock().unwrap();
                            recovery.bytes = bytes;
                            recovery.error = Some(error);
                        }
                        return true;
                    }
                    if pending.max.is_some() {
                        let buf = std::mem::take(&mut pending.buf);
                        if let Some(bytes) =
                            pending.reply.take().expect("pending reply").send_ok(buf)
                        {
                            pending.recovery.lock().unwrap().bytes = bytes;
                        }
                        return true;
                    }
                }
                Ok(ReadOutcome::Blocked) => {
                    pending.buf.truncate(filled);
                    return false;
                }
                Ok(ReadOutcome::Finished) => {
                    pending.buf.truncate(filled);
                    let buf = std::mem::take(&mut pending.buf);
                    if let Some(bytes) = pending.reply.take().expect("pending reply").send_ok(buf) {
                        let mut recovery = pending.recovery.lock().unwrap();
                        recovery.bytes = bytes;
                        recovery.eof = true;
                    }
                    return true;
                }
                Err(e) => {
                    pending.buf.truncate(filled);
                    let prefix = std::mem::take(&mut pending.buf);
                    if let Some((bytes, error)) = pending
                        .reply
                        .take()
                        .expect("pending reply")
                        .send_err(prefix, e)
                    {
                        let mut recovery = pending.recovery.lock().unwrap();
                        recovery.bytes = bytes;
                        recovery.error = Some(error);
                    }
                    return true;
                }
            }
        }
    }
}

// The reply senders live inside `&mut` structs while we may still need the
// struct afterward, so we swap in a throwaway closed channel to take ownership
// of the real sender. (oneshot::Sender is not Clone and send consumes it.)
fn replace_reply_ok(
    slot: &mut oneshot::Sender<Result<(), ConnError>>,
) -> oneshot::Sender<Result<(), ConnError>> {
    let (dead, _) = oneshot::channel();
    std::mem::replace(slot, dead)
}

#[cfg(test)]
mod handoff_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn completed_value_is_reclaimed_when_receiver_drops_before_poll() {
        let reclaimed = Arc::new(AtomicUsize::new(0));
        let seen = reclaimed.clone();
        let (tx, rx) = handoff(
            move |value: usize| {
                seen.store(value, Ordering::SeqCst);
            },
            || {},
        );
        tx.send(41).unwrap();
        drop(rx);
        assert_eq!(reclaimed.load(Ordering::SeqCst), 41);
    }

    #[tokio::test]
    async fn sender_drop_wakes_receiver() {
        let (tx, rx) = handoff(|_: usize| {}, || {});
        drop(tx);
        assert_eq!(rx.receive().await, None);
    }
}

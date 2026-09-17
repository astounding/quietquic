// SPDX-License-Identifier: 0BSD
//! Per-connection state: one `quinn_proto::Connection` plus the stream
//! bookkeeping needed to serve **non-blocking** stream operations.
//!
//! # Why this is not `src/conn.rs`
//!
//! The tokio wrapper's `ConnState` does the same job, but every operation there
//! is *completable later*: a read that finds no data parks a `PendingRead` with
//! a `oneshot::Sender` and the driver fires it once FIN arrives. That design
//! needs channels, and channels need a runtime.
//!
//! The core cannot wait for anything, so it does not try to. Every operation
//! here answers immediately with what is true right now:
//!
//! | old (`src/conn.rs`)                | core                            |
//! |------------------------------------|---------------------------------|
//! | park a `PendingRead`, reply on FIN | return [`ReadOutcome::Blocked`] |
//! | park a `PendingWrite`, resume on `Writable` | return [`WriteOutcome::Blocked`] |
//! | park a `pending_accept` oneshot    | return `Ok(None)`               |
//! | `fail_all()` wakes parked senders  | nothing is parked; nothing to wake |
//!
//! Waiting is the caller's job, and the caller is the one who owns a way to
//! wait: a tokio task parks on the `Blocked`, a hand-rolled `select()` loop goes
//! back around its poll set. Both are told *when* to retry by the
//! [`crate::outcome::Event`] stream that [`ConnState::service_streams`] feeds.
//!
//! # `service_streams` is the sole `poll()` caller
//!
//! `quinn_proto::Connection::poll()` *consumes* events, so exactly one place may
//! drain it or events are lost. That place is [`ConnState::service_streams`],
//! which reacts to what it can (queueing peer-opened streams for `accept_bi`)
//! and hands the rest back as a [`ConnProgress`] for the endpoint to translate
//! into [`crate::outcome::Event`]s.

use std::collections::{HashMap, HashSet, VecDeque};

use quinn_proto::{Dir, StreamId, VarInt};

use crate::outcome::{ConnectionError, ReadOutcome, WriteOutcome};

/// Errors surfaced by connection and stream operations.
///
/// Deliberately tiny, and deliberately *not* extended for the sans-IO core:
/// "no data right now" and "no credit right now" are not errors, they are the
/// [`ReadOutcome::Blocked`] / [`WriteOutcome::Blocked`] outcomes. What is left
/// is genuinely exceptional.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnError {
    /// The connection is gone (closed / lost / the driver owning it dropped), so
    /// no further stream work can be done on it.
    #[error("connection closed")]
    Closed,
    /// The connection terminated with a persistent transport cause.
    #[error("connection lost: {reason}")]
    ConnectionLost { reason: ConnectionError },
    /// The peer asked us to stop sending on this stream.
    #[error("stream stopped by peer: code {code}")]
    Stopped { code: u64 },
    /// The peer reset the receive half of this stream.
    #[error("stream reset by peer: code {code}")]
    Reset { code: u64 },
    /// The stream is closed, unknown, or locally abandoned.
    #[error("closed or unknown stream")]
    ClosedStream,
    /// A bounded convenience read received more bytes than its caller allowed.
    #[error("stream exceeded read limit of {limit} bytes")]
    ReadLimitExceeded { limit: usize },
    /// Application error codes are QUIC varints.
    #[error("invalid application error code {code} (exceeds varint range)")]
    InvalidErrorCode { code: u64 },
    /// Rare transport error that does not have a structured public variant.
    #[error("transport: {0}")]
    Transport(String),
}

/// QuietQUIC-reserved application codes used for automatic stream cleanup.
///
/// Applications must use codes below [`AUTO_CODE_START`]. Keeping automatic
/// cleanup distinguishable makes an abandoned local operation observable at
/// the peer without conflating it with an application protocol error.
pub const AUTO_CODE_START: u64 = (1u64 << 62) - 16;
pub const AUTO_RESET_DROPPED_SEND: u64 = AUTO_CODE_START;
pub const AUTO_STOP_DROPPED_RECV: u64 = AUTO_CODE_START + 1;
pub const AUTO_RESET_CANCELLED_WRITE: u64 = AUTO_CODE_START + 2;
pub const AUTO_RESET_CANCELLED_OPEN: u64 = AUTO_CODE_START + 3;
pub const AUTO_STOP_CANCELLED_OPEN: u64 = AUTO_CODE_START + 4;
pub const AUTO_STOP_READ_LIMIT: u64 = AUTO_CODE_START + 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutomaticCode {
    DroppedSend,
    DroppedRecv,
    CancelledWrite,
    CancelledOpenSend,
    CancelledOpenRecv,
    ReadLimit,
}

impl AutomaticCode {
    pub fn from_code(code: u64) -> Option<Self> {
        Some(match code {
            AUTO_RESET_DROPPED_SEND => Self::DroppedSend,
            AUTO_STOP_DROPPED_RECV => Self::DroppedRecv,
            AUTO_RESET_CANCELLED_WRITE => Self::CancelledWrite,
            AUTO_RESET_CANCELLED_OPEN => Self::CancelledOpenSend,
            AUTO_STOP_CANCELLED_OPEN => Self::CancelledOpenRecv,
            AUTO_STOP_READ_LIMIT => Self::ReadLimit,
            _ => return None,
        })
    }
}

/// Result of requesting a reset of a send stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResetOutcome {
    ResetRequested,
    AlreadyAcknowledged,
    AlreadyReset { code: u64 },
    PeerStopped { code: u64 },
}

/// Terminal/near-terminal state of a stream's send half.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendFin {
    /// `stream_finish` succeeded; FIN is not yet fully acknowledged.
    Queued,
    /// All data and FIN were acknowledged by the peer.
    Acked,
    /// The peer sent STOP_SENDING; the FIN will never be acknowledged.
    Stopped(u64),
    /// The local endpoint requested a reset. This wins over a later FIN ACK.
    Reset(u64),
}

/// What one [`ConnState::service_streams`] pass observed.
///
/// The endpoint turns these into [`crate::outcome::Event`]s for the caller;
/// `ConnState` keeps them structured so it does not have to know a connection's
/// handle (it does not have one — the endpoint owns that mapping).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnProgress {
    /// The caller should schedule another bounded service pass promptly.
    pub more_work: bool,
    /// The handshake completed this pass.
    pub connected: bool,
    /// The connection was lost this pass.
    ///
    /// **Only ever true for a REMOTELY-initiated loss.** See
    /// [`ConnState::is_drained`] for the other half of the story.
    pub lost: Option<ConnectionError>,
    /// Bidirectional streams the peer opened this pass, in arrival order. They
    /// are already queued for [`ConnState::accept_bi`]; this list exists so the
    /// endpoint can emit `StreamOpened`.
    pub opened: Vec<StreamId>,
    /// Streams that gained readable data: a previously `Blocked` read may now
    /// progress.
    pub readable: Vec<StreamId>,
    /// Streams whose flow control opened: a previously `Blocked` write may now
    /// progress.
    pub writable: Vec<StreamId>,
    /// Send streams whose FIN was acknowledged by the peer.
    pub fin_acked: Vec<StreamId>,
    /// Send streams the peer stopped, with the peer's application code.
    pub stopped: Vec<(StreamId, u64)>,
}

/// One connection's state: the owned sans-IO connection plus its stream
/// bookkeeping.
pub struct ConnState {
    conn: quinn_proto::Connection,
    /// Bidirectional streams the peer opened that no `accept_bi` has claimed
    /// yet, FIFO. Filled by [`ConnState::service_streams`].
    ready_accepts: VecDeque<StreamId>,
    /// Streams whose receive half reached clean end-of-stream.
    ///
    /// quinn-proto *frees* a receive stream the moment a read drains it past
    /// FIN, after which `recv_stream(id).read(..)` reports `ClosedStream`.
    /// Remembering the ids here lets a second read answer
    /// [`ReadOutcome::Finished`] instead of an error, so end-of-stream is a
    /// stable, re-readable answer rather than a one-shot.
    ///
    /// # Eviction contract
    ///
    /// One entry is added per stream that reaches clean EOS, and `StreamId`s
    /// increase monotonically, so on a long-lived connection carrying an
    /// unbounded number of short-lived streams — the `squicusock` shape: one
    /// stream per accepted Unix socket connection — this set would grow without
    /// bound if nothing removed from it. Three things do:
    ///
    /// * [`ConnState::forget_stream`], the explicit "I am done with this
    ///   stream" call. **A caller multiplexing many streams over one connection
    ///   must call it.** It is the only bound that does not depend on the peer.
    /// * [`ConnState::stream_reset`] and [`ConnState::stream_stop`], which
    ///   abandon a half of the stream. Removing there can only ever have an
    ///   effect when the receive half already hit EOS — i.e. when the stream is
    ///   terminal in both directions — so it cannot turn a legitimate
    ///   post-FIN read into an error.
    ///
    /// Nothing else removes: a read after FIN must keep answering `Finished`,
    /// which is precisely why the entry cannot simply be dropped on first
    /// observation.
    finished_reads: HashSet<StreamId>,
    /// Streams whose send half reached a stable finish/stop fact.
    send_fins: HashMap<StreamId, SendFin>,
    recv_errors: HashMap<StreamId, ConnError>,
    /// Set by any caller-side operation that can produce something to send, and
    /// cleared by the endpoint's servicing pass that flushes it.
    ///
    /// This is what stops a caller whose loop drains transmits *before* doing
    /// stream work from stalling: while it is set,
    /// [`crate::endpoint::Endpoint::next_timeout`] reports an already-elapsed
    /// deadline, so a caller that sleeps on that value comes straight back
    /// around instead of waiting for the idle timeout. The motivating case is
    /// [`ConnState::stream_read`], whose `Chunks::finalize` releases
    /// MAX_STREAM_DATA/MAX_DATA credit the peer is blocked on: with the credit
    /// unsent the peer sends nothing, and with nothing arriving nothing wakes
    /// the loop.
    dirty: bool,
    draining_opened: bool,
}

impl ConnState {
    /// Wrap an owned connection.
    pub fn new(conn: quinn_proto::Connection) -> Self {
        Self {
            conn,
            ready_accepts: VecDeque::new(),
            finished_reads: HashSet::new(),
            send_fins: HashMap::new(),
            recv_errors: HashMap::new(),
            dirty: false,
            draining_opened: false,
        }
    }

    /// Open a new bidirectional stream.
    ///
    /// Fails rather than blocking when the peer's stream credit is exhausted:
    /// the core has no way to wait for a `MAX_STREAMS` frame, so the caller
    /// decides whether to retry.
    pub fn open_bi(&mut self) -> Result<StreamId, ConnError> {
        if self.conn.is_closed() {
            return Err(ConnError::Closed);
        }
        self.conn
            .streams()
            .open(Dir::Bi)
            .ok_or(ConnError::ClosedStream)
    }

    /// Take the next peer-opened bidirectional stream, or `None` if none is
    /// pending. Never waits.
    ///
    /// Drains the queue [`ConnState::service_streams`] filled first, then asks
    /// quinn-proto directly — so a caller that has not drained events yet still
    /// sees a stream that has already arrived.
    pub fn accept_bi(&mut self) -> Result<Option<StreamId>, ConnError> {
        if let Some(id) = self.ready_accepts.pop_front() {
            return Ok(Some(id));
        }
        Ok(self.conn.streams().accept(Dir::Bi))
    }

    /// Return an accepted stream to the front of the FIFO when delivery to an
    /// application waiter was canceled before handoff.
    pub fn put_back_accepted(&mut self, id: StreamId) {
        self.ready_accepts.push_front(id);
    }

    /// Read whatever is buffered on `id` into `buf`, up to `buf.len()` bytes.
    ///
    /// Three answers, and only three:
    ///
    /// * [`ReadOutcome::Read(n)`] — `n > 0` bytes were copied. There may be more.
    /// * [`ReadOutcome::Blocked`] — nothing is buffered *right now*. Retry after
    ///   the next `StreamReadable` for this stream. **Not an error.**
    /// * [`ReadOutcome::Finished`] — the peer finished the stream and everything
    ///   it sent has been delivered. Idempotent: reading again says the same.
    ///
    /// A peer *reset* is the one thing that surfaces as `Err`, because it means
    /// the data the caller was promised will never arrive.
    ///
    /// [`ReadOutcome::Read(n)`]: ReadOutcome::Read
    pub fn stream_read(&mut self, id: StreamId, buf: &mut [u8]) -> Result<ReadOutcome, ConnError> {
        use quinn_proto::{ReadError, ReadableError};

        if self.finished_reads.contains(&id) {
            return Ok(ReadOutcome::Finished);
        }
        if let Some(err) = self.recv_errors.remove(&id) {
            return Err(err);
        }
        if buf.is_empty() {
            // Nothing was asked for, so nothing was read. Reporting `Read(0)`
            // would be indistinguishable from end-of-stream to a caller looping
            // on `n > 0`.
            return Ok(ReadOutcome::Blocked);
        }

        // A read releases flow-control credit even when it copies nothing, so
        // mark before the work rather than after: the credit still has to reach
        // the peer.
        self.dirty = true;

        let mut recv = self.conn.recv_stream(id);
        let mut chunks = match recv.read(true) {
            Ok(chunks) => chunks,
            // The receive half is gone. If we never observed FIN ourselves the
            // stream was stopped or reset, which is a genuine error.
            Err(ReadableError::ClosedStream) => return Err(ConnError::ClosedStream),
            Err(ReadableError::IllegalOrderedRead) => {
                return Err(ConnError::Transport(
                    "ordered read after unordered read".into(),
                ))
            }
        };

        let mut filled = 0usize;
        let mut finished = false;
        let mut errored: Option<ConnError> = None;
        while filled < buf.len() {
            // `Chunks::next` never yields more than the requested length, so the
            // caller's buffer bounds the read and nothing has to be stashed.
            match chunks.next(buf.len() - filled) {
                Ok(Some(chunk)) => {
                    let n = chunk.bytes.len();
                    buf[filled..filled + n].copy_from_slice(&chunk.bytes);
                    filled += n;
                }
                Ok(None) => {
                    finished = true;
                    break;
                }
                Err(ReadError::Blocked) => break,
                Err(ReadError::Reset(code)) => {
                    errored = Some(ConnError::Reset {
                        code: code.into_inner(),
                    });
                    break;
                }
            }
        }
        // Releases flow-control credit (MAX_STREAM_DATA / MAX_DATA) for what we
        // consumed. The returned `ShouldTransmit` is discarded because it would
        // be redundant: `self.dirty` above already records that this connection
        // has something to send, and the endpoint's next servicing pass turns it
        // into a datagram. Discarding it is only safe BECAUSE of the dirty flag
        // — the credit is exactly what a stalled peer is waiting on, and a
        // caller that drains `poll_transmit` before doing stream work would
        // otherwise never flush it.
        let _ = chunks.finalize();

        if finished {
            self.finished_reads.insert(id);
        }
        // Bytes we did copy are owed to the caller even if the stream then
        // errored — report them now and let the next read surface the error.
        if filled > 0 {
            if let Some(err) = errored {
                self.recv_errors.insert(id, err);
            }
            return Ok(ReadOutcome::Read(filled));
        }
        if let Some(err) = errored {
            return Err(err);
        }
        Ok(if finished {
            ReadOutcome::Finished
        } else {
            ReadOutcome::Blocked
        })
    }

    /// Offer `buf` to the send stream `id`, accepting as much as flow control
    /// allows right now.
    ///
    /// [`WriteOutcome::Wrote(n)`] may be short of `buf.len()`; a caller with
    /// more to send loops, and on [`WriteOutcome::Blocked`] waits for the next
    /// `StreamWritable`. The old tokio path buffered the remainder and replied
    /// once everything landed; here the caller owns that buffer, because only
    /// the caller can wait.
    ///
    /// [`WriteOutcome::Wrote(n)`]: WriteOutcome::Wrote
    pub fn stream_write(&mut self, id: StreamId, buf: &[u8]) -> Result<WriteOutcome, ConnError> {
        use quinn_proto::WriteError;

        if buf.is_empty() {
            return Ok(WriteOutcome::Wrote(0));
        }
        self.dirty = true;
        match self.conn.send_stream(id).write(buf) {
            Ok(n) => Ok(WriteOutcome::Wrote(n)),
            Err(WriteError::Blocked) => Ok(WriteOutcome::Blocked),
            Err(WriteError::Stopped(code)) => Err(ConnError::Stopped {
                code: code.into_inner(),
            }),
            Err(WriteError::ClosedStream) => Err(ConnError::ClosedStream),
        }
    }

    /// Finish (FIN) the send half of `id`, signalling end-of-data to the peer.
    pub fn stream_finish(&mut self, id: StreamId) -> Result<(), ConnError> {
        match self.send_fins.get(&id).copied() {
            Some(SendFin::Stopped(code)) => return Err(ConnError::Stopped { code }),
            Some(SendFin::Acked | SendFin::Queued | SendFin::Reset(_)) => {
                return Err(ConnError::ClosedStream)
            }
            None => {}
        }
        self.dirty = true;
        match self.conn.send_stream(id).finish() {
            Ok(()) => {
                self.send_fins.insert(id, SendFin::Queued);
                Ok(())
            }
            Err(quinn_proto::FinishError::Stopped(code)) => {
                let code = code.into_inner();
                self.record_send_fin(id, SendFin::Stopped(code));
                Err(ConnError::Stopped { code })
            }
            Err(quinn_proto::FinishError::ClosedStream) => Err(ConnError::ClosedStream),
        }
    }

    /// Abandon the send half of `id` with an application error code, discarding
    /// anything not yet delivered.
    ///
    /// The first terminal fact wins and is returned on later calls: an earlier
    /// FIN acknowledgement or peer STOP is not overwritten by reset, and an
    /// accepted reset retains its original code. The opposite receive half and
    /// its EOF/error bookkeeping remain independent.
    pub fn stream_reset(&mut self, id: StreamId, code: u64) -> Result<ResetOutcome, ConnError> {
        if code >= AUTO_CODE_START {
            return Err(ConnError::InvalidErrorCode { code });
        }
        self.stream_reset_inner(id, code)
    }

    #[doc(hidden)]
    pub fn stream_reset_auto(
        &mut self,
        id: StreamId,
        code: u64,
    ) -> Result<ResetOutcome, ConnError> {
        self.stream_reset_inner(id, code)
    }

    fn stream_reset_inner(&mut self, id: StreamId, code: u64) -> Result<ResetOutcome, ConnError> {
        let code = varint(code)?;
        match self.send_fins.get(&id).copied() {
            Some(SendFin::Acked) => return Ok(ResetOutcome::AlreadyAcknowledged),
            Some(SendFin::Stopped(code)) => return Ok(ResetOutcome::PeerStopped { code }),
            Some(SendFin::Reset(code)) => return Ok(ResetOutcome::AlreadyReset { code }),
            Some(SendFin::Queued) | None => {}
        }
        self.dirty = true;
        self.conn
            .send_stream(id)
            .reset(code)
            .map_err(|_| ConnError::ClosedStream)?;
        self.send_fins.insert(id, SendFin::Reset(code.into_inner()));
        Ok(ResetOutcome::ResetRequested)
    }

    /// Tell the peer to stop sending on `id`, with an application error code.
    ///
    /// Also forgets `id`'s end-of-stream bookkeeping: the caller has explicitly
    /// abandoned the receive half, so there is no further read to answer
    /// [`ReadOutcome::Finished`] for. See [`ConnState::forget_stream`].
    pub fn stream_stop(&mut self, id: StreamId, code: u64) -> Result<(), ConnError> {
        if code >= AUTO_CODE_START {
            return Err(ConnError::InvalidErrorCode { code });
        }
        self.stream_stop_inner(id, code)
    }

    #[doc(hidden)]
    pub fn stream_stop_auto(&mut self, id: StreamId, code: u64) -> Result<(), ConnError> {
        self.stream_stop_inner(id, code)
    }

    fn stream_stop_inner(&mut self, id: StreamId, code: u64) -> Result<(), ConnError> {
        let code = varint(code)?;
        self.dirty = true;
        self.finished_reads.remove(&id);
        self.recv_errors.remove(&id);
        self.conn
            .recv_stream(id)
            .stop(code)
            .map_err(|_| ConnError::ClosedStream)
    }

    /// Drop every trace of `id` from this connection's per-stream bookkeeping.
    ///
    /// **Call this when you are done with a stream.** The core remembers one
    /// `StreamId` per stream that reached clean end-of-stream, so that a read
    /// after FIN keeps answering [`ReadOutcome::Finished`] rather than erroring
    /// once quinn-proto has freed the receive stream. That memory is the caller's
    /// to release: on a long-lived connection that carries an unbounded number of
    /// short-lived streams — one per accepted socket, say — never releasing it is
    /// unbounded growth keyed on a monotonically increasing id.
    ///
    /// After this call, `id` is simply unknown: a subsequent
    /// [`ConnState::stream_read`] reports whatever quinn-proto says, which for a
    /// finished-and-freed stream is an error rather than `Finished`. That is the
    /// contract — "done" means done. It is idempotent and never fails; forgetting
    /// a stream that was never known is a no-op.
    pub fn forget_stream(&mut self, id: StreamId) {
        self.forget_recv(id);
        self.forget_send(id);
    }

    /// Release this stream's stable receive end-of-stream fact.
    pub fn forget_recv(&mut self, id: StreamId) {
        self.finished_reads.remove(&id);
        self.recv_errors.remove(&id);
    }

    /// Release this stream's stable send-half finish/stop fact.
    pub fn forget_send(&mut self, id: StreamId) {
        self.send_fins.remove(&id);
    }

    /// The current stable/near-stable send FIN fact, if any.
    pub fn send_fin(&self, id: StreamId) -> Option<SendFin> {
        self.send_fins.get(&id).copied()
    }

    /// Drain the connection's application events, queue peer-opened streams for
    /// [`ConnState::accept_bi`], and report what happened.
    ///
    /// This is the SINGLE place `poll()` may be called on this connection —
    /// `poll()` consumes events, so a second drainer would silently eat them.
    /// Call it after every datagram and every timeout.
    pub fn service_streams(&mut self, max_work: usize) -> ConnProgress {
        use quinn_proto::{Event, StreamEvent};

        let mut progress = ConnProgress::default();
        let mut remaining = max_work.max(1);
        if self.draining_opened {
            while remaining > 0 {
                let Some(id) = self.conn.streams().accept(Dir::Bi) else {
                    self.draining_opened = false;
                    break;
                };
                self.ready_accepts.push_back(id);
                progress.opened.push(id);
                remaining -= 1;
            }
            if self.draining_opened && remaining == 0 {
                progress.more_work = true;
                return progress;
            }
        }
        while remaining > 0 {
            let Some(ev) = self.conn.poll() else { break };
            remaining -= 1;
            match ev {
                Event::Connected => progress.connected = true,
                Event::ConnectionLost { reason } => {
                    progress.lost = Some(ConnectionError::from_quinn(reason));
                }
                Event::Stream(StreamEvent::Opened { dir: Dir::Bi }) => {
                    self.draining_opened = true;
                    while remaining > 0 {
                        let Some(id) = self.conn.streams().accept(Dir::Bi) else {
                            self.draining_opened = false;
                            break;
                        };
                        self.ready_accepts.push_back(id);
                        progress.opened.push(id);
                        remaining -= 1;
                    }
                }
                Event::Stream(StreamEvent::Readable { id }) => progress.readable.push(id),
                Event::Stream(StreamEvent::Writable { id }) => progress.writable.push(id),
                Event::Stream(StreamEvent::Finished { id }) => {
                    if self.record_send_fin(id, SendFin::Acked) {
                        progress.fin_acked.push(id);
                    }
                }
                Event::Stream(StreamEvent::Stopped { id, error_code }) => {
                    let code = error_code.into_inner();
                    if self.record_send_fin(id, SendFin::Stopped(code)) {
                        progress.stopped.push((id, code));
                    }
                }
                _ => {}
            }
        }
        progress.more_work = self.draining_opened || remaining == 0;
        progress
    }

    fn record_send_fin(&mut self, id: StreamId, fact: SendFin) -> bool {
        match self.send_fins.get(&id).copied() {
            Some(SendFin::Acked | SendFin::Stopped(_) | SendFin::Reset(_)) => false,
            Some(SendFin::Queued) | None => {
                self.send_fins.insert(id, fact);
                matches!(fact, SendFin::Acked | SendFin::Stopped(_))
            }
        }
    }

    /// Whether quinn-proto considers this connection fully terminated.
    ///
    /// **A driver must reap on `progress.lost || state.is_drained()`, not on
    /// `progress.lost` alone.** A *remotely*-initiated close (a CONNECTION_CLOSE
    /// frame, a transport error, a reset) sets quinn-proto's internal `error`
    /// field, so `poll()` yields `Event::ConnectionLost` and
    /// [`ConnProgress::lost`] catches it. A *locally*-initiated close does not:
    /// `Connection::close()` merely arms the close timer, and quinn-proto
    /// 0.11's `handle_timeout`/`Timer::Close` arm sets `state = State::Drained`
    /// and pushes `EndpointEventInner::Drained` **without ever touching
    /// `self.error`** — so `poll()` never reports the connection lost for a
    /// self-close.
    ///
    /// Left unhandled, a self-closed connection's state sits in the driver's
    /// map forever even though quinn-proto's endpoint slab has already freed and
    /// can reuse its `ConnectionHandle`; the eventual collision between a reused
    /// handle and stale bookkeeping wedges `accept`. `is_drained()` is
    /// quinn-proto's own signal that the terminal transition completed by
    /// *either* path, which is why both drivers check it. Guarded by
    /// `tests/connection_lifecycle.rs`.
    pub fn is_drained(&self) -> bool {
        self.conn.is_drained()
    }

    /// Whether a caller-side stream operation since the last servicing pass may
    /// have produced something to send.
    ///
    /// Read by [`crate::endpoint::Endpoint::next_timeout`], which reports an
    /// already-elapsed deadline while this is true so that a caller cannot sleep
    /// through unflushed work. See the `dirty` field.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Clear the dirty flag. For the endpoint's servicing pass, called once the
    /// pass has drained this connection's transmits — at which point everything
    /// the caller's stream operations produced is queued for the caller.
    pub fn clear_dirty(&mut self) {
        self.dirty = false;
    }

    /// The owned connection, for the endpoint that drives it: `poll_transmit`,
    /// `poll_timeout`, `handle_timeout`, `poll_endpoint_events`, `handle_event`,
    /// `close`, `remote_address`.
    ///
    /// `poll()` is the one method that must NOT be called through here — see
    /// [`ConnState::service_streams`].
    pub fn conn_mut(&mut self) -> &mut quinn_proto::Connection {
        &mut self.conn
    }

    /// Read-only access to the owned connection.
    pub fn conn(&self) -> &quinn_proto::Connection {
        &self.conn
    }
}

impl std::fmt::Debug for ConnState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnState")
            .field("remote", &self.conn.remote_address())
            .field("ready_accepts", &self.ready_accepts.len())
            .field("finished_reads", &self.finished_reads.len())
            .field("send_fins", &self.send_fins.len())
            .field("dirty", &self.dirty)
            .finish()
    }
}

/// Application error codes are QUIC varints; anything wider is a caller bug, not
/// a transport condition.
fn varint(code: u64) -> Result<VarInt, ConnError> {
    VarInt::from_u64(code).map_err(|_| ConnError::InvalidErrorCode { code })
}

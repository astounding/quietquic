// SPDX-License-Identifier: 0BSD
//! Non-blocking outcome types for the sans-IO core.
//!
//! These express the three states a caller-driven API needs — progress,
//! would-block, and end-of-stream — *without* inventing an error taxonomy.
//! [`crate::conn::ConnError`] remains the error type; "no data right now" is an
//! expected, non-fatal outcome, matching the POSIX shape a hand-rolled event
//! loop already reasons in.

use std::net::SocketAddr;

use quinn_proto::Dir;

/// One datagram the caller should send.
///
/// The core never touches a socket: it hands back bytes and a destination, and
/// the caller performs the actual `send_to`.
#[derive(Debug, Clone)]
pub struct Transmit {
    pub destination: SocketAddr,
    pub contents: Vec<u8>,
}

/// Outcome of feeding one inbound datagram to the endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatagramOutcome {
    /// The datagram produced no connection for the caller to work with. It was
    /// either rejected by the cloaking pre-filter, or accepted into quinn-proto
    /// without yielding a handle.
    ///
    /// # This is NOT the silence invariant
    ///
    /// The invariant that makes a quietquic server invisible is stated in terms
    /// of the pre-filter, not of this variant:
    ///
    /// > **A datagram that fails the cloaking pre-filter queues nothing to
    /// > send.**
    ///
    /// That is structural rather than a property of the caller's control flow:
    /// [`crate::endpoint::Endpoint::handle_datagram`] returns before the packet
    /// reaches quinn-proto at all, so an embedder that faithfully drains
    /// [`crate::endpoint::Endpoint::poll_transmit`] emits zero bytes in response
    /// to an unauthorized peer, because there is nothing to emit.
    ///
    /// `Dropped` is the wider category, and a datagram that *passed* the
    /// pre-filter can be `Dropped` **and** queue a transmit. The reachable case:
    /// the pre-filter is QUIC-version-agnostic, so a peer holding a valid PSK
    /// can send a well-formed selector DCID with a QUIC version we do not
    /// support; quinn-proto answers with a Version Negotiation packet, which is
    /// queued, and no connection is created, so the outcome is `Dropped`. That
    /// peer proved PSK possession before a single byte was queued, so silence is
    /// intact — but "`Dropped` implies nothing queued" is not.
    ///
    /// If you need "did this datagram cause bytes to be queued?", ask
    /// `poll_transmit`; this enum answers "did it give me a connection?".
    Dropped,
    /// The datagram was admitted and routed to this connection.
    Accepted(ConnectionHandle),
}

/// Result of a non-blocking stream read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    /// `n` bytes were copied into the caller's buffer.
    Read(usize),
    /// No data is buffered right now. Try again after a
    /// [`Event::StreamReadable`] for this stream.
    Blocked,
    /// The peer finished the stream (FIN); no more data will arrive.
    Finished,
}

/// Result of a non-blocking stream write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// `n` bytes were accepted — may be fewer than offered.
    Wrote(usize),
    /// Flow control is closed. Try again after a [`Event::StreamWritable`].
    Blocked,
}

/// Something the caller should react to, drained via
/// [`crate::endpoint::Endpoint::poll_event`].
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionError {
    #[error("closed by peer application: code {code}")]
    ApplicationClosed { code: u64, reason: Vec<u8> },
    #[error("closed by peer transport: code {code}")]
    ConnectionClosed {
        code: u64,
        /// QUIC frame type associated with the close, when available.
        ///
        /// quinn-proto 0.11 carries this internally as `FrameType`, but its raw
        /// numeric value is not exposed through the public API. quietquic
        /// therefore leaves this as `None` for now rather than relying on
        /// private layout, parsing debug output, or taking a patched
        /// quinn-proto dependency. The field remains for compatibility with a
        /// future upstream accessor.
        frame_type: Option<u64>,
        reason: Vec<u8>,
    },
    #[error("transport error: code {code}: {reason}")]
    TransportError {
        code: u64,
        /// QUIC frame type associated with the transport error, when available.
        ///
        /// See `ConnectionClosed::frame_type` for why this is currently `None`
        /// with quinn-proto 0.11.
        frame_type: Option<u64>,
        reason: String,
    },
    #[error("stateless reset")]
    Reset,
    #[error("timed out")]
    TimedOut,
    #[error("closed locally")]
    LocallyClosed,
    #[error("version mismatch")]
    VersionMismatch,
    #[error("connection IDs exhausted")]
    CidsExhausted,
    #[error("endpoint failed: {message}")]
    EndpointFailed {
        message: String,
        raw_os_error: Option<i32>,
    },
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
#[error("connection handle is stale or belongs to another endpoint")]
pub struct ConnectionHandleError;

impl ConnectionError {
    pub(crate) fn from_quinn(reason: quinn_proto::ConnectionError) -> Self {
        match reason {
            quinn_proto::ConnectionError::VersionMismatch => Self::VersionMismatch,
            quinn_proto::ConnectionError::TransportError(error) => Self::TransportError {
                code: u64::from(error.code),
                // quinn-proto 0.11 exposes `FrameType` as a public type, but
                // not its raw numeric value. Avoid private-layout or string
                // parsing workarounds; leave room for a future upstream
                // accessor without committing quietquic to carrying a patch.
                frame_type: None,
                reason: error.reason,
            },
            quinn_proto::ConnectionError::ConnectionClosed(close) => Self::ConnectionClosed {
                code: u64::from(close.error_code),
                // Same `FrameType` limitation as above.
                frame_type: None,
                reason: close.reason.to_vec(),
            },
            quinn_proto::ConnectionError::ApplicationClosed(close) => Self::ApplicationClosed {
                code: close.error_code.into_inner(),
                reason: close.reason.to_vec(),
            },
            quinn_proto::ConnectionError::Reset => Self::Reset,
            quinn_proto::ConnectionError::TimedOut => Self::TimedOut,
            quinn_proto::ConnectionError::LocallyClosed => Self::LocallyClosed,
            quinn_proto::ConnectionError::CidsExhausted => Self::CidsExhausted,
        }
    }
}

/// Something the caller should react to, drained via
/// [`crate::endpoint::Endpoint::poll_event`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// A connection completed its handshake and is ready for streams.
    Connected(ConnectionHandle),
    /// The peer opened a stream.
    StreamOpened {
        conn: ConnectionHandle,
        id: quinn_proto::StreamId,
        dir: Dir,
    },
    /// A stream has data buffered; a previously `Blocked` read may now progress.
    StreamReadable {
        conn: ConnectionHandle,
        id: quinn_proto::StreamId,
    },
    /// Flow control opened; a previously `Blocked` write may now progress.
    StreamWritable {
        conn: ConnectionHandle,
        id: quinn_proto::StreamId,
    },
    /// The peer acknowledged this stream's FIN.
    StreamFinAcked {
        conn: ConnectionHandle,
        id: quinn_proto::StreamId,
    },
    /// The peer asked us to stop sending on this stream.
    StreamStopped {
        conn: ConnectionHandle,
        id: quinn_proto::StreamId,
        error_code: u64,
    },
    /// The connection is gone.
    ///
    ///
    /// The handle becomes permanently stale at this point. Generation checking
    /// guarantees that retaining it cannot address a later connection even if
    /// Quinn reuses its internal slab slot.
    ConnectionLost {
        conn: ConnectionHandle,
        reason: ConnectionError,
    },
}
/// A generation-safe identifier for a connection owned by an [`Endpoint`].
///
/// Quinn's internal handle is a reusable slab index. This wrapper pairs that
/// index with a monotonically increasing generation assigned by quietquic, so
/// a handle retained after `ConnectionLost` can never name a later connection.
///
/// [`Endpoint`]: crate::endpoint::Endpoint
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionHandle {
    pub(crate) quinn: quinn_proto::ConnectionHandle,
    pub(crate) generation: u64,
}

impl ConnectionHandle {
    pub(crate) fn new(quinn: quinn_proto::ConnectionHandle, generation: u64) -> Self {
        Self { quinn, generation }
    }

    /// A process-local monotonically increasing generation, useful for logging.
    pub fn generation(self) -> u64 {
        self.generation
    }
}

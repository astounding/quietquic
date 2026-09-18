# Changelog

All notable user-visible changes are recorded here. This project follows
Semantic Versioning while its wire protocol and Rust API remain experimental.

## Unreleased

- Require Rustls 0.23.45 or newer and update its cryptographic dependencies to
  address RUSTSEC-2026-0285.

- Add a socket-owning, cloneable Tokio `Endpoint` with fixed construction-time
  dial, accept, or combined capabilities; repeated connections share one UDP
  socket and fixed local port.
- Add owned-socket construction, admission pause/resume, bounded incoming and
  command queues, connection configuration snapshots, per-attempt overrides,
  persistent endpoint termination causes, and bounded `wait_closed` cleanup.
- Define cancellation and drop behavior for connection attempts, accepts,
  stream opens, reads, writes, FIN waits, and uniquely owned stream halves.
- Add structured reset outcomes, reserved automatic-cleanup codes,
  `try_open_bi`, and partial-prefix ownership in `ReadToEndError`.
- Reject reserved codes in core connection closes with `CloseConnectionError`
  and expose `AutomaticCode` through the Tokio API. Preserve the original reset
  outcome across FIN races, coalesce cancellation wakes, release canceled dial
  queue entries promptly, and retain fatal endpoint errors for late callers.
- Keep `Server` and `Client` as conveniences backed by the shared endpoint
  driver, and document migration to the primary `Endpoint` API.
- Add deterministic and real-UDP coverage for overlapping fixed-port dials,
  simultaneous initiation, sibling isolation, admission control, endpoint
  retention, socket release, and supplied IPv6 sockets.

## 0.1.0-alpha.3 — 2026-07-29

- Skip publishing `0.1.0-alpha.2`; alpha.3 is the next intended public crate
  release.
- Replace the unsplit Tokio `Stream` API with quinn-shaped bidirectional
  halves: `Connection::open_bi` / `accept_bi` now return
  `(SendStream, RecvStream)`.
- Add send-half completion: `SendStream::wait_finished` and
  `finish_and_wait` report FIN acknowledgement or peer STOP_SENDING.
- Add local `SendStream::reset`, `RecvStream::stop`, `Connection::closed`,
  cloneable `Connection`, and close codes/reasons via
  `Connection::close(code, reason)`.
- Replace stringly stream errors with structured `ConnError` variants and add
  `ConnectionError` for terminal connection facts.
- Document that `ConnectionError` transport `frame_type` fields are currently
  `None` with quinn-proto 0.11 because upstream does not expose the raw
  `FrameType` value.
- Extend sans-IO events with `StreamOpened { dir }`, `StreamFinAcked`,
  `StreamStopped`, and `ConnectionLost { reason }`; mark public event/error
  shapes non-exhaustive.
- Pin advertised unidirectional stream credit to zero until uni-stream support
  lands.

## 0.1.0-alpha.2 — 2026-07-27

- Rename the project and crates from `silentquic`/`silentquic-proto` to
  `quietquic`/`quietquic-proto` to avoid confusion with the unrelated
  `silent-quic` Rust crate.
- Advance the prerelease version rather than reuse the existing
  `v0.1.0-alpha.1` tag and former-name crates.io publication identity.
- Rename Rust import paths, documentation, CI, repository metadata, and the
  protocol domain-separation string; refresh the corresponding known-answer
  vectors.

## 0.1.0-alpha.1 — 2026-07-27

- Split the Sans-I/O protocol core (`silentquic-proto`) from the Tokio wrapper.
- Enforce silent rejection before allocating QUIC connection state.
- Add freshness, replay protection, bounded global/per-source rate limiting,
  known-answer vectors, fuzz targets, and cross-platform CI.
- Add incremental and split stream I/O.
- Require an explicit limit for `Stream::read_to_end`.
- Attach the server-configured client identity to accepted connections and
  reject duplicate identities or PSKs.
- Replace reusable raw Quinn connection handles with generation-safe handles.
- Replace the reject-path vector LRU with an O(1) intrusive LRU.

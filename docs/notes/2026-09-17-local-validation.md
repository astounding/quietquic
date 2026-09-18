# Shared-endpoint local validation checklist

Date: 2026-09-17

## Local result

The final workspace run passed **167 tests** (81 Tokio wrapper tests and 86
core tests), with no failures or ignored tests. Seven documentation examples
also compiled successfully. Formatting, workspace/all-target/all-feature Clippy
with warnings denied, rustdoc with warnings denied, dependency policy, and the
runtime-free polling example passed. Both packaged crates compiled in fresh
consumer projects against the candidate core, rather than the registry core.

Environment: Alpine Linux, kernel `6.18.52-0-virt`, x86_64; Rust/Cargo 1.96.1.
Source at the local validation checkpoint: uncommitted work on `main` over
`d9580bae624ca4edd536144956aaceae8699b9f6`. No commit, push, or hosted test had
been performed at that checkpoint. The owner subsequently authorized committing
and pushing `experiment/shared-endpoint-ci-20260917` for hosted testing.
Minimum-Rust runtime, macOS, FreeBSD, and
cross-host results remain separate gates. The detailed matrix below retains
unchecked items where the stronger scenario has not been independently proved;
passing the local command gate does not claim exhaustive race or stress coverage.

This is a working validation checklist for
`docs/plans/2026-09-17-shared-endpoint-release.md`. It records required evidence;
unchecked items are not claims that the behavior is absent or broken. The
release plan, rather than this note, is authoritative.

## Evidence rules

- [ ] Tie every run to `git rev-parse HEAD` and record the dirty-worktree state.
- [ ] Prefer deterministic sans-I/O tests for packet ordering, loss, timers, and
  ordering races. Use Tokio tests for ownership transfer, task cancellation,
  channel pressure, driver fairness, and actual socket release.
- [ ] Put a bounded timeout around every test that can wait on network, driver,
  capacity, cleanup, or a terminal notification. A timeout is a test failure,
  not the expected proof unless silence is the behavior under test.
- [ ] For handoff races, test three positions: before driver dispatch, after the
  driver creates/completes the value but before application receipt, and after
  application receipt.
- [ ] Under cancellation and queue saturation, assert externally observable
  cleanup and resource reuse. A dropped task or successful oneshot send alone
  is insufficient evidence.
- [ ] Use counters or test-only snapshots to check connection/stream/admission
  slots and buffered-byte budgets return to baseline after repeated cycles.
- [ ] Keep real-UDP tests independent of fixed port numbers and preserve source
  address assertions. Skip an address-family case only for a detected platform
  limitation, with the reason visible.

## Decision coverage

Each numbered row corresponds to the plan's complete 51-decision index.

| Decision | Required local evidence |
| --- | --- |
| 1 | [ ] Dropping all public endpoint handles while a connection or either stream half remains leaves that owned object usable; socket lifetime ends after the final retained application owner and cleanup. |
| 2 | [ ] Pause rejects new inbound attempts silently; resume admits again; explicit close is terminal and resume cannot reopen it. |
| 3 | [ ] A handshake admitted before pause can complete, and a queued completed connection can still be accepted during pause. |
| 4 | [ ] Cancel a dial before dispatch and during handshake; neither yields an orphan connection, and admission/connection state returns to baseline. |
| 5 | [ ] Last application connection owner starts bounded close while sibling connections continue. Repeat with stream-retained ownership to disambiguate the owner count. |
| 6 | [ ] Dropping an unfinished send half emits the reserved automatic reset; dropping after successful `finish()` preserves FIN and its eventual acknowledgement. |
| 7 | [ ] Dropping an unfinished receive half emits STOP_SENDING, discards bounded unread data, and leaves the opposite send direction usable. EOF-consumed drop only releases bookkeeping. |
| 8 | [ ] Cancel `write_all` immediately before its start boundary and immediately after it; pre-start sends no bytes/reset, post-start resets the send direction. |
| 9 | [ ] Cancel incremental reads at every handoff point and verify all unreturned bytes are returned once, in order, on later reads. |
| 10 | [ ] Cancel `read_to_end` after several chunks; a later incremental read observes the exact retained prefix plus following bytes without a second full-prefix copy. |
| 11 | [ ] Cancel `wait_finished`, then await again and observe the stable recorded result; cover reset racing with FIN acknowledgement. |
| 12 | [ ] `read_to_end(limit)` overflow returns exactly the up-to-limit prefix with `ReadLimitExceeded`, uses bounded lookahead, stops reception, and preserves the opposite direction. |
| 13 | [ ] Cancel `accept()` before dispatch and after driver assignment/before receipt; another waiter receives the same connection exactly once unless it independently dies. |
| 14 | [ ] Perform the equivalent three-position cancellation test for `accept_bi()` and verify stream credit/bookkeeping. |
| 15 | [ ] Cancel `open_bi()` while waiting for credit and after allocation/before receipt; release the waiter or reset/stop both allocated halves and recover capacity. |
| 16 | [ ] `open_bi()` waits for peer credit; `try_open_bi()` returns promptly with a structured temporary-unavailability cause. |
| 17 | [ ] Fill handshake plus completed-accept admission capacity, verify the next attempt is silently rejected before connection allocation, then free one slot and admit again. |
| 18 | [ ] Inject a fatal socket receive and send failure, verify persistent structured endpoint failure, wake every affected operation, and perform no automatic rebind. Also test transient-error backoff without a busy loop. |
| 19 | [ ] `close()` is idempotent and starts shutdown; `wait_closed()` alone does not start it, is cancellation-safe, and completes only after bounded cleanup and socket release. |
| 20 | [ ] Change defaults between connections and prove each incoming and outgoing connection retains its construction-time snapshot; cover per-attempt override. |
| 21 | [ ] Outgoing handshake defaults to 10 seconds measured from driver initiation, validates overrides, and times out independently of healthy siblings. |
| 22 | [ ] Pending plus accepted peer streams consume one shared QUIC credit limit; releasing/rejecting a stream restores credit. Receive-byte windows are tested separately. |
| 23 | [ ] Incoming handshake defaults to 10 seconds from admission and its timeout releases the admission slot without affecting siblings. |
| 24 | [ ] A completed queued connection survives beyond the handshake duration/default while transport remains alive; closure, endpoint termination, or final endpoint-handle drop releases it. |
| 25 | [ ] Dropping the final Endpoint handle rejects new inbound traffic and cleans admitted handshakes and queued unaccepted connections, while application-owned connections and outgoing attempts continue. |
| 26 | [ ] An outgoing attempt completes after every Endpoint handle is dropped and keeps the socket/driver alive until its own completion or cancellation. |
| 27 | [ ] Saturate ordinary command queues and byte budgets: waiting calls backpressure, non-waiting calls report local unavailability, and mandatory drop/cancel cleanup still arrives and restores resources. |
| 28 | [ ] With multiple continuously busy connections, rotating bounded service lets every connection, timer, cancellation, and cleanup operation progress. Assert a finite bound suitable for the deterministic harness, not bandwidth equality. |
| 29 | [ ] Public API/docs do not implement or promise Tokio `AsyncRead`/`AsyncWrite`; compile/API review is sufficient. |
| 30 | [ ] Advertised unidirectional stream credit is zero and no new uni-stream/datagram API is exposed. |
| 31 | [ ] Migration tests/examples use one ownership and cancellation model; the experimental helper is absent from the supported public API. |
| 32 | [ ] Admission pause leaves outgoing dial and new/existing bidirectional streams fully operational. |
| 33 | [ ] `accept()` drains queued connections during pause and then remains pending across pause until resume/new admission or terminal state. |
| 34 | [ ] `accept()` distinguishes item, orderly end, and structured failure; terminal result and cause repeat for late/concurrent observers; dead queued items are filtered. |
| 35 | [ ] Reserved cleanup-code range rejects explicit application use; automatic reset/stop uses documented values; received raw codes survive and interpretation helpers classify them. |
| 36 | [ ] Compile-time checks/examples show Endpoint and Connection clone; SendStream and RecvStream do not clone and can move independently. Clone drops do not affect peers. |
| 37 | [ ] Multiple `accept()` waiters receive distinct connections in driver-receipt FIFO order; cancel oldest/middle waiters; termination wakes all. |
| 38 | [ ] Repeat decision 37 for `accept_bi()` under multiple peer-opened streams. |
| 39 | [ ] Earlier waiting `open_bi()` callers receive newly available capacity before later `try_open_bi()` calls; canceled waiters are removed. |
| 40 | [ ] Exercise local queue/budget pressure, peer stream credit, and earlier waiters and verify each returns temporary unavailability rather than a connection or operation failure. The public contract does not require separate temporary-cause variants. |
| 41 | [ ] Per-connection cleanup defaults to a 10-second cap; endpoint shutdown uses one overall configurable cap regardless of connection count. Use paused time/test clocks where possible. |
| 42 | [ ] Construct by bind address and by ownership transfer of a prebound socket; verify exact local address/port and source address. Validate incompatible remote/address families. |
| 43 | [ ] One endpoint has one socket owner/reader and retains its bound port across multiple connections; no competing receive loops are created. |
| 44 | [ ] Connection loss and peer reset during `read_to_end` return the accumulated prefix plus the correct failure, never successful EOF, with bounded retained memory. |
| 45 | [ ] A payload larger than the byte budget is sent incrementally without an additional full-input copy; command plus transport buffers remain under the documented bound and another connection progresses. |
| 46 | [ ] Endpoint termination cause becomes observable before cleanup completes, remains identical for late observers, and is distinct from `wait_closed()` completion. |
| 47 | [ ] A single endpoint type supports dial-only, accept-only, and dial+accept; bidirectional streams work regardless of connection initiator. Accept capability requires PSK credentials. |
| 48 | [ ] Capabilities cannot be added after construction; pause/resume cannot turn dial-only into accept-capable. Disabled operations return a stable structured result. |
| 49 | [ ] Zero and invalid/infinite handshake deadlines are rejected for defaults and overrides; finite positive boundary values are accepted. |
| 50 | [ ] Record runtime results, OS, architecture, and candidate commit independently for Linux, macOS, and FreeBSD. Local Linux evidence alone leaves this unchecked. |
| 51 | [ ] Record a real two-host run: overlapping fixed-port connections, traffic in both directions, and sibling survival after close. Record UDP reachability/server startup first. Later CGNAT application tests remain outside this library gate. |

## Implemented evidence index

These references identify focused tests that have been implemented. They do not
mark the aggregate local gate complete: the final full-suite, formatting,
Clippy, rustdoc, dependency, packaging, and minimum-Rust runs are recorded
separately by the release owner. Platform and cross-host gates remain unchecked.

| Decisions | Implemented focused evidence |
| --- | --- |
| 1, 5, 25, 26 | `tests/shared_endpoint.rs`: `streams_retain_connection_after_endpoint_and_connection_handles_drop`, `last_endpoint_drop_closes_queued_inbound_but_owned_connection_survives`, `pending_outgoing_attempt_retains_endpoint_after_last_handle_drop`. |
| 2, 3, 32, 33 | `tests/shared_endpoint.rs`: `pause_preserves_queued_connection_and_rejects_new_admission_until_resume`; `proto/tests/core_endpoint.rs`: `pausing_admission_preserves_an_already_admitted_connection`. |
| 4 | `tests/shared_endpoint.rs`: `dropping_outgoing_attempt_does_not_harm_later_connection`; `src/endpoint.rs`: `canceled_attempt_before_dispatch_never_allocates_transport`, `outgoing_result_abandoned_before_handoff_closes_only_that_connection`. |
| 6, 7, 11, 35 | `tests/stream_drop.rs`: `unfinished_send_drop_resets_peer_and_opposite_direction_survives`, `unfinished_recv_drop_stops_peer_and_opposite_direction_survives`, `dropping_successfully_finished_send_preserves_fin`, `acknowledged_fin_remains_observable_after_connection_cleanup`, `explicit_reset_is_idempotent_and_reserved_codes_are_rejected`; `proto/tests/core_streams.rs`: `reset_is_idempotent_and_retains_original_code`. |
| 8 | `src/endpoint.rs`: `write_canceled_before_dispatch_preserves_the_send_direction`, `write_canceled_after_driver_completion_resets_sending`. |
| 9 | `src/endpoint.rs`: `incremental_read_canceled_after_driver_completion_preserves_bytes`. |
| 10, 12, 44 | `tests/stream_cancellation.rs`: `canceled_read_to_end_preserves_consumed_prefix`, `completed_fin_before_caller_poll_is_reclaimed`; `src/endpoint.rs`: `canceled_partial_failure_retains_prefix_after_connection_is_removed`, `canceled_collector_prefix_survives_a_later_connection_close`, `canceled_complete_collector_obeys_a_smaller_retry_limit`, `reset_during_collection_returns_already_consumed_prefix`; `tests/client_server_roundtrip.rs`: bounded-read overflow coverage. |
| 13, 37 | `tests/shared_endpoint.rs`: `canceling_accept_waiter_preserves_the_next_connection`, `cloned_accept_waiters_are_fifo_at_driver_receipt`. |
| 14 | `src/endpoint.rs`: `stream_accept_canceled_after_driver_completion_returns_same_stream`. |
| 17, 22, 23, 24 | `proto/tests/core_endpoint.rs`: `pending_admission_is_bounded_and_mark_accepted_releases_the_slot`, `unfinished_incoming_handshake_times_out_and_releases_admission_slot`, `completed_unaccepted_connection_has_no_acceptance_deadline`; stream-credit behavior is additionally exercised by the core stream suites. |
| 18, 34, 46 | `src/endpoint.rs`: `fatal_socket_failure_reaches_active_and_late_connection_operations`, `fatal_socket_failure_finishes_queued_attempt_and_persists`; persistent connection terminal facts are covered by `tests/stream_finish.rs`. |
| 19, 41 | `tests/shared_endpoint.rs`: `explicit_shutdown_is_terminal_and_wait_closed_releases_socket`, `endpoint_shutdown_uses_one_cleanup_cap_with_retained_connection_owners`; `proto/tests/core_endpoint.rs`: `force_remove_drains_transport_and_prunes_all_bookkeeping`. |
| 20, 21, 49 | `proto/tests/core_endpoint.rs`: `transport_defaults_are_snapshots_and_zero_timeout_override_is_rejected`, `updated_transport_defaults_affect_only_future_incoming_connections`, `oversized_attempt_deadline_is_rejected_before_allocation`; `src/endpoint.rs`: `queued_attempt_captures_transport_defaults_at_request`. |
| 27 | `src/endpoint.rs`: `stream_drop_cleanup_survives_a_full_ordinary_command_queue`. |
| 28, 45 | `tests/shared_endpoint.rs`: `flow_control_blocked_large_write_does_not_starve_sibling_transfer`; `proto/tests/core_endpoint.rs`: `transfer_recovers_from_deterministic_loss_and_reordering`. |
| 27, 28 (lifecycle cleanup) | `proto/tests/core_endpoint.rs`: `repeated_connection_lifecycle_returns_all_endpoint_counters_to_baseline` runs 32 handshakes, handoffs, and forced cleanups through one endpoint and checks live, cleanup, admission, CID, and queued-event state returns to baseline after every cycle. |
| 31, 36, 43, 47, 48 | `tests/shared_endpoint.rs`: `two_outgoing_connections_share_one_fixed_socket_and_isolate_close`, `simultaneous_dial_and_accept_works_on_both_endpoints`, `construction_capabilities_are_static`; public examples and doctests provide compile-time API-shape evidence. |
| 42 | `tests/shared_endpoint.rs`: `supplied_ipv6_sockets_preserve_address_and_transfer`, `supplied_dual_stack_socket_dials_ipv4_with_fixed_source_port`, `invalid_address_family_dial_does_not_harm_healthy_sibling`. |
| 50 (local runtime modes only) | `tests/shared_endpoint.rs`: current-thread tests plus `shared_endpoint_lifecycle_works_on_multithread_runtime`. This is not minimum-Rust or macOS/FreeBSD evidence. |

Final controlled-driver additions cover allocated-open cancellation, concurrent
stream-accept FIFO delivery, older-opener priority when new MAX_STREAMS credit
arrives, a capacity-blocked accept waking on connection failure, and injected
transient send errors respecting the retry deadline. See the correspondingly
named tests in `src/endpoint.rs`.

Further stress coverage could exercise every open-cancellation handoff position,
every local/peer temporary-capacity trigger, detailed byte-budget instrumentation,
deterministic multi-connection service rotation, and injected receive failures
through the socket adapter. These broader matrix entries remain unchecked.

## Cross-cutting regression suites

- [ ] Sans-I/O routing/silence: known CID, unknown packets, wrong PSK, genuine
  replay, malformed and version-negotiation-shaped packets, mixed-role traffic,
  retired CID, and stateless-reset behavior. Silent cases must assert zero
  transmit, not merely failure to connect.
- [ ] Deterministic network schedule: loss, duplication, and reordering during
  handshake, stream transfer, FIN, reset, close, and cleanup.
- [ ] Real UDP: overlapping outgoing connections on one fixed port,
  simultaneous initiation, byte-exact bidirectional transfer, sibling failure
  isolation, attempt cancellation/timeout, final-owner behavior, queued inbound
  cleanup, and successful rebinding only after `wait_closed()`.
- [ ] Resource exhaustion: admission slots, connection count, peer stream
  credit, ordinary command count, queued write bytes, receive windows, and
  retained canceled-read prefixes. Verify recovery after each limit is freed.
- [ ] Repeated lifecycle stress: create/cancel/close attempts, connections, and
  streams enough times to expose stale waiter entries, generation reuse, CID
  leaks, retained terminal observers, or sockets kept alive by driver-internal
  references.
- [ ] Current-thread and multi-thread Tokio runtimes, because the crate only
  requires Tokio `rt` and must not assume a multi-thread scheduler.
- [x] Core dependency tree contains no Tokio dependency.

## Race and specification review points

- [ ] Define the application-owner accounting unit before testing decisions 1,
  5, 25, and 26. A stream must retain its connection and endpoint, while a
  driver reference or terminal observer must not count as an application owner.
- [ ] Define one application-delivery acknowledgement/guard used by dial,
  `accept`, `accept_bi`, and `open_bi`. A driver reply-channel send is not the
  handoff point because its receiver can be dropped immediately afterward.
- [ ] Define the write-start boundary at the moment the driver commits the first
  payload bytes to transport state. Queue receipt alone would make cancellation
  behavior depend on scheduling rather than actual writes.
- [ ] Define stable precedence for endpoint socket failure, explicit endpoint
  close, per-connection failure, peer reset/stop, local reset, and FIN
  acknowledgement when more than one is ready in the same driver turn.
- [ ] Specify how a canceled `read_to_end` prefix participates in a later
  smaller limit. The later call must neither duplicate the prefix allocation nor
  discard bytes silently.
- [ ] Admission capacity must be reserved before connection allocation and cover
  handshakes plus the completed queue. Confirm whether direct handoff to an
  already waiting acceptor consumes a slot transiently so boundary tests match
  the implementation.
- [ ] Queue fairness is ordered at driver receipt, so tests must control receipt
  order and must not assume Tokio task polling order.
- [ ] A dead connection must be removed before `accept` handoff when death is
  already known; death immediately after acknowledged handoff is an allowed
  network race and needs a distinct test expectation.
- [ ] Endpoint cleanup has one absolute deadline. Per-connection cleanup work
  must use the remaining shared budget rather than each receiving the full cap.
- [ ] Socket transient errors require scheduled retry/backoff or readiness
  gating. Immediate retry can starve timers and cleanup while looking active.
- [ ] The plan labels decisions 10, 12, and 44 as QIMPACT even though the
  cancellation/stream-lifecycle section is headed QREQ/QIMPACT. Treat them as
  required release behavior and tests; do not infer that QIMPACT permits
  omission from this release.
- [ ] "No separate acceptance deadline" does not mean immortal queued state:
  transport failure, peer close, explicit endpoint close, or final Endpoint
  handle drop must still reap it.

## Local command gate

Use the repository-local dependency cache for offline validation:

```sh
CARGO_HOME=/home/ai/quietquic/target/experiment-cargo-home \
  cargo test --offline --locked --workspace --all-targets
CARGO_HOME=/home/ai/quietquic/target/experiment-cargo-home \
  cargo test --offline --locked --workspace --doc
RUSTDOCFLAGS="-D warnings" \
  CARGO_HOME=/home/ai/quietquic/target/experiment-cargo-home \
  cargo doc --offline --locked --workspace --no-deps
CARGO_HOME=/home/ai/quietquic/target/experiment-cargo-home \
  cargo run --offline --locked -p quietquic-proto --example poll_loop
CARGO_HOME=/home/ai/quietquic/target/experiment-cargo-home \
  cargo tree --offline -p quietquic-proto
```

- [x] Workspace all-target tests pass.
- [x] Workspace doc tests pass.
- [x] Rustdoc passes with warnings denied.
- [x] Polling example runs.
- [x] Core dependency tree contains no Tokio.
- [x] `cargo fmt --all -- --check` passes.
- [x] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  passes.
- [x] `cargo deny --offline check` passes; only configured duplicate-version
  warnings were reported.
- [x] Package archive and fresh-consumer checks pass.
- [ ] Minimum-Rust runtime testing (Rust 1.88) remains a hosted gate.

Recorded local dependency evidence: base commit
`d9580bae624ca4edd536144956aaceae8699b9f6` with a dirty working tree, Linux
6.18.52-0-virt x86_64, Alpine rustc 1.96.1. `cargo tree --offline --locked -p
quietquic-proto` contained no Tokio dependency. This identifies development
evidence only; final candidate metadata must be recorded again after the source
is stable.

The owner subsequently installed the Alpine formatting/lint tools and
`cargo-deny`. The locally visible versions are Cargo/Rust 1.96.1, rustfmt 1.9.0,
Clippy 0.1.96, and cargo-deny 0.18.6. Their availability permits the local
commands above; it is not evidence that the final candidate commands passed.
The release owner records those results only after the source is stable. The
existing GitHub workflow installs formatter and Clippy components independently,
and its hosted results remain a separate gate.

## Non-local release evidence

Subsequent evidence: candidate `9f1d887` passed GitHub Linux, macOS, FreeBSD,
and Rust 1.88 jobs and the 2026-09-18 cross-host transport run. See
[the dated evidence note](2026-09-18-cross-host-validation.md). The list below
is the historical local-checkpoint checklist; the later note supersedes its
pending platform and transport status. Application-level CGNAT tests remain
separate.

- [ ] macOS runtime gate, including recorded architecture and commit.
- [ ] FreeBSD runtime gate, including recorded architecture and commit.
- [ ] Cross-host transport gate and later owner-arranged CGNAT run.
- [ ] GitHub workflow results from the exact candidate commit.
- [x] Package archives tested against the candidate local core rather than the
  old registry release. Eventual registry verification follows core publish.
- [ ] Publication, tag, and docs.rs checks. These are outside local validation
  and are not authorized by this note.

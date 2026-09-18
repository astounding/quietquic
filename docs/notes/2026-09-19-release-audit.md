# Shared-endpoint release evidence audit

Date: 2026-09-19. The audit began against candidate
`9f1d887b9e3fd5f39078dcd69c885a9ff6072cf1`, then added uncommitted focused
tests and small fixes for gaps it found. The old hosted and cross-host evidence
still applies to `9f1d887`, not automatically to the resulting working tree.
This audit is not release, version, tag, or publication authorization.

## How to read this audit

- **Verified** means a focused test or static API check directly establishes the
  decision, at the granularity promised by the decision index.
- **Partial** means the implementation is visible and some behavior is tested,
  but a material race, limit, or branch named by the fuller contract is only
  indirect or untested.
- **Gap** means no adequate implementation/evidence was found.
- **Blocking** means the missing evidence belongs to an agreed release contract
  or release gate. **Additional hardening** means the core decision is supported,
  but the plan's stronger stress/race matrix would benefit from direct coverage.

Test names below are exact. Line references identify their definitions at the
audited commit; several controlled-driver tests live beside private driver code
in `src/endpoint.rs`.

## All 51 decisions

| # | Status | Severity | Exact code/test evidence and audit finding |
|---:|---|---|---|
| 1 | Verified | — | `tests/shared_endpoint.rs:202` `streams_retain_connection_after_endpoint_and_connection_handles_drop`; `src/conn.rs:210-220` owner lease. |
| 2 | Verified | — | `tests/shared_endpoint.rs:250` `pause_preserves_queued_connection_and_rejects_new_admission_until_resume`; `:282` terminal shutdown. |
| 3 | Verified | — | Same pause test accepts the queued connection while paused; `proto/tests/core_endpoint.rs:283` preserves admitted work. |
| 4 | Verified | — | `tests/shared_endpoint.rs:183` cancels an active attempt; `src/endpoint.rs:1564` cancels before dispatch, `:1476` reclaims an abandoned completed result, and `repeated_canceled_dials_release_queue_storage_without_driver_service` proves repeated queued cancellation restores accounting. |
| 5 | Verified | — | `tests/shared_endpoint.rs` covers sibling isolation and stream-retained ownership; `last_connection_owner_drop_closes_only_its_transport` directly observes automatic last-owner cleanup while a sibling remains usable. |
| 6 | Verified | — | `tests/stream_drop.rs:62` unfinished send reset; `:159` finished-send FIN preservation. |
| 7 | Verified | — | `tests/stream_drop.rs:108` STOP_SENDING and opposite-direction survival; core bookkeeping release tests at `proto/tests/core_driving.rs:129,166`. |
| 8 | Verified | — | `src/endpoint.rs:1442` pre-dispatch cancellation and `:1457` post-driver-completion reset. |
| 9 | Partial | Additional hardening | `src/endpoint.rs:1296` proves cancellation after driver completion preserves bytes; cancellation before dispatch and after ordinary application completion are not separately asserted. |
| 10 | Verified | — | `tests/stream_cancellation.rs:35` and controlled tests `src/endpoint.rs:1340,1369` preserve a collected prefix, including later close and smaller retry limit. |
| 11 | Verified | — | Stable FIN and ACK/reset facts are covered by the stream/core suites. `canceled_fin_wait_does_not_cancel_fin_and_later_wait_observes_ack` cancels a pending wait and re-awaits the result; `reset_ordered_before_fin_ack_remains_the_stable_terminal_fact` covers race precedence. |
| 12 | Verified | — | `tests/client_server_roundtrip.rs:159` returns the exact prefix and `ReadLimitExceeded`; `tests/stream_drop.rs` `read_limit_stops_only_the_receive_direction_and_returns_bounded_prefix` proves the automatic STOP_SENDING and opposite-direction survival. The implementation consumes only the bounded prefix before stopping. |
| 13 | Verified | — | `tests/shared_endpoint.rs:238` cancels an accept waiter and later receives the connection. Endpoint `accept` has no reply-channel assignment stage: it removes the queued connection only in the same `poll` that returns `Poll::Ready`, so no assigned-but-unclaimed position exists. |
| 14 | Verified | — | `src/endpoint.rs:1122` `stream_accept_canceled_after_driver_completion_returns_same_stream`. |
| 15 | Partial | Additional hardening | `src/endpoint.rs:1140` reclaims both directions after allocated-open cancellation. Cancellation while waiting for peer credit is not separately verified. |
| 16 | Verified | — | `src/endpoint.rs:1237` fills peer stream credit, keeps the older opener first, and returns `TryOpenOutcome::TemporarilyUnavailable`; public shape at `src/conn.rs:404,479-533`. |
| 17 | Verified | — | `proto/tests/core_endpoint.rs` `admission_bound_combines_completed_and_in_progress_connections` fills one combined bound with completed and in-progress connections, proves the next Initial is dropped with unchanged connection/CID counts and no transmit, frees a slot, and admits the exact retry. |
| 18 | Verified | — | `src/endpoint.rs:1491,1545` persist fatal injected endpoint failure to active/late operations; `:1006` tests transient-send backoff. `receive_errors_back_off_or_terminate_without_rebinding` covers receive backoff, fatal termination, retained socket ownership through cleanup, and no automatic rebind. |
| 19 | Verified | — | `tests/shared_endpoint.rs:282` proves idempotent close, terminal behavior, cleanup completion, and rebinding. `src/endpoint.rs` `canceling_wait_closed_neither_starts_nor_stops_shutdown` proves observation alone and cancellation are inert. |
| 20 | Verified | — | Existing core/driver tests cover incoming and queued outgoing snapshots. `per_attempt_transport_override_is_advertised_without_changing_defaults` distinguishes the override, existing connection, and later default connection on the wire. |
| 21 | Verified | — | Ten-second default is set at `proto/src/config.rs:274` and the deadline starts in core connect at `proto/src/endpoint.rs:325-360`. `proto/tests/core_endpoint.rs` `outgoing_override_times_out_without_harming_healthy_sibling` proves an attempt override expires from initiation while a healthy sibling remains live. |
| 22 | Verified | — | `pending_and_accepted_streams_share_credit_and_drops_restore_it` fills the shared 32-stream credit with pending plus accepted peer streams, then gracefully finishes both directions, consumes EOF, drops the halves, and proves restored credit wakes the oldest opener. |
| 23 | Verified | — | Default at `proto/src/config.rs:275`; `proto/tests/core_endpoint.rs:460` proves admission-based timeout and slot release. |
| 24 | Verified | — | `proto/tests/core_endpoint.rs:481` proves a completed unaccepted connection outlives the handshake interval. Final-drop cleanup is covered by decision 25. |
| 25 | Verified | — | `tests/shared_endpoint.rs:546` closes queued inbound work while an application-owned connection survives. |
| 26 | Verified | — | `tests/shared_endpoint.rs:347` pending attempt retains the endpoint after all Endpoint handles drop. |
| 27 | Verified | — | Bounded ordinary channels/budgets are implemented in the endpoint and connection senders; `stream_drop_cleanup_survives_a_full_ordinary_command_queue` proves mandatory cleanup survives saturation. `try_open_reports_local_pressure_and_recovers` and `write_byte_budget_saturates_in_chunks_and_recovers_on_cancellation` prove backpressure and restoration. Cleanup metadata uses a reliable path outside the bounded ordinary queue; wake-only cancellation notifications are coalesced rather than accumulating there. |
| 28 | Partial | Additional hardening | Work budgets and rotating core cursor are implemented; `bounded_service_rotates_transmit_progress_across_connections` proves rotation across three busy connections with one-unit budgets, and the real-UDP large-write test proves sibling progress. A combined continuous-load test including timers, cancellation, and cleanup remains additional hardening. |
| 29 | Verified | — | `SendStream`/`RecvStream` expose inherent async methods in `src/conn.rs`; repository search finds no `AsyncRead`/`AsyncWrite` implementation. Current guide explicitly defers them. |
| 30 | Verified | — | `proto/src/config.rs:403` forces uni credit to zero; no public uni-stream/datagram API is present. |
| 31 | Verified | — | No `experimental_connect` symbol exists in supported code; migration is documented in `docs/shared-endpoints.md:235-251`; conveniences delegate to Endpoint. Historical experiment references remain correctly historical. |
| 32 | Verified | — | Existing pause coverage proves queued acceptance and silent rejection; `paused_admission_preserves_dial_streams_and_waiting_accept_until_resume` proves outgoing dial and new/existing bidirectional streams continue while admission remains paused. |
| 33 | Verified | — | Existing coverage drains a queued connection while paused; `paused_admission_preserves_dial_streams_and_waiting_accept_until_resume` holds the next accept pending across pause and wakes it only after resumed admission. |
| 34 | Verified | — | Public result is `Result<Option<Connection>, EndpointError>` (`src/endpoint.rs:308-314`). Shutdown tests cover orderly end; `fatal_receive_wakes_all_acceptors_and_persists_for_late_operations` covers concurrent and late structured failure; `accept_filters_closed_connection_before_driver_queue_reaping` prevents delivery of a connection already known dead. |
| 35 | Verified | — | Reserved assignments and interpreter are `proto/src/conn.rs:77-113`; explicit use is rejected and reset outcome tested by `tests/stream_drop.rs:189`; received raw codes are retained in structured outcomes. |
| 36 | Verified | — | Endpoint derives Clone (`src/endpoint.rs:139`), Connection derives Clone (`src/conn.rs:424`), stream halves do not; ownership behavior is exercised at `tests/shared_endpoint.rs:202`. |
| 37 | Verified | — | `tests/shared_endpoint.rs:365` proves endpoint accept FIFO at driver receipt; `endpoint_accept_fifo_survives_middle_waiter_cancellation` covers waiter removal without consuming an item; `fatal_receive_wakes_all_acceptors_and_persists_for_late_operations` wakes all waiters. |
| 38 | Partial | Additional hardening | `src/endpoint.rs:1177` proves two `accept_bi` waiters receive FIFO; cancellation and termination of multiple waiters are only covered separately/indirectly. |
| 39 | Verified | — | `src/endpoint.rs:1237` proves a later `try_open_bi` cannot bypass the earlier waiting opener. |
| 40 | Verified | — | `TryOpenOutcome::TemporarilyUnavailable` covers operation budget, command queue, peer credit, and earlier-waiter cases in code. Existing controlled tests cover peer credit/waiter priority, and `try_open_reports_local_pressure_and_recovers` proves local pressure is temporary and distinct from terminal failure. |
| 41 | Verified | — | Defaults at `proto/src/config.rs:276`; `tests/shared_endpoint.rs:575` proves one endpoint-wide cleanup cap with retained owners. |
| 42 | Verified | — | Bind and owned socket API at `src/endpoint.rs:145-165`; real UDP family/source tests at `tests/shared_endpoint.rs:421,456,478` and `tests/client_server_roundtrip.rs:241,286`. |
| 43 | Verified | — | Single socket ownership is explicit in `src/endpoint.rs:138-170`; `tests/shared_endpoint.rs:104` proves multiple overlapping connections retain one fixed port. |
| 44 | Verified | — | Connection failure prefix: `src/endpoint.rs:1312,1340,1491`; peer reset prefix: `:1391`; results are errors, not EOF. |
| 45 | Verified | — | The per-connection semaphore accounts 256 KiB; `write_all` copies only the currently permitted 16 KiB chunk into a command while retaining the caller's remaining slice. `write_byte_budget_saturates_in_chunks_and_recovers_on_cancellation` proves saturation/recovery, and the real-UDP large-write test proves sibling progress. |
| 46 | Verified | — | Persistent terminal watch is `src/endpoint.rs:64-69,117-127,372-398`; `late_dial_retains_original_socket_failure` and `fatal_receive_wakes_all_acceptors_and_persists_for_late_operations` prove stable late causes, while shutdown/cleanup tests separate termination observation from `wait_closed()` completion. |
| 47 | Verified | — | One Endpoint and three capabilities at `proto/src/config.rs:139-156,196-218`; `tests/shared_endpoint.rs:157,313` covers combined operation and static capability errors; credentials required at `proto/src/config.rs:383`. |
| 48 | Verified | — | `tests/shared_endpoint.rs:313` and `proto/src/endpoint.rs:1317` prove capabilities are fixed and pause cannot add them. |
| 49 | Verified | — | `proto/src/config.rs:365` checks all three endpoint deadline defaults at zero, above the 24-hour maximum, and exactly at that finite maximum. Existing core tests reject zero/oversized outgoing overrides before allocation, and `proto/tests/core_endpoint.rs` `maximum_finite_outgoing_override_is_accepted` accepts the exact maximum. |
| 50 | Partial | Blocking | `docs/notes/2026-09-18-cross-host-validation.md` records Linux, macOS, FreeBSD, and Rust 1.88 passes for `9f1d887`; the workflow executes runtime tests, not compile-only. The audit fixes change the candidate, so the same platform jobs must pass again on the final commit. |
| 51 | Partial | Blocking | The dated evidence records plain UDP reachability, server readiness, overlapping fixed-port connections, bidirectional byte-exact traffic, sibling survival, shutdown, and rebinding for `9f1d887`. The final post-audit commit needs the cross-host gate rerun. |

## Blocking findings before release readiness

1. Decisions 50 and 51 passed for `9f1d887`, but the focused fixes produced by
   this audit create a new candidate. Hosted platform and cross-host gates must
   be rerun on the final commit. The four other partial decisions are additional
   race/stress hardening; this audit found no known contract failure in them.
2. The audited base commit is still version `0.1.0-alpha.3` in both manifests and
   the wrapper exactly pins that version. The plan's `alpha.4` is provisional.
   Version choice, lockfile effects, changelog dating, package regeneration,
   tag, registry verification, and docs.rs verification remain release work and
   were intentionally not performed in this audit.

The resulting matrix is **45 verified, four partial additional-hardening rows,
and two partial blocking platform/current-candidate rows, with no gaps**. The
post-audit working tree passed all 190 workspace tests and seven doc tests.
Final local checks all passed on Alpine x86_64 with Rust/Cargo 1.96.1:

- `cargo fmt --all -- --check`
- `cargo test --offline --locked --workspace --all-targets` (190 tests)
- `cargo test --offline --locked --workspace --doc` (seven examples)
- `cargo clippy --offline --locked --workspace --all-targets --all-features -- -D warnings`
- `RUSTDOCFLAGS='-D warnings' cargo doc --offline --locked --workspace --no-deps`
- `cargo deny --offline check` (cached advisory data; configured duplicate-version warnings only)
- `scripts/check-packages.sh --offline` (both archives and fresh consumers)
- `git diff --check`

Commands used the project-local `target/experiment-cargo-home` cache. Test and
package logs are retained locally as `target/audit-20260919-tests.log` and
`target/audit-20260919-packages.log`. The library source remains uncommitted on
top of `9f1d887`; no version, tag, or publication was changed. Package checks
preceded the final documentation-only edits to this audit and status text.

## Concrete fixes produced by the audit

- Repeated canceled dials now release retained queue entries and accounting.
- Endpoint acceptance filters a connection already known dead before handoff.
- Late dialing and accept observers retain the original endpoint failure cause.
- Sans-I/O application connection close rejects the reserved cleanup-code range
  with a typed error without changing connection state.
- `AutomaticCode` publicly interprets every reserved automatic cleanup code
  while preserving the raw received value.
- A reset ordered before FIN acknowledgement remains `AlreadyReset`; cached
  `ClosedStream` state no longer hides that terminal fact.
- Cancellation handoff wakeups are coalesced outside the reliable cleanup queue,
  preventing wake-only metadata growth under repeated cancellation.

These fixes support the qquicusock requirements already marked **[QREQ]** in
the release plan, particularly cancellation, FIN/reset outcomes, bounded
ordinary queues, and persistent failure reporting. No qquicusock files changed.

## Documentation and workflow discrepancies

- `README.md` formerly said platform and cross-host validation “remain separate
  gates,” and `STATUS.md` described only the alpha.3 API. Both current-facing
  files were corrected during this audit to distinguish the published alpha.3
  from the unreleased shared Endpoint candidate and to record the successful
  `9f1d887` platform/transport runs without claiming release readiness.
- `docs/notes/2026-09-17-local-validation.md` now points to later passing
  evidence, but immediately retains unchecked macOS/FreeBSD/cross-host/hosted
  bullets. They are labeled historical, yet are easy to misread. Preserve the
  historical checklist or add a conspicuous superseded marker; do not silently
  rewrite it as if those checks ran at the earlier checkpoint.
- `RELEASE.md` formerly described exact-pin publication ordering as a one-time
  bootstrap. It was corrected during this audit: candidate archives are first
  validated together, then every new exact-pinned core version must be
  published and indexed before ordinary wrapper package verification.
- The CI package check is stronger than `RELEASE.md` suggests: `.github/workflows/ci.yml:35-37`
  uses `scripts/check-packages.sh`, which patches the wrapper consumer to the
  candidate core archive and checks both fresh consumers. The script uses
  `cargo package --no-verify` internally and validates via later `cargo check`;
  this is suitable candidate-pair evidence, but final release work should still
  run ordinary package verification/list inspection for the selected version.
- The workflow uses mutable action tags (`actions/checkout@v4`,
  `dtolnay/rust-toolchain@stable`, `vmactions/freebsd-vm@v1`) rather than commit
  SHA pins. This is supply-chain hardening, not a blocker created by the 51
  decisions. The plan only requires supported action versions.
- The cross-host harness is untracked and the related evidence/plan updates are
  uncommitted. The note explicitly says candidate source was `9f1d887` plus that
  harness, so the evidence is intelligible, but a future release commit must
  deliberately include or exclude the harness and update the evidence linkage.
- The cross-host note says the remote isolated directory retains source, build
  artifacts/cache, and `server.log`. That is not a repository release blocker,
  but it is residual external test state worth cleaning under the owner's host
  policy when no longer needed.

## Additional hardening still named by the plan

The current suite does not exhaust the validation matrix's duplication cases,
all cancellation positions, combined timer/cancellation/cleanup progress under
continuous multi-connection load, all waiter-cancellation permutations, or
repeated stream/attempt resource-exhaustion cycles with counters returning to
baseline. These are real coverage limits, but the audit found no known contract
failure behind them. Keep them visible as hardening work for the experimental
API.

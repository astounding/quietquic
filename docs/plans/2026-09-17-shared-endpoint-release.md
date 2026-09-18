# QuietQUIC shared-endpoint release plan

Status: candidate `9f1d887` passed local validation, hosted Linux, macOS,
FreeBSD, Rust 1.88, and the cross-host transport run. The subsequent
[release audit](../notes/2026-09-19-release-audit.md) adds fixes and focused
tests; its changed candidate needs fresh hosted and cross-host validation.
See [prior cross-host evidence](../notes/2026-09-18-cross-host-validation.md).
Release/version selection and publication remain separate work.
Date: 2026-09-17.
Provisional target: `0.1.0-alpha.4` for both crates, subject to checking release
state before choosing the final version. This is another experimental release,
not a production-hardening claim.

The contracts below are the agreed implementation requirements. Local results
and remaining coverage limits are recorded in
[the validation note](../notes/2026-09-17-local-validation.md).
See the shared-endpoint guide for the implemented API.
Decision numbers refer to the QuietQUIC decision
sequence, not the earlier qquicusock sequence.

## Purpose and cross-project annotations

Promote the demonstrated shared-socket capability into a supported sans-I/O core
and Tokio API, with explicit endpoint ownership, bounded resource use,
cancellation semantics, and independently managed connections.

Use these annotations throughout this plan:

- **[QREQ]**: a requirement for the agreed qquicusock architecture. It must be
  available and validated before that project can rely on the behavior.
- **[QIMPACT]**: may affect qquicusock integration, configuration, diagnostics,
  or migration. It is not necessarily a prerequisite for its first implementation.
- Unmarked items are library engineering or release requirements without a
  separate cross-project designation.

These labels mention dependencies only; this document does not implement or
duplicate the other project's plan. A library capability can be required while
its exact API spelling is still open.

**[QREQ] Boundary:** QuietQUIC owns UDP endpoints, authenticated connection
admission, packet routing, transport configuration, streams, and transport
cleanup. qquicusock owns named services, peer/service authorization, the relay
control protocol, session opening, application deadlines, reconnection/backoff,
duplicate selection, and coordinated draining/replacement. Shared-endpoint
support does not require connection pooling in qquicusock.

## Starting evidence and constraints

- Base commit: `d9580ba`, currently tagged `v0.1.0-alpha.3`.
- An uncommitted `Endpoint::experimental_connect` method and two real-UDP tests
  demonstrate overlapping outgoing connections and simultaneous initiation on
  fixed local sockets. No admission-filter changes were needed for those cases.
- The Linux workspace run passed 104 tests, including the two experiments.
  Doc-test execution succeeded but discovered zero tests. This does not mean the
  new contracts have been tested or implemented.
- The experiment also checked byte-exact 512 KiB transfers, both FIN
  acknowledgements, sibling survival after close, a wrong-PSK handshake timeout
  alongside healthy traffic, and selected silent-rejection cases.
- Packet loss/reordering, platform coverage, the Tokio endpoint API, and the
  full cancellation/lifecycle contracts remain unproven.
- Keep exploratory work uncommitted by default. An experimental branch and
  commits are permitted when needed for tracking/CI; do not commit experiments
  onto main. Do not overwrite concurrent work when reverting experiments.
- Keep credentials out of source, logs, and CI artifacts. CI uses test PSKs.
  No shared account configuration or tool installation is authorized by this plan.

The existing experiment note remains the record of what was actually exercised.

## Agreed scope

### Endpoint and connection ownership [QREQ]

One endpoint owns exactly one UDP socket and a driver (Tokio) or caller-driven
state machine (sans-I/O). It supports multiple connections on that socket.
Endpoint construction can bind an address or take ownership of an already-bound
socket. Socket I/O has one owner; competing readers are not supported.

Capabilities are fixed at construction: dial, accept, or both. Acceptance
requires configured PSK credentials. Pausing admission cannot add a capability.
Each connection remains bidirectional regardless of who initiated it.

Endpoint and Connection handles are cloneable. SendStream and RecvStream are
uniquely owned, movable halves. Connection attempts and stream handles retain
their endpoint independently of Endpoint handles.

| Event | Required automatic behavior |
| --- | --- |
| Drop one Endpoint/Connection clone | Other owners remain unaffected. |
| Drop the last Endpoint handle | Stop new inbound admission; terminate inbound handshakes and queued unaccepted connections. Application-owned connections, streams, and outgoing attempts continue. |
| Drop an outgoing attempt | Cancel that attempt; clean up any connection not handed to the caller. |
| Drop the last application owner of one connection | Close that connection and perform bounded cleanup; siblings continue. |
| Explicit endpoint `close()` | Terminal endpoint-wide shutdown, overriding all retained handles and attempts. |
| Explicit connection close | Terminate that connection, overriding its stream owners; siblings continue. |
| Unrecoverable socket failure | Fail the endpoint and wake affected operations with a structured cause; no automatic rebind/reconnect. |

The driver must not accidentally count its own internal references as application
ownership. Likewise, retained terminal-observation state must not keep a socket
alive after cleanup is complete.

### Admission and operation queues [QREQ]

Explicit pause/resume controls new inbound connection admission. Paused attempts
are silently rejected before allocating connection state. Previously admitted
handshakes continue and queued connections remain acceptable while Endpoint
handles exist. Outgoing dialing and streams on existing connections continue.

`accept()` drains previously queued arrivals during a pause, then waits across
the pause. It returns a connection, orderly end, or structured failure, shaped
like `Result<Option<Connection>, EndpointError>`. Terminal outcomes persist.
Never deliver a connection already known to be dead; closure immediately after
handoff remains a normal network race.

Bound pending inbound admission, including both handshakes and completed
connections awaiting acceptance. Reserve a slot before allocating state. Reject
new attempts silently when full, preserving existing work. Completed arrivals
have no separate acceptance deadline by default; transport failure/timeout,
peer closure, endpoint closure, or final Endpoint-handle drop releases them.

Peer-initiated stream limits count pending and accepted streams together, using
QUIC stream credit. Receive windows separately bound bytes. A dedicated
stream-admission pause switch is deferred; applications reject unwanted streams
using existing reset/stop operations and their own session protocol.

Ordinary command queues are bounded and apply asynchronous backpressure.
Non-waiting operations report local unavailability. Cleanup/cancellation must
remain deliverable when ordinary queues are full; best-effort `try_send` alone
is insufficient for mandatory drop cleanup.

### Cancellation and stream lifecycle [QREQ]

Define an explicit handoff point for every operation: completion means delivery
to the application, not merely placing a value in a driver reply channel.
Cancellation races must not lose bytes, connections, or streams.

| Operation/event | Contract |
| --- | --- |
| Cancel incremental read | Preserve all unreturned bytes, in order, for subsequent reads. |
| Cancel `read_to_end(limit)` | Preserve accumulated unreturned bytes for subsequent reads, using bounded storage. |
| Cancel `accept()` / `accept_bi()` | Remove the waiter; preserve an unreturned connection/stream for another call. |
| Cancel `open_bi()` | Remove pending allocation or clean up the allocated but unreturned stream in both directions. |
| Cancel a started `write_all()` | Reset the sending direction; already delivered bytes cannot be retracted. |
| Cancel `wait_finished()` | Cancel only the wait; sending/FIN processing continues and later waits see the recorded result. |
| Drop unfinished SendStream | Reset that sending direction with a documented automatic-cancellation code. |
| Drop SendStream after successful `finish()` | Do not undo FIN; transport continues while the connection remains alive. |
| Drop unfinished RecvStream | Send STOP_SENDING and discard unread buffered bytes; the opposite sending direction remains usable. |
| Drop RecvStream after consumed EOF | Release bookkeeping. |

Before a write starts, canceling a queued request should remove it without
performing a write. Specify and test the driver-side boundary at which a write
is considered started and the agreed reset-on-cancellation contract applies.

Automatic last-owner connection closure does not promise to flush abandoned
application data. A caller requiring confirmed transmission must finish and
await acknowledgement before releasing ownership. Retaining a stream can retain
the connection even after all Connection handles have been dropped.

Large `write_all` inputs are submitted incrementally under a bounded
per-connection byte budget; the caller retains its source buffer while awaiting
the operation. Account for both command payload and transport send buffers;
bounded command counts alone are insufficient. Avoid an additional full-input
copy. A single busy operation must not monopolize the shared driver.

`read_to_end` limit overflow stops reception and returns ownership of the
up-to-limit prefix with `ReadLimitExceeded`. Discard excess data with only bounded
lookahead and keep the opposite sending direction usable. Connection failure or
peer reset during collection likewise returns accumulated data with the failure
cause. Neither result is successful EOF. Cancellation differs: the prefix stays
with the stream for later reads rather than being returned as an error.

### Reset and error observation [QREQ; public shape QIMPACT]

Reset is idempotent and returns a structured outcome, conceptually:

- `ResetRequested`: local cancellation accepted, not proof of peer receipt;
- `AlreadyAcknowledged`: acknowledgement was recorded before reset processing;
- `AlreadyReset`: retain the original reset code;
- `PeerStopped { code }`: report the already recorded terminal fact.

Connection loss is a structured failure. If reset wins the driver ordering race,
a later acknowledgement does not rewrite its recorded outcome. None of these
operations retracts bytes already delivered to the peer.

Reserve a small documented code range for automatic QuietQUIC cleanup reasons;
explicit application calls cannot use that range. Preserve raw received codes
and supply interpretation helpers. Exact assignments remain implementation work.

Endpoint termination has a persistent structured cause for all observers,
including late observers. It is distinct from cleanup completion:

- An awaitable termination observation reports that the endpoint stopped
  operating and why.
- Idempotent `close()` initiates terminal shutdown.
- `wait_closed()` confirms bounded cleanup and UDP socket release. It does not
  initiate shutdown; canceling it does not stop shutdown.

### Fairness [QIMPACT; bounded cleanup QREQ]

Multiple concurrent `accept()` and `accept_bi()` calls are supported. Deliver
each item exactly once to the oldest active waiter, ordered at driver receipt.
Cancellation removes the waiter without consuming the item. Termination wakes
all waiters. Do not promise task scheduling order across simultaneous calls.

`open_bi()` waits for capacity; a separate `try_open_bi()` does not wait for
future capacity. Existing opening waiters receive capacity first. The
non-waiting operation cannot bypass them. In the Tokio API it may still await a
bounded driver response. Its result distinguishes temporary unavailability
(local capacity, peer credit, or earlier waiters) from connection/operation
failure. Avoid waiting for ordinary command-queue space in the non-waiting path.

Use bounded work per connection and rotating service order. Timers and cleanup
must progress under load. Public priorities, equal-bandwidth guarantees, and
real-time guarantees are not part of this release.

### Configuration and deadlines [QREQ]

Connections take configuration snapshots. Updated defaults apply to future
incoming/outgoing connections, with optional outgoing-attempt overrides.
Existing connections retain their settings. Expose transport idle timeout,
keepalive, stream credit, and buffer/resource limits deliberately rather than
implicitly inheriting all Quinn defaults.

| Setting | Agreed default / scope |
| --- | --- |
| Outgoing handshake | 10 seconds; endpoint default, per-attempt override; starts when driver initiates handshake. |
| Incoming handshake | 10 seconds; configurable; starts at admission and ends at handshake completion. |
| Handshake deadline values | Finite and positive; reject zero/invalid values. |
| Completed connection acceptance | No separate default deadline; bounded queue and transport lifetime apply. |
| Transport cleanup | Protocol-driven, capped at 10 seconds by default; configurable. |
| Endpoint-wide cleanup | One overall deadline, not a deadline multiplied by connection count. |

Library handshake/cleanup timers are not qquicusock session-open, FIN-wait,
application-drain, heartbeat, or reconnect policies. In particular, the current
30-second transport idle default must not silently undermine that project's
40-second control-failure detection plus recovery policy. Validate/document how
applications configure the relevant transport settings, without embedding their
management protocol in QuietQUIC.

**[QIMPACT]** Bind IP/port changes require another socket; connection-default
updates do not change binding. Multiple local sockets use multiple endpoints.
An explicitly supplied dual-stack socket may support both address families where
available; do not silently broaden the requested binding. Test family validation
and source address behavior, especially for supplied sockets.

## Deferred work and compatibility [QIMPACT]

- No key rotation or live credential-management redesign in this version.
- No connection-pool policy, automatic reconnect, duplicate selection, or
  coordinated application replacement in QuietQUIC.
- No Tokio AsyncRead/AsyncWrite implementation or adapters yet.
- No unidirectional-stream API; keep advertised uni-stream credit at zero.
- No QUIC datagram API expansion.
- No dynamic endpoint capability changes or multi-socket endpoint.
- No application payload inspection, HTTP behavior, or NAT rendezvous service.

Breaking Rust API and behavioral changes are allowed. Preserve convenience
entry points only where they follow the new semantics; do not maintain parallel
legacy ownership/cancellation behavior. The experimental method must not ship as
the final interface. Document migration for reset outcomes, partial-read errors,
endpoint ownership, and stream drop/cancellation behavior.

Rust API changes do not automatically imply a cloaking wire-format change.
Assess wire compatibility explicitly; do not change selectors or cryptography
merely to reorganize endpoint lifetime. Reserved cleanup codes need documentation.

## Decision index

This index preserves all 51 agreements for implementation review.

| Decision | Agreement | Cross-project |
| --- | --- | --- |
| 1 | Application connection/stream owners retain the endpoint. | QREQ |
| 2 | Explicit incoming admission pause/resume; close is terminal. | QREQ |
| 3 | Pause preserves already admitted work. | QREQ |
| 4 | Dropped outgoing attempt cancels itself. | QREQ |
| 5 | Last connection application owner triggers connection cleanup. | QREQ |
| 6 | Unfinished send drop resets; finished send drop preserves FIN. | QREQ |
| 7 | Unfinished receive drop sends STOP_SENDING and releases data. | QREQ |
| 8 | Started write_all cancellation resets sending. | QREQ |
| 9 | Incremental reads are cancellation-safe. | QREQ |
| 10 | Canceled read_to_end preserves its accumulated prefix. | QIMPACT |
| 11 | FIN wait cancellation cancels only the wait; follow-up agreement adds idempotent reset outcomes. | QREQ |
| 12 | Limit overflow returns prefix and stops reception. | QIMPACT |
| 13 | accept cancellation preserves an unreturned connection. | QREQ |
| 14 | accept_bi cancellation preserves an unreturned stream. | QREQ |
| 15 | open_bi cancellation releases pending/unreturned work. | QREQ |
| 16 | Waiting open_bi and non-waiting try_open_bi. | QREQ |
| 17 | Bound pending admissions; reject excess silently. | QREQ |
| 18 | Unrecoverable socket failure terminates the endpoint explicitly. | QREQ |
| 19 | Separate close initiation and wait_closed completion. | QREQ |
| 20 | New connections use configuration snapshots. | QREQ |
| 21 | Outgoing handshake default 10 seconds. | QREQ |
| 22 | Incoming stream credit includes pending and accepted streams. | QREQ |
| 23 | Incoming handshake default 10 seconds. | QREQ |
| 24 | No additional default deadline for completed acceptance queue. | QIMPACT |
| 25 | Last Endpoint drop terminates unowned inbound pending work. | QREQ |
| 26 | An owned outgoing attempt retains the endpoint. | QREQ |
| 27 | Bounded commands; backpressure; reliable cleanup delivery. | QREQ |
| 28 | Bounded rotating driver scheduling; no public priorities yet. | QIMPACT |
| 29 | Defer standard async I/O traits/adapters. | QIMPACT |
| 30 | Defer uni streams and datagram APIs. | QIMPACT |
| 31 | Breaking changes allowed; compatible conveniences may remain. | QIMPACT |
| 32 | Connection admission pause does not stop new streams. | QREQ |
| 33 | accept waits across pause after draining its queue. | QREQ |
| 34 | accept distinguishes connection/orderly end/failure persistently. | QREQ |
| 35 | Reserve automatic-cleanup error codes. | QIMPACT |
| 36 | Clone Endpoint/Connection, not stream halves. | QREQ |
| 37 | Multiple accept waiters; oldest active waiter first. | QIMPACT |
| 38 | Same waiter semantics for accept_bi. | QIMPACT |
| 39 | try_open_bi cannot bypass queued openers. | QIMPACT |
| 40 | Structured temporary-unavailability outcomes. | QREQ |
| 41 | Cleanup capped at 10 seconds, one cap for endpoint shutdown. | QREQ |
| 42 | Bind address or take ownership of a prebound socket. | QREQ |
| 43 | One socket per endpoint. | QREQ |
| 44 | Interrupted read_to_end returns prefix plus failure. | QIMPACT |
| 45 | Incremental large writes with bounded internal byte budgets. | QREQ |
| 46 | Persistent endpoint termination cause, separate from cleanup. | QREQ |
| 47 | One endpoint type with dial/accept capabilities. | QREQ |
| 48 | Capabilities fixed at construction. | QIMPACT |
| 49 | Finite positive handshake deadlines. | QREQ |
| 50 | Linux, macOS, and FreeBSD runtime validation gates. | QREQ |
| 51 | Cross-host transport gate; later real-CGNAT application validation. | QREQ |

## Implementation sequence

### Milestone A — API and state-machine review

Produce concrete Rust signatures, error types, and ownership/transition diagrams
from the contracts above. Inventory current behavior that changes. Resolve
mechanical details without silently changing agreed semantics:

- distinguish application owner counts from driver/observer references;
- define handoff and cancellation linearization for each operation;
- choose bounded queue/window defaults and validate contradictory settings;
- specify reserved codes and partial-read error ownership without making every
  common connection error carry a large buffer;
- reconcile a canceled collector's retained prefix with a later, smaller limit:
  never allocate another full prefix or silently lose bytes on cancellation;
- define which structured terminal fact is reported when independent failures
  race, while preserving the reset/ack ordering agreement;
- define socket transient-error classification, retry scheduling, and fatal
  errors; avoid infinite busy retry loops;
- distinguish per-connection EOF/failure from endpoint termination observation.

Review resulting signatures before replacing the experimental API. Material
semantic conflicts return to the user one decision at a time; ordinary
implementation choices do not reopen all agreements.

### Milestone B — Sans-I/O endpoint and configuration [QREQ]

Separate construction from connecting; consolidate shared configuration setup.
Support repeated dial calls and accept-capable endpoints that also dial. Keep
generation handles, CID attribution/reaping, and independent connection timers.
Add capabilities, admission limits/deadlines, immutable connection snapshots,
and explicit connection operation/terminal outcomes.

Preserve silent admission and replay behavior. Test known-CID handshake routing,
unknown packets, wrong PSKs, replayed Initials, malformed/version-negotiation
cases, and mixed-role endpoint responses. Do not assume the successful prototype
proves stateless-reset or retired-CID behavior. Keep the existing threat boundary
visible and investigate any regression before changing routing policy.

The core remains runtime-free and caller-driven. New timers accept caller time;
do not introduce sockets, threads, runtime dependencies, or additional hidden
clock reads. Preserve the documented event/transmit/timeout driving obligations.

### Milestone C — Tokio shared endpoint [QREQ]

Build one socket-owning driver with per-connection and per-attempt state.
Implement owned-socket construction, asynchronous dial/accept, reliable cleanup
delivery, explicit owner retention, termination observation, pause/resume,
bounded admission, and endpoint shutdown. Preserve sibling progress when one
attempt times out, one connection closes, or a waiter is canceled.

Apply bounded scheduling and command/payload budgets from the start. Reuse shared
stream operation logic across incoming and outgoing connections. Update existing
conveniences to delegate to the same semantics rather than running another
lifetime model.

### Milestone D — Stream semantics and cancellation [QREQ / QIMPACT]

Implement unique stream ownership, automatic reset/stop, durable cleanup requests,
cancel-safe acceptance/reads, abandoned-open cleanup, incremental writes,
reset/FIN terminal state, and bounded partial-read recovery. Include deterministic
tests that cancel before dispatch, after driver completion but before handoff,
and after application completion.

Retain enough state to avoid lost bytes or leaked handles, but release completed
state under repeated short-lived streams. A driver oneshot send succeeding is
not sufficient proof of application receipt: design the ownership transfer to
handle the receiver being canceled immediately afterward.

### Milestone E — Integration, migration, and examples [QIMPACT]

Migrate tests and examples to the finalized API. Provide examples showing:

- simple listen/accept and simple dial conveniences;
- one fixed-port endpoint with two outgoing connections;
- simultaneous incoming/outgoing initiation;
- per-connection failure isolation;
- pause, drain, force-close, and wait for actual socket release;
- FIN wait timeout followed by explicit reset-outcome inspection;
- recovery of partial read data, and canceling/retrying incremental reads.

Explain application responsibilities and ownership with concrete examples. Mark
the old experimental helper obsolete and remove it once supported coverage takes
over. Update the README, current API docs, status, changelog, and applicable specs.
Historical documents remain historical.

## Validation matrix and release gates

### Automated correctness gate [QREQ]

Build deterministic core/driver tests for loss, duplication, reordering, ACK/reset
races, cancellation handoff races, delayed admission, resource exhaustion, and
bounded cleanup. Explicitly verify no starvation, no silent byte loss, exactly
once ownership delivery, and no leakage under repeated create/cancel/close cycles.

Real-socket tests must prove fixed-port overlap, simultaneous initiation,
bidirectional stream operation, sibling isolation, last-owner behavior, outgoing
attempt timeout/cancellation, queued inbound cleanup, and rebinding after
`wait_closed()`. Validate supplied sockets and IPv4/IPv6 cases where supported.

All required checks must pass on Linux, macOS, and FreeBSD. Compilation alone
does not satisfy platform validation. GitHub-hosted macOS runtime tests count;
privately owned Mac hardware is not required. Record runner OS/architecture and
the tested source commit.

### GitHub CI preparation

The existing workflow runs on pushes and pull requests and includes Ubuntu,
macOS, and a FreeBSD VM. Repository Actions are enabled with the required actions
allowed and read-only workflow token permissions. SSH authentication and read
access have been confirmed; no experimental branch has yet been pushed or new
hosted-run result established by this planning work.

Prepare an experimental branch when ready for CI, keeping main unaffected.
Before pushing, format/review the changes and ensure the branch contains only
intended project work. Use read-only workflow permissions, deterministic test
credentials, bounded job timeouts, independent platform results (no early matrix
cancellation hiding other platforms), and bounded dependency/build caches.
Cancel superseded branch runs and avoid duplicate push/PR runs where practical.
Keep uploaded diagnostics small and retained for a limited period.

Prefer pinned supported macOS images for release evidence, covering Apple
Silicon and Intel where practical. Platform labels/action versions are execution
details to verify when configuring CI. FreeBSD uses the existing VM approach.
Use focused experiment jobs first to establish timings and platform support.

During development, packaging checks must not accidentally validate an old
registry copy of the core. The wrapper pins the new core's exact version, so
unpublished paired-crate validation needs an explicit local package/consumer
strategy; keep that separate from eventual registry verification.

### Cross-host gate and later CGNAT validation [QREQ]

Before release readiness, run transport tests across actual hosts: overlapping
connections from a fixed local port, transfer in both directions, and closing
one connection while its sibling continues. Check plain UDP reachability and
server startup before attributing network timeouts to the protocol.

After automated tests pass, the owner will arrange a public-IP Linux or FreeBSD
VM on demand. This workspace's CGNAT path to that VM can also satisfy the
cross-host transport gate if the required scenarios are run there. The owner
provided that host and the required run passed on 2026-09-18; results and scope
are recorded in the cross-host evidence note linked above.

Persistent reverse-tunnel idle behavior, management heartbeat detection,
reconnection, and named-service recovery through real CGNAT remain qquicusock
readiness tests. They are not additional QuietQUIC application features.

### Release checklist

Follow `RELEASE.md`, including:

1. Confirm final version, repository metadata, Rust support floor, and matching
   manifests/exact wrapper core pin; inspect lockfile changes deliberately.
2. Run formatting, workspace all-target/doc tests, Clippy with warnings denied,
   rustdoc with warnings denied, dependency/license/source checks, and the polling
   example. Verify the core still has no Tokio dependency.
3. Review ownership/cancellation/admission changes and the bounded-memory model;
   document known limitations rather than implying a cryptographic review.
4. Complete the platform and cross-host gates and archive concise results tied
   to the actual candidate commit. Test the advertised minimum Rust version.
5. Inspect package contents and validate both archives in fresh consumers, using
   the candidate core rather than the previously published version.
6. Prepare migration notes and release bookkeeping. Publication/tagging are
   later concrete actions, not authorized merely by creating this plan.
7. At release, publish the core before the wrapper, verify the registry-resolved
   wrapper against the published core, and confirm docs.rs and package links.

## Completion criteria

The release is ready only when the supported API replaces the experimental
helper, all agreed contracts have implementation/tests, all required release
gates pass, migration documentation is accurate, and no known unresolved issue
invalidates the endpoint/stream ownership or silence guarantees. Prototype
success alone does not satisfy readiness.

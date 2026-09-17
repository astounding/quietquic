# Shared endpoint feasibility experiment — 2026-09-16

This is an uncommitted experiment for the proposed qquicusock redesign, not a
finalized QuietQUIC API or a release change. Key rotation and general connection
pooling are out of scope. No qquicusock source changes were made.

## Question

Can two connections between the same local/remote UDP address pair coexist,
including when both endpoints initiate, while preserving the current admission
filter and allowing one connection to end without interrupting the other?

## Prototype

`Endpoint::experimental_connect` adds an outgoing connection to an existing
sans-I/O endpoint. It duplicates the existing client configuration/selector
setup deliberately, to keep the stable constructors and packet admission code
unchanged. It creates fresh selector material per attempt, assigns a generation
handle, attributes issued CIDs immediately, and updates the endpoint timer.

The test harness owns exactly one nonblocking UDP socket per endpoint, bound to
an explicit local IP/port. If no test port is supplied, it first reserves a port,
then binds that selected port explicitly. The socket is never replaced or
rebound during the experiment. No socket reuse options are used.

The test IP defaults to 127.0.0.1. `QUIETQUIC_TEST_ADDR` can override it.
`QUIETQUIC_TEST_PORT_BASE` selects ports at offsets 3100/3101 and 3110/3111.
The default reserve/rebind has a small test-only port-allocation race; explicit
reserved test ports avoid that race in controlled environments.

## Results so far

Both tests in `proto/tests/experimental_shared_endpoint.rs` passed on Linux
6.18.52-0-virt using Rust 1.96.1 and the existing lockfile (quinn-proto 0.11.15).
The targeted run took approximately 30.5 seconds, primarily waiting for the
wrong-PSK attempt's genuine transport timeout.

The full `cargo test --offline --locked --workspace --all-targets` run also
passed: 104 tests, including the two experimental tests. Workspace doc-tests
completed successfully (zero tests discovered). `git diff --check` passed.

- A client endpoint dialed two connections to one server from the same socket.
- Both connections carried distinct payloads; a 512 KiB transfer was checked
  byte-for-byte. Each completed stream had both halves finished and both FINs
  acknowledged before releasing its bookkeeping.
- Closing the old connection reaped its state on both endpoints. The surviving
  connection then carried a stream initiated by the remote endpoint.
- A further wrong-PSK attempt timed out with `ConnectionError::TimedOut` while
  the healthy connection repeatedly transferred data. The server did not admit
  the unauthorized connection, and the healthy connection still worked after
  the timeout.
- Two server-capable endpoints both dialed before either processed received
  traffic. Both incoming and outgoing handshakes completed on each socket.
  Incoming connections retained the expected PSK-derived identity.
- Either connection could carry remotely initiated streams; closing one left
  the other usable, including for a 512 KiB transfer.
- After legitimate traffic settled, direct injections into the core of junk,
  unknown short-header packets, and captured Initial replays returned Dropped
  and queued zero transmits. Replay checks included a connection already closed
  while its sibling remained alive. These rejection checks feed the core
  directly; handshake/data/FIN/close traffic travels over real UDP sockets.

An initial harness failure exhausted stream credit because the harness closed
only one half of repeatedly created bidirectional streams. The harness was
corrected to finish and acknowledge both halves; no protocol workaround was
needed. This is distinct from a session legitimately remaining half-open.

## Implications, still provisional

The current CID routing is sufficient for the tested mixed-initiation cases
without relaxing the server pre-filter. This narrows the earlier concern that
mixed initiation necessarily requires a routing-policy rewrite. It does not
establish that all endpoint-level response/reset cases are correct.

A long-lived endpoint owning one socket is a viable candidate. The existing
Tokio client still binds a fresh socket and owns one connection per driver;
it does not expose this experimental capability. Production support would need
endpoint-level lifetime, per-attempt completion/cancellation, inbound acceptance,
and per-connection commands without terminating siblings.

Under this model a port chosen from a dial-only range naturally belongs to the
endpoint lifetime, including reconnects. This behavior remains a design proposal.

The locked transport defaults to a 30-second idle timeout. The proposed
qquicusock heartbeat/recovery timing must be reconciled with configurable
transport idle timeouts; a hard transport failure can otherwise precede the
management recovery deadline.

## Not established by these experiments

- macOS, FreeBSD, real CGNAT, or cross-host operation;
- deliberate packet loss/reordering during overlapping handshakes;
- mixed-role stateless-reset handling, CID retirement, or exhaustive malformed
  packet/replay testing;
- simultaneous full-duplex bulk transfers, scheduling fairness, overload, or
  repeated replacement stress;
- the Tokio shared-endpoint API, daemon shutdown, qquicusock control protocol,
  duplicate selection, service opening, or coordinated replacement;
- all configuration settings being replaceable on a shared endpoint: bind
  IP/port changes inherently require another socket, and endpoint-wide settings
  must be distinguished from connection-specific settings.

## Reproduction

Dependencies were fetched into the ignored project-local directory
`target/experiment-cargo-home`; no shared Cargo configuration was changed.

```sh
CARGO_HOME="$PWD/target/experiment-cargo-home" \
  cargo test --offline --locked -p quietquic-proto \
  --test experimental_shared_endpoint -- --nocapture
```

The execution sandbox denies UDP socket creation, so socket tests require the
approved execution context. The first sandbox attempt failed at socket creation,
before exercising protocol behavior.

`cargo fmt` is not installed in this environment. No shared tools were installed.

## Reversibility

The experiment consists of the added `experimental_connect` method, the new
test file, and this note. No branches, commits, manifest changes, lockfile
changes, or key-management changes were made. Remove only those experimental
additions to back out, preserving any subsequent/concurrent work.

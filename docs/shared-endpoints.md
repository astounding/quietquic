# Shared endpoints

The Tokio `Endpoint` API owns one UDP socket and drives every connection that
uses it. Choose it when an application needs several outgoing connections from
one fixed local port, incoming and outgoing connections on the same socket, or
explicit control over admission and shutdown.

This guide describes the implemented Rust API. The dated release plan records
the broader contract and release requirements; unchecked platform and
cross-host gates are not implied by the examples here.

## Construct an endpoint

`EndpointConfig` fixes an endpoint's capabilities at construction:

```rust,no_run
use quietquic::config::ClientEntry;
use quietquic::endpoint::{Endpoint, EndpointConfig};
# async fn example(authorized: Vec<ClientEntry>) -> Result<(), Box<dyn std::error::Error>> {
let dial_only = Endpoint::bind("0.0.0.0:0".parse()?, EndpointConfig::dial()).await?;
let accept_only = Endpoint::bind("0.0.0.0:0".parse()?, EndpointConfig::accept(authorized.clone())).await?;
let both = Endpoint::bind("0.0.0.0:0".parse()?, EndpointConfig::both(authorized)).await?;
# drop((dial_only, accept_only, both));
# Ok(())
# }
```

Accept and combined endpoints require at least one `ClientEntry`. Dial-only
endpoints do not. Capabilities cannot be added later; admission pause and resume
only affect an endpoint that was constructed with accept capability.

To transfer an already-bound socket, pass ownership of a nonblocking-capable
standard UDP socket. `from_socket` sets nonblocking mode and must run inside a
Tokio runtime:

```rust,no_run
use quietquic::endpoint::{Endpoint, EndpointConfig};
use std::net::UdpSocket;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let socket = UdpSocket::bind("127.0.0.1:0")?;
let endpoint = Endpoint::from_socket(socket, EndpointConfig::dial())?;
println!("bound to {}", endpoint.local_addr());
# drop(endpoint);
# Ok(())
# }
```

The endpoint takes exclusive ownership. Do not retain another reader for the
same socket. One endpoint always represents one socket; use multiple endpoints
for multiple local addresses. Supplying an IPv6 or dual-stack socket preserves
the socket's configured address-family policy.

The main resource defaults live on `EndpointConfig`: transport configuration,
pending incoming capacity, outgoing and incoming handshake timeouts, cleanup
timeout, and the per-connection work budget. Handshake and cleanup timeouts and
the capacity/work limits must be positive. Endpoint-managed deadlines are
capped at 24 hours. `set_transport_defaults` changes the snapshot used for
future connections; established connections keep their existing transport
settings.

The default transport snapshot uses a 60-second idle timeout, a 20-second
keepalive interval, 32 peer-initiated bidirectional streams, a 1 MiB per-stream
receive window, and 8 MiB connection receive and send windows. Unidirectional
stream credit is always forced to zero, including for custom
`TransportSettings`, because this release does not expose unidirectional
streams. Build custom settings from an owned Quinn `TransportConfig`; the
result is immutable and cheaply cloneable for connection snapshots.

Each connection permits at most 256 outstanding `open_bi` operations and 256
outstanding `accept_bi` operations. These limits are shared by all clones of
that connection. Waiting operations apply backpressure when their limit is
full, and all waiters wake with the terminal result if the connection closes.
Queued write commands have a separate 256 KiB per-connection byte budget;
large `write_all` calls submit bounded chunks rather than copying the entire
input into the driver queue.

Transient socket errors such as `WouldBlock` and `Interrupted` schedule a
10-millisecond retry. That retry is tracked independently of QUIC transport
timers and endpoint control work, so a temporarily blocked socket does not
erase a protocol deadline or strand cleanup.

## Dial and accept

`connect` returns a cancellation-owned `Connecting` future. Dropping that
future cancels the attempt and cleans up a connection that has not been handed
to the caller. `connect_with` accepts per-attempt `ConnectOptions` for a
transport snapshot and handshake timeout override.

```rust,no_run
use quietquic::config::ClientConfigFile;
use quietquic::endpoint::{ConnectOptions, Endpoint};
use std::time::Duration;

# async fn example(endpoint: &Endpoint, client: ClientConfigFile) -> Result<(), Box<dyn std::error::Error>> {
let conn = endpoint
    .connect_with(
        client,
        ConnectOptions {
            handshake_timeout: Some(Duration::from_secs(5)),
            ..ConnectOptions::default()
        },
    )
    .await?;
# drop(conn);
# Ok(())
# }
```

Repeated calls dial through the same socket and preserve its local port. The
`bind` field in `ClientConfigFile`, when present, must match the endpoint's
socket address. It no longer asks each connection to create another socket.

`accept().await` returns `Result<Option<Connection>, EndpointError>`:

- `Ok(Some(connection))` is one authenticated completed connection.
- `Ok(None)` is orderly terminal end.
- `Err(error)` is a structured endpoint failure.

Canceled accept calls leave an unreturned connection available to another
caller. Multiple accept calls may wait concurrently. A connection already
known to be dead is not handed out, though it can of course close immediately
after a successful handoff as an ordinary network race.

```rust,no_run
use quietquic::endpoint::Endpoint;

# async fn serve(endpoint: Endpoint) -> Result<(), Box<dyn std::error::Error>> {
while let Some(conn) = endpoint.accept().await? {
    tokio::spawn(async move {
        let (_send, mut recv) = conn.accept_bi().await?;
        let message = recv.read_to_end(1024 * 1024).await?;
        println!("received {} bytes", message.len());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
}
# Ok(())
# }
```

Connection and stream operations are identical for incoming and outgoing
connections. `Endpoint` and `Connection` are cloneable. `SendStream` and
`RecvStream` are uniquely owned movable halves.

## Pause admission

`pause_admission()` silently rejects new incoming attempts before connection
state is allocated. It does not stop outgoing dialing or streams on established
connections. Handshakes admitted before the pause continue, and connections
already queued remain acceptable. Once that queue is drained, `accept()` waits
across the pause.

```rust,no_run
# use quietquic::endpoint::Endpoint;
# fn example(endpoint: &Endpoint) -> Result<(), quietquic::endpoint::EndpointError> {
endpoint.pause_admission()?;
// Drain or operate existing work here.
endpoint.resume_admission()?;
# Ok(())
# }
```

Pause is reversible. `close` is terminal. Calling pause or resume on a dial-only
endpoint returns `EndpointError::CapabilityDisabled`.

## Streams, completion, and drops

Open and accept bidirectional streams through `Connection::open_bi` and
`Connection::accept_bi`. `try_open_bi` does not wait for future stream capacity,
though it may await a bounded driver response. It returns either an opened pair
or `TryOpenOutcome::TemporarilyUnavailable` and does not bypass an older waiter
for stream capacity.

Keep both stream halves alive until their work is complete. Dropping an
unfinished `SendStream` resets its direction with QuietQUIC's reserved cleanup
code. Dropping an unfinished `RecvStream` sends STOP_SENDING and discards unread
buffered data. A successfully finished send half preserves its FIN.

```rust,no_run
# use quietquic::conn::{ConnError, Connection};
# async fn send(conn: &Connection) -> Result<(), ConnError> {
let (mut send, recv) = conn.open_bi().await?;
send.write_all(b"request").await?;
send.finish_and_wait().await?;
drop((send, recv));
# Ok(())
# }
```

`finish()` means the local transport accepted FIN. `wait_finished()` waits for
the peer transport to acknowledge the bytes and FIN. Canceling that wait does
not cancel the send; a later wait observes the recorded result. Applications
that require confirmed transport delivery should finish and wait before
releasing their final owners.

`read_to_end(limit)` returns a `ReadToEndError` containing both `prefix` and a
structured `error` when the limit is exceeded, the peer resets the stream, or
the connection fails. Canceling the future differs: unreturned collected bytes
remain associated with the stream for a later read.

## Ownership and shutdown

Connections, outgoing attempts, and stream halves retain the driver and socket
independently of public `Endpoint` handles. Dropping one endpoint or connection
clone does not affect its siblings. Dropping the final Endpoint handle stops
new inbound admission and cleans pending inbound work, while application-owned
connections, streams, and outgoing attempts continue.

Dropping the last application owner of one connection initiates bounded cleanup
for that connection. It does not close sibling connections. A retained stream
half counts as an application owner even after every `Connection` handle has
been dropped.

Explicit endpoint shutdown overrides retained handles:

```rust,no_run
# use quietquic::endpoint::Endpoint;
# use std::time::Duration;
# async fn shutdown(endpoint: &Endpoint) -> Result<(), Box<dyn std::error::Error>> {
endpoint.close(0, b"service shutdown")?;
let cause = endpoint.terminated().await;
println!("endpoint terminated: {cause:?}");
tokio::time::timeout(Duration::from_secs(15), endpoint.wait_closed()).await?;
# Ok(())
# }
```

`close` is idempotent and starts terminal endpoint-wide shutdown. `terminated`
reports the persistent cause as soon as operation stops. `wait_closed` only
observes cleanup; it does not initiate shutdown and returns after the bounded
cleanup completes and the UDP socket is released. An unrecoverable socket error
produces `EndpointTermination::Failed` and does not automatically rebind or
reconnect.

## Migrating from `Server` and `Client`

The convenience `Server` and `Client` APIs delegate to the same shared endpoint
driver and remain supported for one-listener and one-connection use. Move to
`Endpoint` when the socket itself is the long-lived application object:

| Previous shape | Shared-endpoint shape |
| --- | --- |
| `Server::bind(secrets)` | Convert `secrets.clients` to `EndpointConfig::accept` or `both`, then `Endpoint::bind(secrets.listen, config)` |
| `server.accept().await` | `endpoint.accept().await?`, which explicitly distinguishes orderly end from endpoint failure |
| `Client::connect(client_config)` | Construct one `EndpointConfig::dial()` endpoint, then call `endpoint.connect(client_config)` repeatedly |
| One client socket per connect call | One endpoint socket shared by all of its connections |
| Dropping the server/client owner | Final Endpoint-handle drop has the ownership behavior described above |
| No endpoint-wide completion barrier | `close`, `terminated`, and `wait_closed` separate shutdown initiation, cause observation, and socket release |

The stream API remains centered on `Connection`, `SendStream`, and
`RecvStream`, but drop behavior is now part of the public cancellation contract.
Do not leave an unused half to fall out of scope early if the peer still needs
that direction.

## Validation status

There are three distinct evidence levels:

1. Local validation runs deterministic sans-I/O tests and real-loopback Tokio
   tests, plus workspace tests, docs, formatter, Clippy, dependency policy, and
   packaging checks. The working checklist is
   [`notes/2026-09-17-local-validation.md`](notes/2026-09-17-local-validation.md).
2. GitHub validation runs the exact candidate commit on Linux, macOS, and the
   FreeBSD VM workflow. A local Linux pass does not establish those runtime
   gates.
3. The cross-host gate uses two actual hosts for overlapping connections from
   one fixed local port, bidirectional transfer, and sibling survival after one
   close. Later CGNAT relay/application behavior belongs to the consuming
   application rather than this library API.

Do not infer that a code example, compilation, or a prototype test completes
any of these gates. Results must identify the tested commit, OS, and
architecture as required by the release plan.

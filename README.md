# quietquic

**DISCLAIMER:** This was 100% AI coded, prompted by the human "creator" but
planned and designed by multiple AI models including those from Anthropic,
Google, and OpenAI. Trust at your own peril.

> **Repository status:** see [STATUS.md](STATUS.md). Historical
> decisions and corrections are summarized in [HISTORY.md](HISTORY.md).

A cloaked QUIC transport library, in Rust: a server built on `quietquic` is
**invisible to network scanners** — it emits zero bytes in response to any
packet that doesn't prove possession of a valid pre-shared key (PSK) — and its
traffic **camouflages as ordinary QUIC v1/HTTP3**, blending into the large
volume of legitimate internet QUIC. Authentication and per-client key
*selection* are folded into the QUIC `Initial` packet itself, so unauthorized
traffic is rejected before any connection state is allocated and before any
byte is sent back.

`quietquic` is a standalone transport library. It contains no application
logic; it exposes raw authenticated bidirectional byte streams for a caller
(e.g. a backup tool, a config-push agent, anything that wants a QUIC pipe that
doesn't advertise itself to the internet) to frame however it likes.

> **Release status:** `0.1.0-alpha.3` is an experimental preview. The protocol
> has extensive automated tests but has not yet received an independent
> cryptographic review. Do not treat it as production-hardened.

This checkout documents the **unreleased shared-endpoint API**. Candidate
`9f1d887` passed the recorded Linux, macOS, FreeBSD, Rust 1.88, and cross-host
transport runs. The [release audit](docs/notes/2026-09-19-release-audit.md)
subsequently added fixes that pass local validation. Fresh hosted and
cross-host testing of those changes, plus release bookkeeping, remain.

## Architecture: two crates, pick your I/O model

| Crate | What it is | Use it when |
|---|---|---|
| **`quietquic-proto`** (`proto/`) | The **sans-IO core**. No I/O, no async runtime, no threads, and it never blocks or reads the clock — you own the socket and pass `now` in. | You have your own event loop, or you're embedding via FFI |
| **`quietquic`** (repo root) | A thin **tokio** wrapper over the core: owns a UDP socket, runs a driver task, exposes `Endpoint`, `Connection`, and uniquely owned stream halves, with `Server`/`Client` conveniences. | Your application is already `async`/`.await` |

This mirrors `quinn-proto`/`quinn`, and it exists because the two I/O models are
mutually exclusive: tokio owns the thread and parks in `epoll`/`kqueue` when
idle, so it cannot be embedded in a hand-rolled loop that polls with a zero
timeout and has other work to do between passes. Layering serves both instead of
compromising either.

`proto/examples/poll_loop.rs` is the reference for the sans-IO path — a single
thread, non-blocking sockets, nothing that parks — and it is run in CI so it
cannot rot.

Socket integration tests default to loopback. On hosts where loopback UDP is
filtered or where tests must use an allowlisted interface/port range, set
`QUIETQUIC_TEST_ADDR` to the local IP to bind/dial and optionally
`QUIETQUIC_TEST_PORT_BASE` to the first deterministic test port. Without a port
base, tests bind port `0` and let the OS choose ephemeral ports.

**The silence invariant is stronger in the core.** *A datagram that fails the
cloaking pre-filter queues nothing to send.* `handle_datagram` returns before
such a packet ever reaches quinn-proto, so there is nothing for `poll_transmit`
to hand back: an embedder driving the state machine by hand **cannot** reply to
an unauthorized peer, even by mistake. Invisibility is a property of the API
rather than of the caller's control flow.

Full design rationale lives in
[`docs/specs/2026-07-03-quietquic-design.md`](docs/specs/2026-07-03-quietquic-design.md).
See the [`docs` map](docs/README.md) for the distinction between current
normative documentation and archived implementation plans.
This README's Threat Model section below reproduces the spec's threat model
verbatim in substance — read it before deploying.

---

## Quickstart

### 1. Generate a PSK

The PSK is 32 random bytes, hex-encoded (64 hex characters), shared out of
band between server and client — the same trust model as an SSH
`authorized_keys` entry or a WireGuard PSK.

```sh
openssl rand -hex 32
```

### 2. Server config (`server.toml`)

The server holds the listen address and the full set of authorized clients
(`client_id -> psk`). IDs and PSKs must both be unique. The server derives the
authenticated `client_id` from the PSK that admitted the connection. `chmod
600` the file — it is cleartext secret material on disk.

```toml
listen = "0.0.0.0:443"

[[clients]]
client_id = "backup-host-1"
psk = "3f9a1c...<64 hex chars>...b2"

[[clients]]
client_id = "backup-host-2"
psk = "7e02dd...<64 hex chars>...11"
```

### 3. Client config (`client.toml`)

Each client holds a local descriptive label, its PSK, and the server it dials.
The client-side `client_id` is not sent on the wire; the server identifies the
peer from its unique configured PSK entry.

```toml
client_id = "backup-host-1"
psk = "3f9a1c...<64 hex chars>...b2"
server = "203.0.113.7:443"
```

### 4. Bind an accept-capable endpoint

```rust
use quietquic::config::{FileSource, SecretSource};
use quietquic::endpoint::{Endpoint, EndpointConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secrets = FileSource::new("server.toml").load()?;
    let endpoint = Endpoint::bind(
        secrets.listen,
        EndpointConfig::accept(secrets.clients),
    ).await?;
    println!("listening on {}", endpoint.local_addr());

    while let Some(conn) = endpoint.accept().await? {
        tokio::spawn(async move {
            // Only ever fires for a peer that proved PSK possession and
            // completed the QUIC handshake — unauthorized peers never reach
            // this point at all.
            println!("authenticated client: {:?}", conn.client_id());
            let (_send, mut recv) = conn.accept_bi().await.expect("accept stream");
            let msg = recv.read_to_end(1024 * 1024).await.expect("read");
            println!("got {} bytes from {}", msg.len(), conn.remote_address());
        });
    }
    Ok(())
}
```

### 5. Dial and open a stream

```rust
use quietquic::config::ClientConfigFile;
use quietquic::endpoint::{Endpoint, EndpointConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string("client.toml")?;
    let cfg: ClientConfigFile = toml::from_str(&text)?;

    let endpoint = Endpoint::bind("0.0.0.0:0".parse()?, EndpointConfig::dial()).await?;
    let conn = endpoint.connect(cfg).await?;
    let (mut send, recv) = conn.open_bi().await?;
    send.write_all(b"hello over a cloaked pipe").await?;
    send.finish_and_wait().await?;
    drop(recv);
    Ok(())
}
```

The primary Tokio surface is `Endpoint::bind` / `from_socket` / `connect` /
`accept`, `Connection::open_bi` / `accept_bi` / `close(code, reason)` / `closed()`,
`SendStream::write_all` / `finish` / `wait_finished` / `finish_and_wait` /
`reset`, and `RecvStream::read` / `read_to_end(limit)` / `stop`.
`Connection::client_id` identifies an accepted peer on the server. Framing on
top of these raw byte streams is left to the caller. `Server` and `Client`
remain convenience wrappers and delegate to the same endpoint driver. See the
[shared endpoint guide](docs/shared-endpoints.md) for fixed-port multi-dial,
combined dial/accept, admission pause, ownership, and shutdown behavior.

For full-duplex or untrusted-size traffic, keep both halves and read
incrementally:

```rust,no_run
# async fn relay(conn: quietquic::conn::Connection) -> Result<(), quietquic::conn::ConnError> {
let (mut send, mut recv) = conn.open_bi().await?;
let reader = async move {
    loop {
        let chunk = recv.read(16 * 1024).await?;
        if chunk.is_empty() {
            break;
        }
        println!("received {} bytes", chunk.len());
    }
    Ok::<_, quietquic::conn::ConnError>(())
};
let writer = async move {
    send.write_all(b"request").await?;
    send.finish_and_wait().await
};
tokio::try_join!(reader, writer)?;
# Ok(())
# }
```

`finish()` queues FIN and returns once the local transport accepts it.
`wait_finished()` is the completion barrier: it resolves when the peer's
transport acknowledges all stream data and FIN, or returns `ConnError::Stopped`
if the peer sent STOP_SENDING. This is a transport receipt, not proof that the
peer application read or processed the bytes. Wrap it in
`tokio::time::timeout` when a policy deadline matters.

`Connection::closed()` returns a structured `ConnectionError`. Application close
codes and reason bytes are preserved. For transport closes/errors,
`frame_type` is currently `None` with quinn-proto 0.11: quinn-proto carries the
frame type internally but does not expose its raw numeric value publicly.
QuietQUIC deliberately avoids unsafe private-layout extraction, debug-string
parsing, or a patched quinn-proto dependency; the `Option<u64>` field remains in
the API for a future upstream accessor if one appears. This release does not
include, require, or commit to an upstream quinn-proto PR for that accessor.

`Connection::quinn_connection()` returns a cloneable `QuinnHandle` for opening
and accepting uniquely owned bidirectional stream halves through the shared
driver. Reads, writes, and FIN observation use those halves. HTTP/3 integration
remains future work because unidirectional streams are not exposed yet. See
[Limitations](#limitations--not-yet-production-hardened) below.

---

## Threat Model

Stated plainly here so no operator over-trusts this library. (Reproduced from
the design spec, §3.)

### What quietquic defeats

- **Internet-wide active scanning** (nmap, zmap, masscan) and search engines
  (Shodan, Censys): the server never responds to an unauthorized probe, so it
  appears as a closed/filtered UDP port. UDP has no kernel-level handshake to
  refuse, so the silence is complete — no RST, no distinguishable timeout, no
  TLS alert.
- **Netflow / flow-log classification** and **casual DPI**: traffic looks like
  a normal QUIC connection to some IP on UDP/443.

### What quietquic does NOT defeat

- **Global passive traffic analysis.** Backups (or any bulk transfer) are
  large and periodic; the *volume and timing pattern* of traffic to a fixed
  endpoint leaks even when service identity and content do not. No
  packet-level trick hides this.
- **Sophisticated DPI that actively attempts `Initial` decryption on every
  flow.** In stock QUIC, the `Initial` packet is protected with keys derived
  from a *published* version salt plus the on-wire Destination Connection ID
  (DCID), so any observer can unseal it and read the TLS ClientHello. Because
  quietquic **re-keys the `Initial` with the PSK**, such an observer's unseal
  attempt produces garbage — revealing "QUIC whose Initial won't decrypt," an
  anomaly resembling an unknown/broken QUIC variant. This is the irreducible
  cost of folding authentication into the `Initial`; it cannot be avoided
  without surrendering auth secrecy. It is a non-issue against scanning and
  casual detection, and no packet-level design defeats an adversary already
  decrypting every QUIC `Initial` on the link.
- **Resource side-channel via a co-located service.** Even a silent server
  consumes CPU/memory to process-then-drop junk packets. An attacker flooding
  the host and watching an *unrelated* co-located service's latency can infer
  that something is doing per-packet crypto work. Mitigations (bounded,
  allocation-free reject path; cheap MAC compares before any expensive AEAD
  unseal; per-source and global rate limits) bound and flatten this channel
  but cannot close it.

  **Recommended deployment: run quietquic as the only service on its host.**

---

## Limitations / Not Yet Production-Hardened

These are known, deliberate boundaries of the current implementation —
surfaced here so users know exactly where the edges are, rather than
discovering them under load.

1. **Single-task endpoint driver.** Each endpoint runs one sans-IO driver task
   that owns its socket and pumps all of its connections. This is a throughput
   chokepoint: fine for a
   handful of concurrent clients (the expected backup-transport use case), but
   not tuned for high fan-out (hundreds+ of simultaneous connections on one
   server).
2. **`RecvStream::read_to_end(limit)` buffers up to the caller's explicit limit.**
   It returns `ConnError::ReadLimitExceeded` if the peer sends more. Interactive
   applications should still use `RecvStream::read(max)` and process data
   incrementally.
3. **Rate-limit parameters are compile-time constants**, not yet configurable
   via TOML. The per-source and global buckets that bound the unauthenticated
   pre-filter's CPU cost (see Threat Model, resource side-channel) cannot
   currently be tuned per deployment without a code change.
4. **Mid-connection retired connection IDs are pruned only when the
   connection closes.** quinn-proto 0.11 exposes no hook for observing
   per-CID retirement mid-connection (only whole-connection loss), so the
   server's CID→connection routing set grows with every CID a live connection
   is issued and only shrinks when that connection ends. Memory use is
   bounded by `live connections × CIDs issued per connection`, which is small
   in practice, but it is not the tightest possible bound.

   There is also a byte-emission (not just memory) facet: because a
   retired-but-not-yet-pruned CID stays in the active routing set until the
   connection closes, an **on-path** adversary who has already captured a live
   connection's CIDs could send a packet to a retired CID and elicit a
   stateless reset from the server. This is strictly within the documented
   on-path threat boundary — it requires having observed the connection's live
   CIDs in the first place, so it is out of scope for the scanner / off-path
   model this library defends against — but it is worth calling out explicitly
   because it is a case where the server *does* emit bytes.

5. **Handshake loss-recovery leans on the server's PTO, not the client's
   Initial retransmits.** For a given connection attempt the client fixes its
   selector DCID (and therefore the nonce/freshness embedded in it), so every
   Initial the client sends during that attempt carries the *same* (nonce,
   freshness) pair. The server's replay guard records that pair on first sight
   and drops every subsequent datagram carrying it — which includes the
   client's own Initial *retransmits*. Handshakes still converge (they rely on
   the server's PTO-driven retransmissions of its own handshake flight rather
   than on the client re-driving its Initial), but robustness under heavy
   early-flight packet loss is reduced relative to stock QUIC.

   The naive "fix" — making the selector DCID an *active* routing target after
   admission so same-flight retransmits pass through — is **unsafe** and is
   deliberately not implemented: it would let a captured Initial be replayed to
   elicit a response, breaking the silence contract. A future refinement could
   allow same-source in-progress retransmits without weakening replay
   protection, but that requires careful design and is out of scope for v1.

## Non-goals (v1)

- **No HTTP/3.** Standard QUIC stays on the wire specifically so an
  off-the-shelf HTTP/3 stack can be layered on top later via
  `Connection::quinn_connection()` without touching the cloaking layer. This is
  a supported *future direction*, not a drop-in today: the `QuinnHandle`
  currently exposes only bidirectional-stream commands, whereas h3 additionally
  requires unidirectional streams (for its control stream and QPACK
  encoder/decoder). Adding those uni-stream commands to the handle is the
  remaining work; it stays confined to the handle and does not disturb the
  silence-critical routing.
- **No certificate-based authentication.** PSK only; the TLS certificate is a
  throwaway self-signed identity and is not part of the trust model.

---

## License

0BSD (BSD Zero Clause). See [`LICENSE`](LICENSE).

# Cross-host transport validation

Date: 2026-09-18, approximately 13:16–13:18 UTC. Result: **PASS**.

## Candidate and environment

Both hosts used the library source and lockfile from commit
`9f1d887b9e3fd5f39078dcd69c885a9ff6072cf1` on
`experiment/shared-endpoint-ci-20260917`. The only executable-source addition
was the manual `examples/cross_host.rs` validation harness. No library code was
changed for this run.

- Harness SHA-256 on both hosts:
  `3057b1873fdcc062e976188e6cabbce194a4ad424c29706ee014a1f7623e1ea8`.
- Cargo.lock SHA-256 on both hosts:
  `8c93fddc131cde03db58bb993869393cee3c9ad4bb2585bac2addd040c967e6c`.
- Local: Alpine Linux x86_64, kernel `6.18.52-0-virt`, Rust/Cargo 1.96.1.
  Outbound route used `192.168.10.51` through `192.168.10.1`.
- Remote: Alpine Linux x86_64, kernel `6.18.52-0-lts`, Rust/Cargo 1.96.1.
  Public interface address `74.121.191.34` (`sazed.tambler.com`).
- The owner identifies the local network as behind CGNAT. The remote observed
  both QuietQUIC connections as `103.105.213.238:45444`; this directly verifies
  address translation and the shared observed source endpoint, not the number
  of NAT layers.

The remote build used an isolated directory and project-local Cargo cache:
`/home/agentsam/quietquic-cross-host-20260918.JJKpPN`.
The temporary random PSK was transferred over SSH, never printed, and removed
from both hosts after the tests. No firewall, service, or shared account
configuration was changed. The test processes exited; no listener remains.

## Procedure and results

The harness passed loopback validation and Clippy with warnings denied before
the network run. A plain UDP request/response probe passed before starting
QuietQUIC. Server startup was explicitly confirmed before dialing.

1. The public server bound `0.0.0.0:45443`.
2. The local client bound `0.0.0.0:45444` once and initiated two overlapping
   authenticated connections to `74.121.191.34:45443`.
3. Each connection carried a client-initiated bidirectional stream with
   **1 MiB in each direction**, verified byte for byte.
4. On each same connection, the public host initiated another bidirectional
   stream with **512 KiB in each direction**, verified byte for byte. This
   exercises reverse stream initiation over CGNAT-originated connections;
   it does not require a new inbound UDP connection through CGNAT.
5. The client explicitly closed one connection with application code 51.
6. After the server observed that close, the sibling connection carried
   **2 MiB in each direction**, again verified byte for byte.
7. Every payload send used `finish_and_wait()`. Application completion markers
   ensured deliberate connection shutdown did not race the other host's FIN
   wait. The markers themselves did not require FIN acknowledgement.
8. Both endpoints shut down, `wait_closed()` returned, and immediate rebinding
   to the respective UDP port succeeded.

Total checked application payload: **5 MiB each direction, 10 MiB combined**,
across five bidirectional payload streams. Both processes exited with status 0
and printed `PASS cross_host`. A select race between `closed()` and a failing
pending `accept_bi()` was corrected in the harness during loopback development;
either notification of the expected application close is accepted.

Local logs: `target/cross-host-20260918/{client,server,loopback-client,loopback-server}.log`.
The remote directory retains source, build artifacts/cache, and `server.log`
for follow-up work, but no PSK. Logs contain addresses and test results, not keys.

## Reproduction

Generate a fresh 32-byte random PSK as a private, 64-hex-character file on each
host. Build with `cargo build --locked --example cross_host`. Substitute the
chosen addresses/ports in these commands:

```sh
# Public host; bounded one-response reachability check
target/debug/examples/cross_host probe-server 0.0.0.0:45443
# CGNAT-side host
target/debug/examples/cross_host probe-client 0.0.0.0:45444 74.121.191.34:45443

# Public host; start and verify READY before starting the client
target/debug/examples/cross_host server 0.0.0.0:45443 /path/to/psk.hex
# CGNAT-side host
target/debug/examples/cross_host client 0.0.0.0:45444 74.121.191.34:45443 /path/to/psk.hex
```

Each process has a 180-second overall timeout; transport steps use a 45-second
deadline. This is a bounded manual validation program, not a deployed service.

## Gate status and limits

This satisfies the planned QuietQUIC cross-host transport gate: fixed-port
overlapping connections, traffic in both directions, and sibling survival.
The same library candidate also passed the Linux, macOS, FreeBSD, and Rust 1.88
jobs in [GitHub run 35180662812](https://github.com/astounding/quietquic/actions/runs/35180662812).

This run does not establish long-idle CGNAT retention, reconnection, network
migration, or qquicusock/nginx service behavior. Those remain separate
application integration tests. It does not add direct public-to-CGNAT
connection initiation or general NAT traversal.

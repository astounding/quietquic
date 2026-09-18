# Project status

QuietQUIC is an implemented Rust transport with two crates:

- `quietquic-proto`, the sans-I/O state machine in `proto/`;
- `quietquic`, the Tokio socket-owning wrapper at the repository root.

The cloaking, replay, rate-limit, connection lifecycle, stream, fuzz, and
cross-host test work described in [HISTORY.md](HISTORY.md) has been completed.
Known limitations and the threat boundary are documented in [README.md](README.md).

## Release status

Version `0.1.0-alpha.3` is the current prerelease under the QuietQUIC crate
names; `0.1.0-alpha.2` was prepared in git but is being skipped for
publication. It remains an experimental preview, not a production-hardening
claim. Public connection handles are generation-safe, accepted server
connections expose their PSK-derived `client_id`, stream handles use
quinn-shaped send/receive halves, and `read_to_end` requires an explicit memory
bound.

The current checkout also contains an unreleased shared-endpoint API. Candidate
`9f1d887` passed recorded Linux, macOS, FreeBSD, Rust 1.88, and cross-host
transport validation. The [release audit](docs/notes/2026-09-19-release-audit.md)
added fixes that pass local validation; the changed candidate needs fresh hosted
and cross-host results before release. The final next version has not been
selected.

The Ruby binding, backup application, and `squicusock` relay are maintained as
independent projects.

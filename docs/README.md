# Documentation map

- [`specs/wire-protocol.md`](specs/wire-protocol.md) is the normative wire
  protocol for the current experimental release.
- The root [`README.md`](../README.md) is the current user guide, API overview,
  threat model, and limitations list.
- [`shared-endpoints.md`](shared-endpoints.md) is the current Tokio endpoint API,
  ownership, cancellation, shutdown, and migration guide.
- [`notes/2026-09-18-cross-host-validation.md`](notes/2026-09-18-cross-host-validation.md)
  records the CGNAT-to-public-host transport run and the exact tested candidate.
- [`notes/2026-09-19-release-audit.md`](notes/2026-09-19-release-audit.md)
  maps the 51 agreed decisions to evidence and records audit fixes and remaining
  release gates.
- [`../SECURITY.md`](../SECURITY.md) states security status and reporting policy.
- [`plans/2026-09-17-shared-endpoint-release.md`](plans/2026-09-17-shared-endpoint-release.md)
  is the active release-validation plan, consolidating endpoint/stream contracts
  and flagging qquicusock requirements and potential impacts. The current Rust
  API is documented in `shared-endpoints.md`; unchecked plan gates remain work.
- Other files under `plans/` are archived implementation plans. They intentionally
  preserve intermediate APIs, `todo!()` examples, and expected-failure notes;
  they are not current usage documentation.
- Dated files under `specs/` and `notes/` are historical design records unless
  the current wire-protocol document explicitly incorporates them.

When historical text conflicts with current API documentation or code, the
current Rust API and normative wire-protocol document take precedence.

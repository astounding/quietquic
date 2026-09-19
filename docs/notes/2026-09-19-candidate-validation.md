# Post-audit candidate validation

Candidate: `53064c6536671b0d718e2ac53c773ec777cbd785`, pushed to
`experiment/shared-endpoint-ci-20260917` on 2026-09-19. This commit includes
the audited fixes, regression tests, and cross-host harness. No release tag or
version change was made.

## GitHub results

[Run 35392059732](https://github.com/astounding/quietquic/actions/runs/35392059732)
completed with these results:

| Job | Result |
|---|---|
| Linux (`ubuntu-latest`) | Passed |
| Rust 1.88 | Passed |
| FreeBSD VM | Passed |
| macOS (`macos-latest`) | Failed during `cargo test --locked --workspace --all-targets` |

The owner supplied the detailed macOS log after unauthenticated log downloads
returned 403. It identifies
`reset_ordered_before_fin_ack_remains_the_stable_terminal_fact`: the test
asserted that the peer had queued a transmit immediately after receiving FIN.
QUIC may delay ACKs, so this was an invalid test timing assumption. The repair
drives the peer's actual protocol deadlines until its ACK counter advances,
while withholding peer packets until the sender's reset has been applied.
The existing assertions still require the original reset code to survive the
late ACK. No production library source was changed for this repair.

A separate workflow warning reports that the unquoted comma in
`with: { components: clippy, rustfmt }` treats `rustfmt` as an unexpected input.
This was corrected to `components: "clippy, rustfmt"`; formatting itself
passed in this run, so this warning did not explain the test failure.

## Cross-host status

Blocked before authentication: SSH to `sazed.tambler.com:1122` returned
`Connection refused` on both attempts. No remote files or processes were
changed. The owner has been asked to restore the endpoint or provide its new
address/port. No cross-host success is claimed for this candidate.

The exact committed source archive and local harness build are ready:

- Local directory: `target/cross-host-20260919/`.
- `candidate.tar` SHA-256:
  `6337656fb67f381550377f77f6bf5a238b36a467c783e552bbdec7ac13ad08d3`.
- Cargo.lock SHA-256:
  `8c93fddc131cde03db58bb993869393cee3c9ad4bb2585bac2addd040c967e6c`.
- Harness SHA-256:
  `3057b1873fdcc062e976188e6cabbce194a4ad424c29706ee014a1f7623e1ea8`.

The prior successful cross-host run remains evidence for `9f1d887` only.
Release gates remain open pending macOS diagnosis and cross-host execution.

# Local package validation

Date: 2026-09-17

The release-candidate source was packaged without publishing or changing crate
versions. Validation used the project-owned dependency cache and offline mode:

```text
CARGO_HOME=/home/ai/quietquic/target/experiment-cargo-home scripts/check-packages.sh
```

The default remains offline. Hosted CI uses `scripts/check-packages.sh --online`
so a fresh runner can resolve package metadata and fetch consumer dependencies.
The core archive is built first, then the wrapper archive and both consumers.

Results:

- `quietquic-proto` and `quietquic` both packaged successfully.
- Both archive file lists were inspected. The core archive contains the renamed
  supported `tests/shared_endpoint.rs`; neither archive contains temporary
  consumer or build output.
- A fresh consumer compiled against the extracted `quietquic-proto` archive.
- A second fresh consumer compiled against the extracted `quietquic` archive
  with `[patch.crates-io] quietquic-proto` pointing at the extracted candidate
  core archive.
- `cargo tree -i quietquic-proto` confirmed the wrapper resolved the extracted
  candidate core path rather than the published `0.1.0-alpha.3` crate.

The reproducible script creates an isolated temporary directory below `target/`,
copies the source into an isolated staging workspace, uses separate target
directories for both consumers, and removes the temporary directory when it
exits. The repository `Cargo.lock` checksum was unchanged across the run.

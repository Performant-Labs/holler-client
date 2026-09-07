# Releasing

How to cut a release of **this repo** (holler-client). The shared mechanics — how a release is
verified, tag format, CHANGELOG conventions, who cuts one and when — are documented once, in
[holler-server's `docs/releasing.md`](https://github.com/Performant-Labs/holler-server/blob/main/docs/releasing.md),
since each repo releases independently ([ADR 0014](https://github.com/Performant-Labs/holler-server/blob/main/docs/adr/ADR-0014.md))
but the process is identical. This page only covers what's specific to holler-client. See
[issue #54](https://github.com/Performant-Labs/holler-client/issues/54) for the decision record.

## What's specific to this repo

- **Version source of truth**: this repo's own `Cargo.toml` `[package] version` — independent of
  holler-server's version, never synchronized with it.
- **Binary name is `holler`, not `holler-client`** (the crate is `holler-client`; the compiled
  binary is deliberately named `holler` to avoid an install collision with holler-server's own
  `holler-server` binary — see the binary-naming decision this session settled). `--version`/`-V`
  reads `CARGO_PKG_VERSION` the same way, via clap's `#[command(version)]` (`main.rs`).
- **CI job to confirm green before tagging**: same two required checks as holler-server,
  `test (ubuntu-latest)` / `test (macos-latest)`, from [this repo's own `ci.yml`](https://github.com/Performant-Labs/holler-client/actions/workflows/ci.yml) — a separate CI run
  from holler-server's, since they're independent repos.
- **Target platforms** (this repo's own table — a different Windows reason than holler-server's):

  | Platform | CI-tested | Released | Status |
  | --- | --- | --- | --- |
  | `ubuntu-latest` | Yes | Yes | Full support |
  | `macos-latest` | Yes | Yes | Full support |
  | Windows | No | No | Does not currently compile — `src/instance_lock.rs` uses Unix-only APIs. Tracked in [#60](https://github.com/Performant-Labs/holler-client/issues/60) |

- **Release binary**: `cargo build --release` produces `target/release/holler` here (not
  `holler-client`) — that's the file to attach to the GitHub Release, and the file to download
  and run `--version` against as the published-artifact verification step (see holler-server's
  doc's final step) — `./holler --version`, not `./holler-client --version`.
- **Known issues to check before release notes**: `gh issue list --repo Performant-Labs/holler-client
  --label bug --state open` — this repo's own set, distinct from holler-server's (e.g. holler-client#60,
  no Windows support). The policy itself (known issues aren't blockers for a beta, but must be
  checked and named in the notes) is holler-server's `docs/releasing.md` "Known issues" section —
  identical here, not repeated.

Everything else — version bump rules, tag format (`vX.Y.Z`, signed/annotated), `CHANGELOG.md`
conventions, the two-tier "tag+CHANGELOG always, GitHub Release with binaries as a confirmed
extra" shape, and the step-by-step sequence — is exactly as documented in holler-server's page
linked above. For the actual checklist to run through each time, see this repo's pinned
release-checklist issue — [#119](https://github.com/Performant-Labs/holler-client/issues/119) for
the first release; a fresh one gets filed per release going forward.

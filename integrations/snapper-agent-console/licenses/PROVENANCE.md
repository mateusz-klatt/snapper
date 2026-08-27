# License text provenance

Every vendored license text in this directory (all files except this one and
`THIRD-PARTY-INVENTORY.md`, which are authored locally) was fetched once from
a pinned, immutable upstream ref, its SHA-256 recorded below, and committed to
this repository so the image build is deterministic (no network fetch, no
synthetic fallback). To re-verify: `sha256sum <file>` against this table, and
the source URL against the pinned tag or commit. The table is additionally
pinned by `tests/docker/test_agent_console_entrypoint.py`, which recomputes
every digest against the committed file.

## Version evidence, per component (not uniform)

| Component | Version | Evidence |
|---|---|---|
| ripgrep | 15.2.0 | **binary-reported**: `codex-path/rg --version` → `ripgrep 15.2.0 (rev e89fff89ac)` |
| zsh | 5.9.0.3-test | **binary-reported**: `codex-resources/zsh/bin/zsh --version` → `zsh 5.9.0.3-test (x86_64-pc-linux-gnu)` |
| bubblewrap | 0.11.2 | **source-inferred**, NOT binary-reported: `codex-resources/bwrap --version` prints only `bubblewrap built for Codex`; 0.11.2 comes from `meson.build` (`version : '0.11.2'`) in the vendored tree at `openai/codex` @ `rust-v0.150.0` `/codex-rs/vendor/bubblewrap/` |
| libcap | 2.75 | **build-script-pinned**, NOT binary-reported: the shipped `bwrap` carries no libcap version string (`grep -a "libcap-2.75"` → 0 hits), but its statically linked libcap code IS present (libcap `_cap_names[]` table and `cap_mode_name()` literals `NOPRIV`/`PURE1E_INIT`/`PURE1E`/`UNCERTAIN`/`HYBRID`/`UNKNOWN` from `libcap/cap_text.c` appear verbatim in the binary); 2.75 comes from `libcap_version="2.75"` + sha256 pin in `openai/codex` @ `rust-v0.150.0` `/.github/scripts/install-musl-build-tools.sh`, which governs this artifact because the image's `bwrap` is byte-identical (`01fb705f…9935d8`) to the one in the official `codex-package-x86_64-unknown-linux-musl.tar.gz` |
| ratatui | 0.30.2 | **lockfile-derived**: `codex-rs/Cargo.lock` @ `rust-v0.150.0` |

## License texts

| File | Source (pinned) | SHA-256 |
|---|---|---|
| `codex-cli-LICENSE` | `openai/codex` @ `rust-v0.150.0` `/LICENSE` (Apache-2.0) | `d17f227e4df5da1600391338865ce0f3055211760a36688f816941d58232d8dc` |
| `codex-cli-NOTICE` | `openai/codex` @ `rust-v0.150.0` `/NOTICE` (tag peels to commit `3b3b4f8fb3f6403e72c2d0533ed0d2f309c59717`); attributes OpenAI Codex and Ratatui | `9d71575ecfd9a843fc1677b0efb08053c6ba9fd686a0de1a6f5382fd3c220915` |
| `ripgrep-15.2.0-LICENSE-MIT` | `BurntSushi/ripgrep` @ `15.2.0` `/LICENSE-MIT` | `0f96a83840e146e43c0ec96a22ec1f392e0680e6c1226e6f3ba87e0740af850f` |
| `ripgrep-15.2.0-UNLICENSE` | `BurntSushi/ripgrep` @ `15.2.0` `/UNLICENSE` | `7e12e5df4bae12cb21581ba157ced20e1986a0508dd10d0e8a4ab9a4cf94e85c` |
| `ripgrep-15.2.0-COPYING` | `BurntSushi/ripgrep` @ `15.2.0` `/COPYING` (dual-license statement) | `01c266bced4a434da0051174d6bee16a4c82cf634e2679b6155d40d75012390f` |
| `bubblewrap-0.11.2-COPYING` | `openai/codex` @ `rust-v0.150.0` `/codex-rs/vendor/bubblewrap/COPYING` (LGPL-2.0-or-later); byte-exact upstream text, including its original trailing whitespace (a `.gitattributes` entry exempts THIS ONE FILE from whitespace linting rather than editing licensed text) | `b7993225104d90ddd8024fd838faf300bea5e83d91203eab98e29512acebd69c` |
| `libcap-2.75-License` | `libcap` @ tag `libcap-2.75` `/License` — dual `BSD-3-Clause OR GPL-2.0-only` in one upstream file, both texts inside. Double-pinned: bit-identical to `libcap-2.75/License` inside `https://mirrors.edge.kernel.org/pub/linux/libs/security/linux-privs/libcap2/libcap-2.75.tar.xz`, whose own digest `de4e7e064c9ba451d5234dd46e897d7c71c96a9ebf9a0c445bc04f4742d83632` matches the pin in `openai/codex` @ `rust-v0.150.0` `/.github/scripts/install-musl-build-tools.sh` (verified: same digest via `https://git.kernel.org/pub/scm/libs/libcap/libcap.git/plain/License?h=libcap-2.75`) | `68467e731f4744bd6e0bb69e8df9c3a994e09cd6b203d0c41327ac6d079c581d` |
| `zsh-5.9.0.3-test-LICENCE` | `zsh-users/zsh` @ commit `77045ef899e53b9598bebc5a41db93a548a40ca6` `/LICENCE` — the exact source commit the codex zsh asset is built from (see below); bit-identical to the same file at tag `zsh-5.9.0.3-test` (verified: same digest) | `d06fdf3ef9b1ec69d6b9e170b0a9516fbad3523261ff1668bde3bfea6e0ef5f5` |
| `ratatui-0.30.2-LICENSE` | `ratatui/ratatui` @ `ratatui-v0.30.2` `/LICENSE` (MIT) | `50eb43e8d742c9c61a9391e42b2184fce54dbd1893a1bb1c85b8c9ee217ab1f5` |
| `kimi-cli-LICENSE` | `MoonshotAI/kimi-cli` @ commit `c4f2102a51448f5041caa445c8a804d97279debe` `/LICENSE` (Apache-2.0) — the licence of the **legacy Python `kimi-cli` project**, which is NOT the artifact this image ships; kept as the closest available upstream text, NOT as a grant for the shipped binary (see caveat) | `58d1e17ffe5109a7ae296caafcadfdbe6a7d176f0bc4ab01e12a689b0499d8bd` |

## The zsh helper is a PATCHED codex asset, not a stock zsh build

The shipped `codex-resources/zsh/bin/zsh` is built by OpenAI's release
workflow `.github/workflows/rust-release-zsh.yml` (@ `rust-v0.150.0`), which
pins:

- upstream source: `zsh-users/zsh` commit
  `77045ef899e53b9598bebc5a41db93a548a40ca6` (`ZSH_COMMIT`),
- patch: `/codex-rs/shell-escalation/patches/zsh-exec-wrapper.patch`
  (@ `rust-v0.150.0`, verified present).

The vendored LICENCE text above is taken from exactly that source commit; the
patch adds an exec wrapper and does not carry its own license terms. Anyone
rebuilding or auditing the patched binary should start from that
commit + patch + workflow triple, not from a release tag of stock zsh.

## Recorded absences and caveats

- **codex NOTICE — a CORRECTED claim.** Earlier revisions of this file (and
  the v9 Dockerfile's fetch fallback) recorded the NOTICE as absent at
  HTTP 404. That was WRONG: the file exists at the pinned tag and returns 200
  (242 bytes, digest above, attributing OpenAI Codex and Ratatui). The
  mistaken absence was carried forward across revisions without re-measuring;
  it was caught in exact review, re-verified against both the tag and the
  commit it peels to, and the NOTICE is now vendored and digest-pinned like
  every other text. Nothing here is synthesized.
- **bubblewrap (LGPL) source / relink material**: the binary is OpenAI's own
  build of the tree vendored at
  `https://github.com/openai/codex/tree/rust-v0.150.0/codex-rs/vendor/bubblewrap`.
  That pinned tree is the corresponding source.
- **kimi — TWO corrections, and the second reversed the first.** The history is
  worth keeping in full, because both errors were about the same claim.
  (a) v10-v12 stated the shipped 0.38.0 binary had "no matching public tag".
  The probes behind that tried three guessed spellings (`0.38.0`, `v0.38.0`,
  `kimi-code-0.38.0`) against an unpaginated listing; exhaustive enumeration
  (146 tags over 5 pages) does find a tag named `0.38`, so the claim as
  written was false.
  (b) v13 then mapped that tag to our binary — and THAT was also wrong, for a
  deeper reason: `MoonshotAI/kimi-cli` @ `0.38` is the **legacy Python**
  project (`pyproject.toml`: name `kimi-cli`, `requires-python >=3.13`, module
  `kimi_cli`), whereas this image downloads **Kimi Code CLI** from
  `code.kimi.ai`, a separate product that upstream documents as a migration
  away from the Python tool. Matching version numbers do not make them the
  same artifact.
  **Measured on the shipped binary** (182 MB ELF at `/usr/local/bin/kimi`):
  9587 occurrences of `node`, 11 of `NODE_MODULE`, 83 of `v8::`, 248 of
  `kimi-code`; and **zero** of `Py_Initialize`, `PyInstaller` and `_MEIPASS`.
  It is a Node.js bundle, not a PyInstaller build of the Python project.
  **Therefore: no licence has been located for the exact artifact this image
  ships, and its redistribution grant is UNCONFIRMED.** The vendored text is
  the legacy project's Apache-2.0 licence, retained as the closest upstream
  material and explicitly NOT as a grant for the binary. The image stays
  private regardless (see README "Never publish this image").
  Lesson, distinct from the one in (a): finding *a* plausible source is not
  provenance. The question is never "does a matching version exist somewhere"
  but "is THIS artifact the output of THAT source" — and here the binary
  itself answered it.
- **node LICENSE** is copied in the Dockerfile from the `node:26-slim` image
  itself (the artifact carries its own text), not from this directory.

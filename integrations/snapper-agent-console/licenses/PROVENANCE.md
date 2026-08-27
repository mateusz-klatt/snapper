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
| Kimi Code CLI | 0.38.0 | **binary-reported AND source-matched**: the shipped 182 062 272-byte ELF at `/usr/local/bin/kimi` hashes to the `cli-manifest.toml` pin `7f18b701…a914b00` and contains the literal `0.38.0` (1 hit) and `kimi-code` (248 hits); upstream `apps/kimi-code/package.json` at the matching tag declares `"name": "@moonshot-ai/kimi-code"`, `"version": "0.38.0"`, `"license": "MIT"` |
| Node runtime embedded in `kimi` | 24.15.0 | **binary-reported AND build-pinned**: `v24.15.0` occurs 5 times in that same shipped ELF, and upstream pins it independently — `.nvmrc` contains exactly `24.15.0` and `.github/workflows/_native-build.yml` selects the toolchain with `node-version-file: .nvmrc`. The binary is a copy of that Node executable: `scripts/native/03-inject.mjs` does `copyFile(process.execPath, out)` and then `postject`-injects `NODE_SEA_BLOB` (both markers present in the shipped file) |
| pi-tui (`@moonshot-ai/pi-tui`) | 0.84.4 | **manifest-derived**: `packages/pi-tui/package.json` at the pinned kimi-code commit. Present in the artifact as bundled JS (`pi-tui`, 71 hits); on linux-x64 it ships **no** native helper — `scripts/native/native-deps.mjs` maps `linux-x64` to an empty `nativeFileRelatives` list |
| clipboard napi module | 0.3.9 | **manifest-derived AND binary-confirmed**: `apps/kimi-code/package.json` depends on `@mariozechner/clipboard@^0.3.9`, `native-deps.mjs` collects the per-target subpackage `@mariozechner/clipboard-linux-x64-gnu` as a `native-files` asset, and that package name appears 12 times in the shipped ELF |

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
| `kimi-code-0.38.0-LICENSE` | `MoonshotAI/kimi-code` @ tag `@moonshot-ai/kimi-code@0.38.0` — an **annotated tag object** `488fe6bb311959227c8c2602e12486e48f8b5446` that peels to commit `0999454bdcb5ddd98f39bffee434dcf0a810f394` — `/LICENSE` (MIT, "Copyright (c) 2026 Moonshot AI"). Fetched twice, once through the URL-encoded tag (`%40moonshot-ai%2Fkimi-code%400.38.0`) and once through the peeled commit sha: both HTTP 200, same digest. This is the grant for the **Kimi Code CLI source**, and the repository is the product this image actually ships | `23cc68e17992e0b512ae2e80afc5787d7d8e0fbfbdb4fff54ec0245508fa400e` |
| `node-24.15.0-LICENSE` | `nodejs/node` @ tag `v24.15.0` (annotated tag `a20a24415694b80361d661d6ecc1ea0e260d9c32`, peels to commit `848430679556aed0bd073f2bc263331ad84fa119`) `/LICENSE` — the full composite Node text, 2946 lines / 156 926 bytes, covering Node itself plus every library Node embeds (OpenSSL, V8, ICU, zlib, llhttp, …). Double-pinned: `cmp` exits 0 against `node-v24.15.0-linux-x64/LICENSE` extracted from the official `https://nodejs.org/dist/v24.15.0/node-v24.15.0-linux-x64.tar.xz`, whose downloaded sha256 `472655581fb851559730c48763e0c9d3bc25975c59d518003fc0849d3e4ba0f6` was checked against the entry in `https://nodejs.org/dist/v24.15.0/SHASUMS256.txt` (`sha256sum -c` → OK). Vendored because the shipped `kimi` ELF embeds this runtime but **not** its licence text — see the measurement in the caveat | `4573185d56580da2b890ba34a85a409257640f1c5632eade4300137266194d18` |
| `pi-tui-0.84.4-LICENSE` | `MoonshotAI/kimi-code` @ the same peeled commit `0999454bdcb5ddd98f39bffee434dcf0a810f394` `/packages/pi-tui/LICENSE` (MIT, "Copyright (c) 2025 Mario Zechner") — the workspace TUI package whose JS is bundled into the SEA main bundle. Byte-exact, including the absence of a trailing newline upstream | `0457f5bcec3b3b211605dfb5d1a49042fd638f3686a410fe099c24a25af13c48` |
| `opentui-7da92b40-LICENSE` | `sst/opentui` (the GitHub API now redirects that name to `anomalyco/opentui`; both forms resolve to the same repository) @ commit `7da92b4088aebfe27b9f691c04163a48821e49fd` `/LICENSE` (MIT, "Copyright (c) 2025 opentui"). That exact commit is the one `packages/pi-tui/src/keys.ts` cites by URL; `packages/pi-tui/src/stdin-buffer.ts` separately states its code is "Based on code from OpenTUI … MIT License - Copyright (c) 2025 opentui". Upstream kimi-code ships **no** OpenTUI licence file of its own — the attribution exists only as those two source-header comments — so the text is taken from OpenTUI's own repository. Byte-exact, no trailing newline upstream | `d9c397a5dc5ac77a2a08fc967ea06a65317a4b5b8de48f6f732ffcb21737e3d2` |

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
- **kimi — a claim corrected three times, and now a split verdict.** The
  code grant is confirmed MIT; full redistribution closure for the shipped
  bundle is not. Because the history of the three wrong versions is the
  instructive part, it has its own section below rather than a bullet here:
  see "The kimi claim" and "Hard UNKNOWNs — the kimi bundle".
- **node LICENSE** is copied in the Dockerfile from the `node:26-slim` image
  itself (the artifact carries its own text), not from this directory. That
  copy covers the image's own Node 26 interpreter and **not** the Node
  24.15.0 runtime embedded inside the `kimi` SEA binary, which is why
  `node-24.15.0-LICENSE` is vendored here separately.

## The kimi claim — three wrong versions, and what is actually established

One sentence about this artifact has now been corrected three times. The
history is kept in full because each version was wrong in a *different* way,
and the shape of each error is more instructive than the answer it got wrong.

*(a) v10-v12 — "the shipped 0.38.0 binary has no matching public tag."*
FALSE. The probes behind it tried three guessed spellings (`0.38.0`,
`v0.38.0`, `kimi-code-0.38.0`) against an unpaginated tag listing. That is
guessing, not searching: a listing truncated at page 1 and a spelling that
happens to be wrong produce exactly the same empty result as a genuine
absence.

*(b) v13 — "`MoonshotAI/kimi-cli` @ `0.38` is its source."* FALSE, and
worse than (a), because it looked like a fix. That repository is the
**legacy Python** project (`pyproject.toml`: name `kimi-cli`,
`requires-python >=3.13`, module `kimi_cli`), while this image downloads
**Kimi Code CLI** from `code.kimi.ai`. Measured on the shipped binary:
9587 occurrences of `node`, 11 of `NODE_MODULE`, 83 of `v8::`, 248 of
`kimi-code`, and **zero** of `Py_Initialize`, `PyInstaller`, `_MEIPASS`
(re-measured on the current image, still zero, against a nonzero `ELF`
control on the same probe). A version number that matches is not
provenance.

*(c) v14 — "no licence has been located for the exact artifact."* ALSO
FALSE, and it is the error this revision repairs. (b) had correctly shown
that the *legacy Python repo* was the wrong source; the conclusion drawn
from it silently widened to *no source exists*, which nothing had been
measured to support. The right next step after (b) was to search for a repo
named after the **product** — and `MoonshotAI/kimi-code` exists, is public,
is **MIT**, and its description is literally "Kimi Code CLI — The Starting
Point for Next-Gen Agents". Exhaustive paginated enumeration of its tags
(68 tags, `--paginate`, all pages) returns exactly one tag mentioning 0.38,
and it is npm-scoped: `@moonshot-ai/kimi-code@0.38.0`. That spelling is
precisely what three hand-guessed forms could never have hit — the lesson of
(a) recurring one level up.

**Current state — two different questions, two different answers.**

1. **The code grant is CONFIRMED.** `MoonshotAI/kimi-code` @
    `@moonshot-ai/kimi-code@0.38.0` is MIT at the repository root, and
    `apps/kimi-code/package.json` at that same commit reads `"name":
    "@moonshot-ai/kimi-code"`, `"version": "0.38.0"`, `"license": "MIT"`.
    The text is vendored above as `kimi-code-0.38.0-LICENSE`.
2. **Full redistribution closure for the SHIPPED BINARY remains UNKNOWN.**
    This is a claim about the *bundled contents*, not about the Kimi Code
    source, and the two must not be collapsed again. The official artifact
    is a **single file** — 182 062 272 bytes, no LICENSE, no NOTICE, no
    SBOM beside it — because the upstream build emits no inventory:
    `scripts/native/03-inject.mjs` copies `process.execPath` and
    `postject`-injects a SEA blob, and `scripts/native/02-sea-blob.mjs`
    packs the JS bundle, the per-target native assets and the whole
    `dist-web` tree into that blob. Nothing in that pipeline produces a
    third-party manifest. An exhaustive walk of the upstream repository tree
    at the pinned commit (4889 blobs, `truncated=false`) finds exactly three
    licence files anywhere in it — `/LICENSE`, `/apps/vscode/LICENSE`,
    `/packages/pi-tui/LICENSE` — and no NOTICE and no third-party inventory
    of any kind.

**Minimum known vendor floor** (all four now vendored above, each measured
as actually present in the artifact rather than merely declared in the
source): the Kimi Code MIT grant; the complete composite Node **24.15.0**
LICENSE; the pi-tui MIT text; and OpenTUI's MIT text.

The Node layer deserves an explicit measurement, because it is the one that
would be easiest to wave away. The shipped ELF contains `v24.15.0` five
times, and `.nvmrc` at the pinned upstream commit contains exactly
`24.15.0` — the artifact and the source agree independently. But the ELF
contains **zero** occurrences of `Node.js is licensed for use as follows`
and **zero** of `The externally maintained libraries used by Node.js are`,
while the same probe on the same file returned 106 hits for `MIT License`,
208 for `Unicode, Inc.` and 68 for `OpenSSL`. The apparatus demonstrably
finds licence text in this binary when it is there; the Node composite
licence is simply not there. A full Node runtime is therefore redistributed
with no copy of its own licence, which is exactly why
`node-24.15.0-LICENSE` is vendored. Note that the image's existing
`/usr/local/share/licenses/node/LICENSE`, copied from `node:26-slim`, does
**not** cover this: that is Node 26's text, and the SEA embeds 24.15.0.
`Mario Zechner` likewise returns zero hits, so pi-tui's copyright notice is
not in the artifact either.

**The legacy Apache-2.0 text has been REMOVED from this directory.** v13/v14
kept `kimi-cli-LICENSE` as "the closest available upstream material". That
rationale died the moment the real grant was located, and this directory is
copied wholesale into `/usr/local/share/licenses/vendored`, where a file's
presence is an affirmative statement that the image contains the component
it licenses. The image contains no part of the legacy Python `kimi-cli`
(measured: zero Python markers), so shipping its licence was a false
statement of the same family as the three above. The history is preserved
here, in prose, which is where history belongs; the licence directory is a
claim about contents. For the record, the removed file was
`MoonshotAI/kimi-cli` @ commit `c4f2102a51448f5041caa445c8a804d97279debe`
`/LICENSE`, sha256
`58d1e17ffe5109a7ae296caafcadfdbe6a7d176f0bc4ab01e12a689b0499d8bd` — which
is the stock Apache-2.0 boilerplate, byte-identical to
`apps/vscode/LICENSE` in the kimi-code repo.

The image stays private regardless (see README "Never publish this image"),
and that conclusion is unchanged by this revision.

## Hard UNKNOWNs — the kimi bundle

These are not "not yet done" items dressed up as unknowns. Each one is named
together with the specific reason it cannot be closed from public evidence
today, so that a future reader can tell which ones more work would close and
which ones only upstream can. Anything not on this list is either closed in
the section above or was never in scope.

1. **Transitive Rust crate licence closure inside the clipboard napi
    module** (`@mariozechner/clipboard-linux-x64-gnu@0.3.9`, 12 hits in the
    shipped ELF). What IS known is more than v14 assumed: the npm registry
    carries a **SLSA v1 provenance attestation** binding that exact tarball
    to source commit `3a0f58eb7250a9a46ad77863bc4618eee099a248` of
    `github.com/badlogic/clipboard` (now `earendil-works/clipboard`), built
    by `.github/workflows/CI.yml`. So the binary IS linked to a source
    commit by signed provenance. What is NOT known is the licence set of
    what was compiled in: `Cargo.lock` at that commit names **122**
    packages and carries **zero** licence fields, and no `cargo-about` /
    SBOM output is published anywhere in the repository. Closing this needs
    `cargo metadata` against that commit, which is the M12 SBOM work.
2. **The clipboard component's own copyright notice does not exist
    upstream.** MIT is declared only as an SPDX string in `package.json`
    (`"license": "MIT"`, both at the provenance commit and at HEAD, and in
    the registry metadata for both the host and the linux-x64 packages).
    An exhaustive walk of that repository (63 blobs, `truncated=false`)
    finds **no** LICENSE, COPYING or NOTICE file at all, and the Rust
    `Cargo.toml` has no `license` key; `/LICENSE` at the provenance commit
    returns HTTP 404 (probe run with stderr visible and exit code checked).
    There is therefore no MIT copyright line to redistribute, and none can
    be manufactured here. This is an upstream defect, not a local gap.
3. **The full module set inside the SEA blob is not enumerable from the
    shipped file.** `scripts/native/01-bundle.mjs` force-bundles the JS
    dependency graph through `tsdown` into `main.cjs` before the blob is
    built, so individual package identities and their licences are erased at
    bundle time. `apps/kimi-code/package.json` lists the direct dependencies,
    but the repository publishes no lockfile-derived licence inventory and
    the artifact carries none. Reconstructing it means resolving the pnpm
    lock at the pinned commit and running a licence scan — again M12.
4. **`dist-web` (534 files, ~34 MB, 1072 `dist-web` hits in the ELF) comes
    from a source repository that is not public.** This is stated by
    upstream itself, not inferred: `apps/kimi-code/scripts/check-web-assets.mjs`
    says verbatim that "apps/kimi-web no longer exists in this repo: the web
    UI is developed in the code-app repo (apps/web) and the built bundle is
    synced here and committed at apps/kimi-code/dist-web (gitignored,
    force-added)". Exhaustive enumeration of the `MoonshotAI` organisation's
    public repositories (43 repos, `--paginate`) contains no `code-app`;
    direct lookups of four plausible spellings all return HTTP 404. The
    committed bundle is minified build output with no accompanying licence
    manifest, so neither its own sources nor its bundled web dependencies
    (a Vite build including KaTeX fonts, among others) can be inventoried
    from public material. If that repository is private, this cannot be
    closed by us at all — only upstream can publish the inventory.
5. **Whether the embedded Node build is byte-identical to any published
    Node distribution is not established.** The version matches (5 hits for
    `v24.15.0`, `.nvmrc` = `24.15.0`), and the composite licence we vendored
    is bit-identical between `nodejs/node` @ `v24.15.0` and the official
    linux-x64 tarball. But CI resolves its toolchain through
    `actions/setup-node`, which serves repackaged builds, and `postject`
    then rewrites the executable — so no byte-level equality claim is made
    here, and the licence is vendored on the strength of the version match
    rather than on binary identity.

## Enumeration performed, so the next reader can tell searching from guessing

Three revisions of the kimi claim were wrong because a probe that failed and a
search that genuinely found nothing were recorded identically. Every negative
statement above rests on one of the enumerations below; each was run with
stderr visible and exit codes checked, and each is re-runnable.

| Negative claim | Enumeration actually performed |
|---|---|
| The 0.38.0 tag is `@moonshot-ai/kimi-code@0.38.0` and no other tag matches | `gh api --paginate 'repos/MoonshotAI/kimi-code/tags?per_page=100'` — **68 tags**, every page, one hit for `0.38` |
| Upstream ships no NOTICE and no third-party inventory | full recursive git tree at the pinned commit — **4889 blobs**, response `truncated=false`, only `/LICENSE`, `/apps/vscode/LICENSE`, `/packages/pi-tui/LICENSE` match a licence/notice/copying pattern |
| OpenTUI attribution exists only as source comments, not as a file | GitHub code search across the repository — **3 hits**, all under `packages/pi-tui` (`src/stdin-buffer.ts`, `src/keys.ts`, `test/stdin-buffer.test.ts`); zero paths in the tree named for it |
| The `code-app` repository holding the `dist-web` sources is not public | `gh api --paginate 'orgs/MoonshotAI/repos?per_page=100'` — **43 repositories**, no `code-app`; plus four direct spellings each HTTP 404. This bounds the MoonshotAI org only; a differently-owned or differently-named public mirror would not be excluded by it |
| The clipboard component publishes no licence text | full recursive git tree of `earendil-works/clipboard` — **63 blobs**, `truncated=false`, zero licence/copying/notice matches; `/LICENSE` at the provenance commit HTTP 404; no `license` key in `Cargo.toml` |
| The shipped binary does not embed Node's composite licence | `grep -a -c -F` on the 182 MB ELF for two distinctive Node LICENSE sentences — both **0**, against same-run positive controls `ELF`=51, `MIT License`=106, `OpenSSL`=68 on the same file, so the probe demonstrably finds licence text when present |
| The shipped binary contains no Python runtime | `Py_Initialize`, `PyInstaller`, `_MEIPASS` each **0** on the same ELF, same run, same positive controls |

The binary measured in the last two rows hashes to
`7f18b701ea751d14bf051776747ca0339f8d59693aec9051276933067a914b00`, which is
the value pinned in `cli-manifest.toml` — the measurements are on the pinned
artifact, not on some other build.

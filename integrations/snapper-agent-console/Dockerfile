# Interactive agent console: the blackbox Python delegate PLUS the native CLIs
# of the popular coding agents, one image, attached to over a PTY.
#
# Base is the blackbox delegate image itself (operator direction 2026-08-26:
# one container carrying every CLI and our Python OpenAI-API code), so
# `python -m snapper_delegate.pid1` keeps working here unchanged. Every CLI is
# fetched from the pinned artifact URL recorded in cli-manifest.toml and
# verified against the checksum recorded there — no vendor install.sh runs in
# this build. Single-file CLIs land straight in /usr/local/bin; the two
# multi-file packages (codex, cursor) live under /usr/local/lib with their
# entrypoints linked into /usr/local/bin, because their binaries resolve
# helpers by relative path.
#
# Build (from the repo root; the blackbox image must exist first; context is
# integrations/ so the build can bake snapper-mcp from the sibling package):
#   docker build -t snapper-delegate:blackbox integrations/snapper-delegate
#   docker build -t snapper-agent-console \
#     -f integrations/snapper-agent-console/Dockerfile integrations
# Attach:
#   docker compose -f integrations/snapper-agent-console/compose.agent-console.yml \\
#     --profile agent-console up -d agent-console
#   docker compose -f ... exec -it agent-console tmux new-session -A -s agent

ARG BASE_IMAGE=snapper-delegate:blackbox

FROM ${BASE_IMAGE} AS base
ARG TARGETARCH
USER root
RUN [ "${TARGETARCH:-amd64}" = "amd64" ] && [ "$(uname -m)" = "x86_64" ] \
    || { echo "agent-console: all pinned CLI artifacts are x86_64; refusing TARGETARCH=${TARGETARCH} uname=$(uname -m)" >&2; exit 1; }
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl git less procps tini tmux zstd \
    && rm -rf /var/lib/apt/lists/*

FROM base AS cli-claude
ARG CLAUDE_VERSION=2.1.246
ARG CLAUDE_SHA256=1a0a662dc1bb938eaec38545abce9a4a69113d7d7f7c5e1a553ea276617b906a
RUN curl -fsSL --proto '=https' --tlsv1.2 "https://downloads.claude.ai/claude-code-releases/${CLAUDE_VERSION}/linux-x64/claude" \
        -o /usr/local/bin/claude \
    && echo "${CLAUDE_SHA256}  /usr/local/bin/claude" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/claude

FROM base AS cli-codex
ARG CODEX_VERSION=0.150.0
ARG CODEX_SHA256=2d27b8569ca760eeb2bf21fa12bd31f59ac42b81617b340cf8958e9b38d743d5
RUN curl -fsSL --proto '=https' --tlsv1.2 "https://releases.openai.com/codex/releases/${CODEX_VERSION}/codex-package-x86_64-unknown-linux-musl.tar.gz" \
        -o /tmp/codex.tar.gz \
    && echo "${CODEX_SHA256}  /tmp/codex.tar.gz" | sha256sum -c - \
    && mkdir -p /usr/local/lib/codex \
    && tar -xzf /tmp/codex.tar.gz -C /usr/local/lib/codex \
    && rm /tmp/codex.tar.gz \
    && test -x /usr/local/lib/codex/bin/codex \
    && ln -s /usr/local/lib/codex/bin/codex /usr/local/bin/codex

FROM base AS cli-copilot
ARG COPILOT_VERSION=1.0.80
ARG COPILOT_SHA256=039933c9247686131c4406abb1d439bdbf68103edc1ff585bd70d5b0dc940f72
RUN curl -fsSL --proto '=https' --tlsv1.2 "https://github.com/github/copilot-cli/releases/download/v${COPILOT_VERSION}/copilot-linux-x64.tar.gz" \
        -o /tmp/copilot.tar.gz \
    && echo "${COPILOT_SHA256}  /tmp/copilot.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/copilot.tar.gz -C /usr/local/bin copilot \
    && rm /tmp/copilot.tar.gz \
    && chmod 0755 /usr/local/bin/copilot

FROM base AS cli-kimi
ARG KIMI_VERSION=0.38.0
ARG KIMI_SHA256=7f18b701ea751d14bf051776747ca0339f8d59693aec9051276933067a914b00
RUN curl -fsSL --proto '=https' --tlsv1.2 "https://code.kimi.ai/kimi-code/binaries/${KIMI_VERSION}/kimi-code-linux-x64" \
        -o /usr/local/bin/kimi \
    && echo "${KIMI_SHA256}  /usr/local/bin/kimi" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/kimi

FROM base AS cli-agy
ARG AGY_VERSION=1.1.21-6424454201475072
ARG AGY_SHA512=3de7552ef089c136c0f570cdc9c04652278e02c1d41ed3911ad5f9f1b8c3bd567643aa401a1916060ea32a1b17fbaf90cbb417071db33f467880fcd848868d92
RUN curl -fsSL --proto '=https' --tlsv1.2 "https://storage.googleapis.com/antigravity-public/antigravity-cli/${AGY_VERSION}/linux-x64/cli_linux_x64.tar.gz" \
        -o /tmp/agy.tar.gz \
    && echo "${AGY_SHA512}  /tmp/agy.tar.gz" | sha512sum -c - \
    && mkdir -p /tmp/agy && tar -xzf /tmp/agy.tar.gz -C /tmp/agy \
    && install -m 0755 "$(find /tmp/agy -maxdepth 2 -type f -name antigravity | head -1)" /usr/local/bin/agy \
    && rm -rf /tmp/agy /tmp/agy.tar.gz

FROM base AS cli-cursor
ARG CURSOR_VERSION=2026.08.25-3e8eec8
ARG CURSOR_SHA256=7a212e5a17ff9316f5acc78808e33c536940d5455645022e6388d99ba48c8425
RUN curl -fsSL --proto '=https' --tlsv1.2 "https://downloads.cursor.com/lab/${CURSOR_VERSION}/linux/x64/agent-cli-package.tar.gz" \
        -o /tmp/cursor.tar.gz \
    && echo "${CURSOR_SHA256}  /tmp/cursor.tar.gz" | sha256sum -c - \
    && mkdir -p /usr/local/lib/cursor-agent \
    && tar -xzf /tmp/cursor.tar.gz -C /usr/local/lib/cursor-agent --strip-components=1 \
    && rm /tmp/cursor.tar.gz \
    && printf '#!/bin/sh\nexec /usr/local/lib/cursor-agent/cursor-agent --disable-auto-update "$@"\n' \
        > /usr/local/bin/cursor-agent \
    && chmod 0755 /usr/local/bin/cursor-agent

FROM node:26-slim AS mcp-build
COPY snapper-mcp /build/snapper-mcp
WORKDIR /build/snapper-mcp
RUN npm ci --ignore-scripts && npm run build && npm prune --omit=dev

FROM base AS cli-grok
ARG GROK_VERSION=1.0.5
ARG GROK_SHA256=9ba87444e1819e8f6104adbbf4676a870c204380aa5c3e1c38a926c4ea677238
RUN curl -fsSL --proto '=https' --tlsv1.2 "https://x.ai/cli/grok-${GROK_VERSION}-linux-x86_64" \
        -o /usr/local/bin/grok \
    && echo "${GROK_SHA256}  /usr/local/bin/grok" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/grok

# Pin the inherited base to THIS checkout. BASE_IMAGE is a mutable tag and the
# console's own tests exercise the working tree's pid1 — so a base built from an
# older delegate would let every test pass while the image shipped different
# code, a different dependency set, or a different import path. Runtime COPYs
# the manifests this stage produces, so the check cannot be skipped. The
# TEMPORARY copy of the expected build inputs that this stage takes from the
# context does not reach the runtime layer; the runtime does of course inherit
# /app/src and /opt/venv from BASE_IMAGE itself, which is the thing being
# checked here.
#
# WHAT THIS STAGE VERIFIES about BASE_IMAGE:
#   1. dependencies — every distribution in /opt/venv matches this checkout's
#      integrations/snapper-delegate/requirements.lock by name AND version, in
#      both directions, with PEP 508 markers evaluated by the image's own
#      interpreter. Identical .py files over a stale or substituted dependency
#      set is the case this catches.
#   2. package content — the delegate package tree is byte-identical to this
#      checkout recursively and for EVERY file type, not just top-level *.py:
#      subdirectories, data files and a dropped .pth all count. registration.py
#      is absent from the context exactly as the blackbox build excludes it, so
#      an image that ships it fails as extra content. The image must also carry
#      no bytecode cache and nothing beside the package on PYTHONPATH, and
#      site-packages must carry no .pth / sitecustomize import hook.
#   3. base recipe — this checkout's integrations/snapper-delegate/Dockerfile
#      still hashes to DELEGATE_BASE_DOCKERFILE_SHA256, and the image satisfies
#      what that file declares: interpreter major.minor, venv prefix, package
#      destination, PYTHONPATH, delegate uid, hash-pinned lock install.
#   4. import path — PYTHONPATH and the package location agree with the recipe
#      and with what the console entrypoint expects.
#   5. import proof — `import snapper_delegate.pid1` actually succeeds in the
#      image's own environment and resolves under the declared package
#      directory. File equality never proved the runtime import worked: a base
#      with identical sources but an inherited PYTHONPATH=/tmp/invalid passed
#      the old source-only predicate and failed at runtime.
#
# WHAT IT DOES NOT VERIFY — do not read more into the manifests than this:
#   - not the python:3.14-slim image digest the base was built FROM; only the
#     declared major.minor of the interpreter that ended up in the image;
#   - not that BASE_IMAGE was actually PRODUCED BY that Dockerfile. The digest
#     pin only forces a human to re-pin and rebuild when the recipe changes;
#   - not the base image config (ENTRYPOINT, USER, LABEL, declared ENV as
#     opposed to inherited ENV), which a build stage cannot read;
#   - not anything outside /opt/venv and the delegate package — the OS layer,
#     /usr/local, and pip/setuptools/wheel themselves, which `pip freeze`
#     excludes by design, are not compared;
#   - not against a hostile operator: BASE_IMAGE and the digest pin are both
#     build args and overriding them is trusted.
FROM base AS lineage
ARG BASE_IMAGE
# sha256 of integrations/snapper-delegate/Dockerfile as reviewed for this image.
ARG DELEGATE_BASE_DOCKERFILE_SHA256=4ae640a31e2d47714b1321aed17986ce21ab58437e486e33a47073a351066618
COPY snapper-delegate/src/snapper_delegate /tmp/lineage/delegate
COPY snapper-delegate/requirements.lock /tmp/lineage/requirements.lock
COPY snapper-delegate/Dockerfile /tmp/lineage/base.Dockerfile
RUN python - <<'PY'
"""Bind BASE_IMAGE to this checkout. See the stage comment for the exact scope."""

import hashlib
import importlib
import os
import pathlib
import pwd
import re
import subprocess
import sys
import sysconfig

CONTEXT = pathlib.Path("/tmp/lineage")
EXPECTED_PACKAGE = CONTEXT / "delegate"
LOCK = CONTEXT / "requirements.lock"
BASE_DOCKERFILE = CONTEXT / "base.Dockerfile"
OUT = pathlib.Path("/out")

REBUILD = "rebuild snapper-delegate:blackbox from this checkout first, then rebuild this image"
REPIN = (
    "review the base build change, rebuild snapper-delegate:blackbox, then update "
    "DELEGATE_BASE_DOCKERFILE_SHA256 in snapper-agent-console/Dockerfile"
)

failures = []


def fail(message, remedy):
    """Record one named drift plus the remedy for exactly that drift."""
    failures.append("agent-console lineage: " + message + "\n    remedy: " + remedy)


def stop_if_failed():
    """Print every drift found so far to stderr and abort the build."""
    if failures:
        print("", file=sys.stderr)
        for line in failures:
            print(line, file=sys.stderr)
        print(
            "agent-console lineage: BASE_IMAGE=" + os.environ.get("BASE_IMAGE", "?")
            + " is not the image this checkout describes; refusing to build.",
            file=sys.stderr,
        )
        raise SystemExit(1)


def digest(path):
    """Return the hex sha256 of one file."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical(name):
    """Normalise a distribution name per PEP 503 so casing and -_. do not matter."""
    return re.sub(r"[-_.]+", "-", name).strip().lower()


try:
    from packaging.markers import Marker
except ModuleNotFoundError:
    try:
        from pip._vendor.packaging.markers import Marker
    except ModuleNotFoundError:
        Marker = None

if Marker is None:
    fail(
        "no PEP 508 marker evaluator is importable in the image (neither packaging nor "
        "pip._vendor.packaging), so requirements.lock cannot be filtered for this "
        "environment and the dependency set cannot be verified",
        "install packaging into /opt/venv or restore pip, then rebuild",
    )
    stop_if_failed()

recipe = BASE_DOCKERFILE.read_text()
recipe_sha = digest(BASE_DOCKERFILE)
pinned_sha = os.environ["DELEGATE_BASE_DOCKERFILE_SHA256"]
if recipe_sha != pinned_sha:
    fail(
        "integrations/snapper-delegate/Dockerfile in this checkout hashes to sha256:"
        + recipe_sha
        + " but this image pins sha256:"
        + pinned_sha
        + " — the recipe that builds BASE_IMAGE changed",
        REPIN,
    )


def declared(pattern, what):
    """Return the match for one base-Dockerfile declaration, or record its loss."""
    match = re.search(pattern, recipe, re.M)
    if match is None:
        fail(
            "the base Dockerfile no longer declares " + what + " in a form this check "
            "understands (searched " + repr(pattern) + "), so BASE_IMAGE cannot be "
            "verified against it",
            REPIN,
        )
    return match


runtime_from = declared(r"^FROM\s+python:(\d+)\.(\d+)-slim\s+AS\s+runtime\b", "its runtime base as python:<major>.<minor>-slim")
venv_decl = declared(r"^RUN\s+python -m venv (\S+)\s*$", "the venv directory")
copy_decl = declared(r"^COPY\s+src/snapper_delegate\s+(\S+)\s*$", "the delegate package destination")
path_decl = declared(r"^\s*PYTHONPATH=(\S+)", "PYTHONPATH")
uid_decl = declared(r"^RUN\s+useradd\s+--uid\s+(\d+)\b", "the delegate uid")
install_decl = declared(r"^RUN\s+python -m pip install (.*)-r /tmp/requirements.lock\s*$", "the requirements.lock install")
if any(match is None for match in (runtime_from, venv_decl, copy_decl, path_decl, uid_decl, install_decl)):
    stop_if_failed()

want_python = (int(runtime_from.group(1)), int(runtime_from.group(2)))
venv_dir = venv_decl.group(1)
package_dir = pathlib.Path(copy_decl.group(1))
want_pythonpath = path_decl.group(1)
delegate_uid = int(uid_decl.group(1))

if "--require-hashes" not in install_decl.group(1):
    fail(
        "the base Dockerfile installs requirements.lock without --require-hashes, so the "
        "dependency set it produces is not pinned to the hashes this stage compares against",
        REPIN,
    )

if sys.version_info[:2] != want_python:
    fail(
        "BASE_IMAGE runs CPython " + sys.version.split()[0] + " but its Dockerfile declares "
        "python:" + str(want_python[0]) + "." + str(want_python[1]) + "-slim",
        REBUILD,
    )

if sys.prefix != venv_dir:
    fail(
        "the interpreter on PATH reports prefix " + repr(sys.prefix) + ", not the "
        + repr(venv_dir) + " the base Dockerfile builds and installs the lock into",
        REBUILD,
    )

if os.environ.get("PYTHONPATH") != want_pythonpath:
    fail(
        "BASE_IMAGE carries PYTHONPATH=" + repr(os.environ.get("PYTHONPATH"))
        + " but its Dockerfile declares PYTHONPATH=" + repr(want_pythonpath)
        + "; the console entrypoint imports snapper_delegate.pid1 from that path",
        REBUILD,
    )

if str(package_dir.parent) != want_pythonpath:
    fail(
        "the base Dockerfile copies the delegate package to " + str(package_dir)
        + ", which does not sit directly on the PYTHONPATH it declares (" + want_pythonpath + ")",
        REPIN,
    )

try:
    pwd.getpwuid(delegate_uid)
except KeyError:
    fail(
        "BASE_IMAGE has no account for uid " + str(delegate_uid)
        + " although its Dockerfile creates one; the console runs USER 888:888",
        REBUILD,
    )

REQUIREMENT = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[[^\]]*\])?==([^\s;]+)\s*(?:;\s*(.+))?$")
locked = {}
lock_order = []
current = None
for raw in LOCK.read_text().splitlines():
    line = raw.strip()
    if line.endswith("\\"):
        line = line[:-1].strip()
    if not line or line.startswith("#"):
        continue
    if line.startswith("--hash="):
        if current is not None:
            locked[current]["hashes"] += 1
        continue
    match = REQUIREMENT.match(line)
    if match is None:
        fail(
            "requirements.lock line " + repr(raw) + " is not a name==version requirement and "
            "this stage will not silently skip it",
            "fix or regenerate integrations/snapper-delegate/requirements.lock",
        )
        current = None
        continue
    name, version, marker = match.group(1), match.group(2), match.group(3)
    current = canonical(name)
    lock_order.append(current)
    locked[current] = {
        "name": name,
        "version": version,
        "marker": marker,
        "applies": True if marker is None else bool(Marker(marker).evaluate()),
        "hashes": 0,
    }

unhashed = sorted(key for key, entry in locked.items() if entry["hashes"] == 0)
if unhashed:
    fail(
        "requirements.lock pins no hash for " + ", ".join(unhashed)
        + ", so --require-hashes cannot bind those distributions",
        "regenerate integrations/snapper-delegate/requirements.lock with --generate-hashes",
    )

installed = {}
try:
    frozen = subprocess.run(
        [sys.executable, "-m", "pip", "freeze", "--disable-pip-version-check"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
except (OSError, subprocess.CalledProcessError) as exc:
    frozen = ""
    fail(
        "could not enumerate the distributions installed in " + venv_dir + ": " + repr(exc),
        REBUILD,
    )

for line in frozen.splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    match = REQUIREMENT.match(line)
    if match is None:
        fail(
            "the venv reports the distribution " + repr(line) + ", which is not a plain "
            "name==version install and therefore cannot come from a hash-pinned lock",
            REBUILD,
        )
        continue
    installed[canonical(match.group(1))] = {"name": match.group(1), "version": match.group(2)}

for key in lock_order:
    entry = locked[key]
    have = installed.get(key)
    if entry["applies"] and have is None:
        fail(
            "dependency " + entry["name"] + "==" + entry["version"] + " is required by this "
            "checkout's requirements.lock but is NOT installed in " + venv_dir,
            REBUILD,
        )
    elif entry["applies"] and have["version"] != entry["version"]:
        fail(
            "dependency " + entry["name"] + " version drift: " + venv_dir + " has "
            + have["version"] + ", this checkout's requirements.lock pins " + entry["version"],
            REBUILD,
        )
    elif not entry["applies"] and have is not None:
        fail(
            "dependency " + entry["name"] + "==" + have["version"] + " is installed in "
            + venv_dir + " although requirements.lock restricts it to '" + str(entry["marker"])
            + "', which is false for this image",
            REBUILD,
        )

for key in sorted(installed):
    if key not in locked:
        fail(
            "dependency " + installed[key]["name"] + "==" + installed[key]["version"]
            + " is installed in " + venv_dir
            + " but appears nowhere in this checkout's requirements.lock",
            REBUILD,
        )


def tree(root, keep_bytecode):
    """Map every file under root to its sha256, keyed by posix relative path."""
    found = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if not keep_bytecode and (relative.endswith(".pyc") or "__pycache__/" in relative):
            continue
        found[relative] = digest(path)
    return found


if not package_dir.is_dir():
    fail(
        "BASE_IMAGE has no delegate package at " + str(package_dir)
        + " although its Dockerfile copies one there",
        REBUILD,
    )
    stop_if_failed()

shipped = tree(package_dir, True)
expected = tree(EXPECTED_PACKAGE, False)

bytecode = sorted(name for name in shipped if name.endswith(".pyc") or "__pycache__/" in name)
if bytecode:
    fail(
        "BASE_IMAGE carries bytecode under " + str(package_dir) + ": " + ", ".join(bytecode)
        + " — a cached .pyc can execute code the matching .py does not contain",
        REBUILD,
    )
    for name in bytecode:
        shipped.pop(name)

for name in sorted(set(shipped) - set(expected)):
    fail(
        "BASE_IMAGE carries " + str(package_dir / name) + " which this checkout does not have "
        "(sha256:" + shipped[name] + ")",
        REBUILD,
    )
for name in sorted(set(expected) - set(shipped)):
    fail(
        "BASE_IMAGE is missing " + str(package_dir / name) + " which this checkout has "
        "(sha256:" + expected[name] + ")",
        REBUILD,
    )
for name in sorted(set(shipped) & set(expected)):
    if shipped[name] != expected[name]:
        fail(
            "BASE_IMAGE carries a different " + str(package_dir / name) + ": image sha256:"
            + shipped[name] + ", checkout sha256:" + expected[name],
            REBUILD,
        )

siblings = sorted(entry.name for entry in package_dir.parent.iterdir() if entry.name != package_dir.name)
if siblings:
    fail(
        str(package_dir.parent) + " carries " + ", ".join(siblings) + " beside the delegate "
        "package; this checkout puts nothing else on PYTHONPATH",
        REBUILD,
    )

hooks = []
for site_dir in sorted({sysconfig.get_paths()["purelib"], sysconfig.get_paths()["platlib"]}):
    site_path = pathlib.Path(site_dir)
    if not site_path.is_dir():
        continue
    hooks.extend(str(path) for path in sorted(site_path.glob("*.pth")))
    hooks.extend(
        str(site_path / name)
        for name in ("sitecustomize.py", "usercustomize.py")
        if (site_path / name).is_file()
    )
if hooks:
    fail(
        "site-packages carries the import hook(s) " + ", ".join(hooks) + " which this "
        "checkout's requirements.lock does not install; a .pth or sitecustomize can rebind "
        "snapper_delegate without changing a single .py",
        REBUILD,
    )

try:
    package_module = importlib.import_module("snapper_delegate")
    entry_module = importlib.import_module("snapper_delegate.pid1")
except BaseException as exc:
    fail(
        "`import snapper_delegate.pid1` FAILS inside BASE_IMAGE with its own environment: "
        + type(exc).__name__ + ": " + str(exc)
        + " — matching source files do not prove the runtime import works",
        REBUILD,
    )
else:
    for module, wanted in ((package_module, package_dir / "__init__.py"), (entry_module, package_dir / "pid1.py")):
        resolved = pathlib.Path(module.__file__ or "<none>")
        if resolved != wanted:
            fail(
                module.__name__ + " imports from " + str(resolved) + ", not the "
                + str(wanted) + " this checkout and the console entrypoint expect",
                REBUILD,
            )

stop_if_failed()

OUT.mkdir(parents=True, exist_ok=True)
(OUT / "delegate-lineage.sha256").write_text(
    "".join(shipped[name] + "  " + name + "\n" for name in sorted(shipped))
)
manifest = [
    "schema=1",
    "generated-by=snapper-agent-console Dockerfile, stage lineage",
    "scope=see the stage comment; this records what was compared, not a full build-input lineage",
    "base-image-arg=" + os.environ.get("BASE_IMAGE", "?"),
    "base-dockerfile-sha256=" + recipe_sha,
    "base-dockerfile-pin-sha256=" + pinned_sha,
    "requirements-lock-sha256=" + digest(LOCK),
    "requirements-lock-entries=" + str(len(lock_order)),
    "requirements-lock-applicable-here=" + str(sum(1 for entry in locked.values() if entry["applies"])),
    "python-runtime=" + sys.version.split()[0],
    "python-declared-base=python:" + str(want_python[0]) + "." + str(want_python[1]) + "-slim (image digest NOT verified)",
    "python-executable=" + sys.executable,
    "venv-prefix=" + sys.prefix,
    "delegate-package-dir=" + str(package_dir),
    "delegate-pythonpath=" + want_pythonpath,
    "delegate-uid=" + str(delegate_uid),
    "delegate-files=" + str(len(shipped)),
    "",
    "[installed-distributions]",
]
manifest.extend(installed[key]["name"] + "==" + installed[key]["version"] for key in sorted(installed))
manifest.extend(["", "[delegate-package-sha256]"])
manifest.extend(shipped[name] + "  " + name for name in sorted(shipped))
(OUT / "delegate-lineage.manifest").write_text("\n".join(manifest) + "\n")
print("agent-console lineage: BASE_IMAGE verified against this checkout (" + str(len(shipped)) + " package files, " + str(len(installed)) + " distributions)")
PY

FROM base AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends libatomic1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=lineage /out/delegate-lineage.sha256 /usr/local/share/agent-console/delegate-lineage.sha256
COPY --from=lineage /out/delegate-lineage.manifest /usr/local/share/agent-console/delegate-lineage.manifest
COPY --from=cli-claude /usr/local/bin/claude /usr/local/bin/claude
COPY --from=cli-codex /usr/local/lib/codex /usr/local/lib/codex
RUN ln -s /usr/local/lib/codex/bin/codex /usr/local/bin/codex
COPY --from=cli-copilot /usr/local/bin/copilot /usr/local/bin/copilot
COPY --from=cli-kimi /usr/local/bin/kimi /usr/local/bin/kimi
COPY --from=cli-agy /usr/local/bin/agy /usr/local/bin/agy
COPY --from=cli-cursor /usr/local/lib/cursor-agent /usr/local/lib/cursor-agent
COPY --from=cli-cursor /usr/local/bin/cursor-agent /usr/local/bin/cursor-agent
COPY --from=cli-grok /usr/local/bin/grok /usr/local/bin/grok
COPY --from=node:26-slim /usr/local/bin/node /usr/local/bin/node
COPY --from=node:26-slim /usr/local/LICENSE /usr/local/share/licenses/node/LICENSE
COPY --from=mcp-build /build/snapper-mcp/dist /usr/local/lib/snapper-mcp/dist
COPY --from=mcp-build /build/snapper-mcp/node_modules /usr/local/lib/snapper-mcp/node_modules
COPY --from=mcp-build /build/snapper-mcp/package.json /usr/local/lib/snapper-mcp/package.json
COPY snapper-mcp/LICENSE /usr/local/lib/snapper-mcp/LICENSE
# License texts are vendored in the repo so this layer needs no network and
# can never substitute a synthetic fallback for a fetch failure.
# licenses/PROVENANCE.md records the pinned source ref and SHA-256 of every
# text, and states the version evidence PER COMPONENT: rg and zsh report their
# versions from the binary, bwrap's is inferred from the vendored meson source,
# ratatui's comes from the Cargo.lock.
COPY snapper-agent-console/licenses /usr/local/share/licenses/vendored
COPY snapper-agent-console/codex-requirements.toml /etc/codex/requirements.toml
COPY snapper-agent-console/snapper-mcp-wrapper.sh /usr/local/bin/snapper-mcp
RUN chmod 0755 /usr/local/bin/snapper-mcp
COPY snapper-agent-console/entrypoint.sh /usr/local/bin/agent-console-entrypoint
RUN chmod 0755 /usr/local/bin/agent-console-entrypoint \
    && mkdir -p /home/delegate && chown 888:888 /home/delegate

ENV HOME=/home/delegate \
    SHELL=/bin/bash \
    DISABLE_AUTOUPDATER=1 \
    COPILOT_AUTO_UPDATE=false \
    GROK_DISABLE_AUTOUPDATER=1 \
    KIMI_NO_MODIFY_PATH=1

USER 888:888
ENTRYPOINT ["tini", "--", "/usr/local/bin/agent-console-entrypoint"]

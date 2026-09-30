# CCXT dependency metadata patch

Snapper temporarily uses `ccxt 4.5.84+snapper.1` because upstream CCXT 4.5.84
requires exactly `urllib3 2.7.0`. That version is affected by
[GHSA-gh4c-6fx4-qh6g](https://github.com/advisories/GHSA-gh4c-6fx4-qh6g),
[GHSA-vxq7-64xx-v4gw](https://github.com/advisories/GHSA-vxq7-64xx-v4gw), and
[GHSA-8988-9cw3-xx77](https://github.com/advisories/GHSA-8988-9cw3-xx77).
All three are fixed in urllib3 2.8.0.

The wheel changes only the distribution version and the urllib3 requirement
to `urllib3==2.8.0`, renames the distribution metadata directory, and regenerates
`RECORD`. All 721 Python files and the upstream MIT license remain byte-identical.
The Python module still reports upstream's code version, `4.5.84`.

| Artifact | SHA-256 |
| --- | --- |
| Upstream `ccxt-4.5.84-py3-none-any.whl` | `b920f7d92c0fb62873a900c96ee2cfb76ec6ffeaa7343bf5be0cc01b9b2e9617` |
| Patched `ccxt-4.5.84+snapper.1-py3-none-any.whl` | `5f867a8ca8f679b44fcf93d3024806505cf1a76b3f4ad1c3a07029f7629860a3` |

## Rebuild

Download the fixed upstream artifact, then run the deterministic repacker from
the repository root. The script rejects any input with a different SHA-256.

```bash
curl --fail --location \
  'https://files.pythonhosted.org/packages/c9/a2/1f6fd14591a951e608fe7afc8d77de6ef35434c2e430b4fff105ffb216b0/ccxt-4.5.84-py3-none-any.whl' \
  --output /tmp/ccxt-4.5.84-py3-none-any.whl
python3 scripts/repack_ccxt_wheel.py /tmp/ccxt-4.5.84-py3-none-any.whl
```

The generated wheel uses fixed ordering, timestamps, permissions and compression
settings. Its complete file list and hashes are recorded in its `RECORD`.

## Installation and retirement

Poetry installs this repository-relative wheel; Docker copies it before resolving
the locked application environment. `make refresh` preserves file dependencies.
Build and install Snapper from its source checkout or Docker context: a standalone
Snapper wheel records an absolute `file://` dependency and is not portable without
this vendored dependency and an appropriate installation environment.

When an official CCXT release permits a patched urllib3 version, replace the path
dependency with that release, regenerate `poetry.lock`, remove this wheel,
repacker and Docker copy, and run the complete quality gate. Do not return to the
unpatched upstream 4.5.84 dependency metadata.

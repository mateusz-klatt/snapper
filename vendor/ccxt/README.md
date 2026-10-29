# CCXT dependency security constraints

Snapper uses the official `ccxt 4.5.91` distribution from PyPI. This release
allows the fixed dependency versions that previously required the local
`4.5.84+snapper.3` metadata patch. The vendored wheel, repacker, and Docker copy
have been retired; application and exchange safety guards are unchanged.

The root manifest keeps explicit security minimums:

- `urllib3 >= 2.8.0`
- `multidict >= 6.9.1`
- `certifi >= 2026.10.23`

Upstream 4.5.91 requires urllib3 2.8.0 and multidict 6.9.1 exactly. Its runtime
certifi requirement is a range, so Snapper retains its own fixed-version floor.
The optional upstream `dev` extra is not requested. Resolve and install the
committed Poetry lock to preserve the complete tested dependency graph.

| Artifact | SHA-256 |
| --- | --- |
| Official `ccxt-4.5.91-py3-none-any.whl` | `883d36142f3921a7383b413851915a2ea6e55ff3dce7e1a8af3856c7a5fa6b72` |
| Retired `ccxt-4.5.84+snapper.3-py3-none-any.whl` | `687e19db719d172ddf89e3082423f4773135fa3a6195ed6664a1994c46ae91b6` |

The official wheel and its publisher provenance are available from the
[PyPI release](https://pypi.org/project/ccxt/4.5.91/). The retired patch changed
only distribution metadata; its source and reproducible repacker remain in Git
history. Upstream CCXT retains its MIT license and distribution notices.

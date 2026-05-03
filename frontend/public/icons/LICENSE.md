# Vendored icon assets — third-party attribution

The SVG files under `crypto/` and `flags/` are vendored from third-party
open-source projects. They are bundled here so the trading UI works in
air-gapped environments and is independent of any third-party CDN.

## crypto/ — cryptocurrency-icons

- **Upstream**: <https://github.com/spothq/cryptocurrency-icons>
- **License**: CC0 1.0 Universal (public domain dedication)
- **Vendored subset**: a curated selection matching the assets the UI
  knows about today. Tokens not present here trigger the in-app
  textual-badge fallback (`RemoteSvg.tsx`).

CC0 places no restrictions on use. The notice here is courtesy
attribution — you may use, modify, and redistribute these files freely.

## flags/ — circle-flags

- **Upstream**: <https://github.com/HatScripts/circle-flags>
- **License**: MIT
- **Vendored subset**: the country codes referenced in
  `frontend/src/components/InstrumentIcon/registry/fiat.ts`.

The MIT license text required for redistribution is reproduced below.

```
MIT License

Copyright (c) 2026 HatScripts

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Updating the vendored subset

```sh
# Re-sync from upstream (requires git + curl):
pnpm icons:vendor
```

The script is `frontend/scripts/vendor-icons.sh`. It clones both upstream
repos to a temporary directory, copies the requested subset, and updates
this LICENSE file with the current upstream copyright year. Run it after
adding new entries to `registry/crypto.ts` or `registry/fiat.ts`.

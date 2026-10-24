# Order execution support

Snapper distinguishes catalog discovery, implemented execution, account eligibility
and live activation. A venue client method or a visible price does not establish
all four. The execution kill switch, scoped wallet credentials, exact instrument
identity, product capabilities and trading caps remain required.

## Product boundaries

| Product | Catalog and sizing | Creation boundary |
| --- | --- | --- |
| Kraken ordinary spot | Existing base-asset sizing | Existing authorization and risk checks |
| Kraken tokenized assets / xStocks | Separate quote markets, catalog-derived asset class, direct precision and status evidence | Refused until immutable quantity units and corporate-action accounting are integrated |
| Kraken Futures | Signed quantity precision, explicit payoff and currency evidence, native order units | Refused until derivative cash, margin and engine accounting are integrated |
| Kraken equities / FCM feed | Market data and contract discovery | No implemented order client |
| Paper | Cash-only spot simulation | Manual REST/MCP require an authoritative supported source profile; no derivative demo |

The application refuses tokenized creation with
`tokenized_unit_accounting_unavailable` and derivative creation with
`derivative_accounting_unavailable`, even if an old cached capability says
`can_trade=true`. A catalog denial may be returned earlier as
`instrument_market_data_only`. The executor repeats the accounting check for
its actual execution venue before acquiring an order client. These creation
checks do not block cancellation.
Kraken creation also requires a positive catalog product classification. A cold,
mixed or unknown classification returns `execution_product_identity_unavailable`,
including when an independently retained CCXT alias could still resolve.

Manual paper admission verifies the original source venue and exact instrument
UUID. Directly selected paper instruments follow their current stored source mapping.
Missing, unmapped or unsupported source profiles return
`paper_source_identity_unavailable`; known contract and token profiles retain
their accounting refusals. Positively classified Kraken currency products may
use ordinary spot paper without an optional specification. Polygon and
Walutomat require an explicit compatible source `spot` or `etf` profile. Legacy
Polygon equity labels alone are insufficient, so unattested equity paper remains
unavailable until that source profile is established.

The Polygon updater derives cash profiles from active `stocks` references with
explicit `CS` or `ETF` type, matching ticker and quote currency, and the
[provider accuracy timestamp](https://massive.com/docs/rest/stocks/tickers/all-tickers).
It revokes an earlier profile when the current reference becomes unsupported,
inactive, contradictory or incomplete. Cache reads preserve the provider time;
older caches lacking it remain unattested until an ordinary reference refresh.

Kraken's [current xStocks availability rules](https://support.kraken.com/articles/xstocks-availability)
separate EEA buy/sell/convert access from Pro/API order-book access. Account
eligibility requires independent venue evidence.

## Quantity and monetary evidence

The Futures [instrument catalog](https://docs.kraken.com/api-reference/instrument-details/get-instruments)
defines quantity precision independently of `contractSize`. For signed precision
`p`, the increment is `10^-p`: precision `4` means `0.0001`, while `-3` means
`1000`. Missing or unusable precision does not fall back to the multiplier.

The catalog's `tradeable` flag describes a product that is or has ever been a
market. It does not establish current availability. Snapper derives current
status from the separate expiry, suspension and post-only flags: explicit
expiry or suspension is inactive; missing status evidence is unknown; and a
post-only restriction remains distinct from unrestricted activity. The source
flags are bound into the specification version. Current Futures submission
requires the corrected `s2a-v4` interpretation and an active market; unknown,
inactive and post-only states are refused by the current admission path.
Account-specific eligibility and dispatch freshness require separate evidence.

Economic unit certification survives a market becoming inactive. The supported
`s2a-v3` and `s2a-v4` quantity interpretations remain compatible for reconciliation
under the existing freshness rules. Historical unit snapshots retain their
original revisions and canonical bytes; they do not authorize another order.

Published Futures categories distinguish equity, commodity, Forex and index
exposure from crypto markets. Category corrections preserve market UUIDs and
aliases. An equity-related perpetual remains a derivative; the classification
does not establish share ownership or execution eligibility.

`native_order_quantity_unit` records the venue's order-entry dimension.
Verified unit-sized `flexible_futures` use `base_asset`; supported legacy
contract definitions use `contract_count`. Existing internal position quantities
retain their compatibility representation. The two labels are disclosed
separately; they are not interchangeable for an arbitrary contract.

Specifications also retain an optional atomic monetary tuple:
`contract_payoff_model`, `contract_value_currency` and `settlement_currency`.
Linear multipliers are denominated in base units; inverse multipliers are
denominated in quote currency. Settlement currency describes economic P&L before
an account's payout conversion preferences. Unknown historical values remain
null. Migration 0055 refuses a downgrade that would delete recorded evidence.
It acquires a database write fence before checking for evidence and keeps the
check and schema change in the caller's transaction. PostgreSQL requires
`READ COMMITTED`; an unsupported transaction configuration is refused.

An internal immutable snapshot store binds quantity and monetary interpretation
to an exact instrument/specification revision and a durable submission or fill.
It checks account and venue scope, historical request material, original raw
numeric evidence and its stored numeric mirrors. Database guards reject snapshot
updates and deletion; a populated snapshot table also prevents a lossy downgrade.
Submission and Futures fill producers bind actual route, original command,
source receipt and financial-row identities to this store. Fill evidence
separates provider execution time from local recording time. Original admission
units remain authoritative when a later catalog refresh changes current
metadata. Active executor dispatch, accounting consumers and financial-state
holds still require integration before tokenized or derivative creation.

The USD cap oracle uses full exposure, independently of leverage or collateral:

- Linear contracts: `abs(quantity) × multiplier × price`.
- Inverse contracts: `abs(quantity) × multiplier` in quote currency.
- Spot: `quantity × price`.

Derivative valuation requires current direct-source provenance, fresh unit
certification, explicit compatible currencies and known unexpired maturity for
dated contracts. A cached price cannot extend certification or preserve a
revoked specification. Missing derivative semantics never use spot arithmetic.
Legacy spot valuation is limited to known spot sources; unknown venues or
unrecognized quantity and product dimensions are refused.
The implemented profile requires positive prices and USD quotes; it does not
provide general commodity, option, margin or collateral valuation.

A pure contract position fold uses arithmetic entry averaging for linear
contracts and harmonic entry averaging for inverse contracts. It retains an
unchanged payoff specification and realizes settlement P&L on the closed portion
of a fill, including partial closes and flips. It transfers no cash and assigns
no fee currency. This component is not yet connected to engine projections,
margin, funding or portfolio valuation; derivative creation remains refused.

Kraken [Multi-M definitions](https://support.kraken.com/articles/4844429542676-trading-multi-collateral-derivatives)
use base quantities. A separate pure Decimal valuation component provides linear
and inverse notional and settlement P&L formulas. Its presence does not enable
the spot-oriented execution engine to account for derivatives.

## Order entry and discovery

REST manual orders accept only the implemented `GTC` input policy. Other
time-in-force values are rejected rather than silently stored without execution.
This input vocabulary does not override the venue's own market-order behavior.
Maker-only and reduce-only instructions remain subject to the shared venue and
order-type rules.

Durable command status changes preserve maker-only instructions so recovery
reconstructs the original request. At the Futures client boundary, take-profit
requires its trigger price and retains an independently optional limit leg.
Native trailing creation is refused until the request contract carries the
required deviation value and unit. Existing trailing orders remain readable
and cancellable. Manual REST/MCP entry retains its four supported order types.

MCP clients can call `list_instruments` and then
`get_order_instrument(exchange, instrument)` to obtain the exact instrument UUID,
native quantity unit, increment, multiplier, expiry, source, certification and
unambiguous catalog order constraints. The read also discloses cached tradability
and the actual venue's product accounting refusal. It requires `READ_MARKET_DATA`
and does not check account permissions, balances or live activation. The separate
paper source-profile proof runs at manual admission; the catalog read alone does
not certify it. Manual writes retain their own identity, authorization,
capability and risk checks.

The web ticket retains an idempotency key for the same effective order after a
lost response and rotates it when the payload changes. Request generations keep
an earlier completion from changing a reopened ticket. Confirmation amounts use
the selected quote currency. Supported maker-only and reduce-only controls are
shown according to the selected execution route.

## Durable request consistency

The outbox and the final executor creation boundary verify that a durable
command's historical request material stayed unchanged. Status revisions may
change lifecycle fields; they cannot change quantities, prices, modifiers,
ownership or other request material. A historical conflict records a durable
local dispatch barrier for that command. Missing or unavailable proof holds
creation without inventing a venue rejection. An outbox proof-storage failure
propagates to its supervisor rather than repeatedly revising retry status.

Executors also require the queued request to match the proven command and the
actual wallet, venue and execution mode. Paper execution requires Paper mode;
a live venue cannot accept a frame merely labelled as Paper. Existing venue
orders can still be adopted and cancelled.

This barrier applies to one logical command. It does not reserve cash or
exposure, freeze an engine or prevent a new command after the engine's existing
60-second in-flight timeout. Durable scope holds, recovery of those holds and
explicit resolution remain separate work.

## Remaining accounting work

An internal Futures recording transaction now joins one actual execution,
source event, immutable unit witness, source-seeded position and durable
publication intent under the wallet write fence. Position quantities and
modeled settlement P&L remain separate from native cash entries. Cash changes
come from the venue's retained account-log balance changes; trade notional and
modeled realized P&L do not become cash transfers.

Recording notifications use
`accounting.contract_position.<exchange>.<environment>.<wallet>.<instrument>`
and carry references to the committed facts. A retry retains the same message
identity. A delivery receipt means the local publisher returned successfully;
it does not prove broker persistence or consumer acknowledgement. Consumers
must recover the durable ledger and deduplicate independently. These internal
writers are not yet connected to the active executor and engine lifecycle.

The Futures executor accepts an optional `environment` in its encrypted
credential envelope. Exact values are `production` and `demo`; absence retains
the legacy production selection. Blank, non-string or other values are refused
before client construction. Public endpoint evidence must agree with the
selected environment. This route is distinct from Paper simulation. Demo
financial eligibility additionally requires catalog evidence from the actual
demo source and account-specific reconciliation; the routing option alone does
not establish either.

Native account evidence must preserve each margin profile and its original
source clock. Flattened balances across profiles and collateral valuations
cannot establish one account's native cash. Captured stream continuity also
does not prove that all earlier fills were recorded or that collateral is
available for a new order. Those boundaries remain part of the integration.

The source-profile proof currently applies at manual REST/MCP admission. The
final Paper executor and recovery path do not yet repeat that asynchronous proof
from a source instrument UUID. Their synchronous actual-venue check does not
certify preexisting queued or strategy-origin paper commands. This remaining
boundary must be integrated independently; the cash simulator is not a
contract-aware derivatives demo.

Paper instruments retain their identity when their source mapping changes.
Position and projection-checkpoint identities currently omit that source.
Consequently, proof for a newly admitted command cannot establish the source
of retained cash, positions or P&L. Enabling a new source profile requires an
immutable original-source attestation and a compatible financial-state anchor,
or an explicit migration/quarantine policy for older state.

xStocks quantities may be raw SPV tokens or rebased underlying-equity units.
Kraken's [execution stream](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/executions)
defaults to rebased quantities, while the
[open-order query](https://docs.kraken.com/api-reference/account-data/get-open-orders)
offers a unit convention selector. Immutable order and fill evidence must retain
the reported convention and the applicable corporate-action multiplier before
these products can enter accounting. A later multiplier must not reinterpret an
earlier fill. Corporate actions require their own adjustment events.

Derivative enablement additionally needs position accounting independent of spot
cash transfers, settlement-currency realized P&L, fees, funding, collateral and
margin reconciliation, expiry settlement, account-specific accessible instruments
and demo recovery evidence. The existing FCM market-data feed is separate from
Kraken's US Perps API and does not establish a CME order route.
Listed futures additionally need daily settlement and variation cash flows
while positions remain open, product-specific settlement rounding and delivery
or closeout calendars. Closed-position P&L and perpetual funding alone do not
provide that accounting.

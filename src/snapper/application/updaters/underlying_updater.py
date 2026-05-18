"""Underlying asset updater — syncs YAML definitions to the database.

Reads underlying_mappings.yaml, matches patterns against active instruments,
adds deterministic coverage for active instruments not listed explicitly,
and upserts UnderlyingAsset + InstrumentUnderlyingMapping rows via SCD2.
"""

import re
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Literal
from typing import Self
from uuid import uuid7

import yaml
from loguru import logger
from pydantic import BaseModel
from pydantic import ValidationInfo
from pydantic import field_validator
from pydantic import model_validator
from sqlalchemy import select

from snapper.core.types import AssetTypeEnum
from snapper.core.types import ExchangeEnum
from snapper.core.types import InstrumentKindEnum
from snapper.core.types import RelationshipTypeEnum
from snapper.data.models import Instrument
from snapper.data.models import InstrumentUnderlyingMapping
from snapper.data.models import Symbol
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
from snapper.data.repository_types import InstrumentSpecRow

_DEFAULT_YAML = Path(__file__).resolve().parents[2] / "data" / "underlying_mappings.yaml"

_SAFETY_THRESHOLD = 0.25
_ASSET_PRIORITY = {
    AssetTypeEnum.FOREX.value: 0,
    AssetTypeEnum.EQUITY.value: 1,
    AssetTypeEnum.INDEX.value: 2,
    AssetTypeEnum.COMMODITY.value: 3,
    AssetTypeEnum.YIELD.value: 4,
    AssetTypeEnum.CRYPTO.value: 5,
}
_DERIVATIVE_EXCHANGES = {ExchangeEnum.KRAKEN_FUTURES.value}
_FUTURES_VENUES = {"CME", "CBOT", "COMEX", "NYMEX"}


class PatternRule(BaseModel):
    """Single pattern rule matching instruments to an underlying asset.

    Fields ``instrument_type`` and ``expiry_override`` are accepted from YAML
    for (front-month rollover) but do not participate in
    pattern matching. They are only used to detect intra-underlying metadata
    conflicts when multiple rules match the same instrument.
    """

    exchange: ExchangeEnum
    match_type: Literal["exact", "regex"]
    pattern: str
    relationship_type: RelationshipTypeEnum = RelationshipTypeEnum.EXACT
    instrument_type: InstrumentKindEnum | None = None
    contract_family: str | None = None
    expiry_override: datetime | None = None

    @field_validator("pattern")
    @classmethod
    def validate_regex(cls, v: str, info: ValidationInfo) -> str:
        """Compile regex patterns at validation time for fail-fast behaviour.

        Args:
            v: Pattern string to validate.
            info: Pydantic validation context with sibling field values.

        Returns:
            The validated pattern string.
        """
        if info.data.get("match_type") == "regex":
            re.compile(v)
        return v


class UnderlyingDefinition(BaseModel):
    """Definition of an underlying asset with its matching patterns."""

    ticker: str
    name: str
    asset_class: AssetTypeEnum
    sector: str | None = None
    description: dict[str, str] | None = None
    patterns: list[PatternRule]

    @model_validator(mode="before")
    @classmethod
    def normalize_description(cls, data: object) -> object:
        """Normalize legacy scalar descriptions to the multilingual map shape.

        Args:
            data: Raw Pydantic input before field validation.

        Returns:
            Input with legacy scalar ``description`` moved under ``en``.
        """
        if isinstance(data, dict):
            description = data.get("description")
            if isinstance(description, str):
                normalized = dict(data)
                normalized["description"] = {"en": description}
                return normalized
        return data


class UnderlyingMappingConfig(BaseModel):
    """Root schema for the underlying_mappings.yaml file."""

    underlyings: list[UnderlyingDefinition]

    @model_validator(mode="after")
    def unique_tickers_and_names(self) -> Self:
        """Reject duplicate tickers or names in the mapping file.

        Returns:
            Validated config instance.
        """
        for attr in ("ticker", "name"):
            values = [getattr(u, attr) for u in self.underlyings]
            if len(values) != len(set(values)):
                seen: set[str] = set()
                dupes: list[str] = []
                for v in values:
                    if v in seen:
                        dupes.append(v)
                    seen.add(v)
                msg = f"Duplicate {attr}s in mapping file: {dupes}"
                raise ValueError(msg)
        return self


def rule_matches(rule: PatternRule, native_symbol: str) -> bool:
    """Test whether a pattern rule matches a native symbol string.

    Args:
        rule: Pattern rule with match_type and pattern.
        native_symbol: Symbol string to test against.

    Returns:
        True if the rule matches the symbol.
    """
    if rule.match_type == "exact":
        return native_symbol == rule.pattern
    return re.fullmatch(rule.pattern, native_symbol) is not None


class _InstrumentInfo:
    """Lightweight struct for an active instrument during matching."""

    __slots__ = (
        "asset_category",
        "base_symbol",
        "exchange",
        "instrument_public_id",
        "native_symbol",
        "quote_symbol",
    )

    def __init__(
        self,
        instrument_public_id: str,
        exchange: str,
        native_symbol: str,
        base: str,
        quote: str | None,
        asset_type: str,
    ) -> None:
        self.instrument_public_id = instrument_public_id
        self.exchange = exchange
        self.native_symbol = native_symbol
        self.base_symbol = base
        self.quote_symbol = quote
        self.asset_category = asset_type


class _MatchResult:
    """Accumulated match for one instrument across all underlyings."""

    __slots__ = (
        "underlying_ticker",
        "relationship_type",
        "contract_family",
        "instrument_type",
        "expiry_override",
    )

    def __init__(
        self,
        underlying_ticker: str,
        relationship_type: str,
        contract_family: str | None,
        instrument_type: str | None = None,
        expiry_override: datetime | None = None,
    ) -> None:
        self.underlying_ticker = underlying_ticker
        self.relationship_type = relationship_type
        self.contract_family = contract_family
        self.instrument_type = instrument_type
        self.expiry_override = expiry_override


class _RuleCandidate:
    """Indexed rule with enough ordering metadata to rebuild match results."""

    __slots__ = ("rule", "rule_index", "ticker")

    def __init__(
        self,
        ticker: str,
        rule: PatternRule,
        rule_index: int,
    ) -> None:
        self.ticker = ticker
        self.rule = rule
        self.rule_index = rule_index


class _RuleMatcher:
    """Exchange-scoped rule index for repeated instrument matching."""

    __slots__ = ("_definition_indexes", "_exact", "_regex_by_exchange")

    def __init__(self) -> None:
        self._definition_indexes: dict[str, int] = {}
        self._exact: dict[tuple[str, str], list[_RuleCandidate]] = {}
        self._regex_by_exchange: dict[str, list[_RuleCandidate]] = {}

    @classmethod
    def from_definitions(cls, definitions: list[UnderlyingDefinition]) -> Self:
        """Build a matcher from a validated list of definitions.

        Args:
            definitions: Underlying definitions in YAML order.

        Returns:
            Matcher containing exact and regex indexes.
        """
        matcher = cls()
        for definition_index, definition in enumerate(definitions):
            matcher.add_definition(definition, definition_index)
        return matcher

    def add_definition(
        self,
        definition: UnderlyingDefinition,
        definition_index: int,
    ) -> None:
        """Add a full underlying definition to the index.

        Args:
            definition: Underlying definition to index.
            definition_index: Stable position of the definition in config order.
        """
        self._definition_indexes[definition.ticker] = definition_index
        for rule_index, rule in enumerate(definition.patterns):
            self.add_rule(definition.ticker, rule, rule_index)

    def add_rule(self, ticker: str, rule: PatternRule, rule_index: int) -> None:
        """Add one rule for an already indexed definition.

        Args:
            ticker: Ticker of the already indexed definition.
            rule: Pattern rule to add.
            rule_index: Stable position of the rule in its definition.
        """
        candidate = _RuleCandidate(
            ticker=ticker,
            rule=rule,
            rule_index=rule_index,
        )
        if rule.match_type == "exact":
            self._exact.setdefault((rule.exchange.value, rule.pattern), []).append(candidate)
            return
        self._regex_by_exchange.setdefault(rule.exchange.value, []).append(candidate)

    def find_matches(self, inst: _InstrumentInfo) -> tuple[list[_MatchResult], bool]:
        """Find matches using exact lookup plus exchange-scoped regex scan.

        Args:
            inst: Instrument to match.

        Returns:
            ``(matches, has_intra_conflict)`` with result semantics matching
            :meth:`UnderlyingUpdater._find_matches`.
        """
        per_underlying: dict[str, list[_RuleCandidate]] = {}

        for candidate in self._exact.get((inst.exchange, inst.native_symbol), []):
            per_underlying.setdefault(candidate.ticker, []).append(candidate)

        for candidate in self._regex_by_exchange.get(inst.exchange, []):
            if rule_matches(candidate.rule, inst.native_symbol):
                per_underlying.setdefault(candidate.ticker, []).append(candidate)

        results: list[_MatchResult] = []
        has_intra_conflict = False
        for ticker, candidates in sorted(
            per_underlying.items(),
            key=lambda item: self._definition_indexes[item[0]],
        ):
            ordered = sorted(candidates, key=lambda candidate: candidate.rule_index)
            rules = [candidate.rule for candidate in ordered]
            match = UnderlyingUpdater._build_underlying_match(ticker, rules, inst)
            if match is None:
                has_intra_conflict = True
                continue
            results.append(match)

        return results, has_intra_conflict


class UnderlyingUpdater:
    """Syncs underlying asset definitions from YAML into the database."""

    def __init__(
        self,
        db_url: str,
        force: bool = False,
        yaml_path: Path | None = None,
    ) -> None:
        """Initialize the underlying updater.

        Args:
            db_url: Database connection URL.
            force: Bypass safety guard for stale cleanup.
            yaml_path: Path to YAML mapping file (defaults to built-in).
        """
        self._db_url = db_url
        self._force = force
        self._yaml_path = yaml_path or _DEFAULT_YAML
        self._repo: Repository | None = None

    async def run(self) -> None:
        """Execute the full update cycle."""
        self._repo = get_repository(self._db_url)
        now = datetime.now(UTC)

        config = self._load_config()
        instruments = await self._load_instruments(now)
        config = self._expand_auto_coverage(config, instruments)
        underlying_ids = await self._upsert_underlyings(config, now)
        await self._cleanup_stale_underlyings(config, now)
        await self._match_and_upsert(config, underlying_ids, instruments, now)

    def _load_config(self) -> UnderlyingMappingConfig:
        """Load and validate the YAML mapping file."""
        logger.info(f"Loading mapping config from {self._yaml_path}")
        with open(self._yaml_path) as f:
            raw = yaml.safe_load(f)
        return UnderlyingMappingConfig.model_validate(raw)

    async def _upsert_underlyings(
        self,
        config: UnderlyingMappingConfig,
        now: datetime,
    ) -> dict[str, str]:
        """Upsert all underlying asset definitions. Returns {ticker: public_id}."""
        assert self._repo is not None
        result: dict[str, str] = {}
        counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0}
        session_id = str(uuid7())

        for seq, defn in enumerate(config.underlyings, start=1):
            public_id, status = await self._repo.upsert_underlying_asset(
                ticker=defn.ticker,
                name=defn.name,
                asset_class=defn.asset_class.value,
                session_id=session_id,
                sequence_id=seq,
                timestamp=now,
                sector=defn.sector,
                description=defn.description,
            )
            result[defn.ticker] = public_id
            counts[status] += 1

        logger.info(
            f"Underlyings: {counts['created']} created, "
            f"{counts['updated']} updated, {counts['unchanged']} unchanged"
        )
        return result

    async def _cleanup_stale_underlyings(
        self,
        config: UnderlyingMappingConfig,
        now: datetime,
    ) -> None:
        """Close underlying assets that are no longer present in the mapping config."""
        assert self._repo is not None
        config_tickers = {defn.ticker for defn in config.underlyings}
        active = await self._repo.get_underlying_assets(now)
        session_id = str(uuid7())
        stale = [row for row in active if row["ticker"] not in config_tickers]
        for seq, row in enumerate(stale, start=1):
            await self._repo.close_underlying_asset(
                public_id=row["public_id"],
                session_id=session_id,
                sequence_id=seq,
                timestamp=now,
            )
        if stale:
            logger.info(f"Closed {len(stale)} stale underlying(s) removed from mapping config")

    async def _load_instruments(self, now: datetime) -> list[_InstrumentInfo]:
        """Load all active instruments with their native symbols."""
        assert self._repo is not None
        async with self._repo.session() as s:
            stmt = (
                select(
                    Instrument.public_id.label("instrument_public_id"),
                    Instrument.exchange,
                    Symbol.native_symbol,
                    Symbol.base,
                    Symbol.quote,
                    Symbol.asset_type,
                )
                .join(Symbol, Symbol.public_id == Instrument.symbol_public_id)
                .where(*where_active(Instrument, now), *where_active(Symbol, now))
            )
            rows = (await s.execute(stmt)).all()
        return [
            _InstrumentInfo(
                instrument_public_id=r.instrument_public_id,
                exchange=r.exchange,
                native_symbol=r.native_symbol,
                base=self._coerce_symbol_part(getattr(r, "base", None), r.native_symbol),
                quote=self._coerce_optional_symbol_part(getattr(r, "quote", None)),
                asset_type=self._coerce_asset_type(getattr(r, "asset_type", None)),
            )
            for r in rows
        ]

    @staticmethod
    def _coerce_symbol_part(value: object, native_symbol: str) -> str:
        """Return a usable symbol base from a database row or test double."""
        if isinstance(value, str) and value:
            return value
        return native_symbol.split("-", maxsplit=1)[0]

    @staticmethod
    def _coerce_optional_symbol_part(value: object) -> str | None:
        """Return a usable optional symbol quote from a database row or test double."""
        if isinstance(value, str) and value:
            return value
        return None

    @staticmethod
    def _coerce_asset_type(value: object) -> str:
        """Return a known asset type from a database row or test double."""
        if isinstance(value, str) and value in _ASSET_PRIORITY:
            return value
        return AssetTypeEnum.CRYPTO.value

    def _expand_auto_coverage(
        self,
        config: UnderlyingMappingConfig,
        instruments: list[_InstrumentInfo],
    ) -> UnderlyingMappingConfig:
        """Add exact rules for active instruments not matched by explicit YAML.

        Skips instruments whose fallback ticker already exists in the
        config under a different ``asset_class`` — preserves explicit
        YAML intent against semantic-collision contamination (e.g.
        ``SPX`` index vs SPX6900 crypto memecoin sharing
        ``base='SPX'``; ``CVX`` Chevron equity vs Convex Finance
        crypto). The append-after-validate ordering also prevents
        leaving an empty generated underlying behind when
        :meth:`_append_fallback_rule` rejects an instrument with an
        unrecognised exchange.

        Construction of the resulting ``UnderlyingMappingConfig``
        happens once at the end. Rebuilding it per iteration would
        re-run the O(n) duplicate-ticker validator, making the loop
        O(n²) on the active-instrument count.
        """
        definitions = list(config.underlyings)
        by_ticker = {definition.ticker: definition for definition in definitions}
        matcher = _RuleMatcher.from_definitions(definitions)
        new_tickers: set[str] = set()
        extended_tickers: set[str] = set()
        generated_patterns = 0

        for inst in sorted(instruments, key=self._auto_coverage_sort_key):
            kind = self._auto_coverage_apply(inst, definitions, by_ticker, matcher)
            if kind == "skipped":
                continue
            ticker = self._fallback_ticker(inst)
            if kind == "new":
                new_tickers.add(ticker)
            else:
                extended_tickers.add(ticker)
            generated_patterns += 1

        if not generated_patterns:
            return config

        logger.info(
            f"Auto-covered {generated_patterns} active instrument(s) "
            f"across {len(new_tickers)} new + {len(extended_tickers)} extended underlying(s)"
        )
        return UnderlyingMappingConfig(underlyings=definitions)

    def _auto_coverage_apply(
        self,
        inst: _InstrumentInfo,
        definitions: list[UnderlyingDefinition],
        by_ticker: dict[str, UnderlyingDefinition],
        matcher: _RuleMatcher,
    ) -> Literal["skipped", "new", "extended"]:
        """Apply auto-coverage for a single instrument.

        Args:
            inst: The instrument to consider.
            definitions: Mutable list of definitions being built. The
                method appends new definitions here after rule validation.
            by_ticker: Mutable ticker → definition lookup.
            matcher: Mutable rule index kept in sync with generated
                fallback rules.

        Returns:
            ``"new"`` when a new underlying was created and a rule
            appended, ``"extended"`` when a rule was appended to an
            existing same-class underlying, and ``"skipped"`` for
            already-matched, asset-class collision, or invalid-input
            cases.
        """
        matches, has_intra_conflict = matcher.find_matches(inst)
        if matches or has_intra_conflict:
            return "skipped"

        definition = self._fallback_definition_for_instrument(inst)
        if definition is None:
            return "skipped"

        existing = by_ticker.get(definition.ticker)
        if existing is not None and existing.asset_class != definition.asset_class:
            return "skipped"

        target = existing if existing is not None else definition
        if not self._append_fallback_rule(target, inst):
            return "skipped"

        if existing is None:
            definitions.append(definition)
            by_ticker[definition.ticker] = definition
            matcher.add_definition(definition, len(definitions) - 1)
            return "new"
        matcher.add_rule(existing.ticker, target.patterns[-1], len(target.patterns) - 1)
        return "extended"

    @staticmethod
    def _auto_coverage_sort_key(inst: _InstrumentInfo) -> tuple[int, str, str]:
        """Prefer semantic non-crypto rows when an exchange symbol is duplicated."""
        return (
            _ASSET_PRIORITY.get(inst.asset_category, len(_ASSET_PRIORITY)),
            inst.exchange,
            inst.native_symbol,
        )

    @staticmethod
    def _fallback_definition_for_instrument(
        inst: _InstrumentInfo,
    ) -> UnderlyingDefinition | None:
        """Build a fallback underlying definition from symbol metadata."""
        ticker = UnderlyingUpdater._fallback_ticker(inst)
        if not ticker:
            return None
        try:
            asset_class = AssetTypeEnum(inst.asset_category)
        except ValueError:
            return None
        return UnderlyingDefinition(
            ticker=ticker,
            name=UnderlyingUpdater._fallback_name(inst, ticker),
            asset_class=asset_class,
            patterns=[],
        )

    @staticmethod
    def _fallback_ticker(inst: _InstrumentInfo) -> str:
        """Return the canonical fallback ticker for an instrument."""
        if inst.asset_category == AssetTypeEnum.FOREX.value and inst.quote_symbol:
            return f"{inst.base_symbol}{inst.quote_symbol}"
        return inst.base_symbol

    @staticmethod
    def _fallback_name(inst: _InstrumentInfo, ticker: str) -> str:
        """Return the fallback display name for an instrument."""
        if inst.asset_category == AssetTypeEnum.FOREX.value and inst.quote_symbol:
            return f"{inst.base_symbol} / {inst.quote_symbol}"
        return ticker

    @staticmethod
    def _append_fallback_rule(definition: UnderlyingDefinition, inst: _InstrumentInfo) -> bool:
        """Append an exact fallback rule if the definition does not already match."""
        try:
            exchange = ExchangeEnum(inst.exchange)
        except ValueError:
            return False
        rule = PatternRule(
            exchange=exchange,
            match_type="exact",
            pattern=inst.native_symbol,
            relationship_type=UnderlyingUpdater._fallback_relationship_type(inst),
        )
        if any(existing == rule for existing in definition.patterns):
            return False
        definition.patterns.append(rule)
        return True

    @staticmethod
    def _fallback_relationship_type(inst: _InstrumentInfo) -> RelationshipTypeEnum:
        """Return relationship metadata for a fallback rule."""
        if inst.exchange in _DERIVATIVE_EXCHANGES:
            return RelationshipTypeEnum.DERIVATIVE
        suffix = inst.native_symbol.rsplit("-", maxsplit=1)[-1]
        if inst.exchange == ExchangeEnum.KRAKEN_EQUITIES.value and suffix in _FUTURES_VENUES:
            return RelationshipTypeEnum.DERIVATIVE
        return RelationshipTypeEnum.EXACT

    async def _match_and_upsert(
        self,
        config: UnderlyingMappingConfig,
        underlying_ids: dict[str, str],
        instruments: list[_InstrumentInfo],
        now: datetime,
    ) -> None:
        """Match instruments to underlyings and upsert/close mappings."""
        assert self._repo is not None
        session_id = str(uuid7())

        desired: dict[str, _MatchResult] = {}
        conflicted: set[str] = set()
        unmapped: list[tuple[str, str]] = []
        matcher = _RuleMatcher.from_definitions(config.underlyings)

        for inst in instruments:
            matches, has_intra_conflict = matcher.find_matches(inst)

            if has_intra_conflict:
                conflicted.add(inst.instrument_public_id)
                continue

            if len(matches) == 0:
                unmapped.append((inst.native_symbol, inst.exchange))
                continue

            if len(matches) > 1:
                tickers = [m.underlying_ticker for m in matches]
                logger.error(
                    f"Conflict: {inst.native_symbol} ({inst.exchange}) matches "
                    f"multiple underlyings: {tickers} — skipping"
                )
                conflicted.add(inst.instrument_public_id)
                continue

            match = matches[0]
            desired[inst.instrument_public_id] = match

        counts: dict[str, int] = {"created": 0, "updated": 0, "unchanged": 0}
        for seq, (ipid, match) in enumerate(desired.items(), start=1):
            status = await self._repo.upsert_instrument_underlying_mapping(
                instrument_public_id=ipid,
                underlying_public_id=underlying_ids[match.underlying_ticker],
                relationship_type=match.relationship_type,
                session_id=session_id,
                sequence_id=seq,
                timestamp=now,
                contract_family=match.contract_family,
            )
            counts[status] += 1

        spec_applied = await self._apply_yaml_spec_fallbacks(desired, session_id, now)
        closed = await self._cleanup_stale(desired, conflicted, session_id, now)

        logger.info(
            f"Mappings: {counts['created']} created, {counts['updated']} updated, "
            f"{counts['unchanged']} unchanged, {closed} closed"
        )
        if spec_applied:
            logger.info(f"YAML spec fallbacks applied: {spec_applied}")
        if unmapped:
            logger.info(f"Unmapped instruments ({len(unmapped)}): {unmapped[:20]}")
        if conflicted:
            logger.warning(f"Conflicted instruments: {len(conflicted)}")

    def _find_matches(
        self,
        config: UnderlyingMappingConfig,
        inst: _InstrumentInfo,
    ) -> tuple[list[_MatchResult], bool]:
        """Find all underlying matches for a single instrument.

        Args:
            config: Validated mapping configuration.
            inst: Instrument to match against.

        Returns:
            Tuple of (matches, has_intra_conflict). When has_intra_conflict
            is True the instrument should be treated as conflicted even if
            matches is empty, to prevent stale cleanup from removing its
            existing mapping.
        """
        return _RuleMatcher.from_definitions(config.underlyings).find_matches(inst)

    def _find_matches_in(
        self,
        definitions: list[UnderlyingDefinition],
        inst: _InstrumentInfo,
    ) -> tuple[list[_MatchResult], bool]:
        """Find matches against an arbitrary list of underlying definitions.

        Bypasses :class:`UnderlyingMappingConfig` so callers iterating
        over a live, mutating definition list (e.g.
        :meth:`_expand_auto_coverage`) avoid paying the O(n) duplicate
        validator on every call.

        Args:
            definitions: Mapping definitions to scan.
            inst: Instrument to match against.

        Returns:
            ``(matches, has_intra_conflict)`` — see :meth:`_find_matches`.
        """
        return _RuleMatcher.from_definitions(definitions).find_matches(inst)

    @staticmethod
    def _build_underlying_match(
        ticker: str,
        rules: list[PatternRule],
        inst: _InstrumentInfo,
    ) -> _MatchResult | None:
        """Build the resolved match for one underlying or report a conflict."""
        first = rules[0]
        if UnderlyingUpdater._has_conflicting_rule_metadata(first, rules[1:]):
            logger.error(
                f"Intra-underlying conflict for {inst.native_symbol} "
                f"({inst.exchange}) in {ticker}: rules disagree on metadata"
            )
            return None
        return _MatchResult(
            underlying_ticker=ticker,
            relationship_type=first.relationship_type.value,
            contract_family=first.contract_family,
            instrument_type=first.instrument_type,
            expiry_override=first.expiry_override,
        )

    @staticmethod
    def _has_conflicting_rule_metadata(
        first: PatternRule,
        rules: list[PatternRule],
    ) -> bool:
        """Detect whether multiple matching rules disagree on persisted metadata."""
        return any(UnderlyingUpdater._rule_metadata_differs(first, rule) for rule in rules)

    @staticmethod
    def _rule_metadata_differs(first: PatternRule, rule: PatternRule) -> bool:
        """Compare the persisted metadata carried by two matching rules."""
        return (
            rule.relationship_type != first.relationship_type
            or rule.contract_family != first.contract_family
            or rule.instrument_type != first.instrument_type
            or rule.expiry_override != first.expiry_override
        )

    async def _apply_yaml_spec_fallbacks(
        self,
        desired: dict[str, _MatchResult],
        session_id: str,
        now: datetime,
    ) -> int:
        """Apply YAML instrument_type and expiry_override as fallbacks.

        Only writes to InstrumentSpec when the current value is NULL,
        preserving API-sourced data as authoritative.
        """
        assert self._repo is not None
        applied = 0
        for seq, (ipid, match) in enumerate(desired.items(), start=1):
            if not match.instrument_type and not match.expiry_override:
                continue
            existing = await self._repo.get_instrument_spec(ipid, now)
            spec = self._build_fallback_spec(match, existing)
            if spec is None:
                continue
            await self._repo.revise_instrument_spec(
                instrument_public_id=ipid,
                session_id=session_id,
                sequence_id=seq,
                timestamp=now,
                spec=spec,
            )
            applied += 1
        return applied

    @staticmethod
    def _build_fallback_spec(
        match: _MatchResult,
        existing: InstrumentSpecRow | None,
    ) -> InstrumentSpecInput | None:
        """Build an InstrumentSpecInput applying YAML fallbacks only where NULL.

        Args:
            match: Match result with optional instrument_type and expiry_override.
            existing: Current spec row, or None.

        Returns:
            InstrumentSpecInput with fallbacks applied, or None if nothing to change.
        """
        base_spec = UnderlyingUpdater._base_fallback_spec(existing)
        current_kind = base_spec.instrument_kind
        current_expiry = base_spec.expiry_at
        needs_kind = match.instrument_type is not None and current_kind is None
        needs_expiry = match.expiry_override is not None and current_expiry is None
        if not needs_kind and not needs_expiry:
            return None
        return InstrumentSpecInput(
            tick_size=base_spec.tick_size,
            lot_size=base_spec.lot_size,
            min_order_size=base_spec.min_order_size,
            max_order_size=base_spec.max_order_size,
            cost_decimals=base_spec.cost_decimals,
            qty_decimals=base_spec.qty_decimals,
            margin_initial=base_spec.margin_initial,
            position_limit_long=base_spec.position_limit_long,
            position_limit_short=base_spec.position_limit_short,
            status=base_spec.status,
            expiry_at=match.expiry_override if needs_expiry else current_expiry,
            instrument_kind=match.instrument_type if needs_kind else current_kind,
            funding_type=base_spec.funding_type,
            funding_frequency_hours=base_spec.funding_frequency_hours,
            rollover_rate_long=base_spec.rollover_rate_long,
            rollover_rate_short=base_spec.rollover_rate_short,
            max_funding_rate=base_spec.max_funding_rate,
        )

    @staticmethod
    def _base_fallback_spec(existing: InstrumentSpecRow | None) -> InstrumentSpecInput:
        """Build the baseline spec payload from the existing instrument spec."""
        if existing is None:
            return InstrumentSpecInput()
        return InstrumentSpecInput(
            tick_size=existing["tick_size"],
            lot_size=existing["lot_size"],
            min_order_size=existing["min_order_size"],
            max_order_size=existing["max_order_size"],
            cost_decimals=existing["cost_decimals"],
            qty_decimals=existing["qty_decimals"],
            margin_initial=existing["margin_initial"],
            position_limit_long=existing["position_limit_long"],
            position_limit_short=existing["position_limit_short"],
            status=existing["status"],
            expiry_at=existing["expiry_at"],
            instrument_kind=existing["instrument_kind"],
        )

    async def _cleanup_stale(
        self,
        desired: dict[str, _MatchResult],
        conflicted: set[str],
        session_id: str,
        now: datetime,
    ) -> int:
        """Close mappings that are no longer in the desired set.

        Conflicted instruments are excluded from cleanup — they retain
        their existing mapping until the YAML conflict is resolved.
        """
        assert self._repo is not None
        async with self._repo.session() as s:
            existing_rows = (
                (
                    await s.execute(
                        select(InstrumentUnderlyingMapping.instrument_public_id).where(
                            *where_active(InstrumentUnderlyingMapping, now)
                        )
                    )
                )
                .scalars()
                .all()
            )
        existing = set(existing_rows)
        to_close = existing - set(desired.keys()) - conflicted

        if not to_close:
            return 0

        if len(existing) > 0 and len(to_close) / len(existing) > _SAFETY_THRESHOLD:
            if not self._force:
                logger.warning(
                    f"Safety guard: {len(to_close)}/{len(existing)} mappings "
                    f"({len(to_close)/len(existing):.0%}) would be closed. "
                    f"Use --force to proceed."
                )
                return 0

        closed = 0
        for seq, ipid in enumerate(to_close, start=1):
            if await self._repo.close_instrument_underlying_mapping(
                instrument_public_id=ipid,
                session_id=session_id,
                sequence_id=seq,
                timestamp=now,
            ):
                closed += 1
        return closed

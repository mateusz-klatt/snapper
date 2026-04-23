"""Underlying asset updater — syncs YAML definitions to the database.

Reads underlying_mappings.yaml, matches patterns against active instruments,
and upserts UnderlyingAsset + InstrumentUnderlyingMapping rows via SCD2.
"""

import re
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Literal
from typing import Self

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
    description: str | None = None
    patterns: list[PatternRule]


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

    __slots__ = ("instrument_public_id", "exchange", "native_symbol")

    def __init__(self, instrument_public_id: str, exchange: str, native_symbol: str) -> None:
        self.instrument_public_id = instrument_public_id
        self.exchange = exchange
        self.native_symbol = native_symbol


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
        underlying_ids = await self._upsert_underlyings(config, now)
        await self._cleanup_stale_underlyings(config, now)
        instruments = await self._load_instruments(now)
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
        session_id = f"underlying-updater-{now:%Y%m%d%H%M%S}"

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
        """Close underlying assets that are no longer defined in YAML."""
        assert self._repo is not None
        yaml_tickers = {defn.ticker for defn in config.underlyings}
        active = await self._repo.get_underlying_assets(now)
        session_id = f"underlying-updater-{now:%Y%m%d%H%M%S}"
        stale = [row for row in active if row["ticker"] not in yaml_tickers]
        for seq, row in enumerate(stale, start=1):
            await self._repo.close_underlying_asset(
                public_id=row["public_id"],
                session_id=session_id,
                sequence_id=seq,
                timestamp=now,
            )
        if stale:
            logger.info(f"Closed {len(stale)} stale underlying(s) removed from YAML")

    async def _load_instruments(self, now: datetime) -> list[_InstrumentInfo]:
        """Load all active instruments with their native symbols."""
        assert self._repo is not None
        async with self._repo.session() as s:
            stmt = (
                select(
                    Instrument.public_id.label("instrument_public_id"),
                    Instrument.exchange,
                    Symbol.native_symbol,
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
            )
            for r in rows
        ]

    async def _match_and_upsert(
        self,
        config: UnderlyingMappingConfig,
        underlying_ids: dict[str, str],
        instruments: list[_InstrumentInfo],
        now: datetime,
    ) -> None:
        """Match instruments to underlyings and upsert/close mappings."""
        assert self._repo is not None
        session_id = f"underlying-updater-{now:%Y%m%d%H%M%S}"

        desired: dict[str, _MatchResult] = {}
        conflicted: set[str] = set()
        unmapped: list[tuple[str, str]] = []

        for inst in instruments:
            matches, has_intra_conflict = self._find_matches(config, inst)

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
        per_underlying: dict[str, list[PatternRule]] = {}

        for defn in config.underlyings:
            matching_rules: list[PatternRule] = []
            for rule in defn.patterns:
                if rule.exchange.value != inst.exchange:
                    continue
                if rule_matches(rule, inst.native_symbol):
                    matching_rules.append(rule)
            if matching_rules:
                per_underlying[defn.ticker] = matching_rules

        results: list[_MatchResult] = []
        has_intra_conflict = False
        for ticker, rules in per_underlying.items():
            first = rules[0]
            conflict = False
            for r in rules[1:]:
                if (
                    r.relationship_type != first.relationship_type
                    or r.contract_family != first.contract_family
                    or r.instrument_type != first.instrument_type
                    or r.expiry_override != first.expiry_override
                ):
                    logger.error(
                        f"Intra-underlying conflict for {inst.native_symbol} "
                        f"({inst.exchange}) in {ticker}: rules disagree on metadata"
                    )
                    conflict = True
                    has_intra_conflict = True
                    break
            if not conflict:
                results.append(
                    _MatchResult(
                        underlying_ticker=ticker,
                        relationship_type=first.relationship_type.value,
                        contract_family=first.contract_family,
                        instrument_type=first.instrument_type,
                        expiry_override=first.expiry_override,
                    )
                )

        return results, has_intra_conflict

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
        return replace(
            base_spec,
            expiry_at=match.expiry_override if needs_expiry else current_expiry,
            instrument_kind=match.instrument_type if needs_kind else current_kind,
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

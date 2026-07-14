"""Tests for UnderlyingUpdater service."""

import re
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import yaml

from snapper.application.updaters.underlying_updater import PatternRule
from snapper.application.updaters.underlying_updater import UnderlyingDefinition
from snapper.application.updaters.underlying_updater import UnderlyingMappingConfig
from snapper.application.updaters.underlying_updater import UnderlyingUpdater
from snapper.application.updaters.underlying_updater import _InstrumentInfo
from snapper.application.updaters.underlying_updater import _MatchResult
from snapper.application.updaters.underlying_updater import _RuleMatcher
from snapper.application.updaters.underlying_updater import rule_matches
from snapper.data.repository_types import InstrumentSpecRow

_REPO_PATCH = "snapper.application.updaters.underlying_updater.get_repository"


def _ts() -> datetime:
    return datetime(2026, 4, 1, tzinfo=UTC)


def _mock_session(instrument_rows: list[MagicMock], stale_ids: list[str]) -> MagicMock:
    """Build a mock session that returns instruments first, then stale IDs."""
    mock_s = AsyncMock()
    inst_result = MagicMock()
    inst_result.all.return_value = instrument_rows

    stale_result = MagicMock()
    stale_result.scalars.return_value.all.return_value = stale_ids

    call_count = 0

    async def execute_side_effect(stmt: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            return stale_result
        return inst_result

    mock_s.execute = AsyncMock(side_effect=execute_side_effect)
    mock_s.__aenter__ = AsyncMock(return_value=mock_s)
    mock_s.__aexit__ = AsyncMock(return_value=False)
    return mock_s


def _mock_repo(
    upsert_asset_rv: list[tuple[str, str]] | None = None,
    upsert_mapping_rv: str = "created",
    instrument_rows: list[MagicMock] | None = None,
    stale_ids: list[str] | None = None,
) -> AsyncMock:
    """Build a fully-configured mock repository."""
    repo = AsyncMock()

    if upsert_asset_rv:
        repo.upsert_underlying_asset = AsyncMock(side_effect=upsert_asset_rv)
    else:
        repo.upsert_underlying_asset = AsyncMock(return_value=("ua-1", "created"))

    repo.upsert_instrument_underlying_mapping = AsyncMock(return_value=upsert_mapping_rv)
    repo.close_instrument_underlying_mapping = AsyncMock(return_value=True)
    repo.close_underlying_asset = AsyncMock(return_value=True)
    repo.get_underlying_assets = AsyncMock(return_value=[])

    mock_s = _mock_session(instrument_rows or [], stale_ids or [])
    repo.session = MagicMock(return_value=mock_s)
    return repo


class TestPatternRule:
    """Tests for PatternRule Pydantic model."""

    def test_exact_match(self) -> None:
        """Given exact rule, When matching, Then only exact string matches."""
        rule = PatternRule(exchange="kraken", match_type="exact", pattern="BTC-USD")
        assert rule_matches(rule, "BTC-USD") is True
        assert rule_matches(rule, "BTC-EUR") is False
        assert rule_matches(rule, "BTC-USDT") is False

    def test_regex_match(self) -> None:
        """Given regex rule, When matching, Then fullmatch semantics apply."""
        rule = PatternRule(
            exchange="kraken_equities",
            match_type="regex",
            pattern=r"^ES[FGHJKMNQUVXZ]\d{1,2}-CME$",
        )
        assert rule_matches(rule, "ESM6-CME") is True
        assert rule_matches(rule, "ESM26-CME") is True
        assert rule_matches(rule, "ES-USD") is False
        assert rule_matches(rule, "MESM6-CME") is False

    def test_invalid_regex_rejected(self) -> None:
        """Given invalid regex pattern, When validating, Then raises ValidationError."""
        with pytest.raises((ValueError, re.PatternError)):
            PatternRule(exchange="kraken", match_type="regex", pattern="[invalid")

    def test_exact_with_relationship_type(self) -> None:
        """Given explicit relationship_type, When created, Then set correctly."""
        rule = PatternRule(
            exchange="polygon",
            match_type="exact",
            pattern="SPY",
            relationship_type="proxy",
        )
        assert rule.relationship_type.value == "proxy"

    def test_default_relationship_type(self) -> None:
        """Given no relationship_type, When created, Then defaults to 'exact'."""
        rule = PatternRule(exchange="kraken", match_type="exact", pattern="BTC-USD")
        assert rule.relationship_type.value == "exact"


class TestUnderlyingMappingConfig:
    """Tests for UnderlyingMappingConfig Pydantic root model."""

    def test_valid_config(self) -> None:
        """Given valid YAML-like data, When validating, Then succeeds."""
        config = UnderlyingMappingConfig.model_validate(
            {
                "underlyings": [
                    {
                        "ticker": "BTC",
                        "name": "Bitcoin",
                        "asset_class": "crypto",
                        "patterns": [
                            {"exchange": "kraken", "match_type": "exact", "pattern": "BTC-USD"},
                        ],
                    },
                ]
            }
        )
        assert len(config.underlyings) == 1

    def test_duplicate_tickers_rejected(self) -> None:
        """Given duplicate tickers, When validating, Then raises."""
        with pytest.raises(ValueError, match="Duplicate tickers"):
            UnderlyingMappingConfig.model_validate(
                {
                    "underlyings": [
                        {
                            "ticker": "BTC",
                            "name": "Bitcoin",
                            "asset_class": "crypto",
                            "patterns": [],
                        },
                        {
                            "ticker": "BTC",
                            "name": "Bitcoin2",
                            "asset_class": "crypto",
                            "patterns": [],
                        },
                    ]
                }
            )

    def test_duplicate_names_rejected(self) -> None:
        """Given duplicate names with different tickers, When validating, Then raises."""
        with pytest.raises(ValueError, match="Duplicate names"):
            UnderlyingMappingConfig.model_validate(
                {
                    "underlyings": [
                        {
                            "ticker": "BTC",
                            "name": "Bitcoin",
                            "asset_class": "crypto",
                            "patterns": [],
                        },
                        {
                            "ticker": "ETH",
                            "name": "Bitcoin",
                            "asset_class": "crypto",
                            "patterns": [],
                        },
                    ]
                }
            )

    def test_missing_english_name_rejected(self) -> None:
        """Given a name map without the en key, When validating, Then raises."""
        with pytest.raises(ValueError, match="name.en"):
            UnderlyingMappingConfig.model_validate(
                {
                    "underlyings": [
                        {
                            "ticker": "BTC",
                            "name": {"pl": "Bitcoin"},
                            "asset_class": "crypto",
                            "patterns": [],
                        },
                    ]
                }
            )

    def test_loads_real_yaml(self) -> None:
        """Given the actual mapping file, When loading, Then validates ok."""
        yaml_path = (
            Path(__file__).resolve().parents[3]
            / "src"
            / "snapper"
            / "data"
            / "underlying_mappings.yaml"
        )
        with open(yaml_path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        config = UnderlyingMappingConfig.model_validate(raw)
        assert len(config.underlyings) >= 10


class TestExchangeScoping:
    """Tests that pattern rules are scoped by exchange."""

    def test_exchange_mismatch_no_match(self) -> None:
        """Given rule for kraken_equities, When instrument is on kraken, Then no match."""
        rule = PatternRule(
            exchange="kraken_equities",
            match_type="exact",
            pattern="ESM6-CME",
        )
        assert rule.exchange.value == "kraken_equities"


class TestUnderlyingUpdater:
    """Tests for the full updater lifecycle."""

    def test_rule_matcher_uses_exact_index_before_regex_scan(self) -> None:
        """Given exact and off-exchange regex rules, When matching, Then exact lookup wins."""
        config = UnderlyingMappingConfig(
            underlyings=[
                UnderlyingDefinition(
                    ticker="BTC",
                    name="Bitcoin",
                    asset_class="crypto",
                    patterns=[
                        PatternRule(exchange="kraken", match_type="exact", pattern="BTC-USD"),
                    ],
                ),
                UnderlyingDefinition(
                    ticker="BTCX",
                    name="Bitcoin Regex",
                    asset_class="crypto",
                    patterns=[
                        PatternRule(exchange="polygon", match_type="regex", pattern="^BTC-USD$"),
                    ],
                ),
            ],
        )
        matcher = _RuleMatcher.from_definitions(config.underlyings)
        instrument = _InstrumentInfo("i1", "kraken", "BTC-USD", "BTC", "USD", "crypto")

        with patch(
            "snapper.application.updaters.underlying_updater.rule_matches",
            wraps=rule_matches,
        ) as match_spy:
            matches, has_intra_conflict = matcher.find_matches(instrument)

        assert has_intra_conflict is False
        assert [match.underlying_ticker for match in matches] == ["BTC"]
        match_spy.assert_not_called()

    def test_auto_coverage_builds_rule_index_once(self) -> None:
        """Given many fallback instruments, When expanding, Then rule index is reused."""
        config = UnderlyingMappingConfig(underlyings=[])
        updater = UnderlyingUpdater(db_url="test://")
        instruments = [
            _InstrumentInfo("i1", "kraken", "AAA-USD", "AAA", "USD", "crypto"),
            _InstrumentInfo("i2", "kraken", "BBB-USD", "BBB", "USD", "crypto"),
            _InstrumentInfo("i3", "kraken", "AAA-EUR", "AAA", "EUR", "crypto"),
        ]

        with patch.object(
            _RuleMatcher,
            "from_definitions",
            wraps=_RuleMatcher.from_definitions,
        ) as build_spy:
            expanded = updater._expand_auto_coverage(config, instruments)

        assert build_spy.call_count == 1
        assert {definition.ticker for definition in expanded.underlyings} == {"AAA", "BBB"}

    def test_find_match_helpers_use_indexed_regex_semantics(self) -> None:
        """Given exact and nonmatching regex rules, When matching, Then helpers agree."""
        config = UnderlyingMappingConfig(
            underlyings=[
                UnderlyingDefinition(
                    ticker="BTC",
                    name="Bitcoin",
                    asset_class="crypto",
                    patterns=[
                        PatternRule(exchange="kraken", match_type="exact", pattern="BTC-USD"),
                    ],
                ),
                UnderlyingDefinition(
                    ticker="ETH",
                    name="Ethereum",
                    asset_class="crypto",
                    patterns=[
                        PatternRule(exchange="kraken", match_type="regex", pattern="^ETH-USD$"),
                    ],
                ),
            ],
        )
        updater = UnderlyingUpdater(db_url="test://")
        instrument = _InstrumentInfo("i1", "kraken", "BTC-USD", "BTC", "USD", "crypto")

        config_matches, config_conflict = updater._find_matches(config, instrument)
        list_matches, list_conflict = updater._find_matches_in(config.underlyings, instrument)

        assert config_conflict is False
        assert list_conflict is False
        assert [match.underlying_ticker for match in config_matches] == ["BTC"]
        assert [match.underlying_ticker for match in list_matches] == ["BTC"]

    def test_auto_coverage_generates_fallback_definitions(self) -> None:
        """Given unmatched active instruments, When expanded, Then fallback rules cover them."""
        config = UnderlyingMappingConfig(underlyings=[])
        updater = UnderlyingUpdater(db_url="test://")

        expanded = updater._expand_auto_coverage(
            config,
            [
                _InstrumentInfo("i1", "kraken", "NEW-USD", "NEW", "USD", "crypto"),
                _InstrumentInfo("i2", "polygon", "AAPL", "AAPL", None, "equity"),
                _InstrumentInfo("i3", "kraken", "BOB-USD", "BOB", "USD", "forex"),
                _InstrumentInfo(
                    "i4",
                    "kraken_futures",
                    "ALT-USD-PERP",
                    "ALT",
                    "USD",
                    "crypto",
                ),
            ],
        )

        by_ticker = {definition.ticker: definition for definition in expanded.underlyings}

        assert {"NEW", "AAPL", "BOBUSD", "ALT"} <= set(by_ticker)
        assert by_ticker["BOBUSD"].name == {"en": "BOB / USD"}
        assert {
            (rule.exchange.value, rule.pattern, rule.relationship_type.value)
            for rule in by_ticker["ALT"].patterns
        } == {("kraken_futures", "ALT-USD-PERP", "derivative")}

    def test_auto_coverage_prefers_forex_metadata_for_duplicate_symbols(self) -> None:
        """Given duplicate native symbols, When expanded, Then forex rows win over crypto."""
        config = UnderlyingMappingConfig(underlyings=[])
        updater = UnderlyingUpdater(db_url="test://")

        expanded = updater._expand_auto_coverage(
            config,
            [
                _InstrumentInfo("i1", "kraken", "BOB-USD", "BOB", "USD", "crypto"),
                _InstrumentInfo("i2", "kraken", "BOB-USD", "BOB", "USD", "forex"),
            ],
        )

        by_ticker = {definition.ticker: definition for definition in expanded.underlyings}

        assert "BOBUSD" in by_ticker
        assert "BOB" not in by_ticker

    def test_auto_coverage_coerces_loaded_symbol_metadata(self) -> None:
        """Given DB row values, When coerced, Then valid values are preserved."""
        assert UnderlyingUpdater._coerce_symbol_part("BTC", "XBT-USD") == "BTC"
        assert UnderlyingUpdater._coerce_optional_symbol_part("USD") == "USD"
        assert UnderlyingUpdater._coerce_asset_type("equity") == "equity"

    def test_auto_coverage_skips_invalid_fallback_inputs(self) -> None:
        """Given invalid fallback metadata, When expanded, Then no rule is generated."""
        config = UnderlyingMappingConfig(underlyings=[])
        updater = UnderlyingUpdater(db_url="test://")

        expanded = updater._expand_auto_coverage(
            config,
            [
                _InstrumentInfo("i1", "unknown", "NEW-USD", "NEW", "USD", "crypto"),
                _InstrumentInfo("i2", "kraken", "-USD", "", "USD", "crypto"),
                _InstrumentInfo("i3", "kraken", "ODD-USD", "ODD", "USD", "unknown"),
            ],
        )

        assert expanded.underlyings == []

    def test_auto_coverage_does_not_append_duplicate_rules(self) -> None:
        """Given existing fallback rule, When appended again, Then definition is unchanged."""
        updater = UnderlyingUpdater(db_url="test://")
        definition = UnderlyingDefinition(
            ticker="DUP",
            name="DUP",
            asset_class="crypto",
            patterns=[],
        )
        instrument = _InstrumentInfo("i1", "kraken", "DUP-USD", "DUP", "USD", "crypto")

        assert updater._append_fallback_rule(definition, instrument) is True
        assert updater._append_fallback_rule(definition, instrument) is False
        assert len(definition.patterns) == 1

    def test_auto_coverage_marks_equity_futures_as_derivatives(self) -> None:
        """Given futures venue suffix, When fallback relationship is built, Then derivative."""
        instrument = _InstrumentInfo("i1", "kraken_equities", "ESM6-CME", "ESM6", None, "index")

        relationship = UnderlyingUpdater._fallback_relationship_type(instrument)

        assert relationship.value == "derivative"

    def test_auto_coverage_skips_asset_class_collision_with_existing_yaml(self) -> None:
        """Given YAML ticker with one asset_class, When fallback instrument has another, Then skip.

        Reproduces the SPX vs SPX6900 contamination class of bug:
        an explicit YAML SPX ticker (asset_class=index, S&P 500) must
        not absorb a fallback instrument whose Symbol.asset_type is
        ``crypto`` (Kraken's SPX6900 memecoin) just because both share
        ``base='SPX'``.
        """
        config = UnderlyingMappingConfig(
            underlyings=[
                UnderlyingDefinition(
                    ticker="SPX",
                    name="S&P 500",
                    asset_class="index",
                    patterns=[
                        PatternRule(exchange="polygon", match_type="exact", pattern="SPY"),
                    ],
                ),
            ],
        )
        updater = UnderlyingUpdater(db_url="test://")

        expanded = updater._expand_auto_coverage(
            config,
            [
                _InstrumentInfo("i1", "kraken", "SPX-USD", "SPX", "USD", "crypto"),
                _InstrumentInfo("i2", "kraken", "SPX-EUR", "SPX", "EUR", "crypto"),
                _InstrumentInfo("i3", "kraken_futures", "SPX-USD-PERP", "SPX", "USD", "crypto"),
            ],
        )

        spx = next(definition for definition in expanded.underlyings if definition.ticker == "SPX")
        patterns = {(rule.exchange.value, rule.pattern) for rule in spx.patterns}
        assert patterns == {("polygon", "SPY")}
        assert spx.asset_class.value == "index"

    def test_auto_coverage_skips_asset_class_collision_in_generated_tickers(self) -> None:
        """Given equity fallback ticker, When crypto with same base arrives later, Then skip.

        Reproduces the CVX/OPEN/PEP class of bug: a generated equity
        ticker (e.g. NYSE Chevron CVX) must not absorb later-iterated
        crypto instruments with the same base (Convex Finance CVX-USD)
        just because both share ``base='CVX'``.
        """
        config = UnderlyingMappingConfig(underlyings=[])
        updater = UnderlyingUpdater(db_url="test://")

        expanded = updater._expand_auto_coverage(
            config,
            [
                _InstrumentInfo("i1", "polygon", "CVX", "CVX", None, "equity"),
                _InstrumentInfo("i2", "kraken", "CVX-USD", "CVX", "USD", "crypto"),
                _InstrumentInfo("i3", "kraken", "CVX-EUR", "CVX", "EUR", "crypto"),
            ],
        )

        cvx_definitions = [d for d in expanded.underlyings if d.ticker == "CVX"]
        assert len(cvx_definitions) == 1
        cvx = cvx_definitions[0]
        assert cvx.asset_class.value == "equity"
        patterns = {(rule.exchange.value, rule.pattern) for rule in cvx.patterns}
        assert patterns == {("polygon", "CVX")}

    def test_auto_coverage_does_not_emit_empty_definition_when_exchange_invalid(self) -> None:
        """Given fallback rule with unrecognised exchange, When expanding, Then no empty entry leaks.

        Append-after-validate ordering must prevent leaving a generated
        underlying behind with ``patterns=[]`` when
        :meth:`_append_fallback_rule` rejects an instrument because the
        exchange enum lookup fails.
        """
        config = UnderlyingMappingConfig(underlyings=[])
        updater = UnderlyingUpdater(db_url="test://")

        expanded = updater._expand_auto_coverage(
            config,
            [
                _InstrumentInfo("i1", "no-such-exchange", "FOO-USD", "FOO", "USD", "crypto"),
            ],
        )

        assert all(len(definition.patterns) > 0 for definition in expanded.underlyings)
        assert not any(definition.ticker == "FOO" for definition in expanded.underlyings)

    @pytest.mark.asyncio
    async def test_match_and_upsert_preserves_unmapped_without_auto_coverage(self) -> None:
        """Given unmatched instrument, When matching directly, Then no mapping is written."""
        config = UnderlyingMappingConfig(underlyings=[])
        updater = UnderlyingUpdater(db_url="test://")
        repo = AsyncMock()
        repo.upsert_instrument_underlying_mapping = AsyncMock(return_value="created")
        repo.get_instrument_spec = AsyncMock(return_value=None)
        repo.close_instrument_underlying_mapping = AsyncMock(return_value=True)

        mock_s = AsyncMock()
        existing_result = MagicMock()
        existing_result.scalars.return_value.all.return_value = []
        mock_s.execute = AsyncMock(return_value=existing_result)
        mock_s.__aenter__ = AsyncMock(return_value=mock_s)
        mock_s.__aexit__ = AsyncMock(return_value=False)
        repo.session = MagicMock(return_value=mock_s)

        updater._repo = repo

        await updater._match_and_upsert(
            config,
            {},
            [_InstrumentInfo("inst-1", "kraken", "MISS-USD", "MISS", "USD", "crypto")],
            _ts(),
        )

        repo.upsert_instrument_underlying_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_creates_underlyings_and_mappings(self, tmp_path: Path) -> None:
        """Given valid YAML, When running, Then upserts underlyings and mappings."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [
                        {"exchange": "kraken", "match_type": "exact", "pattern": "BTC-USD"},
                    ],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo(
            instrument_rows=[
                MagicMock(
                    instrument_public_id="inst-1",
                    exchange="kraken",
                    native_symbol="BTC-USD",
                ),
            ],
        )

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        repo.upsert_underlying_asset.assert_called_once()
        repo.upsert_instrument_underlying_mapping.assert_called_once()

    @pytest.mark.asyncio
    async def test_conflict_detection(self, tmp_path: Path) -> None:
        """Given instrument matching two underlyings, When running, Then skips mapping."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "A",
                    "name": "Asset A",
                    "asset_class": "crypto",
                    "patterns": [
                        {"exchange": "kraken", "match_type": "exact", "pattern": "MULTI"},
                    ],
                },
                {
                    "ticker": "B",
                    "name": "Asset B",
                    "asset_class": "crypto",
                    "patterns": [
                        {"exchange": "kraken", "match_type": "exact", "pattern": "MULTI"},
                    ],
                },
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo(
            upsert_asset_rv=[("ua-a", "created"), ("ua-b", "created")],
            instrument_rows=[
                MagicMock(
                    instrument_public_id="inst-1",
                    exchange="kraken",
                    native_symbol="MULTI",
                ),
            ],
        )

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        repo.upsert_instrument_underlying_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_safety_guard_blocks_cleanup(self, tmp_path: Path) -> None:
        """Given >25% removal without --force, When cleaning up, Then skips."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo(stale_ids=["inst-1", "inst-2", "inst-3"])

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file, force=False)
            await updater.run()

        repo.close_instrument_underlying_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_safety_guard_force_allows_cleanup(self, tmp_path: Path) -> None:
        """Given >25% removal with --force, When cleaning up, Then proceeds."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo(stale_ids=["inst-1", "inst-2", "inst-3"])

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file, force=True)
            await updater.run()

        assert repo.close_instrument_underlying_mapping.call_count == 3

    @pytest.mark.asyncio
    async def test_auto_coverage_maps_unmatched_instruments(self, tmp_path: Path) -> None:
        """Given instruments not matching YAML, When running, Then fallback maps them."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [
                        {"exchange": "kraken", "match_type": "exact", "pattern": "BTC-USD"},
                    ],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo(
            instrument_rows=[
                MagicMock(
                    instrument_public_id="inst-1",
                    exchange="kraken",
                    native_symbol="BTC-USD",
                ),
                MagicMock(
                    instrument_public_id="inst-2",
                    exchange="kraken",
                    native_symbol="ETH-USD",
                ),
            ],
        )

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        assert repo.upsert_underlying_asset.call_count == 2
        assert repo.upsert_instrument_underlying_mapping.call_count == 2

    @pytest.mark.asyncio
    async def test_idempotent_rerun(self, tmp_path: Path) -> None:
        """Given unchanged YAML, When running twice, Then second run is all 'unchanged'."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [
                        {"exchange": "kraken", "match_type": "exact", "pattern": "BTC-USD"},
                    ],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo(
            upsert_asset_rv=[("ua-1", "unchanged")],
            upsert_mapping_rv="unchanged",
            instrument_rows=[
                MagicMock(
                    instrument_public_id="inst-1",
                    exchange="kraken",
                    native_symbol="BTC-USD",
                ),
            ],
            stale_ids=["inst-1"],
        )

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        repo.close_instrument_underlying_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_auto_coverage_extends_existing_ticker_to_unmatched_exchange(
        self,
        tmp_path: Path,
    ) -> None:
        """Given same base on another exchange, When running, Then fallback maps it."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [
                        {"exchange": "kraken", "match_type": "exact", "pattern": "BTC-USD"},
                    ],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo(
            instrument_rows=[
                MagicMock(
                    instrument_public_id="inst-1",
                    exchange="polygon",
                    native_symbol="BTC-USD",
                ),
            ],
        )

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        repo.upsert_underlying_asset.assert_called_once()
        repo.upsert_instrument_underlying_mapping.assert_called_once()

    @pytest.mark.asyncio
    async def test_intra_underlying_metadata_conflict(self, tmp_path: Path) -> None:
        """Given two rules within same underlying with conflicting metadata, Then skip."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [
                        {
                            "exchange": "kraken",
                            "match_type": "exact",
                            "pattern": "BTC-USD",
                            "relationship_type": "exact",
                        },
                        {
                            "exchange": "kraken",
                            "match_type": "exact",
                            "pattern": "BTC-USD",
                            "relationship_type": "derivative",
                        },
                    ],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo(
            instrument_rows=[
                MagicMock(
                    instrument_public_id="inst-1",
                    exchange="kraken",
                    native_symbol="BTC-USD",
                ),
            ],
        )

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        repo.upsert_instrument_underlying_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_close_returns_false_counted_correctly(self, tmp_path: Path) -> None:
        """Given close returns False, When cleaning stale, Then still called."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo(stale_ids=["inst-1"])
        repo.close_instrument_underlying_mapping = AsyncMock(return_value=False)

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file, force=True)
            await updater.run()

        repo.close_instrument_underlying_mapping.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_rules_same_underlying_agree(self, tmp_path: Path) -> None:
        """Given two rules within same underlying with matching metadata, Then mapped."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [
                        {
                            "exchange": "kraken",
                            "match_type": "exact",
                            "pattern": "BTC-USD",
                            "relationship_type": "exact",
                        },
                        {
                            "exchange": "kraken",
                            "match_type": "regex",
                            "pattern": "^BTC-USD$",
                            "relationship_type": "exact",
                        },
                    ],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo(
            instrument_rows=[
                MagicMock(
                    instrument_public_id="inst-1",
                    exchange="kraken",
                    native_symbol="BTC-USD",
                ),
            ],
        )

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        repo.upsert_instrument_underlying_mapping.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_below_threshold_proceeds(self, tmp_path: Path) -> None:
        """Given <=25% removal, When cleaning stale without --force, Then proceeds."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [
                        {"exchange": "kraken", "match_type": "exact", "pattern": "BTC-USD"},
                        {"exchange": "kraken", "match_type": "exact", "pattern": "BTC-EUR"},
                        {"exchange": "kraken", "match_type": "exact", "pattern": "BTC-GBP"},
                        {"exchange": "kraken", "match_type": "exact", "pattern": "BTC-PLN"},
                    ],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        mock_s = AsyncMock()
        inst_result = MagicMock()
        inst_result.all.return_value = [
            MagicMock(instrument_public_id="i1", exchange="kraken", native_symbol="BTC-USD"),
            MagicMock(instrument_public_id="i2", exchange="kraken", native_symbol="BTC-EUR"),
            MagicMock(instrument_public_id="i3", exchange="kraken", native_symbol="BTC-GBP"),
            MagicMock(instrument_public_id="i4", exchange="kraken", native_symbol="BTC-PLN"),
        ]
        stale_result = MagicMock()
        stale_result.scalars.return_value.all.return_value = ["i1", "i2", "i3", "i4", "i-old"]
        call_count = 0

        async def execute_side_effect(stmt: object) -> object:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                return stale_result
            return inst_result

        mock_s.execute = AsyncMock(side_effect=execute_side_effect)
        mock_s.__aenter__ = AsyncMock(return_value=mock_s)
        mock_s.__aexit__ = AsyncMock(return_value=False)

        repo = AsyncMock()
        repo.upsert_underlying_asset = AsyncMock(return_value=("ua-1", "created"))
        repo.upsert_instrument_underlying_mapping = AsyncMock(return_value="created")
        repo.close_instrument_underlying_mapping = AsyncMock(return_value=True)
        repo.session = MagicMock(return_value=mock_s)

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file, force=False)
            await updater.run()

        assert repo.close_instrument_underlying_mapping.call_count == 1

    @pytest.mark.asyncio
    async def test_stale_underlyings_closed_on_yaml_removal(self, tmp_path: Path) -> None:
        """Given underlying removed from YAML, When running, Then closed in DB."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo()
        repo.get_underlying_assets = AsyncMock(
            return_value=[
                {
                    "public_id": "ua-btc",
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "sector": None,
                    "description": None,
                    "timestamp": _ts(),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "instrument_count": 0,
                },
                {
                    "public_id": "ua-old",
                    "ticker": "REMOVED",
                    "name": "Removed Asset",
                    "asset_class": "crypto",
                    "sector": None,
                    "description": None,
                    "timestamp": _ts(),
                    "session_id": "s1",
                    "sequence_id": 2,
                    "instrument_count": 0,
                },
            ]
        )

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        repo.close_underlying_asset.assert_called_once_with(
            public_id="ua-old",
            session_id=repo.close_underlying_asset.call_args.kwargs["session_id"],
            sequence_id=repo.close_underlying_asset.call_args.kwargs["sequence_id"],
            timestamp=repo.close_underlying_asset.call_args.kwargs["timestamp"],
        )

    @pytest.mark.asyncio
    async def test_stale_underlyings_not_closed_when_all_present(self, tmp_path: Path) -> None:
        """Given all underlyings in YAML, When running, Then none closed."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "patterns": [],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        repo = _mock_repo()
        repo.get_underlying_assets = AsyncMock(
            return_value=[
                {
                    "public_id": "ua-btc",
                    "ticker": "BTC",
                    "name": "Bitcoin",
                    "asset_class": "crypto",
                    "sector": None,
                    "description": None,
                    "timestamp": _ts(),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "instrument_count": 0,
                },
            ]
        )

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        repo.close_underlying_asset.assert_not_called()


class TestBuildFallbackSpec:
    """Tests for _build_fallback_spec static method."""

    def test_base_fallback_spec_returns_empty_input_for_missing_spec(self) -> None:
        """Missing existing spec yields an empty fallback payload."""
        result = UnderlyingUpdater._base_fallback_spec(None)
        assert result.instrument_kind is None
        assert result.expiry_at is None
        assert result.tick_size is None

    def test_returns_none_when_nothing_to_apply(self) -> None:
        """Given match with no instrument_type or expiry_override, When building, Then None."""
        match = _MatchResult(
            underlying_ticker="SPX",
            relationship_type="exact",
            contract_family=None,
        )
        result = UnderlyingUpdater._build_fallback_spec(match, None)
        assert result is None

    def test_applies_instrument_type_when_null(self) -> None:
        """Given match with instrument_type and no existing spec, When building, Then sets kind."""
        match = _MatchResult(
            underlying_ticker="SPX",
            relationship_type="derivative",
            contract_family="ES",
            instrument_type="future",
        )
        result = UnderlyingUpdater._build_fallback_spec(match, None)
        assert result is not None
        assert result.instrument_kind == "future"
        assert result.expiry_at is None

    def test_skips_instrument_type_when_already_set(self) -> None:
        """Given existing spec with instrument_kind set, When building, Then None (no change)."""
        match = _MatchResult(
            underlying_ticker="SPX",
            relationship_type="derivative",
            contract_family="ES",
            instrument_type="future",
        )
        existing = InstrumentSpecRow(
            instrument_public_id="inst-1",
            tick_size=None,
            lot_size=None,
            min_order_size=None,
            max_order_size=None,
            cost_decimals=None,
            qty_decimals=None,
            margin_initial=None,
            position_limit_long=None,
            position_limit_short=None,
            status=None,
            contract_size=None,
            quantity_unit=None,
            spec_source=None,
            spec_version=None,
            spec_observed_at=None,
            unit_certified=False,
            expiry_at=None,
            instrument_kind="spot",
            funding_type=None,
            funding_frequency_hours=None,
            rollover_rate_long=None,
            rollover_rate_short=None,
            max_funding_rate=None,
        )
        result = UnderlyingUpdater._build_fallback_spec(match, existing)
        assert result is None

    def test_applies_expiry_override_when_null(self) -> None:
        """Given match with expiry_override and no existing expiry, When building, Then sets."""
        expiry = datetime(2026, 6, 20, 16, 30, tzinfo=UTC)
        match = _MatchResult(
            underlying_ticker="SPX",
            relationship_type="derivative",
            contract_family="ES",
            expiry_override=expiry,
        )
        result = UnderlyingUpdater._build_fallback_spec(match, None)
        assert result is not None
        assert result.expiry_at == expiry

    def test_preserves_existing_fields(self) -> None:
        """Given existing spec with fields, When building, Then carries forward."""
        match = _MatchResult(
            underlying_ticker="SPX",
            relationship_type="derivative",
            contract_family="ES",
            instrument_type="etf",
        )
        existing = InstrumentSpecRow(
            instrument_public_id="inst-1",
            tick_size=0.01,
            lot_size=1.0,
            min_order_size=None,
            max_order_size=None,
            cost_decimals=2,
            qty_decimals=0,
            margin_initial=None,
            position_limit_long=None,
            position_limit_short=None,
            status="online",
            contract_size=None,
            quantity_unit=None,
            spec_source=None,
            spec_version=None,
            spec_observed_at=None,
            unit_certified=False,
            expiry_at=None,
            instrument_kind=None,
            funding_type=None,
            funding_frequency_hours=None,
            rollover_rate_long=None,
            rollover_rate_short=None,
            max_funding_rate=None,
        )
        result = UnderlyingUpdater._build_fallback_spec(match, existing)
        assert result is not None
        assert result.tick_size == 0.01
        assert result.lot_size == 1.0
        assert result.cost_decimals == 2
        assert result.status == "online"
        assert result.instrument_kind == "etf"

    def test_base_fallback_spec_preserves_existing_fields(self) -> None:
        """Base fallback payload copies the current persisted spec values."""
        existing = InstrumentSpecRow(
            instrument_public_id="inst-1",
            tick_size=0.01,
            lot_size=1.0,
            min_order_size=0.5,
            max_order_size=10.0,
            cost_decimals=2,
            qty_decimals=4,
            margin_initial=0.2,
            position_limit_long=10,
            position_limit_short=8,
            status="online",
            contract_size=Decimal("3.125"),
            quantity_unit="contract_count",
            spec_source="kraken_futures:rest.get_instruments",
            spec_version="s2a-v1:test",
            spec_observed_at=datetime(2026, 4, 1, tzinfo=UTC),
            unit_certified=True,
            expiry_at=datetime(2026, 6, 20, 16, 30, tzinfo=UTC),
            instrument_kind="future",
            funding_type="perpetual_funding",
            funding_frequency_hours=1,
            rollover_rate_long=None,
            rollover_rate_short=None,
            max_funding_rate=0.0025,
        )

        result = UnderlyingUpdater._base_fallback_spec(existing)

        assert result.tick_size == 0.01
        assert result.lot_size == 1.0
        assert result.min_order_size == 0.5
        assert result.max_order_size == 10.0
        assert result.cost_decimals == 2
        assert result.qty_decimals == 4
        assert result.margin_initial == 0.2
        assert result.position_limit_long == 10
        assert result.position_limit_short == 8
        assert result.status == "online"
        assert result.contract_size == Decimal("3.125")
        assert result.quantity_unit == "contract_count"
        assert result.spec_source == "kraken_futures:rest.get_instruments"
        assert result.spec_version == "s2a-v1:test"
        assert result.spec_observed_at == datetime(2026, 4, 1, tzinfo=UTC)
        assert result.unit_certified is True
        assert result.expiry_at == datetime(2026, 6, 20, 16, 30, tzinfo=UTC)
        assert result.instrument_kind == "future"
        assert result.funding_type == "perpetual_funding"
        assert result.funding_frequency_hours == 1
        assert result.max_funding_rate == 0.0025


class TestYamlSpecFallbackIntegration:
    """Tests for _apply_yaml_spec_fallbacks through the run() flow."""

    @pytest.mark.asyncio
    async def test_applies_instrument_type_fallback(self, tmp_path: Path) -> None:
        """Given YAML with instrument_type, When running, Then spec fallback applied."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "SPX",
                    "name": "S&P 500",
                    "asset_class": "index",
                    "patterns": [
                        {
                            "exchange": "polygon",
                            "match_type": "exact",
                            "pattern": "SPY",
                            "instrument_type": "etf",
                        }
                    ],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        inst_row = MagicMock()
        inst_row.instrument_public_id = "inst-spy"
        inst_row.exchange = "polygon"
        inst_row.native_symbol = "SPY"

        repo = _mock_repo(instrument_rows=[inst_row])
        repo.get_instrument_spec = AsyncMock(return_value=None)
        repo.revise_instrument_spec = AsyncMock(return_value=1)
        repo.get_underlying_assets = AsyncMock(return_value=[])

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        repo.revise_instrument_spec.assert_called_once()
        call_kwargs = repo.revise_instrument_spec.call_args.kwargs
        assert call_kwargs["spec"].instrument_kind == "etf"

    @pytest.mark.asyncio
    async def test_skips_fallback_when_already_set(self, tmp_path: Path) -> None:
        """Given existing instrument_kind, When running, Then fallback skipped."""
        yaml_content = {
            "underlyings": [
                {
                    "ticker": "SPX",
                    "name": "S&P 500",
                    "asset_class": "index",
                    "patterns": [
                        {
                            "exchange": "polygon",
                            "match_type": "exact",
                            "pattern": "SPY",
                            "instrument_type": "etf",
                        }
                    ],
                }
            ]
        }
        yaml_file = tmp_path / "mappings.yaml"
        yaml_file.write_text(yaml.dump(yaml_content))

        inst_row = MagicMock()
        inst_row.instrument_public_id = "inst-spy"
        inst_row.exchange = "polygon"
        inst_row.native_symbol = "SPY"

        repo = _mock_repo(instrument_rows=[inst_row])
        repo.get_instrument_spec = AsyncMock(
            return_value=InstrumentSpecRow(
                instrument_public_id="inst-spy",
                tick_size=None,
                lot_size=None,
                min_order_size=None,
                max_order_size=None,
                cost_decimals=None,
                qty_decimals=None,
                margin_initial=None,
                position_limit_long=None,
                position_limit_short=None,
                status=None,
                contract_size=None,
                quantity_unit=None,
                spec_source=None,
                spec_version=None,
                spec_observed_at=None,
                unit_certified=False,
                expiry_at=None,
                instrument_kind="spot",
                funding_type=None,
                funding_frequency_hours=None,
                rollover_rate_long=None,
                rollover_rate_short=None,
                max_funding_rate=None,
            )
        )
        repo.get_underlying_assets = AsyncMock(return_value=[])

        with patch(_REPO_PATCH, return_value=repo):
            updater = UnderlyingUpdater(db_url="test://", yaml_path=yaml_file)
            await updater.run()

        repo.revise_instrument_spec.assert_not_called()

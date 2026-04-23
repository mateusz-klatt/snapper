"""Tests for UnderlyingUpdater service."""

import re
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import yaml

from snapper.application.updaters.underlying_updater import PatternRule
from snapper.application.updaters.underlying_updater import UnderlyingMappingConfig
from snapper.application.updaters.underlying_updater import UnderlyingUpdater
from snapper.application.updaters.underlying_updater import _MatchResult
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

    def test_loads_real_yaml(self) -> None:
        """Given the actual mapping file, When loading, Then validates ok."""
        yaml_path = (
            Path(__file__).resolve().parents[3]
            / "src"
            / "snapper"
            / "data"
            / "underlying_mappings.yaml"
        )
        with open(yaml_path) as f:
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
    async def test_unmapped_instruments_not_mapped(self, tmp_path: Path) -> None:
        """Given instruments not matching any pattern, When running, Then not mapped."""
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

        repo.upsert_instrument_underlying_mapping.assert_called_once()

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
    async def test_exchange_scoping_skips_wrong_exchange(self, tmp_path: Path) -> None:
        """Given pattern for kraken, When instrument on polygon, Then no match."""
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

        repo.upsert_instrument_underlying_mapping.assert_not_called()

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
            expiry_at=datetime(2026, 6, 20, 16, 30, tzinfo=UTC),
            instrument_kind="future",
            funding_type=None,
            funding_frequency_hours=None,
            rollover_rate_long=None,
            rollover_rate_short=None,
            max_funding_rate=None,
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
        assert result.expiry_at == datetime(2026, 6, 20, 16, 30, tzinfo=UTC)
        assert result.instrument_kind == "future"


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

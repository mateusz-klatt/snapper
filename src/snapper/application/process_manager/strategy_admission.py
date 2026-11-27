"""Validate ownership of live strategy shards from persisted process configs."""

from dataclasses import dataclass
from decimal import Decimal
from typing import cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.process_manager.config_resolver import load_current_process_configs
from snapper.application.process_manager.config_resolver import read_current_process_configs
from snapper.application.process_manager.process_parameters import StrategyProcessParameters
from snapper.application.process_manager.strategy_scope import StrategyProcessClassification
from snapper.application.process_manager.strategy_scope import StrategyScopeError
from snapper.application.process_manager.strategy_scope import classify_strategy_process
from snapper.application.process_manager.strategy_scope import (
    enforce_classified_strategy_scope_complete,
)
from snapper.application.process_manager.strategy_scope import strategy_registry_name
from snapper.core.json_types import JsonObject
from snapper.core.strategy_order_cap import LiveStrategyOrderPolicy
from snapper.core.strategy_order_cap import validate_strategy_order_cap
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.messaging.schemas.data import SignalData


@dataclass(frozen=True)
class LiveStrategyScope:
    """A live strategy's validated parameters and resolved wallet identity."""

    process_name: str
    parameters: StrategyProcessParameters
    wallet: UUID


def classify_process_config(name: str, config: JsonObject) -> StrategyProcessClassification:
    """Classify a persisted process with its original role and parameters.

    Args:
        name: Persisted process name.
        config: Stored process configuration.

    Returns:
        Strategy classification and any parameters subject to scope enforcement.
    """
    return classify_strategy_process(
        raw_role=config.get("role"),
        class_path=config["class"],
        raw_parameters=config.get("parameters", {}),
        registry_name=strategy_registry_name(name, config.get("template")),
    )


def live_strategy_parameters(
    classification: StrategyProcessClassification,
) -> StrategyProcessParameters | None:
    """Validate live strategy parameters and any supplied paper cap.

    Args:
        classification: Classification of the persisted strategy configuration.

    Returns:
        Validated parameters when the configuration is for a live strategy.
    """
    parameters = classification.parameters
    if parameters is None:
        return None
    try:
        if parameters.get("exchange", ExchangeEnum.PAPER) == ExchangeEnum.PAPER:
            validate_strategy_order_cap(ExchangeEnum.PAPER, parameters.get("max_order_base_qty"))
            return None
        return StrategyProcessParameters.model_validate(parameters)
    except ValueError as exc:
        raise StrategyScopeError(str(exc)) from exc


async def _resolve_live_scope(
    repository: Repository,
    name: str,
    classification: StrategyProcessClassification,
    operators: list[str] | None = None,
) -> LiveStrategyScope:
    """Resolve live strategy labels, defaults, grants and output coverage."""
    scope = await enforce_classified_strategy_scope_complete(
        repository,
        classification=classification,
        principal_operator_public_ids=operators,
        allow_admin_lookup_without_operator=False,
        allow_unscoped_paper=False,
        require_operator_for_explicit_wallet=True,
    )
    parameters = StrategyProcessParameters.model_validate(scope.parameters)
    return LiveStrategyScope(name, parameters, UUID(scope.wallet_public_id))


async def _live_shard_contender(
    repository: Repository,
    name: str,
    config: JsonObject,
    exchange: str,
    outputs: set[str],
) -> LiveStrategyScope | None:
    """Resolve an enabled strategy that may own one of the requested outputs."""
    if not config.get("enabled"):
        return None
    classification = classify_process_config(name, config)
    parameters = classification.parameters
    if parameters is None or parameters.get("exchange") != exchange:
        return None
    configured_outputs = parameters.get("outputs")
    if isinstance(configured_outputs, list) and not any(
        isinstance(output, str) and output in outputs for output in configured_outputs
    ):
        return None
    return await _resolve_live_scope(repository, name, classification)


async def _ensure_exclusive_outputs(
    repository: Repository,
    owner: LiveStrategyScope,
    current: dict[str, JsonObject],
) -> None:
    """Refuse an enabled live strategy with a competing wallet output."""
    parameters = owner.parameters
    outputs = set(parameters.outputs)
    for name, config in current.items():
        if name == owner.process_name:
            continue
        contender = await _live_shard_contender(
            repository, name, config, parameters.exchange, outputs
        )
        if contender is not None and contender.wallet == owner.wallet:
            raise StrategyScopeError(f"Live strategy outputs overlap enabled process '{name}'")


async def validate_process_config_write(
    repository: Repository,
    session: AsyncSession,
    name: str,
    config: JsonObject,
) -> None:
    """Require a valid cap and check enabled shard ownership under the write fence.

    Args:
        repository: Repository used to resolve wallet and grant scope.
        session: Transaction reserved for the process configuration write.
        name: Process name of the candidate version.
        config: Candidate configuration to validate before writing it.
    """
    classification = classify_process_config(name, config)
    if live_strategy_parameters(classification) is None or not config.get("enabled"):
        return
    current = await load_current_process_configs(session)
    owner = await _resolve_live_scope(repository, name, classification)
    await _ensure_exclusive_outputs(repository, owner, current)


async def resolve_live_strategy_start(
    repository: Repository,
    name: str,
    operators: list[str] | None = None,
) -> JsonObject:
    """Reload the enabled strategy and recheck all of its outputs before launch.

    Args:
        repository: Repository holding the current configs and scopes.
        name: Process name to launch.
        operators: Allowed operator identities for an authenticated caller, when present.

    Returns:
        Constructor parameters with resolved scope references.
    """
    current = await read_current_process_configs(repository)
    config = current.get(name)
    if config is None or not config.get("enabled"):
        raise StrategyScopeError(f"Enable live strategy '{name}' before starting it")
    classification = classify_process_config(name, config)
    if live_strategy_parameters(classification) is None:
        raise StrategyScopeError(f"Process '{name}' is no longer a live strategy")
    owner = await _resolve_live_scope(repository, name, classification, operators)
    await _ensure_exclusive_outputs(repository, owner, current)
    return cast(JsonObject, owner.parameters.model_dump(exclude_unset=True))


async def resolve_live_signal_policy(
    repository: Repository, signal: SignalData
) -> LiveStrategyOrderPolicy:
    """Resolve exactly one enabled owner for a live signal's full shard identity.

    Args:
        repository: Repository holding the current configs and scopes.
        signal: Routed live strategy signal.

    Returns:
        The configured strategy identity and its exact order quantity cap.
    """
    current = await read_current_process_configs(repository)
    wallet = UUID(signal.wallet_public_id)
    owners: list[LiveStrategyScope] = []
    for name, config in current.items():
        contender = await _live_shard_contender(
            repository, name, config, signal.exchange, {signal.instrument}
        )
        if contender is not None and contender.wallet == wallet:
            owners.append(contender)
    if len(owners) != 1:
        raise StrategyScopeError(f"Expected one live shard owner, found {len(owners)}")
    parameters = owners[0].parameters
    if (parameters.name, parameters.operator_public_id) != (
        signal.strategy_name,
        signal.operator_public_id,
    ):
        raise StrategyScopeError("Live signal does not match the strategy and operator")
    cap = parameters.max_order_base_qty
    assert cap is not None
    return LiveStrategyOrderPolicy(
        parameters.name,
        parameters.wallet_public_id,
        parameters.operator_public_id,
        Decimal(str(cap)),
    )

"""Shared strategy scope classification and wallet resolution."""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Protocol

from pydantic import ValidationError

from snapper.application.process_manager.process_parameters import StrategyProcessParameters
from snapper.application.process_manager.registry import get_registered_processes
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.core.wallet_resolution import WalletResolutionRepository
from snapper.core.wallet_resolution import resolve_wallet_or_default
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import OperatorRow

_STRATEGY_PARAMETER_KEYS = frozenset(("inputs", "outputs"))
_STRATEGY_OPERATOR_REQUIRED_DETAIL = (
    "operator_public_id required for live strategy wallet resolution"
)
_LABEL_REFERENCE_PREFIX = "label:"


class StrategyScopeError(ValueError):
    """Raised when a strategy launch cannot be classified or validated safely."""

    def __init__(self, detail: str) -> None:
        """Store a caller-facing diagnostic detail."""
        self.detail = detail
        super().__init__(detail)


class StrategyOperatorScopeError(StrategyScopeError):
    """Raised when a persisted strategy operator is outside caller scope."""


class StrategyGrantScopeError(StrategyScopeError):
    """Raised when the operator has no active wallet grant."""


class StrategyOutputCoverageError(StrategyScopeError):
    """Raised when strategy outputs are not fully grant-covered."""


class StrategyLabelInvalidError(StrategyScopeError):
    """Raised when a ``label:`` scope reference is blank after the prefix."""


class StrategyLabelUnresolvedError(StrategyScopeError):
    """Raised when a ``label:`` scope reference matches no active row."""


class StrategyLabelAmbiguousError(StrategyScopeError):
    """Raised when a ``label:`` scope reference matches multiple active rows."""


class StrategyScopeRepository(WalletResolutionRepository, Protocol):
    """Wallet-resolution repository extended with the operator catalogue.

    The strategy scope chokepoint resolves ``label:`` operator references
    against the active operator catalogue in addition to the wallet
    lookups already required for wallet autolookup.
    """

    async def list_active_operators(self, as_of: datetime) -> list[OperatorRow]:
        """Return active operator rows at ``as_of``.

        Args:
            as_of: Temporal anchor for the active-operator catalogue.

        Returns:
            Active operator rows in repository-defined deterministic order.
        """
        ...


@dataclass(frozen=True)
class StrategyProcessClassification:
    """Strategy classification result for persisted process parameters."""

    treat_as_strategy: bool
    parameters: dict[str, object] | None
    row_role: ProcessRoleEnum | None
    registry_role: ProcessRoleEnum | None


@dataclass(frozen=True)
class StrategyWalletScope:
    """Resolved strategy wallet scope ready for process launch."""

    treat_as_strategy: bool
    parameters: dict[str, object] | None
    operator_public_id: str
    wallet_public_id: str
    mode: ExecutionModeEnum | None


def parse_persisted_process_role(raw_role: object) -> ProcessRoleEnum | None:
    """Parse a persisted role value without accepting unknown roles.

    Args:
        raw_role: Persisted role value from a process configuration.

    Returns:
        Parsed role enum, or None when absent or unknown.
    """
    if isinstance(raw_role, ProcessRoleEnum):
        return raw_role
    if isinstance(raw_role, str) and raw_role:
        try:
            return ProcessRoleEnum(raw_role)
        except ValueError:
            return None
    return None


def has_strategy_parameter_shape(raw_parameters: object) -> bool:
    """Return whether persisted parameters carry strategy I/O fields.

    Args:
        raw_parameters: Persisted process constructor parameters.

    Returns:
        True when the parameters include the strategy I/O signature.
    """
    return isinstance(raw_parameters, dict) and _STRATEGY_PARAMETER_KEYS.issubset(raw_parameters)


def resolve_role_for_class_path(class_path: str) -> ProcessRoleEnum | None:
    """Look up a registered process entry by class path.

    Args:
        class_path: Fully qualified class path from persisted config.

    Returns:
        Registered process role, or None when no registry entry matches.
    """
    registry = get_registered_processes()
    for entry in registry.values():
        if entry.class_path == class_path:
            return entry.role
    return None


def _copy_string_keyed_parameters(raw_parameters: object) -> dict[str, object] | None:
    """Return a string-keyed parameter copy when ``raw_parameters`` is a dict."""
    if not isinstance(raw_parameters, dict):
        return None
    copied: dict[str, object] = {}
    for key, value in raw_parameters.items():
        if not isinstance(key, str):
            raise StrategyScopeError("invalid persisted strategy parameters")
        copied[key] = value
    return copied


def classify_strategy_process(
    *,
    raw_role: object,
    class_path: object,
    raw_parameters: object,
) -> StrategyProcessClassification:
    """Classify persisted process parameters for strategy scope enforcement.

    A positive strategy signal from either the persisted row role or the
    trusted registry role requires strategy enforcement. A non-strategy
    row role is only a self-claim; strategy-shaped parameters with no
    trusted registry classification fail closed.

    Args:
        raw_role: Persisted row role value.
        class_path: Persisted class path value.
        raw_parameters: Persisted constructor parameters.

    Returns:
        Strategy classification with copied parameters when enforcement applies.

    Raises:
        StrategyScopeError: The shape or role cannot be handled safely.
    """
    row_role = parse_persisted_process_role(raw_role)
    raw_role_present = raw_role is not None and raw_role != ""
    if raw_role_present and row_role is None and has_strategy_parameter_shape(raw_parameters):
        raise StrategyScopeError("invalid persisted process role for strategy launch")
    registry_role: ProcessRoleEnum | None = None
    if isinstance(class_path, str) and class_path:
        registry_role = resolve_role_for_class_path(class_path)
    treat_as_strategy = (
        row_role is ProcessRoleEnum.STRATEGY or registry_role is ProcessRoleEnum.STRATEGY
    )
    if (
        not treat_as_strategy
        and registry_role is None
        and has_strategy_parameter_shape(raw_parameters)
    ):
        raise StrategyScopeError("unable to classify persisted strategy process")
    parameters = _copy_string_keyed_parameters(raw_parameters)
    if treat_as_strategy and parameters is None:
        raise StrategyScopeError("invalid persisted strategy parameters")
    return StrategyProcessClassification(
        treat_as_strategy=treat_as_strategy,
        parameters=parameters if treat_as_strategy else None,
        row_role=row_role,
        registry_role=registry_role,
    )


def strategy_wallet_resolution_mode(parameters: dict[str, object]) -> ExecutionModeEnum:
    """Derive wallet resolution mode from validated strategy parameters.

    Args:
        parameters: Strategy process parameters to validate.

    Returns:
        Paper mode for paper exchange, otherwise live mode.

    Raises:
        StrategyScopeError: Parameters do not validate as strategy params.
    """
    try:
        validated = StrategyProcessParameters.model_validate(parameters)
    except ValidationError as exc:
        raise StrategyScopeError("invalid strategy parameters for wallet resolution") from exc
    return (
        ExecutionModeEnum.PAPER
        if validated.exchange == ExchangeEnum.PAPER
        else ExecutionModeEnum.LIVE
    )


def _empty_strategy_wallet_scope() -> StrategyWalletScope:
    """Return the scope sentinel used for non-strategy processes."""
    return StrategyWalletScope(False, None, "", "", None)


def _string_parameter(parameters: dict[str, object], key: str) -> str:
    """Return a string parameter value or the empty string."""
    value = parameters.get(key, "")
    if isinstance(value, str):
        return value
    return ""


def _raise_missing_strategy_operator() -> None:
    """Raise the canonical missing-operator strategy error."""
    raise StrategyScopeError(_STRATEGY_OPERATOR_REQUIRED_DETAIL)


def _resolve_no_operator_strategy_scope(
    parameters: dict[str, object],
    wallet_public_id: str,
    wallet_resolution_mode: ExecutionModeEnum,
    *,
    allow_admin_lookup_without_operator: bool,
    allow_unscoped_paper: bool,
    require_operator_for_explicit_wallet: bool,
) -> StrategyWalletScope | None:
    """Resolve or reject a strategy scope that has no operator."""
    if wallet_resolution_mode != ExecutionModeEnum.PAPER:
        if wallet_public_id:
            raise StrategyScopeError("wallet_public_id supplied without operator_public_id")
        _raise_missing_strategy_operator()
    if wallet_public_id:
        if require_operator_for_explicit_wallet:
            raise StrategyScopeError("wallet_public_id supplied without operator_public_id")
        return None
    if allow_unscoped_paper:
        return StrategyWalletScope(True, parameters, "", "", wallet_resolution_mode)
    if not allow_admin_lookup_without_operator:
        _raise_missing_strategy_operator()
    return None


def _enforce_operator_membership(
    operator_public_id: str,
    principal_operator_public_ids: list[str] | None,
) -> None:
    """Verify the chosen operator belongs to the caller scope."""
    if principal_operator_public_ids is None:
        return
    if not operator_public_id:
        return
    if operator_public_id in principal_operator_public_ids:
        return
    raise StrategyOperatorScopeError(operator_public_id)


def _wallet_lookup_operator_ids(operator_public_id: str) -> list[str]:
    """Return operator IDs for wallet lookup."""
    if operator_public_id:
        return [operator_public_id]
    return []


async def _resolve_missing_strategy_wallet(
    repository: WalletResolutionRepository,
    parameters: dict[str, object],
    operator_public_id: str,
    wallet_resolution_mode: ExecutionModeEnum,
) -> str:
    """Resolve and store the default wallet for a strategy scope."""
    wallet_public_id = await resolve_wallet_or_default(
        repository,
        explicit_wallet_public_id=None,
        operator_public_ids=_wallet_lookup_operator_ids(operator_public_id),
        is_admin=not operator_public_id,
        mode=wallet_resolution_mode,
    )
    parameters["wallet_public_id"] = wallet_public_id
    return wallet_public_id


def _label_from_reference(value: str) -> str:
    """Return the trimmed label from a ``label:`` reference value.

    Args:
        value: Parameter value already known to carry the label prefix.

    Returns:
        The non-empty label following the prefix.

    Raises:
        StrategyLabelInvalidError: The reference is blank after the prefix.
    """
    label = value.removeprefix(_LABEL_REFERENCE_PREFIX).strip()
    if not label:
        raise StrategyLabelInvalidError(f"blank label reference: {value!r}")
    return label


def _single_label_match(matches: list[str], *, kind: str, label: str) -> str:
    """Return the sole match or fail closed on zero or many.

    Args:
        matches: Candidate public IDs sharing the resolved label.
        kind: Reference kind for diagnostics (``operator`` or ``wallet``).
        label: The label being resolved, for diagnostics.

    Returns:
        The single matching public ID.

    Raises:
        StrategyLabelUnresolvedError: No active row matched the label.
        StrategyLabelAmbiguousError: More than one active row matched.
    """
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise StrategyLabelUnresolvedError(f"no active {kind} matches label {label!r}")
    raise StrategyLabelAmbiguousError(f"label {label!r} matches {len(matches)} active {kind}s")


async def _resolve_operator_label(
    repository: StrategyScopeRepository,
    value: str,
    principal_operator_public_ids: list[str] | None,
    as_of: datetime,
) -> str:
    """Resolve a ``label:`` operator reference to a canonical public ID.

    The label must match exactly one active operator, scope-qualified to
    the caller's operator memberships when supplied so a reference can
    never resolve outside the caller's authority.

    Args:
        repository: Repository exposing the active operator catalogue.
        value: Parameter value carrying the label prefix.
        principal_operator_public_ids: Caller operator memberships, or
            None for the trusted boot or admin path.
        as_of: Temporal anchor for the active-operator catalogue.

    Returns:
        The resolved operator public ID.

    Raises:
        StrategyLabelInvalidError: The reference is blank after the prefix.
        StrategyLabelUnresolvedError: No in-scope operator matched.
        StrategyLabelAmbiguousError: More than one in-scope operator matched.
    """
    label = _label_from_reference(value)
    operators = await repository.list_active_operators(as_of)
    matches = [
        operator["public_id"]
        for operator in operators
        if operator["label"] == label
        and (
            principal_operator_public_ids is None
            or operator["public_id"] in principal_operator_public_ids
        )
    ]
    return _single_label_match(matches, kind="operator", label=label)


async def _resolve_wallet_label(
    repository: StrategyScopeRepository,
    value: str,
    operator_public_id: str,
    wallet_resolution_mode: ExecutionModeEnum,
    as_of: datetime,
) -> str:
    """Resolve a ``label:`` wallet reference to a canonical public ID.

    The label must match exactly one active wallet of the resolution mode
    (paper vs live), scoped to the resolved operator's accessible set when
    an operator is pinned or the admin catalogue otherwise. The mode
    filter prevents a live strategy from binding a paper wallet, or the
    reverse, when a label collides across modes.

    Args:
        repository: Repository exposing the wallet catalogues.
        value: Parameter value carrying the label prefix.
        operator_public_id: Resolved operator scope, or empty for admin.
        wallet_resolution_mode: Paper or live wallet selection mode.
        as_of: Temporal anchor for the wallet catalogue.

    Returns:
        The resolved wallet public ID.

    Raises:
        StrategyLabelInvalidError: The reference is blank after the prefix.
        StrategyLabelUnresolvedError: No in-scope wallet of the mode matched.
        StrategyLabelAmbiguousError: More than one in-scope wallet matched.
    """
    label = _label_from_reference(value)
    if operator_public_id:
        wallets = await repository.list_accessible_wallets_for_operators(
            [operator_public_id], as_of
        )
    else:
        wallets = await repository.list_active_wallets(as_of)
    want_paper = wallet_resolution_mode == ExecutionModeEnum.PAPER
    matches = [
        wallet["public_id"]
        for wallet in wallets
        if wallet["label"] == label and wallet["is_paper"] == want_paper
    ]
    return _single_label_match(matches, kind="wallet", label=label)


async def _resolve_scope_reference_labels(
    repository: StrategyScopeRepository,
    parameters: dict[str, object],
    wallet_resolution_mode: ExecutionModeEnum,
    principal_operator_public_ids: list[str] | None,
) -> None:
    """Rewrite ``label:`` operator and wallet references in place.

    Only ``label:``-prefixed values are touched: empty strings and any
    other value (a canonical public ID or a legacy explicit ID) pass
    through unchanged, preserving the empty-wallet autolookup and
    explicit-ID paths. Resolution is fail-closed — zero or multiple active
    matches raise rather than silently mis-scoping the launch. The
    operator reference resolves first so a wallet reference can then match
    within the resolved operator's accessible set.

    Args:
        repository: Repository exposing operator and wallet catalogues.
        parameters: Mutable strategy parameters rewritten in place.
        wallet_resolution_mode: Paper or live wallet selection mode.
        principal_operator_public_ids: Caller operator memberships, or
            None for the trusted boot or admin path.
    """
    as_of = datetime.now(UTC)
    operator_value = _string_parameter(parameters, "operator_public_id")
    if operator_value.startswith(_LABEL_REFERENCE_PREFIX):
        parameters["operator_public_id"] = await _resolve_operator_label(
            repository, operator_value, principal_operator_public_ids, as_of
        )
    wallet_value = _string_parameter(parameters, "wallet_public_id")
    if wallet_value.startswith(_LABEL_REFERENCE_PREFIX):
        parameters["wallet_public_id"] = await _resolve_wallet_label(
            repository,
            wallet_value,
            _string_parameter(parameters, "operator_public_id"),
            wallet_resolution_mode,
            as_of,
        )


async def resolve_classified_strategy_scope(
    repository: StrategyScopeRepository,
    *,
    classification: StrategyProcessClassification,
    principal_operator_public_ids: list[str] | None,
    allow_admin_lookup_without_operator: bool,
    allow_unscoped_paper: bool,
    require_operator_for_explicit_wallet: bool,
) -> StrategyWalletScope:
    """Resolve wallet scope for an already-classified strategy process.

    Args:
        repository: Repository exposing wallet-resolution lookups.
        classification: Classification from :func:`classify_strategy_process`.
        principal_operator_public_ids: Optional caller operator memberships.
        allow_admin_lookup_without_operator: Use the active wallet catalogue
            when no operator is pinned for non-live strategy launches.
        allow_unscoped_paper: Preserve legacy paper launches with no
            operator and no wallet.
        require_operator_for_explicit_wallet: Reject explicit wallets
            without an operator.

    Returns:
        Resolved wallet scope. Non-strategies return an empty scope.

    Raises:
        StrategyScopeError: Strategy parameters cannot be validated safely.
        StrategyOperatorScopeError: The strategy operator is outside caller scope.
        WalletUnresolvedError: No wallet matched the lookup scope.
        WalletAmbiguousError: More than one wallet matched the lookup scope.
    """
    if not classification.treat_as_strategy or classification.parameters is None:
        return _empty_strategy_wallet_scope()
    parameters = dict(classification.parameters)
    operator_public_id = _string_parameter(parameters, "operator_public_id")
    wallet_public_id = _string_parameter(parameters, "wallet_public_id")
    wallet_resolution_mode = strategy_wallet_resolution_mode(parameters)
    if operator_public_id.startswith(_LABEL_REFERENCE_PREFIX) or wallet_public_id.startswith(
        _LABEL_REFERENCE_PREFIX
    ):
        await _resolve_scope_reference_labels(
            repository, parameters, wallet_resolution_mode, principal_operator_public_ids
        )
        operator_public_id = _string_parameter(parameters, "operator_public_id")
        wallet_public_id = _string_parameter(parameters, "wallet_public_id")
    if not operator_public_id:
        scope = _resolve_no_operator_strategy_scope(
            parameters,
            wallet_public_id,
            wallet_resolution_mode,
            allow_admin_lookup_without_operator=allow_admin_lookup_without_operator,
            allow_unscoped_paper=allow_unscoped_paper,
            require_operator_for_explicit_wallet=require_operator_for_explicit_wallet,
        )
        if scope is not None:
            return scope
    _enforce_operator_membership(operator_public_id, principal_operator_public_ids)
    if not wallet_public_id:
        wallet_public_id = await _resolve_missing_strategy_wallet(
            repository,
            parameters,
            operator_public_id,
            wallet_resolution_mode,
        )
    return StrategyWalletScope(
        True,
        parameters,
        operator_public_id,
        wallet_public_id,
        wallet_resolution_mode,
    )


async def enforce_wallet_grant_exists(
    repository: SQLAlchemyRepository,
    operator_public_id: str,
    wallet_public_id: str,
    as_of: datetime,
) -> None:
    """Verify the operator holds at least one active grant on the wallet.

    Args:
        repository: SQL repository with scope-grant reads.
        operator_public_id: Operator that will launch the strategy.
        wallet_public_id: Resolved wallet that will be passed to the strategy.
        as_of: Temporal anchor for active grant reads.

    Raises:
        StrategyGrantScopeError: The operator has no active grant on the wallet.
    """
    grants = await repository.list_active_scope_grants_for_wallet(
        wallet_public_id=wallet_public_id,
        as_of=as_of,
    )
    matching = [grant for grant in grants if grant["operator_public_id"] == operator_public_id]
    if matching:
        return
    raise StrategyGrantScopeError(
        f"Operator '{operator_public_id}' has no active scope grant on "
        f"wallet '{wallet_public_id}'"
    )


async def enforce_strategy_outputs_covered(
    repository: SQLAlchemyRepository,
    parameters: dict[str, object],
    operator_public_id: str,
    wallet_public_id: str,
    as_of: datetime,
) -> None:
    """Verify every live strategy output instrument is grant-covered.

    Args:
        repository: SQL repository with instrument and grant coverage reads.
        parameters: Validated strategy parameters after wallet resolution.
        operator_public_id: Operator that will launch the strategy.
        wallet_public_id: Resolved wallet that will be passed to the strategy.
        as_of: Temporal anchor for active grant reads.

    Raises:
        StrategyOutputCoverageError: At least one output is not covered.
    """
    raw_outputs = parameters.get("outputs", [])
    raw_exchange = parameters.get("exchange", "")
    if not isinstance(raw_outputs, list) or not isinstance(raw_exchange, str):
        return
    outputs = [output for output in raw_outputs if isinstance(output, str)]
    if not outputs or not raw_exchange or raw_exchange == ExchangeEnum.PAPER:
        return
    covered = await repository.list_grant_covered_instrument_public_ids(
        operator_public_id=operator_public_id,
        wallet_public_id=wallet_public_id,
        as_of=as_of,
    )
    uncovered = await find_uncovered_outputs(
        repository,
        outputs,
        raw_exchange,
        covered,
        as_of,
    )
    if not uncovered:
        return
    raise StrategyOutputCoverageError(
        f"Operator '{operator_public_id}' has no active grant covering "
        f"instruments {sorted(uncovered)} on wallet '{wallet_public_id}'"
    )


async def find_uncovered_outputs(
    repository: SQLAlchemyRepository,
    outputs: list[str],
    exchange: str,
    covered: set[str],
    as_of: datetime,
) -> list[str]:
    """Return output symbols whose resolved instrument is not grant-covered.

    Args:
        repository: SQL repository with symbol-to-instrument reads.
        outputs: Strategy output native symbols.
        exchange: Exchange used by the strategy.
        covered: Instrument public IDs covered by the active grant set.
        as_of: Temporal anchor for instrument resolution.

    Returns:
        Output symbols not covered by the grant set.
    """
    instrument_public_ids = await repository.get_instrument_public_ids_by_symbols(
        native_symbols=set(outputs),
        exchange=exchange,
        as_of=as_of,
    )
    return [symbol for symbol in outputs if instrument_public_ids.get(symbol) not in covered]


async def enforce_classified_strategy_scope_complete(
    repository: StrategyScopeRepository,
    *,
    classification: StrategyProcessClassification,
    principal_operator_public_ids: list[str] | None,
    allow_admin_lookup_without_operator: bool,
    allow_unscoped_paper: bool,
    require_operator_for_explicit_wallet: bool,
) -> StrategyWalletScope:
    """Resolve and fully enforce scope for an already-classified strategy.

    The complete enforcement sequence is shared by REST start and boot
    autostart: classify upstream, resolve the final wallet, then enforce
    active grant existence and strategy output coverage for SQL repositories.

    Args:
        repository: Repository used for wallet and optional SQL grant reads.
        classification: Strategy classification result.
        principal_operator_public_ids: Optional caller operator memberships.
        allow_admin_lookup_without_operator: Use active wallet catalogue
            when no operator is pinned.
        allow_unscoped_paper: Preserve legacy unscoped paper launches.
        require_operator_for_explicit_wallet: Reject explicit wallets
            without an operator.

    Returns:
        Resolved strategy scope with parameters ready for launch.

    Raises:
        StrategyScopeError: Strategy classification or validation failed.
        StrategyOperatorScopeError: The strategy operator is outside caller scope.
        StrategyGrantScopeError: No active grant exists for the final pair.
        StrategyOutputCoverageError: Live output instruments are not covered.
    """
    scope = await resolve_classified_strategy_scope(
        repository,
        classification=classification,
        principal_operator_public_ids=principal_operator_public_ids,
        allow_admin_lookup_without_operator=allow_admin_lookup_without_operator,
        allow_unscoped_paper=allow_unscoped_paper,
        require_operator_for_explicit_wallet=require_operator_for_explicit_wallet,
    )
    if (
        not scope.treat_as_strategy
        or scope.parameters is None
        or not scope.operator_public_id
        or not scope.wallet_public_id
        or not isinstance(repository, SQLAlchemyRepository)
    ):
        return scope
    as_of = datetime.now(UTC)
    await enforce_wallet_grant_exists(
        repository,
        scope.operator_public_id,
        scope.wallet_public_id,
        as_of,
    )
    await enforce_strategy_outputs_covered(
        repository,
        scope.parameters,
        scope.operator_public_id,
        scope.wallet_public_id,
        as_of,
    )
    return scope


async def resolve_strategy_process_scope(
    repository: StrategyScopeRepository,
    *,
    raw_role: object,
    class_path: object,
    raw_parameters: object,
    principal_operator_public_ids: list[str] | None,
    allow_admin_lookup_without_operator: bool,
    allow_unscoped_paper: bool,
    require_operator_for_explicit_wallet: bool,
) -> StrategyWalletScope:
    """Classify a persisted process and resolve strategy wallet scope.

    Args:
        repository: Repository exposing wallet-resolution lookups.
        raw_role: Persisted row role value.
        class_path: Persisted class path value.
        raw_parameters: Persisted constructor parameters.
        principal_operator_public_ids: Optional caller operator memberships.
        allow_admin_lookup_without_operator: Use active wallet catalogue
            when no operator is pinned.
        allow_unscoped_paper: Preserve legacy paper launches with no
            operator and no wallet.
        require_operator_for_explicit_wallet: Reject explicit wallets
            without an operator.

    Returns:
        Resolved strategy wallet scope, or empty scope for non-strategies.

    Raises:
        StrategyScopeError: Classification or validation failed closed.
        StrategyOperatorScopeError: Operator is outside caller scope.
        WalletUnresolvedError: No wallet matched the lookup scope.
        WalletAmbiguousError: More than one wallet matched the lookup scope.
    """
    classification = classify_strategy_process(
        raw_role=raw_role,
        class_path=class_path,
        raw_parameters=raw_parameters,
    )
    return await resolve_classified_strategy_scope(
        repository,
        classification=classification,
        principal_operator_public_ids=principal_operator_public_ids,
        allow_admin_lookup_without_operator=allow_admin_lookup_without_operator,
        allow_unscoped_paper=allow_unscoped_paper,
        require_operator_for_explicit_wallet=require_operator_for_explicit_wallet,
    )


async def enforce_strategy_process_scope_complete(
    repository: StrategyScopeRepository,
    *,
    raw_role: object,
    class_path: object,
    raw_parameters: object,
    principal_operator_public_ids: list[str] | None,
    allow_admin_lookup_without_operator: bool,
    allow_unscoped_paper: bool,
    require_operator_for_explicit_wallet: bool,
) -> StrategyWalletScope:
    """Classify, resolve, and fully enforce strategy process scope.

    Args:
        repository: Repository used for wallet and optional SQL grant reads.
        raw_role: Persisted or registry role signal.
        class_path: Persisted process class path.
        raw_parameters: Persisted process constructor parameters.
        principal_operator_public_ids: Optional caller operator memberships.
        allow_admin_lookup_without_operator: Use active wallet catalogue
            when no operator is pinned.
        allow_unscoped_paper: Preserve legacy unscoped paper launches.
        require_operator_for_explicit_wallet: Reject explicit wallets
            without an operator.

    Returns:
        Resolved strategy scope, or empty scope for non-strategies.
    """
    classification = classify_strategy_process(
        raw_role=raw_role,
        class_path=class_path,
        raw_parameters=raw_parameters,
    )
    return await enforce_classified_strategy_scope_complete(
        repository,
        classification=classification,
        principal_operator_public_ids=principal_operator_public_ids,
        allow_admin_lookup_without_operator=allow_admin_lookup_without_operator,
        allow_unscoped_paper=allow_unscoped_paper,
        require_operator_for_explicit_wallet=require_operator_for_explicit_wallet,
    )

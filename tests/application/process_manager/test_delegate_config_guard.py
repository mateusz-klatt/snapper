"""Tests for the delegate process reference-only configuration boundary."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

import snapper.application.process_manager.delegate_config_guard as guard_module
import snapper.application.process_manager.registry_syncer as registry_syncer_module
import snapper.application.process_manager.spawner as spawner_module
from snapper.application.process_manager.delegate_config_guard import DelegateConfigReferenceError
from snapper.application.process_manager.delegate_config_guard import is_delegate_config
from snapper.application.process_manager.delegate_config_guard import (
    validate_delegate_config_references,
)
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import DelegateProcessParameters
from snapper.application.process_manager.registry_syncer import ProcessRegistrySyncer
from snapper.application.process_manager.spawner import _build_process_command
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum

_DELEGATE_NAME = "delegate_runner"


class _RegisteredProcess(RegisterableProcess):
    async def start(self) -> None:
        """Satisfy the managed-process contract for registry metadata."""


def _entry(tags: tuple[str, ...], delegate_model: bool) -> ProcessRegistryEntry:
    return ProcessRegistryEntry(
        class_ref=_RegisteredProcess,
        class_path="tests.RegisteredProcess",
        method="start",
        description="Test process",
        priority=50,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=tags,
        parameters_model=DelegateProcessParameters if delegate_model else None,
        parameters_schema=None,
        enabled=False,
        mode=ProcessModeEnum.PROCESS,
    )


def _delegate_entry() -> ProcessRegistryEntry:
    return _entry(("delegate", "runner"), True)


def _valid_parameters() -> JsonObject:
    return {
        "model_alias": "research-primary",
        "base_url": "https://delegate.invalid/v1",
        "api_key_file": "/run/secrets/delegate/api_key",
        "delegate_token_file": "/run/secrets/delegate/delegate_token",
        "max_tool_rounds": 4,
    }


def _replace(field: str, value: JsonValue) -> JsonObject:
    parameters = _valid_parameters()
    parameters[field] = value
    return parameters


def _launcher() -> ProcessLauncherService:
    settings = AppSettings(
        BootstrapSettingsLoader(DB_URL="sqlite+aiosqlite:///:memory:"),
        settings_service=None,
    )
    return ProcessLauncherService(settings)


def _config(
    name: str,
    template: str | None,
    parameters: JsonObject,
    tags: tuple[str, ...] = (),
) -> ProcessConfigModel:
    return ProcessConfigModel(
        name=name,
        enabled=True,
        mode=ProcessModeEnum.PROCESS,
        class_path="tests.RegisteredProcess",
        method="start",
        parameters=parameters,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=tags,
        template=template,
    )


def _install_registry(
    monkeypatch: pytest.MonkeyPatch,
    entries: dict[str, ProcessRegistryEntry],
) -> None:
    monkeypatch.setattr(guard_module, "get_registered_processes", lambda: entries)


def _mock_update_repository(
    monkeypatch: pytest.MonkeyPatch,
    stored_config: JsonObject,
) -> tuple[ProcessRegistrySyncer, AsyncMock]:
    settings = AppSettings(
        BootstrapSettingsLoader(DB_URL="sqlite+aiosqlite:///:memory:"),
        settings_service=None,
    )
    syncer = ProcessRegistrySyncer(settings)
    existing = SimpleNamespace(
        value=json.dumps(stored_config),
        category="process",
        description=None,
        is_encrypted=False,
    )
    result = MagicMock()
    result.scalar_one_or_none.return_value = existing
    session = MagicMock()
    session.execute = AsyncMock(return_value=result)
    session.commit = AsyncMock()
    repository = MagicMock()
    repository.session.return_value.__aenter__ = AsyncMock(return_value=session)
    repository.session.return_value.__aexit__ = AsyncMock(return_value=False)
    close_and_insert = AsyncMock()

    def _get_repository(db_url: str) -> MagicMock:
        del db_url
        return repository

    monkeypatch.setattr(registry_syncer_module, "get_repository", _get_repository)
    monkeypatch.setattr(registry_syncer_module, "close_and_insert", close_and_insert)
    return syncer, close_and_insert


_REJECTED_PARAMETERS: tuple[JsonObject, ...] = (
    {**_valid_parameters(), "unexpected": "value"},
    _replace("model_alias", ""),
    _replace("model_alias", 7),
    _replace("model_alias", "https://delegate.invalid/model"),
    _replace("model_alias", "sk-live-credential-shaped-value"),
    _replace("base_url", ""),
    _replace("base_url", 7),
    _replace("base_url", " https://delegate.invalid/v1"),
    _replace("base_url", "https://delegate .invalid/v1"),
    _replace("base_url", "http://delegate.invalid/v1"),
    _replace("base_url", "https://user:password@delegate.invalid/v1"),
    _replace("base_url", "https://delegate.invalid/v1?token=secret"),
    _replace("base_url", "https://delegate.invalid/v1#secret"),
    _replace("base_url", "https://delegate.invalid:invalid/v1"),
    _replace("base_url", "https://delegate.invalid:0/v1"),
    _replace("base_url", "https:///v1"),
    _replace("api_key_file", ""),
    _replace("api_key_file", None),
    _replace("api_key_file", "Bearer inline-credential-value"),
    _replace("api_key_file", "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo1MjM0NTY="),
    _replace("api_key_file", "sk-live-inline-credential"),
    _replace("api_key_file", "/sk-live-inline-credential-shaped-value"),
    _replace("api_key_file", "relative/secrets/api_key"),
    _replace("api_key_file", "/run/secrets/../api_key"),
    _replace("api_key_file", "/run/secrets/api key"),
    _replace("api_key_file", "/run/secrets/api\x00key"),
    _replace(
        "delegate_token_file",
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkZWxlZ2F0ZSJ9.c2lnbmF0dXJl",
    ),
    _replace("delegate_token_file", "/run/secrets/delegate/"),
    _replace("delegate_token_file", "/"),
    _replace("max_tool_rounds", 0),
    _replace("max_tool_rounds", 9),
    _replace("max_tool_rounds", True),
    _replace("max_tool_rounds", "4"),
)


@pytest.mark.parametrize("parameters", _REJECTED_PARAMETERS)
def test_delegate_validator_rejects_non_reference_shapes(
    parameters: JsonObject,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delegate values outside the reference grammar fail closed.

    Given: Registry metadata for a delegate and a secret-shaped or malformed field,
    When: The reference-only validator examines the complete parameter mapping,
    Then: A dedicated boundary exception rejects it without serialization.
    """
    _install_registry(monkeypatch, {_DELEGATE_NAME: _delegate_entry()})

    with pytest.raises(DelegateConfigReferenceError):
        validate_delegate_config_references(_DELEGATE_NAME, None, parameters)


@pytest.mark.parametrize(
    ("name", "template"),
    (
        (_DELEGATE_NAME, None),
        ("delegate_research_primary", _DELEGATE_NAME),
    ),
)
def test_delegate_validator_accepts_reference_only_config(
    name: str,
    template: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Valid delegate references pass for native and configured copies.

    Given: An authoritative delegate entry and the complete reference-only shape,
    When: A registry-native or template-derived process is validated,
    Then: Validation returns without changing the parameter values.
    """
    parameters = (
        _replace("base_url", "https://delegate.invalid:443/v1")
        if template is not None
        else _valid_parameters()
    )
    before = dict(parameters)
    _install_registry(monkeypatch, {_DELEGATE_NAME: _delegate_entry()})

    validate_delegate_config_references(name, template, parameters)

    assert parameters == before


@pytest.mark.parametrize(
    ("name", "template", "entries", "expected"),
    (
        (_DELEGATE_NAME, None, {_DELEGATE_NAME: _delegate_entry()}, True),
        (
            "delegate_research_primary",
            _DELEGATE_NAME,
            {_DELEGATE_NAME: _delegate_entry()},
            True,
        ),
        ("ordinary", None, {"ordinary": _entry(("core",), False)}, False),
        (
            "partial",
            None,
            {"partial": _entry(("delegate", "runner"), False)},
            False,
        ),
        ("missing", None, {}, False),
        (
            _DELEGATE_NAME,
            "ordinary_template",
            {
                _DELEGATE_NAME: _delegate_entry(),
                "ordinary_template": _entry(("core",), False),
            },
            False,
        ),
    ),
)
def test_delegate_predicate_uses_authoritative_registry_metadata(
    name: str,
    template: str | None,
    entries: dict[str, ProcessRegistryEntry],
    expected: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delegate classification uses only the owning registry entry.

    Given: Native, template-derived, partial, ordinary, or absent registry metadata,
    When: The public delegate configuration predicate classifies the process,
    Then: Only entries with both delegate tags and the exact parameter model match.
    """
    _install_registry(monkeypatch, entries)

    assert is_delegate_config(name, template) is expected


@pytest.mark.parametrize(
    "entry",
    (
        _entry(("delegate", "runner"), False),
        _entry(("core",), True),
        _entry(("core",), False),
    ),
)
def test_registry_requires_delegate_tags_and_parameter_model(
    entry: ProcessRegistryEntry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Partial or absent delegate metadata leaves other configs untouched.

    Given: A registry entry lacking the tag pair, the exact model, or both,
    When: Secret-shaped arbitrary parameters cross the conditional validator,
    Then: The mapping passes unchanged because the entry is not a delegate.
    """
    parameters: JsonObject = {"inline_secret": "sentinel-secret-value"}
    _install_registry(monkeypatch, {"ordinary": entry})

    validate_delegate_config_references("ordinary", None, parameters)

    assert parameters == {"inline_secret": "sentinel-secret-value"}


def test_missing_registry_entry_leaves_unclassified_config_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent registry entry is never replaced by mutable config metadata.

    Given: No authoritative registry entry for a delegate-tagged stored config,
    When: Arbitrary parameters cross the conditional validator,
    Then: The guard does not infer delegate identity from untrusted metadata.
    """
    parameters: JsonObject = {"inline_secret": "sentinel-secret-value"}
    _install_registry(monkeypatch, {})

    validate_delegate_config_references("unregistered", None, parameters)

    assert parameters == {"inline_secret": "sentinel-secret-value"}


def test_registry_template_takes_precedence_over_configured_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The explicit source template owns delegate classification.

    Given: A configured name colliding with a non-delegate registry entry,
    When: Its explicit source template is the authoritative delegate entry,
    Then: Secret-shaped parameters are rejected despite the name collision.
    """
    _install_registry(
        monkeypatch,
        {
            "ordinary": _entry(("core",), False),
            _DELEGATE_NAME: _delegate_entry(),
        },
    )

    with pytest.raises(DelegateConfigReferenceError):
        validate_delegate_config_references(
            "ordinary",
            _DELEGATE_NAME,
            {"inline_secret": "sentinel-secret-value"},
        )


def test_non_delegate_template_takes_precedence_over_delegate_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-delegate source template remains outside the boundary.

    Given: A configured name colliding with the native delegate registry key,
    When: Its explicit source template identifies an ordinary process instead,
    Then: Arbitrary parameters pass unchanged because roles and names are irrelevant.
    """
    parameters: JsonObject = {"inline_secret": "sentinel-secret-value"}
    _install_registry(
        monkeypatch,
        {
            _DELEGATE_NAME: _delegate_entry(),
            "ordinary_template": _entry(("core",), False),
        },
    )

    validate_delegate_config_references(_DELEGATE_NAME, "ordinary_template", parameters)

    assert parameters == {"inline_secret": "sentinel-secret-value"}


def test_exception_message_never_echoes_rejected_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Boundary failures expose only one generic diagnostic.

    Given: A unique inline credential that must be treated as sensitive,
    When: Delegate validation raises its dedicated exception,
    Then: Neither the exception text nor representation contains the credential.
    """
    rejected_value = "sk-unique-do-not-echo-7ca895c59084"
    parameters = _replace("api_key_file", rejected_value)
    _install_registry(monkeypatch, {_DELEGATE_NAME: _delegate_entry()})

    with pytest.raises(DelegateConfigReferenceError) as caught:
        validate_delegate_config_references(_DELEGATE_NAME, None, parameters)

    assert rejected_value not in str(caught.value)
    assert rejected_value not in repr(caught.value)
    assert str(caught.value) == (
        "Delegate process configuration violates the reference-only boundary"
    )


def test_delegate_parameter_model_caps_tool_rounds() -> None:
    """The delegate parameter schema advertises the same upper bound.

    Given: A structurally typed delegate payload with nine tool rounds,
    When: The registered Pydantic parameter model validates the payload,
    Then: Validation fails instead of advertising a looser spawn contract.
    """
    with pytest.raises(ValidationError):
        DelegateProcessParameters.model_validate(_replace("max_tool_rounds", 9))


@pytest.mark.asyncio
async def test_create_persistence_rejects_delegate_values_before_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delegate creation rejects inline values before persistence.

    Given: A configured copy of the delegate template carrying an inline key,
    When: The launcher is asked to create its persistent configuration,
    Then: The registry writer is never reached and the boundary error escapes.
    """
    launcher = _launcher()
    writer = AsyncMock()
    launcher._registry_syncer.create_process_config = writer
    _install_registry(monkeypatch, {_DELEGATE_NAME: _delegate_entry()})

    with pytest.raises(DelegateConfigReferenceError):
        await launcher.create_process_config(
            name="delegate_research_primary",
            class_path="tests.RegisteredProcess",
            method="start",
            enabled=True,
            mode=ProcessModeEnum.PROCESS,
            parameters=_replace("api_key_file", "sk-inline-create-secret"),
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            template=_DELEGATE_NAME,
        )

    writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_writer_independently_rejects_delegate_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The concrete persistence writer enforces the delegate boundary.

    Given: A direct writer call for a configured delegate with an inline key,
    When: Registry synchronization is asked to serialize the new config,
    Then: Validation rejects it before any repository lookup or database write.
    """
    settings = AppSettings(
        BootstrapSettingsLoader(DB_URL="sqlite+aiosqlite:///:memory:"),
        settings_service=None,
    )
    syncer = ProcessRegistrySyncer(settings)
    repository_lookup = MagicMock()
    _install_registry(monkeypatch, {_DELEGATE_NAME: _delegate_entry()})
    monkeypatch.setattr(registry_syncer_module, "get_repository", repository_lookup)

    with pytest.raises(DelegateConfigReferenceError):
        await syncer.create_process_config(
            name="delegate_research_primary",
            class_path="tests.RegisteredProcess",
            method="start",
            enabled=True,
            mode=ProcessModeEnum.PROCESS,
            parameters=_replace("api_key_file", "sk-inline-writer-secret"),
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            template=_DELEGATE_NAME,
        )

    repository_lookup.assert_not_called()


@pytest.mark.asyncio
async def test_create_writer_leaves_non_delegate_parameters_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The concrete writer preserves non-delegate configuration behavior.

    Given: An ordinary registry entry with arbitrary secret-shaped parameters,
    When: The registry writer directly persists the process configuration,
    Then: Its JSON contains the original mapping and the write commits normally.
    """
    settings = AppSettings(
        BootstrapSettingsLoader(DB_URL="sqlite+aiosqlite:///:memory:"),
        settings_service=None,
    )
    syncer = ProcessRegistrySyncer(settings)
    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    session = MagicMock()
    session.execute = AsyncMock(return_value=result)
    session.commit = AsyncMock()
    repository = MagicMock()
    repository.session.return_value.__aenter__ = AsyncMock(return_value=session)
    repository.session.return_value.__aexit__ = AsyncMock(return_value=False)
    parameters: JsonObject = {"inline_secret": "sentinel-secret-value"}
    _install_registry(monkeypatch, {"ordinary": _entry(("core",), False)})

    def _get_repository(db_url: str) -> MagicMock:
        del db_url
        return repository

    monkeypatch.setattr(registry_syncer_module, "get_repository", _get_repository)

    await syncer.create_process_config(
        name="ordinary",
        class_path="tests.RegisteredProcess",
        method="start",
        enabled=True,
        mode=ProcessModeEnum.PROCESS,
        parameters=parameters,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=("delegate", "runner"),
    )

    added_setting = session.add.call_args.args[0]
    persisted = json.loads(added_setting.value)
    assert persisted["parameters"] == parameters
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_persistence_leaves_non_delegate_parameters_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-delegate creation preserves the existing persistence behavior.

    Given: An ordinary registry entry and arbitrary secret-shaped parameters,
    When: The launcher creates that non-delegate process configuration,
    Then: The writer receives the original mapping without boundary rejection.
    """
    launcher = _launcher()
    writer = AsyncMock()
    launcher._registry_syncer.create_process_config = writer
    parameters: JsonObject = {"inline_secret": "sentinel-secret-value"}
    _install_registry(monkeypatch, {"ordinary": _entry(("core",), False)})

    await launcher.create_process_config(
        name="ordinary",
        class_path="tests.RegisteredProcess",
        method="start",
        enabled=True,
        mode=ProcessModeEnum.PROCESS,
        parameters=parameters,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=("delegate", "runner"),
    )

    awaited_write = writer.await_args
    if awaited_write is None:
        pytest.fail("Expected the non-delegate persistence writer to run")
    assert awaited_write.kwargs["parameters"] is parameters


@pytest.mark.asyncio
async def test_update_persistence_rejects_delegate_values_before_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delegate updates resolve stored template identity before persistence.

    Given: A configured delegate copy whose mutable stored tags are absent,
    When: Its full parameters are replaced with an inline delegate token,
    Then: The temporal writer is not called and the boundary error escapes.
    """
    stored_config: JsonObject = {
        "template": _DELEGATE_NAME,
        "parameters": _valid_parameters(),
    }
    syncer, close_and_insert = _mock_update_repository(monkeypatch, stored_config)
    _install_registry(monkeypatch, {_DELEGATE_NAME: _delegate_entry()})

    with pytest.raises(DelegateConfigReferenceError):
        await syncer.update_process_config_parameters(
            name="delegate_research_primary",
            parameters=_replace("delegate_token_file", "inline-delegate-token"),
            updated_by="operator",
        )

    close_and_insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_persistence_leaves_non_delegate_parameters_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-delegate updates preserve arbitrary parameter mappings.

    Given: An ordinary persisted process with arbitrary parameters,
    When: The temporal parameter-update path stores a secret-shaped mapping,
    Then: The same mapping is returned and serialized without delegate checks.
    """
    stored_config: JsonObject = {"parameters": {}}
    syncer, close_and_insert = _mock_update_repository(monkeypatch, stored_config)
    parameters: JsonObject = {"inline_secret": "sentinel-secret-value"}
    _install_registry(monkeypatch, {"ordinary": _entry(("core",), False)})

    returned = await syncer.update_process_config_parameters(
        name="ordinary",
        parameters=parameters,
        updated_by="operator",
    )

    awaited_write = close_and_insert.await_args
    if awaited_write is None:
        pytest.fail("Expected the non-delegate temporal writer to run")
    persisted = json.loads(awaited_write.kwargs["new_values"]["value"])
    assert returned == parameters
    assert persisted["parameters"] == parameters


@pytest.mark.asyncio
async def test_run_persistence_rejects_delegate_values_before_recorder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run creation cannot record an invalid delegate parameter set.

    Given: A template-derived delegate run carrying an inline API key,
    When: The launcher attempts to create its run-history record,
    Then: The error is not swallowed and the run recorder remains untouched.
    """
    launcher = _launcher()
    recorder = AsyncMock(return_value="run-id")
    launcher._create_process_run_record = recorder
    config = _config(
        "delegate_research_primary",
        _DELEGATE_NAME,
        _replace("api_key_file", "sk-inline-run-secret"),
    )
    _install_registry(monkeypatch, {_DELEGATE_NAME: _delegate_entry()})

    with pytest.raises(DelegateConfigReferenceError):
        await launcher._try_create_run_record(config)

    recorder.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_persistence_leaves_non_delegate_parameters_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-delegate run creation records arbitrary parameters unchanged.

    Given: A non-delegate registry entry despite delegate-shaped mutable tags,
    When: The launcher creates a process run record,
    Then: Its original parameters reach the recorder exactly as before.
    """
    launcher = _launcher()
    recorder = AsyncMock(return_value="run-id")
    launcher._create_process_run_record = recorder
    parameters: JsonObject = {"inline_secret": "sentinel-secret-value"}
    config = _config("ordinary", None, parameters, tags=("delegate", "runner"))
    _install_registry(monkeypatch, {"ordinary": _entry(("core",), False)})

    public_id = await launcher._try_create_run_record(config)

    awaited_record = recorder.await_args
    if awaited_record is None:
        pytest.fail("Expected the non-delegate run recorder to run")
    recorded_parameters = awaited_record.args[1]["parameters"]
    assert public_id == "run-id"
    assert recorded_parameters is parameters


def test_argv_serialization_rejects_delegate_values_before_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delegate argv serialization fails before JSON sees an inline value.

    Given: A template-derived delegate carrying an inline API credential,
    When: The spawner builds the process command,
    Then: The JSON serializer is never called and the boundary error escapes.
    """
    serializer = MagicMock()
    _install_registry(monkeypatch, {_DELEGATE_NAME: _delegate_entry()})
    monkeypatch.setattr(spawner_module.json, "dumps", serializer)

    with pytest.raises(DelegateConfigReferenceError):
        _build_process_command(
            "delegate_research_primary",
            "tests.RegisteredProcess",
            "start",
            _replace("api_key_file", "sk-inline-argv-secret"),
            template_name=_DELEGATE_NAME,
        )

    serializer.assert_not_called()


def test_argv_serialization_leaves_non_delegate_parameters_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-delegate argv serialization keeps its established payload.

    Given: An ordinary registry entry and arbitrary secret-shaped parameters,
    When: The spawner builds the subprocess command despite delegate-like role data,
    Then: The JSON config contains the same unrestricted parameter mapping.
    """
    parameters: JsonObject = {"inline_secret": "sentinel-secret-value"}
    _install_registry(monkeypatch, {"ordinary": _entry(("core",), False)})

    command = _build_process_command(
        "ordinary",
        "tests.RegisteredProcess",
        "start",
        parameters,
        role=ProcessRoleEnum.CORE,
    )

    serialized = json.loads(command[-1])
    assert serialized["parameters"] == parameters

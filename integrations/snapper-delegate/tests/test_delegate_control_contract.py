"""Replay the delegate-control corpus against the Python implementation.

Generated wire types keep the frame shape aligned across languages, but nothing
generated can prove that two implementations *decide* the same way. The corpus
in ``fixtures/delegate-control-v1.json`` is the behavioural half of that
contract: a drift caught here is a drift that would otherwise surface as one
client working while another silently held.

The corpus lives beside the implementation that owns the protocol rather than
in the public plugin repository. Nothing else replays it yet — the server side
of this protocol is not built — and a contract published before the peer that
must honour it exists would be a version number without an agreement behind it.
When the backend and the TypeScript plugin implement the protocol, each takes a
copy and a drift test against this file, which is what makes the corpus shared
in the sense that matters.
"""

from pathlib import Path
from typing import Literal

import pytest
from pydantic import BaseModel
from pydantic import ConfigDict

from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper_delegate.delegate_control import CONTROL_PROTOCOL_VERSION
from snapper_delegate.delegate_control import ControlDirective
from snapper_delegate.delegate_control import ControlGate
from snapper_delegate.delegate_control import ControlState
from snapper_delegate.delegate_control import applied_echo
from snapper_delegate.delegate_control import parse_control_directive

_CORPUS_PATH = Path(__file__).resolve().parent / "fixtures" / "delegate-control-v1.json"
_REQUIRED_CASES = frozenset(
    {
        "boot_directive_in_auth_complete",
        "live_hold_push",
        "live_resume_push",
        "malformed_boot_without_revision",
        "unknown_state_is_refused",
        "negative_revision_is_refused",
        "stale_revision_after_hold",
        "client_applied_echo",
        "reconnect_resets_to_held",
    }
)

type _PayloadSource = Literal["auth_complete.control", "control_frame", "client_echo"]


class _CorpusModel(BaseModel):
    """Reject any corpus field this implementation does not understand."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _Directive(_CorpusModel):
    """One control state paired with the revision that ordered it."""

    state: str
    revision: int


class _Expectation(_CorpusModel):
    """What every implementation must conclude for one wire case."""

    parsed: bool
    directive: _Directive | None
    accepts_consults: bool
    applied_revision: int | None
    echo: JsonObject | None


class _Case(_CorpusModel):
    """One canonical wire case with its transport and prior history."""

    name: str
    intent: str
    payload_source: _PayloadSource
    applied_before: list[_Directive]
    reset_before: bool = False
    """Whether a reconnect clears the replayed history before the case frame."""

    frame: JsonObject
    expect: _Expectation


class _Corpus(_CorpusModel):
    """The versioned corpus every implementation of the protocol replays."""

    version: str
    purpose: str
    rules: dict[str, str]
    cases: list[_Case]


_CORPUS = _Corpus.model_validate_json(_CORPUS_PATH.read_bytes())


def _directive(directive: _Directive) -> ControlDirective:
    """Rebuild one already-trusted corpus directive.

    Args:
        directive: State and revision recorded in the corpus.

    Returns:
        The directive a gate can adopt directly.
    """
    return ControlDirective(state=ControlState(directive.state), revision=directive.revision)


def _payload(case: _Case) -> JsonValue:
    """Return the directive body this case's transport carries.

    Args:
        case: One corpus case.

    Returns:
        The raw payload handed to the parser, which is absent when the auth
        frame carries no control block.
    """
    if case.payload_source == "auth_complete.control":
        return case.frame.get("control")
    return case.frame


def test_the_corpus_pins_the_protocol_and_every_required_case() -> None:
    """The shared corpus cannot silently lose the version or a case it pins.

    Given the corpus file consumed by every implementation of this protocol,
    When it is loaded by the Python runner,
    Then it declares the protocol version this code implements and still names
    every scenario the contract depends on, so truncation fails loudly here
    rather than reappearing as a client that quietly disagrees.
    """
    names = [case.name for case in _CORPUS.cases]

    assert _CORPUS.version == CONTROL_PROTOCOL_VERSION
    assert len(names) == len(set(names))
    assert _REQUIRED_CASES.issubset(names)


@pytest.mark.parametrize("case", _CORPUS.cases, ids=[case.name for case in _CORPUS.cases])
def test_the_python_control_gate_agrees_with_the_shared_corpus(case: _Case) -> None:
    """Every canonical wire case decides the same way in this implementation.

    Given a fresh gate replaying one case's prior directives,
    When the case payload is parsed and applied,
    Then the parse outcome, the resulting duty, the applied revision, and the
    echo all match what the corpus obliges every implementation to conclude.
    """
    gate = ControlGate()
    for prior in case.applied_before:
        gate.apply(_directive(prior))
    if case.reset_before:
        gate.reset_for_reconnect()

    if case.payload_source != "client_echo":
        parsed = parse_control_directive(_payload(case))
        expected = None if case.expect.directive is None else _directive(case.expect.directive)
        assert (parsed is not None) is case.expect.parsed
        assert parsed == expected
        gate.apply(parsed)

    applied = gate.applied
    echo = None if applied is None else applied_echo(applied)
    assert gate.accepts_consults is case.expect.accepts_consults
    assert (None if applied is None else applied.revision) == case.expect.applied_revision
    assert echo == case.expect.echo
    if case.payload_source == "client_echo":
        assert echo == case.frame

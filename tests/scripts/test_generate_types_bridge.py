"""Tests for the ``--bridge`` mode of scripts/generate_types.py.

Covers the bridge wire-contract emitter end-to-end:

    - allow-list selection produces 19 deterministic interfaces
    - class names preserved verbatim from the backend Pydantic source
    - alphabetical class ordering (after the ``FrameEnvelope`` base)
    - per-class field order matches Pydantic dataclass declaration order
    - envelope fields are excluded from per-class bodies (live on FrameEnvelope)
    - header-only docs (no per-interface JSDoc / forbidden tokens)
    - type discriminator is emitted as a string literal
    - JSON-Schema → TypeScript type mapping covers the cases the
      allow-listed classes actually produce
    - CLI flag wiring (--bridge / --bridge-output)
"""

from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from scripts.generate_types import _BRIDGE_HEADER
from scripts.generate_types import _BRIDGE_OUTPUT_DEFAULT
from scripts.generate_types import GenerateTypesArgs
from scripts.generate_types import _bridge_allowlist
from scripts.generate_types import _bridge_envelope_spec
from scripts.generate_types import _bridge_render_class
from scripts.generate_types import _bridge_render_field
from scripts.generate_types import _bridge_render_type
from scripts.generate_types import _bridge_resolve_ref
from scripts.generate_types import _run_bridge_generator
from scripts.generate_types import generate_bridge_wire_contract
from scripts.generate_types import main
from snapper.api.schemas.base import StrictDataSchema
from snapper.interface.websocket.schemas import WSAuthFailedResponse
from snapper.interface.websocket.schemas import WSAuthRequiredResponse
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.messaging.schemas.data import AiReviewRequestFrameData


def _make_args(**overrides: object) -> GenerateTypesArgs:
    """Build a fully-populated CLI args object for the runner under test."""
    args = GenerateTypesArgs()
    args.openapi = False
    args.export = False
    args.frontend = False
    args.frontend_ws = False
    args.frontend_api = False
    args.entities = False
    args.permissions = False
    args.ios = False
    args.bridge = False
    args.bridge_output = None
    args.strip_eslint_disable = False
    args.postprocess_openapi_types = False
    args.all = False
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


class TestBridgeAllowList:
    """Allow-list shape and ordering invariants."""

    def test_allowlist_contains_expected_19_classes(self) -> None:
        """The allow-list MUST be exactly the 19 classes the bridge consumes."""
        names = [name for name, _ in _bridge_allowlist()]
        assert names == sorted(names), "allow-list must be alphabetical"
        expected = {
            "AiReviewCapsViolationFrameData",
            "AiReviewDecisionAckFrameData",
            "AiReviewRequestFrameData",
            "OrderEventData",
            "SignalData",
            "WSAuthCompleteResponse",
            "WSAuthExpiredResponse",
            "WSAuthFailedResponse",
            "WSAuthOkResponse",
            "WSAuthRequiredResponse",
            "WSAuthenticateRequest",
            "WSErrorResponse",
            "WSPingRequest",
            "WSPongResponse",
            "WSReauthOkResponse",
            "WSReauthRequest",
            "WSReauthRequiredResponse",
            "WSSubscribeRequest",
            "WSSubscriptionSuccessResponse",
        }
        assert set(names) == expected

    def test_allowlist_pairs_are_pydantic_classes(self) -> None:
        """Every allow-list entry MUST resolve to a BaseModel subclass."""
        for name, model in _bridge_allowlist():
            assert issubclass(model, BaseModel)
            assert model.__name__ == name


class TestBridgeRenderType:
    """JSON-Schema → TypeScript mapping for the property shapes the bridge sees."""

    def test_string_maps_to_string(self) -> None:
        """Plain string property maps to the TS ``string`` keyword."""
        assert _bridge_render_type({"type": "string"}, {}) == "string"

    def test_integer_and_number_map_to_number(self) -> None:
        """Both integer and number map to the TS ``number`` keyword."""
        assert _bridge_render_type({"type": "integer"}, {}) == "number"
        assert _bridge_render_type({"type": "number"}, {}) == "number"

    def test_boolean_maps_to_boolean(self) -> None:
        """Boolean maps to the TS ``boolean`` keyword."""
        assert _bridge_render_type({"type": "boolean"}, {}) == "boolean"

    def test_object_maps_to_record_unknown(self) -> None:
        """Object schemas (no properties) map to ``Readonly<Record<string, unknown>>``."""
        assert _bridge_render_type({"type": "object"}, {}) == "Readonly<Record<string, unknown>>"

    def test_string_enum_maps_to_string_literal_union(self) -> None:
        """String enums become a ``"a" | "b"`` literal union, alphabetised by source."""
        prop = {"type": "string", "enum": ["buy", "sell"]}
        assert _bridge_render_type(prop, {}) == '"buy" | "sell"'

    def test_string_const_maps_to_string_literal(self) -> None:
        """Const strings map to a single string-literal type."""
        assert _bridge_render_type({"const": "hello"}, {}) == '"hello"'

    def test_array_of_string_maps_to_readonly_array(self) -> None:
        """Arrays use the ``readonly T[]`` form for immutability hints."""
        prop = {"type": "array", "items": {"type": "string"}}
        assert _bridge_render_type(prop, {}) == "readonly string[]"

    def test_anyof_with_null_yields_nullable_type(self) -> None:
        """``str | None`` from Pydantic emits as ``string | null``."""
        prop = {"anyOf": [{"type": "string"}, {"type": "null"}]}
        assert _bridge_render_type(prop, {}) == "string | null"

    def test_ref_resolves_through_defs(self) -> None:
        """``$ref`` follows the ``$defs`` map to render the referent type."""
        defs = {"UserRole": {"type": "string", "enum": ["viewer", "admin"]}}
        prop = {"$ref": "#/$defs/UserRole"}
        assert _bridge_render_type(prop, defs) == '"viewer" | "admin"'

    def test_unknown_const_value_raises(self) -> None:
        """Non-string const values are not supported on the bridge surface."""
        with pytest.raises(ValueError, match="string const"):
            _bridge_render_type({"const": 42}, {})

    def test_non_string_enum_raises(self) -> None:
        """Enums of non-string values are not supported on the bridge surface."""
        with pytest.raises(ValueError, match="string enums"):
            _bridge_render_type({"type": "integer", "enum": [1, 2]}, {})

    def test_unsupported_property_raises(self) -> None:
        """A schema fragment with no recognised shape raises ValueError."""
        with pytest.raises(ValueError, match="cannot render"):
            _bridge_render_type({"weird": "shape"}, {})

    def test_array_without_items_dict_raises(self) -> None:
        """Array schemas missing an items dict raise ValueError."""
        with pytest.raises(ValueError, match="items dict"):
            _bridge_render_type({"type": "array"}, {})

    def test_anyof_with_two_non_null_members_raises(self) -> None:
        """AnyOf with multiple non-null members is not supported."""
        prop = {"anyOf": [{"type": "string"}, {"type": "integer"}]}
        with pytest.raises(ValueError, match="non-null members"):
            _bridge_render_type(prop, {})

    def test_anyof_with_non_object_member_raises(self) -> None:
        """Malformed anyOf members are rejected before nullable-union rendering."""
        prop = {"anyOf": [{"type": "string"}, "null"]}
        with pytest.raises(ValueError, match="malformed anyOf"):
            _bridge_render_type(prop, {})


class TestBridgeResolveRef:
    """Ref resolution against the backend ``$defs`` map."""

    def test_resolves_local_dollar_defs_ref(self) -> None:
        """``#/$defs/Name`` refs find their entry by trailing name."""
        defs = {"Foo": {"type": "string"}}
        assert _bridge_resolve_ref("#/$defs/Foo", defs) == {"type": "string"}

    def test_resolves_local_definitions_ref(self) -> None:
        """``#/definitions/Name`` refs are also supported (legacy form)."""
        defs = {"Foo": {"type": "string"}}
        assert _bridge_resolve_ref("#/definitions/Foo", defs) == {"type": "string"}

    def test_missing_ref_raises(self) -> None:
        """A ref to a missing def raises ValueError."""
        with pytest.raises(ValueError, match="missing"):
            _bridge_resolve_ref("#/$defs/Missing", {})

    def test_external_ref_raises(self) -> None:
        """Cross-document refs are not supported on the bridge surface."""
        with pytest.raises(ValueError, match="non-local"):
            _bridge_resolve_ref("https://example.com/schema.json#Foo", {})


class TestBridgeRenderField:
    """Single-field emission shape."""

    def test_type_discriminator_emits_string_literal(self) -> None:
        """The ``type`` field uses the ``const`` value as a string literal."""
        line = _bridge_render_field("type", {"const": "auth_required"}, {})
        assert line == '  readonly type: "auth_required";\n'

    def test_regular_field_emits_readonly(self) -> None:
        """Non-discriminator fields emit ``readonly <name>: <type>;``."""
        line = _bridge_render_field("timeout", {"type": "integer"}, {})
        assert line == "  readonly timeout: number;\n"


class TestBridgeEnvelopeSpec:
    """Derive the FrameEnvelope shape from StrictDataSchema (no hard-coding)."""

    def test_returns_field_set_and_declaration(self) -> None:
        """Returns a (frozenset of skip-fields, declaration block) pair."""
        envelope_fields, declaration = _bridge_envelope_spec()
        assert isinstance(envelope_fields, frozenset)
        assert declaration.startswith("export interface FrameEnvelope {\n")
        assert declaration.endswith("}\n")

    def test_excludes_type_discriminator(self) -> None:
        """``type`` is per-subclass and must NOT appear on FrameEnvelope."""
        envelope_fields, declaration = _bridge_envelope_spec()
        assert "type" not in envelope_fields
        assert "  readonly type:" not in declaration

    def test_envelope_mirrors_strict_data_schema_fields(self) -> None:
        """The envelope mirrors every StrictDataSchema field except ``type``.

        The set adapts automatically as ``StrictDataSchema`` evolves —
        adding the topic field extended the base from 4 fields to 5
        without further generator changes. Pinning against the live
        Pydantic schema instead of a hard-coded set keeps the
        derivation contract honest as the base evolves.
        """
        envelope_fields, declaration = _bridge_envelope_spec()
        expected = frozenset(name for name in StrictDataSchema.model_fields if name != "type")
        assert envelope_fields == expected
        for field in envelope_fields:
            assert (
                f"readonly {field}:" in declaration
            ), f"envelope declaration missing field {field}"

    def test_envelope_includes_topic_field(self) -> None:
        """Envelope carries ``topic: string | null`` reflecting the publisher chokepoint."""
        envelope_fields, declaration = _bridge_envelope_spec()
        assert "topic" in envelope_fields
        assert "readonly topic: string | null;" in declaration

    def test_field_order_matches_strict_data_schema_declaration_order(self) -> None:
        """Pydantic dataclass-field order is preserved in the declaration."""
        _, declaration = _bridge_envelope_spec()
        emitted_fields = [
            line.split(":", 1)[0].strip().removeprefix("readonly ")
            for line in declaration.splitlines()
            if line.startswith("  readonly ")
        ]
        expected_order = [name for name in StrictDataSchema.model_fields if name != "type"]
        assert emitted_fields == expected_order


class TestBridgeRenderClass:
    """Whole-interface emission for an allow-listed class."""

    def test_skips_envelope_fields_and_emits_type_literal(self) -> None:
        """Envelope fields stay on FrameEnvelope; per-class body skips them."""
        envelope_fields, _ = _bridge_envelope_spec()
        block = _bridge_render_class(
            "WSAuthRequiredResponse", WSAuthRequiredResponse, envelope_fields
        )
        assert "WSAuthRequiredResponse extends FrameEnvelope" in block
        assert 'readonly type: "auth_required";' in block
        assert "readonly timeout: number;" in block
        for envelope_field in envelope_fields:
            assert (
                f"readonly {envelope_field}" not in block
            ), f"envelope field {envelope_field} must not be emitted on the per-class body"

    def test_nullable_field_emits_string_or_null(self) -> None:
        """``str | None`` Pydantic fields emit as ``string | null`` on the bridge wire."""
        envelope_fields, _ = _bridge_envelope_spec()
        block = _bridge_render_class("WSAuthFailedResponse", WSAuthFailedResponse, envelope_fields)
        assert "readonly reason: string | null;" in block

    def test_jsonobject_field_emits_record_unknown(self) -> None:
        """JsonObject Pydantic fields emit as ``Readonly<Record<string, unknown>>``."""
        envelope_fields, _ = _bridge_envelope_spec()
        block = _bridge_render_class(
            "AiReviewRequestFrameData", AiReviewRequestFrameData, envelope_fields
        )
        assert "readonly signal_envelope: Readonly<Record<string, unknown>>;" in block
        assert "readonly instrument_metadata: Readonly<Record<string, unknown>>;" in block

    def test_array_of_string_emits_readonly_string_array(self) -> None:
        """``list[str]`` Pydantic fields emit as ``readonly string[]``."""
        envelope_fields, _ = _bridge_envelope_spec()
        block = _bridge_render_class("WSSubscribeRequest", WSSubscribeRequest, envelope_fields)
        assert "readonly topics: readonly string[];" in block


class TestGenerateBridgeWireContract:
    """End-to-end emitter integration."""

    def test_emits_19_interfaces_in_alphabetical_order(self, tmp_path: Path) -> None:
        """The full file emits 19 interfaces sorted alphabetically after FrameEnvelope."""
        output_path = tmp_path / "wire-contract.ts"
        content = generate_bridge_wire_contract(output_path)
        interface_lines = [
            line for line in content.splitlines() if line.startswith("export interface ")
        ]
        envelope_line = "export interface FrameEnvelope {"
        assert interface_lines[0] == envelope_line
        class_names = [
            line.removeprefix("export interface ").split(" ", 1)[0] for line in interface_lines[1:]
        ]
        assert class_names == sorted(class_names)
        assert len(class_names) == 19

    def test_writes_deterministic_output(self, tmp_path: Path) -> None:
        """Two consecutive regenerations produce byte-identical output."""
        first = tmp_path / "first.ts"
        second = tmp_path / "second.ts"
        content_first = generate_bridge_wire_contract(first)
        content_second = generate_bridge_wire_contract(second)
        assert content_first == content_second
        assert first.read_bytes() == second.read_bytes()

    def test_header_is_canonical_and_no_per_interface_jsdoc(self, tmp_path: Path) -> None:
        """File begins with the canonical header and never opens a JSDoc block."""
        output_path = tmp_path / "wire-contract.ts"
        content = generate_bridge_wire_contract(output_path)
        assert content.startswith(_BRIDGE_HEADER)
        assert "/**" not in content, "v1 generator must NOT emit per-interface JSDoc"

    def test_envelope_interface_includes_derived_fields(self, tmp_path: Path) -> None:
        """The emitted envelope block matches the dynamically-derived spec exactly."""
        output_path = tmp_path / "wire-contract.ts"
        content = generate_bridge_wire_contract(output_path)
        _, envelope_declaration = _bridge_envelope_spec()
        assert envelope_declaration in content
        envelope_block = content.split("export interface FrameEnvelope {")[1].split("}", 1)[0]
        assert "readonly topic: string | null;" in envelope_block

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        """The emitter creates the output directory tree if missing."""
        output_path = tmp_path / "deep" / "nested" / "wire-contract.ts"
        generate_bridge_wire_contract(output_path)
        assert output_path.is_file()


class TestBridgeRunner:
    """CLI-runner integration: ``_run_bridge_generator`` and ``main``."""

    def test_runner_skips_when_flag_unset(self, tmp_path: Path) -> None:
        """The runner is a no-op when --bridge is not requested (excluded from --all)."""
        args = _make_args(all=True, bridge=False)
        with patch("scripts.generate_types.generate_bridge_wire_contract") as mock_gen:
            _run_bridge_generator(args, tmp_path)
        mock_gen.assert_not_called()

    def test_runner_invokes_default_path(self, tmp_path: Path) -> None:
        """When --bridge is set without --bridge-output, the default path is used."""
        args = _make_args(bridge=True)
        with patch("scripts.generate_types.generate_bridge_wire_contract") as mock_gen:
            _run_bridge_generator(args, tmp_path)
        mock_gen.assert_called_once()
        called_path = mock_gen.call_args[0][0]
        assert called_path == (tmp_path / _BRIDGE_OUTPUT_DEFAULT).resolve()

    def test_runner_honours_relative_override(self, tmp_path: Path) -> None:
        """Relative --bridge-output paths resolve against the project root."""
        args = _make_args(bridge=True, bridge_output="custom/path/out.ts")
        with patch("scripts.generate_types.generate_bridge_wire_contract") as mock_gen:
            _run_bridge_generator(args, tmp_path)
        called_path = mock_gen.call_args[0][0]
        assert called_path == (tmp_path / "custom/path/out.ts").resolve()

    def test_runner_honours_absolute_override(self, tmp_path: Path) -> None:
        """Absolute --bridge-output paths are passed through verbatim."""
        absolute = tmp_path / "absolute" / "out.ts"
        args = _make_args(bridge=True, bridge_output=str(absolute))
        with patch("scripts.generate_types.generate_bridge_wire_contract") as mock_gen:
            _run_bridge_generator(args, tmp_path)
        called_path = mock_gen.call_args[0][0]
        assert called_path == absolute

    def test_main_with_bridge_flag_writes_file(self, tmp_path: Path) -> None:
        """Running ``main()`` with --bridge end-to-end writes the file to disk."""
        target = tmp_path / "wire-contract.ts"
        argv = ["scripts/generate_types.py", "--bridge", "--bridge-output", str(target)]
        with patch("sys.argv", argv):
            exit_code = main()
        assert exit_code == 0
        assert target.is_file()
        assert target.read_text(encoding="utf-8").startswith(_BRIDGE_HEADER)

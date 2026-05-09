"""Tests for ``snapper dev-mint-pat`` CLI subcommand."""

import base64 as _b64
import json
import os
import re
import stat
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest
from typer.testing import CliRunner

from snapper.cli import dev_pat as dev_pat_module
from snapper.cli.app import app
from snapper.cli.dev_pat import DEFAULT_BASE_URL
from snapper.cli.dev_pat import _decode_jwt_exp
from snapper.cli.dev_pat import _stamp_provenance
from snapper.cli.dev_pat import dev_mint_pat
from snapper.cli.dev_pat import redact_token
from snapper.cli.dev_pat import resolve_admin_credentials_from_seed
from snapper.data.seed.loader import SeedProfile
from snapper.data.seed.loader import SeedUser


@pytest.fixture
def runner() -> CliRunner:
    """Provide an isolated Typer CLI runner."""
    return CliRunner()


@pytest.fixture
def output_path(tmp_path: Path) -> Path:
    """Provide a writable temp output path."""
    return tmp_path / "dev-pat.json"


def _login_response(access_token: str = "ADMIN_ACCESS_TOKEN") -> dict[str, object]:
    """Build a fake POST /api/auth/login response envelope."""
    return {
        "type": "login_response",
        "payload": {
            "access_token": access_token,
            "refresh_token": "ADMIN_REFRESH_TOKEN",
        },
        "sequence_id": 1,
        "public_id": "01970000-0000-7000-8000-000000000001",
        "timestamp": "2026-04-29T00:00:00.000+00:00",
        "session_id": "01970000-0000-7000-8000-000000000010",
    }


def _delegate_created_response(
    access_token: str = "DELEGATE_LONG_LIVED_TOKEN",
) -> dict[str, object]:
    """Build a fake POST /api/ai-delegates response envelope."""
    return {
        "type": "delegate_create_response",
        "payload": {
            "delegate_public_id": "01970000-0000-7000-8000-000000000020",
            "username": "ai-local-dev-aaaa",
            "label": "Local Dev MCP",
            "operator_public_id": "01970000-0000-7000-8000-00000000000a",
            "is_active": True,
            "access_token": access_token,
        },
        "sequence_id": 2,
        "public_id": "01970000-0000-7000-8000-000000000002",
        "timestamp": "2026-04-29T00:00:01.000+00:00",
        "session_id": "01970000-0000-7000-8000-000000000010",
    }


def _build_mock_transport(
    *,
    login_status: int = 200,
    login_body: object | None = None,
    delegate_status: int = 200,
    delegate_body: object | None = None,
    raise_connect_error: bool = False,
    captured_requests: list[httpx.Request] | None = None,
) -> httpx.MockTransport:
    """Build a deterministic httpx transport for CLI tests.

    ``captured_requests`` (if provided) accumulates the requests issued by
    the CLI so tests can assert on the wire shape.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if captured_requests is not None:
            captured_requests.append(request)
        if raise_connect_error:
            raise httpx.ConnectError("simulated connection refused", request=request)
        if request.url.path == "/api/auth/login":
            body = _login_response() if login_body is None else login_body
            return (
                httpx.Response(login_status, json=body)
                if isinstance(body, dict)
                else httpx.Response(login_status, text=str(body))
            )
        if request.url.path == "/api/ai-delegates":
            body = _delegate_created_response() if delegate_body is None else delegate_body
            return (
                httpx.Response(delegate_status, json=body)
                if isinstance(body, dict)
                else httpx.Response(delegate_status, text=str(body))
            )
        return httpx.Response(404, text="not found")

    return httpx.MockTransport(handler)


def _assert_private_file_mode(path: Path) -> None:
    """Assert POSIX hosts persist the requested private file mode."""
    if os.name != "posix":
        return
    mode = stat.S_IMODE(path.stat().st_mode)
    assert mode == 0o600, f"expected 0o600, got {oct(mode)}"


@pytest.fixture
def patch_httpx_client(monkeypatch: pytest.MonkeyPatch) -> Callable[[httpx.MockTransport], None]:
    """Patch httpx.Client so the CLI uses a MockTransport."""

    def _apply(transport: httpx.MockTransport) -> None:
        original_init = httpx.Client.__init__

        def _patched_init(self: httpx.Client, *args: object, **kwargs: object) -> None:
            kwargs["transport"] = transport
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(httpx.Client, "__init__", _patched_init)

    return _apply


class TestRedactToken:
    """Cover the JWT-shape redaction helper."""

    def test_redacts_jwt_shape(self) -> None:
        """Given a JWT-shape value, when redacted, then placeholder substitutes the token."""
        jwt = "eyJabcdefghijklmnopqrst.eyJabcdefghijklmnopqrst.eyJabcdefghijklmnopqrst1234"
        assert redact_token(jwt) != jwt
        assert "<jwt-" in redact_token(jwt)
        assert "ending-" in redact_token(jwt)

    def test_passes_through_short_strings(self) -> None:
        """Given a short string, when redacted, then it is unchanged."""
        assert redact_token("hello") == "hello"

    def test_passes_through_urls(self) -> None:
        """Given a URL, when redacted, then it is unchanged (no JWT-shape in path)."""
        url = "https://snapper.example.com/api/mcp"
        assert redact_token(url) == url

    def test_redacts_jwt_inside_other_text(self) -> None:
        """Given an error body containing a JWT, when redacted, then placeholder appears in the body."""
        body = (
            "Auth fail for token "
            "eyJabcdefghijklmnopqrst.eyJabcdefghijklmnopqrst.eyJabcdefghijklmnopqrst1234"
            " in request 42"
        )
        redacted = redact_token(body)
        assert "<jwt-" in redacted
        assert "in request 42" in redacted


class TestStampProvenance:
    """Cover the PayloadRequest envelope helper."""

    def test_stamps_required_fields(self) -> None:
        """Given a payload, when stamped, then the envelope carries every required field."""
        envelope = _stamp_provenance(
            "login_request",
            {"username": "admin", "password": "x"},
            sequence_id=42,
            session_id="01970000-0000-7000-8000-000000000010",
        )
        assert envelope["type"] == "login_request"
        assert envelope["payload"] == {"username": "admin", "password": "x"}
        assert envelope["sequence_id"] == 42
        assert envelope["session_id"] == "01970000-0000-7000-8000-000000000010"
        assert isinstance(envelope["public_id"], str)
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            envelope["public_id"],
        ), "public_id must be UUID7"
        assert isinstance(envelope["timestamp"], str)
        assert envelope["timestamp"].endswith("+00:00")


class TestDevMintPatHappyPath:
    """Cover the happy path: login + delegate-create + file write."""

    def test_writes_pat_file_with_4_keys_and_mode_0600(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given mocked 200 login + 200 delegate-create, when invoked, then file lands at mode 0600 with 4 SNAPPER_* keys."""
        captured: list[httpx.Request] = []
        patch_httpx_client(_build_mock_transport(captured_requests=captured))

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code == 0, f"stderr={result.stderr}"
        assert output_path.exists()
        _assert_private_file_mode(output_path)

        document = json.loads(output_path.read_text())
        assert set(document.keys()) == {
            "SNAPPER_BASE_URL",
            "SNAPPER_ACCESS_TOKEN",
        }
        assert document["SNAPPER_BASE_URL"] == f"{DEFAULT_BASE_URL}/api/mcp"
        assert document["SNAPPER_ACCESS_TOKEN"] == "DELEGATE_LONG_LIVED_TOKEN"

    def test_request_envelope_shapes_match_payload_request_contract(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given mocked 200 responses, when invoked, then both POST bodies carry full PayloadRequest envelope."""
        captured: list[httpx.Request] = []
        patch_httpx_client(_build_mock_transport(captured_requests=captured))

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code == 0
        assert len(captured) == 2

        login_req = captured[0]
        assert login_req.url.path == "/api/auth/login"
        assert login_req.url.params.get("return_tokens") == "true"
        login_body = json.loads(login_req.content)
        assert login_body["type"] == "login_request"
        assert login_body["payload"] == {
            "username": "admin",
            "password": "AdminSnapper2026!",
            "remember_me": False,
        }
        for required_field in ("sequence_id", "public_id", "timestamp", "session_id"):
            assert required_field in login_body, f"login envelope missing {required_field}"

        delegate_req = captured[1]
        assert delegate_req.url.path == "/api/ai-delegates"
        assert delegate_req.headers.get("Authorization") == "Bearer ADMIN_ACCESS_TOKEN"
        delegate_body = json.loads(delegate_req.content)
        assert delegate_body["type"] == "delegate_create_request"
        assert delegate_body["payload"] == {"label": "Local Dev MCP"}
        for required_field in ("sequence_id", "public_id", "timestamp", "session_id"):
            assert required_field in delegate_body, f"delegate envelope missing {required_field}"

        assert (
            login_body["session_id"] == delegate_body["session_id"]
        ), "session_id must be reused across the two POSTs"


class TestDevMintPatErrorPaths:
    """Cover login + delegate-create + file write error paths."""

    def test_connection_refused_exits_1_with_actionable_stderr(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given a refused connection, when invoked, then exit 1 with `make dev-backend` hint."""
        patch_httpx_client(_build_mock_transport(raise_connect_error=True))

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "Connection refused" in result.stderr
        assert "make dev-backend" in result.stderr
        assert not output_path.exists()

    def test_login_401_exits_1_with_credentials_hint(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given login 401, when invoked, then exit 1 with --admin-password hint."""
        patch_httpx_client(
            _build_mock_transport(
                login_status=401,
                login_body={"detail": "invalid credentials"},
            ),
        )

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "Login failed" in result.stderr
        assert "SNAPPER_DEV_ADMIN_PASSWORD" in result.stderr
        assert not output_path.exists()

    def test_login_5xx_exits_1_with_redacted_body(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given login 503, when invoked, then exit 1 and stderr quotes the redacted body."""
        leaked_jwt = "eyJabcdefghijklmnopqrst.eyJabcdefghijklmnopqrst.eyJabcdefghijklmnopqrst1234"
        patch_httpx_client(
            _build_mock_transport(
                login_status=503,
                login_body=f"Backend down. Token in trace: {leaked_jwt} reason=foo",
            ),
        )

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "503" in result.stderr
        assert leaked_jwt not in result.stderr
        assert "<jwt-" in result.stderr
        assert not output_path.exists()

    def test_delegate_409_proliferation_cap_exits_1_with_deactivate_hint(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given delegate-create 409, when invoked, then exit 1 with deactivate hint."""
        patch_httpx_client(
            _build_mock_transport(
                delegate_status=409,
                delegate_body={"detail": "max delegates per owner reached"},
            ),
        )

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "proliferation cap reached" in result.stderr
        assert "deactivate" in result.stderr.lower()
        assert not output_path.exists()

    def test_delegate_401_admin_token_rejected_exits_1(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given delegate-create 401, when invoked, then exit 1 with re-run hint."""
        patch_httpx_client(
            _build_mock_transport(
                delegate_status=401,
                delegate_body={"detail": "token expired"},
            ),
        )

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "Admin token rejected" in result.stderr
        assert not output_path.exists()

    def test_delegate_500_exits_1_with_redacted_body(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given delegate-create 500, when invoked, then exit 1 and stderr is redacted."""
        patch_httpx_client(
            _build_mock_transport(
                delegate_status=500,
                delegate_body="internal error: backend rolled back",
            ),
        )

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "500" in result.stderr

    def test_login_payload_missing_access_token_exits_1(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given a login 200 without payload.access_token, when invoked, then exit 1."""
        patch_httpx_client(
            _build_mock_transport(
                login_body={"type": "login_response", "payload": {"refresh_token": "x"}},
            ),
        )

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "payload.access_token" in result.stderr

    def test_delegate_payload_missing_access_token_exits_1(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given delegate-create 200 without payload.access_token, when invoked, then exit 1."""
        patch_httpx_client(
            _build_mock_transport(
                delegate_body={"type": "delegate_create_response", "payload": {"label": "x"}},
            ),
        )

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "payload.access_token" in result.stderr

    def test_os_open_failure_exits_1_with_no_tmp_left_behind(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable[[httpx.MockTransport], None],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Given os.open failure on the tmp path, when invoked, then exit 1 and tmp.exists() False branch fires."""
        patch_httpx_client(_build_mock_transport())
        original_os_open = os.open

        def _selective_open(path: object, flags: int, mode: int = 0o777) -> int:
            if isinstance(path, (str, os.PathLike)) and ".tmp." in str(path):
                raise OSError("simulated EACCES on tmp create")
            return original_os_open(path, flags, mode)

        monkeypatch.setattr(os, "open", _selective_open)

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "Failed to write" in result.stderr
        assert "simulated EACCES on tmp create" in result.stderr
        assert not output_path.exists()
        leftover_tmp_files = list(output_path.parent.glob(f"{output_path.name}.tmp.*"))
        assert leftover_tmp_files == [], f"tmp files left behind: {leftover_tmp_files}"

    def test_file_write_failure_exits_1_and_cleans_tmp(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Given os.rename failure, when invoked, then exit 1 and tmp file removed."""
        patch_httpx_client(_build_mock_transport())

        def _fail_rename(src: object, dst: object) -> None:
            raise OSError("disk full")

        monkeypatch.setattr(os, "rename", _fail_rename)

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "Failed to write" in result.stderr
        assert not output_path.exists()
        leftover_tmp_files = list(output_path.parent.glob(f"{output_path.name}.tmp.*"))
        assert leftover_tmp_files == [], f"tmp files left behind: {leftover_tmp_files}"


class TestDecodeJwtExp:
    """Cover the success-banner exp-claim decoder."""

    def test_decodes_iso_for_valid_jwt(self) -> None:
        """Given a synthetic JWT with exp claim, when decoded, then ISO 8601 timestamp returned."""
        header = _b64.urlsafe_b64encode(b'{"alg":"HS256","typ":"JWT"}').rstrip(b"=").decode()
        payload = _b64.urlsafe_b64encode(b'{"exp":2051222400}').rstrip(b"=").decode()
        signature = _b64.urlsafe_b64encode(b"sig").rstrip(b"=").decode()
        jwt = f"{header}.{payload}.{signature}"
        result = _decode_jwt_exp(jwt)
        assert result is not None
        assert result.startswith("2035-01-01"), f"expected 2035-01-01..., got {result}"

    def test_returns_none_for_malformed_jwt(self) -> None:
        """Given a non-JWT string, when decoded, then None returned (no raise)."""
        assert _decode_jwt_exp("not-a-jwt") is None

    def test_returns_none_for_jwt_without_exp(self) -> None:
        """Given a JWT missing exp claim, when decoded, then None returned."""
        header = _b64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
        payload = _b64.urlsafe_b64encode(b'{"sub":"x"}').rstrip(b"=").decode()
        signature = _b64.urlsafe_b64encode(b"sig").rstrip(b"=").decode()
        jwt = f"{header}.{payload}.{signature}"
        assert _decode_jwt_exp(jwt) is None

    def test_returns_none_for_non_object_payload(self) -> None:
        """Given a JWT with non-object payload, when decoded, then None returned."""
        header = _b64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
        payload = _b64.urlsafe_b64encode(b'["not-object"]').rstrip(b"=").decode()
        signature = _b64.urlsafe_b64encode(b"sig").rstrip(b"=").decode()
        jwt = f"{header}.{payload}.{signature}"
        assert _decode_jwt_exp(jwt) is None

    def test_returns_none_for_unparseable_payload_segment(self) -> None:
        """Given a JWT with non-JSON payload segment, when decoded, then None returned."""
        header = _b64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
        payload = _b64.urlsafe_b64encode(b"not-json").rstrip(b"=").decode()
        signature = _b64.urlsafe_b64encode(b"sig").rstrip(b"=").decode()
        jwt = f"{header}.{payload}.{signature}"
        assert _decode_jwt_exp(jwt) is None

    def test_returns_none_for_overflow_exp(self) -> None:
        """Given a JWT with exp far beyond datetime max, when decoded, then None returned."""
        header = _b64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
        payload = _b64.urlsafe_b64encode(b'{"exp":99999999999999}').rstrip(b"=").decode()
        signature = _b64.urlsafe_b64encode(b"sig").rstrip(b"=").decode()
        jwt = f"{header}.{payload}.{signature}"
        assert _decode_jwt_exp(jwt) is None

    def test_returns_none_for_string_exp(self) -> None:
        """Given a JWT with non-numeric exp claim, when decoded, then None returned."""
        header = _b64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
        payload = _b64.urlsafe_b64encode(b'{"exp":"never"}').rstrip(b"=").decode()
        signature = _b64.urlsafe_b64encode(b"sig").rstrip(b"=").decode()
        jwt = f"{header}.{payload}.{signature}"
        assert _decode_jwt_exp(jwt) is None


class TestDevMintPatFileWriteHardening:
    """Cover Codex post-impl review fixes — atomic file create + parent mode warning + mkdir failure."""

    def test_mkdir_failure_routes_through_fatal_stderr(
        self,
        runner: CliRunner,
        tmp_path: Path,
        patch_httpx_client: Callable[[httpx.MockTransport], None],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Given Path.mkdir failure, when invoked, then exit 1 via _fatal stderr (no traceback)."""
        patch_httpx_client(_build_mock_transport())
        target = tmp_path / "deeper" / "dev-pat.json"

        original_mkdir = Path.mkdir

        def _fail_mkdir(self: Path, *args: object, **kwargs: object) -> None:
            if self == target.parent:
                raise OSError("simulated EROFS on mkdir")
            original_mkdir(self, *args, **kwargs)

        monkeypatch.setattr(Path, "mkdir", _fail_mkdir)

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(target)],
        )

        assert result.exit_code != 0
        assert "Failed to write" in result.stderr
        assert "simulated EROFS on mkdir" in result.stderr
        assert not target.exists()

    def test_loose_parent_mode_emits_warning_but_continues(
        self,
        runner: CliRunner,
        tmp_path: Path,
        patch_httpx_client: Callable[[httpx.MockTransport], None],
    ) -> None:
        """Given parent dir at 0o755, when invoked, then warning printed and file still written 0600."""
        patch_httpx_client(_build_mock_transport())
        loose_dir = tmp_path / "shared"
        loose_dir.mkdir(mode=0o755)
        target = loose_dir / "dev-pat.json"

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(target)],
        )

        assert result.exit_code == 0
        assert target.exists()
        _assert_private_file_mode(target)
        assert "warn:" in result.stderr
        assert str(loose_dir) in result.stderr

    def test_strict_parent_mode_does_not_emit_warning(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable[[httpx.MockTransport], None],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Given parent dir at 0o700, when invoked, then no loose-dir warning is printed."""
        patch_httpx_client(_build_mock_transport())

        def _strict_mode(mode: int) -> int:
            return mode & 0o700

        with monkeypatch.context() as context:
            context.setattr(dev_pat_module.stat, "S_IMODE", _strict_mode)
            result = runner.invoke(
                app,
                ["dev-mint-pat", "--output", str(output_path)],
            )

        assert result.exit_code == 0
        assert output_path.exists()
        _assert_private_file_mode(output_path)
        assert "warn:" not in result.stderr

    def test_atomic_create_uses_o_excl_mode_0600(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable[[httpx.MockTransport], None],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Given a 0644 process umask, when invoked, then the temp file is created at 0600 directly via os.open."""
        patch_httpx_client(_build_mock_transport())

        captured_modes: list[int] = []
        original_open = os.open

        def _capture_open(path: object, flags: int, mode: int = 0o777) -> int:
            captured_modes.append(mode)
            return original_open(path, flags, mode)

        monkeypatch.setattr(os, "open", _capture_open)

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code == 0
        assert any(
            m == 0o600 for m in captured_modes
        ), f"expected an os.open with mode 0o600, got modes {[oct(m) for m in captured_modes]}"


class TestDevMintPatConnectionFailure:
    """Cover delegate-create connection failure (Codex post-impl review finding)."""

    def test_delegate_connect_error_after_login_routes_through_fatal(
        self,
        runner: CliRunner,
        output_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Given login 200 + delegate-create ConnectError, when invoked, then exit 1 via _fatal stderr."""

        def _selective_handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/api/auth/login":
                return httpx.Response(200, json=_login_response())
            raise httpx.ConnectError("simulated drop after login", request=request)

        original_init = httpx.Client.__init__

        def _patched_init(self: httpx.Client, *args: object, **kwargs: object) -> None:
            kwargs["transport"] = httpx.MockTransport(_selective_handler)
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(httpx.Client, "__init__", _patched_init)

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert "Connection refused" in result.stderr
        assert "between login and delegate creation" in result.stderr
        assert not output_path.exists()


class TestDevMintPatDirectCall:
    """Cover invoking dev_mint_pat as a Python function (not via Typer)."""

    def test_direct_invocation_with_explicit_args_writes_file(
        self,
        tmp_path: Path,
        patch_httpx_client: Callable[[httpx.MockTransport], None],
    ) -> None:
        """Given direct call (bypasses Typer), when invoked with explicit args, then file is written."""
        patch_httpx_client(_build_mock_transport())
        output_file = tmp_path / "direct.json"

        dev_mint_pat(
            base_url="http://localhost:8000",
            admin_username="admin",
            admin_password="AdminSnapper2026!",
            output=output_file,
            label="Direct Invocation",
        )

        assert output_file.exists()
        assert (
            json.loads(output_file.read_text())["SNAPPER_ACCESS_TOKEN"]
            == "DELEGATE_LONG_LIVED_TOKEN"
        )


class TestDevMintPatTokenRedaction:
    """Cover the policy: tokens NEVER appear in stderr."""

    def test_no_jwt_shape_in_stderr_across_error_paths(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given a JWT-shape value in any HTTP error body, when invoked, then no raw JWT reaches stderr."""
        leaked_jwt = "eyJabcdefghijklmnopqrst.eyJabcdefghijklmnopqrst.eyJabcdefghijklmnopqrst1234"
        patch_httpx_client(
            _build_mock_transport(
                login_status=500,
                login_body=f"trace: {leaked_jwt}",
            ),
        )

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code != 0
        assert leaked_jwt not in result.stderr
        assert leaked_jwt not in result.stdout


class TestDevMintPatEnvVarResolution:
    """Cover env-var defaults for CLI flags."""

    def test_admin_password_resolves_from_env(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Given SNAPPER_DEV_ADMIN_PASSWORD env, when invoked without flag, then env value reaches login body."""
        captured: list[httpx.Request] = []
        patch_httpx_client(_build_mock_transport(captured_requests=captured))

        monkeypatch.setenv("SNAPPER_DEV_ADMIN_PASSWORD", "FromEnv2026!")

        result = runner.invoke(
            app,
            ["dev-mint-pat", "--output", str(output_path)],
        )

        assert result.exit_code == 0
        login_body = json.loads(captured[0].content)
        assert login_body["payload"]["password"] == "FromEnv2026!"

    def test_base_url_resolves_from_flag(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
    ) -> None:
        """Given --base-url, when invoked, then both POSTs target the override host and the file embeds the override URL."""
        captured: list[httpx.Request] = []
        patch_httpx_client(_build_mock_transport(captured_requests=captured))

        result = runner.invoke(
            app,
            [
                "dev-mint-pat",
                "--output",
                str(output_path),
                "--base-url",
                "https://custom.example/",
            ],
        )

        assert result.exit_code == 0
        assert all(req.url.host == "custom.example" for req in captured)
        document = json.loads(output_path.read_text())
        assert document["SNAPPER_BASE_URL"] == "https://custom.example/api/mcp"


class TestResolveAdminCredentialsFromSeed:
    """Cover the seed-TOML resolution helper.

    The helper drives ``snapper.data.seed.loader.load_seed_profile`` over
    the (mcp, dev) tuple and picks the first admin user found. Tests use
    monkeypatching to inject controlled seed profiles instead of writing
    real TOML files — that keeps the CWD-dependent file lookup out of
    the unit-test scope (it's exercised by the seed loader's own tests).
    """

    def test_returns_admin_from_mcp_profile_when_present(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Given a non-empty mcp profile with an admin user, when invoked, then mcp credentials win."""
        mcp_profile = SeedProfile(
            users=[
                SeedUser(
                    username="mcp-admin",
                    email="mcp-admin@x",
                    password="MCP_PROFILE_PW",
                    role="admin",
                ),
            ],
        )
        dev_profile = SeedProfile(
            users=[
                SeedUser(
                    username="dev-admin",
                    email="dev-admin@x",
                    password="DEV_PROFILE_PW",
                    role="admin",
                ),
            ],
        )

        def _loader(profile: str) -> SeedProfile:
            if profile == "mcp":
                return mcp_profile
            return dev_profile

        monkeypatch.setattr(dev_pat_module, "load_seed_profile", _loader)
        username, password = resolve_admin_credentials_from_seed()
        assert username == "mcp-admin"
        assert password == "MCP_PROFILE_PW"

    def test_falls_back_to_dev_profile_when_mcp_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Given mcp profile not on disk, when invoked, then dev profile admin wins."""
        dev_profile = SeedProfile(
            users=[
                SeedUser(
                    username="dev-admin",
                    email="dev-admin@x",
                    password="DEV_FALLBACK_PW",
                    role="admin",
                ),
            ],
        )

        def _loader(profile: str) -> SeedProfile:
            if profile == "mcp":
                raise FileNotFoundError("no mcp.toml on disk in any tier")
            return dev_profile

        monkeypatch.setattr(dev_pat_module, "load_seed_profile", _loader)
        username, password = resolve_admin_credentials_from_seed()
        assert username == "dev-admin"
        assert password == "DEV_FALLBACK_PW"

    def test_skips_non_admin_users_in_mcp_profile(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Given mcp profile lacks an admin role, when invoked, then dev profile admin wins."""
        mcp_profile = SeedProfile(
            users=[
                SeedUser(
                    username="mcp-viewer",
                    email="v@x",
                    password="VIEW_PW",
                    role="viewer",
                ),
                SeedUser(
                    username="mcp-operator",
                    email="o@x",
                    password="OP_PW",
                    role="operator",
                ),
            ],
        )
        dev_profile = SeedProfile(
            users=[
                SeedUser(
                    username="dev-admin",
                    email="da@x",
                    password="DEV_PW",
                    role="admin",
                ),
            ],
        )

        def _loader(profile: str) -> SeedProfile:
            return mcp_profile if profile == "mcp" else dev_profile

        monkeypatch.setattr(dev_pat_module, "load_seed_profile", _loader)
        username, password = resolve_admin_credentials_from_seed()
        assert username == "dev-admin"

    def test_exits_when_no_profile_yields_admin(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Given neither profile resolves an admin user, when invoked, then exit 1 with stderr message."""

        def _loader(profile: str) -> SeedProfile:
            raise FileNotFoundError(f"{profile} profile missing")

        monkeypatch.setattr(dev_pat_module, "load_seed_profile", _loader)
        with pytest.raises(Exception) as exc_info:
            resolve_admin_credentials_from_seed()
        message = str(exc_info.value).lower() + " " + getattr(exc_info.value, "code", "").__str__()
        assert exc_info.value.__class__.__name__ in {"Exit", "SystemExit"} or "exit" in message

    def test_exits_when_profiles_have_users_but_no_admin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Given both profiles exist but neither has admin role, when invoked, then exit 1."""
        non_admin_profile = SeedProfile(
            users=[
                SeedUser(
                    username="viewer",
                    email="v@x",
                    password="VIEW",
                    role="viewer",
                ),
            ],
        )
        monkeypatch.setattr(
            dev_pat_module,
            "load_seed_profile",
            lambda _profile: non_admin_profile,
        )
        with pytest.raises(Exception) as exc_info:
            resolve_admin_credentials_from_seed()
        assert exc_info.value.__class__.__name__ in {"Exit", "SystemExit"}


class TestDevMintPatSeedFallback:
    """Cover end-to-end seed-resolution behaviour at the CLI entry point."""

    def test_no_args_no_env_resolves_full_creds_from_seed(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Given neither flag nor env, when invoked, then seed admin reaches the login body."""
        seed_profile = SeedProfile(
            users=[
                SeedUser(
                    username="seed-admin",
                    email="sa@x",
                    password="SEEDED_ADMIN_PW",
                    role="admin",
                ),
            ],
        )
        monkeypatch.setattr(
            dev_pat_module,
            "load_seed_profile",
            lambda _profile: seed_profile,
        )
        monkeypatch.delenv("SNAPPER_DEV_ADMIN_USERNAME", raising=False)
        monkeypatch.delenv("SNAPPER_DEV_ADMIN_PASSWORD", raising=False)

        captured: list[httpx.Request] = []
        patch_httpx_client(_build_mock_transport(captured_requests=captured))

        result = runner.invoke(app, ["dev-mint-pat", "--output", str(output_path)])
        assert result.exit_code == 0
        login_body = json.loads(captured[0].content)
        assert login_body["payload"]["username"] == "seed-admin"
        assert login_body["payload"]["password"] == "SEEDED_ADMIN_PW"

    def test_password_env_with_seed_username(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Given only password env var, when invoked, then username comes from seed and password from env."""
        seed_profile = SeedProfile(
            users=[
                SeedUser(
                    username="seed-admin",
                    email="sa@x",
                    password="WONT_BE_USED",
                    role="admin",
                ),
            ],
        )
        monkeypatch.setattr(
            dev_pat_module,
            "load_seed_profile",
            lambda _profile: seed_profile,
        )
        monkeypatch.delenv("SNAPPER_DEV_ADMIN_USERNAME", raising=False)
        monkeypatch.setenv("SNAPPER_DEV_ADMIN_PASSWORD", "FROM_ENV_PW")

        captured: list[httpx.Request] = []
        patch_httpx_client(_build_mock_transport(captured_requests=captured))

        result = runner.invoke(app, ["dev-mint-pat", "--output", str(output_path)])
        assert result.exit_code == 0
        login_body = json.loads(captured[0].content)
        assert login_body["payload"]["username"] == "seed-admin"
        assert login_body["payload"]["password"] == "FROM_ENV_PW"

    def test_both_flags_skip_seed_lookup(
        self,
        runner: CliRunner,
        output_path: Path,
        patch_httpx_client: Callable,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Given both flags supplied, when invoked, then seed loader is never called."""
        loader_calls: list[str] = []

        def _loader(profile: str) -> SeedProfile:
            loader_calls.append(profile)
            raise AssertionError("seed loader must not be called when both flags supplied")

        monkeypatch.setattr(dev_pat_module, "load_seed_profile", _loader)
        monkeypatch.delenv("SNAPPER_DEV_ADMIN_USERNAME", raising=False)
        monkeypatch.delenv("SNAPPER_DEV_ADMIN_PASSWORD", raising=False)

        captured: list[httpx.Request] = []
        patch_httpx_client(_build_mock_transport(captured_requests=captured))

        result = runner.invoke(
            app,
            [
                "dev-mint-pat",
                "--output",
                str(output_path),
                "--admin-username",
                "explicit",
                "--admin-password",
                "explicit-pw",
            ],
        )
        assert result.exit_code == 0
        assert loader_calls == []
        login_body = json.loads(captured[0].content)
        assert login_body["payload"]["username"] == "explicit"
        assert login_body["payload"]["password"] == "explicit-pw"

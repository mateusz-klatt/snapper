"""Tests for the payload redaction utility."""

import json

from snapper.core.redact import redact


class TestRedact:
    """Tests for redact() function covering sensitive key masking."""

    def test_none_returns_none(self) -> None:
        """None input produces None output."""
        assert redact(None) is None

    def test_dict_masks_password(self) -> None:
        """Top-level password key is redacted."""
        result = redact({"user": "admin", "password": "secret123"})
        assert result is not None
        parsed = json.loads(result)
        assert parsed["user"] == "admin"
        assert parsed["password"] == "[REDACTED]"

    def test_dict_masks_api_key(self) -> None:
        """Top-level api_key is redacted."""
        result = redact({"api_key": "abc123", "name": "test"})
        assert result is not None
        parsed = json.loads(result)
        assert parsed["api_key"] == "[REDACTED]"
        assert parsed["name"] == "test"

    def test_dict_masks_multiple_sensitive_keys(self) -> None:
        """All known sensitive keys are redacted in a single dict."""
        payload: dict[str, str] = {
            "password": "p",
            "api_key": "k",
            "secret": "s",
            "token": "t",
            "bearer": "b",
            "ws_token": "w",
            "refresh_token": "r",
            "access_token": "a",
            "safe": "ok",
        }
        result = redact(payload)
        assert result is not None
        parsed = json.loads(result)
        for key in (
            "password",
            "api_key",
            "secret",
            "token",
            "bearer",
            "ws_token",
            "refresh_token",
            "access_token",
        ):
            assert parsed[key] == "[REDACTED]"
        assert parsed["safe"] == "ok"

    def test_nested_dict_is_redacted(self) -> None:
        """Sensitive keys inside nested dicts are also redacted."""
        payload = {"outer": {"password": "deep_secret", "ok": "visible"}}
        result = redact(payload)
        assert result is not None
        parsed = json.loads(result)
        assert parsed["outer"]["password"] == "[REDACTED]"
        assert parsed["outer"]["ok"] == "visible"

    def test_list_of_dicts_redacted(self) -> None:
        """Sensitive keys inside list elements are redacted."""
        payload = {"items": [{"token": "t1"}, {"token": "t2", "name": "x"}]}
        result = redact(payload)
        assert result is not None
        parsed = json.loads(result)
        assert parsed["items"][0]["token"] == "[REDACTED]"
        assert parsed["items"][1]["token"] == "[REDACTED]"
        assert parsed["items"][1]["name"] == "x"

    def test_string_input_parsed_and_redacted(self) -> None:
        """JSON string input is parsed, redacted, and re-serialized."""
        payload_str = json.dumps({"password": "secret", "user": "me"})
        result = redact(payload_str)
        assert result is not None
        parsed = json.loads(result)
        assert parsed["password"] == "[REDACTED]"
        assert parsed["user"] == "me"

    def test_string_non_dict_json_returned_as_is(self) -> None:
        """JSON string that is not a dict is returned unchanged."""
        payload_str = json.dumps([1, 2, 3])
        result = redact(payload_str)
        assert result == payload_str

    def test_case_insensitive_key_matching(self) -> None:
        """Sensitive key matching is case-insensitive."""
        result = redact({"Password": "x", "API_KEY": "y", "Safe": "ok"})
        assert result is not None
        parsed = json.loads(result)
        assert parsed["Password"] == "[REDACTED]"
        assert parsed["API_KEY"] == "[REDACTED]"
        assert parsed["Safe"] == "ok"

    def test_empty_dict(self) -> None:
        """Empty dict produces empty JSON object."""
        result = redact({})
        assert result == "{}"

    def test_no_sensitive_keys(self) -> None:
        """Dict without sensitive keys is returned unchanged."""
        payload = {"name": "test", "value": 42}
        result = redact(payload)
        assert result is not None
        parsed = json.loads(result)
        assert parsed == payload

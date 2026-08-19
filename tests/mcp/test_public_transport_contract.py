"""Public routing contract for the mounted MCP transport."""

from fastapi.testclient import TestClient

from snapper.server.app import create_app


def _build_client() -> TestClient:
    """Return a parent-app client that exposes redirects as responses."""
    return TestClient(create_app(), follow_redirects=False)


class TestPublicTransportContract:
    """Exercise MCP requests through the real parent FastAPI mount."""

    def test_post_mount_variants_reach_bearer_auth(self) -> None:
        """Both public POST spellings reach auth without a redirect or 405."""
        client = _build_client()
        for path in ("/api/mcp", "/api/mcp/"):
            response = client.post(path, json={})
            assert response.status_code == 401
            assert response.headers.get("location") is None
            assert response.headers["www-authenticate"] == "Bearer"
            assert response.json()["error_code"] == "missing_bearer_token"

    def test_get_mount_variants_reject_only_standalone_sse(self) -> None:
        """Both public GET spellings identify the transport root as SSE."""
        client = _build_client()
        for path in ("/api/mcp", "/api/mcp/"):
            response = client.get(path)
            assert response.status_code == 405
            assert response.headers["allow"] == "POST, DELETE"
            assert response.json()["error_code"] == "method_not_allowed"

    def test_endpoint_relative_discovery_is_not_treated_as_sse(self) -> None:
        """A future discovery route reaches auth instead of the root GET gate."""
        client = _build_client()
        response = client.get("/api/mcp/.well-known/oauth-protected-resource")
        assert response.status_code == 401
        assert response.json()["error_code"] == "missing_bearer_token"

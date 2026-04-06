"""Real-broker paper-mode E2E integration tests.

These tests spin up a live ZmqBrokerThread, PaperOrderExecutor, and
TraderCoordinator to exercise the full signal -> trader -> engine ->
executor -> DB -> ZMQ event flow without mocked transports. They are
intentionally excluded from the default `make test` / `make check-all`
run via the `integration` pytest marker (configured in pyproject.toml
[tool.pytest.ini_options]) and run only via `make test-integration`.
"""

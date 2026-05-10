"""Coverage for executor template / per-wallet instance name classification."""

from snapper.application.process_manager.executor_naming import is_executor_instance
from snapper.application.process_manager.executor_naming import is_executor_template
from snapper.application.process_manager.executor_naming import parent_template_for_instance
from snapper.application.process_manager.executor_naming import parse_executor_instance


class TestIsExecutorInstance:
    """Unit coverage for ``is_executor_instance``."""

    def test_simple_exchange_instance(self) -> None:
        """``executor_kraken_wabcdef123456`` is an instance."""
        assert is_executor_instance("executor_kraken_wabcdef123456") is True

    def test_compound_exchange_instance(self) -> None:
        """``executor_kraken_futures_wabcdef123456`` is an instance.

        Compound exchange names (``kraken_futures``) keep their
        underscore — the regex anchors on the trailing ``_w<12-hex>``.
        """
        assert is_executor_instance("executor_kraken_futures_wabcdef123456") is True

    def test_template_is_not_instance(self) -> None:
        """Bare ``executor_kraken`` template is not an instance."""
        assert is_executor_instance("executor_kraken") is False

    def test_short_wallet_id_rejected(self) -> None:
        """Wallet short suffix shorter than 12 hex chars is rejected."""
        assert is_executor_instance("executor_kraken_wabcdef") is False

    def test_uppercase_hex_rejected(self) -> None:
        """Wallet short suffix with uppercase hex chars is rejected.

        The spawner lowercases the wallet UUID7 prefix; uppercase
        wouldn't appear in a real instance name and rejecting it keeps
        the classifier strict.
        """
        assert is_executor_instance("executor_kraken_wABCDEF123456") is False

    def test_non_executor_name_rejected(self) -> None:
        """Non-executor process names are rejected."""
        assert is_executor_instance("zmq_broker") is False
        assert is_executor_instance("kraken_feed_publisher") is False


class TestIsExecutorTemplate:
    """Unit coverage for ``is_executor_template``."""

    def test_simple_exchange_template(self) -> None:
        """``executor_kraken`` is a template."""
        assert is_executor_template("executor_kraken") is True

    def test_compound_exchange_template(self) -> None:
        """``executor_kraken_futures`` is a template."""
        assert is_executor_template("executor_kraken_futures") is True

    def test_instance_is_not_template(self) -> None:
        """Per-wallet instance is not a template."""
        assert is_executor_template("executor_kraken_wabcdef123456") is False

    def test_compound_instance_is_not_template(self) -> None:
        """Compound-exchange per-wallet instance is not a template."""
        assert is_executor_template("executor_kraken_futures_wabcdef123456") is False

    def test_non_executor_name_rejected(self) -> None:
        """Non-executor process names are rejected."""
        assert is_executor_template("zmq_broker") is False
        assert is_executor_template("kraken_feed_publisher") is False
        assert is_executor_template("momentum_strategy") is False


class TestParseExecutorInstance:
    """Unit coverage for ``parse_executor_instance``."""

    def test_parse_simple(self) -> None:
        """Simple instance parses into ``(exchange, short)`` tuple."""
        assert parse_executor_instance("executor_kraken_wabcdef123456") == (
            "kraken",
            "abcdef123456",
        )

    def test_parse_compound(self) -> None:
        """Compound exchange parses preserving the underscore."""
        assert parse_executor_instance("executor_kraken_futures_wabcdef123456") == (
            "kraken_futures",
            "abcdef123456",
        )

    def test_parse_template_returns_none(self) -> None:
        """Template name is unparseable as instance."""
        assert parse_executor_instance("executor_kraken") is None

    def test_parse_unrelated_name_returns_none(self) -> None:
        """Unrelated process name parses to None."""
        assert parse_executor_instance("zmq_broker") is None


class TestParentTemplateForInstance:
    """Unit coverage for ``parent_template_for_instance``."""

    def test_simple_parent(self) -> None:
        """Simple instance parents to ``executor_<exchange>``."""
        assert parent_template_for_instance("executor_kraken_wabcdef123456") == ("executor_kraken")

    def test_compound_parent(self) -> None:
        """Compound instance parents to compound template."""
        assert parent_template_for_instance("executor_kraken_futures_wabcdef123456") == (
            "executor_kraken_futures"
        )

    def test_template_parents_to_none(self) -> None:
        """Template name has no parent (it IS the template)."""
        assert parent_template_for_instance("executor_kraken") is None

    def test_unrelated_name_parents_to_none(self) -> None:
        """Unrelated process names have no parent template."""
        assert parent_template_for_instance("zmq_broker") is None

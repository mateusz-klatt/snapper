"""Focused tests for shared market-data schema fields."""

from snapper.messaging.schemas.data import RelatedInstrumentsUnderlying


def test_related_instruments_underlying_description_field_optional() -> None:
    """Related-instruments underlying accepts a null resolved description.

    Given: a related-instruments underlying payload without resolved copy,
    When: the schema is constructed,
    Then: the description field accepts ``None``.
    """
    model = RelatedInstrumentsUnderlying(
        public_id="ua-1",
        ticker="SPX",
        name="S&P 500",
        asset_class="index",
        sector="US Large Cap",
        description=None,
    )
    assert model.description is None

"""Tests for the reviewed never-submitted exclusions and their digest."""

import hashlib
import math

import pytest

from snapper.application.portfolio.never_submitted_commands import NEVER_SUBMITTED_COMMAND_DIGESTS
from snapper.application.portfolio.never_submitted_commands import NEVER_SUBMITTED_DOCUMENT_SCHEMA
from snapper.application.portfolio.never_submitted_commands import never_submitted_digest


def test_no_exclusion_is_pinned_until_a_reviewed_change_records_one() -> None:
    """The shipped pinned set is empty, so the gate behaves exactly as before.

    Given: The module as shipped.
    When: Its pinned digests are read.
    Then: There are none, and the schema tag names the document's version.
    """
    assert isinstance(NEVER_SUBMITTED_COMMAND_DIGESTS, frozenset)
    assert len(NEVER_SUBMITTED_COMMAND_DIGESTS) == 0
    assert NEVER_SUBMITTED_DOCUMENT_SCHEMA == "snapper.never_submitted_command.v1"


def test_the_digest_hashes_the_canonical_json() -> None:
    """Keys are sorted, separators compact, and non-ASCII characters escaped.

    Given: A document with unsorted keys and a non-ASCII value.
    When: Its digest is computed.
    Then: It is the SHA-256 of exactly the canonical bytes.
    """
    document = {"b": [{"y": "é", "x": None}], "a": 1}
    canonical = b'{"a":1,"b":[{"x":null,"y":"\\u00e9"}]}'
    assert never_submitted_digest(document) == hashlib.sha256(canonical).hexdigest()


def test_the_digest_does_not_depend_on_key_order() -> None:
    """Two documents with the same facts in a different key order hash alike.

    Given: The same facts inserted in two orders.
    When: Their digests are computed.
    Then: They are equal.
    """
    assert never_submitted_digest({"a": 1, "b": 2}) == never_submitted_digest({"b": 2, "a": 1})


def test_a_non_finite_number_cannot_be_hashed() -> None:
    """A document carrying NaN is refused instead of hashed.

    Given: A document with a NaN value.
    When: Its digest is computed.
    Then: It raises ``ValueError``.
    """
    with pytest.raises(ValueError):
        never_submitted_digest({"a": math.nan})

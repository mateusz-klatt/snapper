"""Authoritative direct tokenized-asset catalog provenance and precision bounds."""

from typing import Final

TOKENIZED_SPEC_SOURCE: Final[str] = "kraken:rest.AssetPairs:tokenized_asset"
TOKENIZED_SPEC_ETL_VERSION: Final[str] = "tokenized-rest-v1"
TOKENIZED_MAX_DECIMALS: Final[int] = 256

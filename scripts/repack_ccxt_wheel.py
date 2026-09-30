"""Repackage verified CCXT 4.5.84 metadata with its urllib3 security update.

Download the exact upstream wheel from ``UPSTREAM_URL`` separately. This command
checks its fixed SHA-256 before preserving every package and license byte,
changing the distribution version and urllib3 requirement, and rebuilding RECORD.
Archive ordering, timestamps, permissions and compression settings are fixed.
"""

import argparse
import base64
import csv
import hashlib
import io
import zipfile
from pathlib import Path
from typing import Final

UPSTREAM_URL: Final = (
    "https://files.pythonhosted.org/packages/c9/a2/"
    "1f6fd14591a951e608fe7afc8d77de6ef35434c2e430b4fff105ffb216b0/"
    "ccxt-4.5.84-py3-none-any.whl"
)
UPSTREAM_SHA256: Final = "b920f7d92c0fb62873a900c96ee2cfb76ec6ffeaa7343bf5be0cc01b9b2e9617"
SOURCE_DIST_INFO: Final = "ccxt-4.5.84.dist-info/"
TARGET_DIST_INFO: Final = "ccxt-4.5.84+snapper.1.dist-info/"
WHEEL_NAME: Final = "ccxt-4.5.84+snapper.1-py3-none-any.whl"


def _patch_metadata(metadata: bytes) -> bytes:
    """Replace exactly the expected version and urllib3 requirement headers."""
    replacements = (
        (b"Version: 4.5.84\n", b"Version: 4.5.84+snapper.1\n"),
        (b"Requires-Dist: urllib3==2.7.0\n", b"Requires-Dist: urllib3==2.8.0\n"),
    )
    for before, after in replacements:
        if metadata.count(before) != 1:
            raise ValueError("Upstream metadata does not match the expected patch")
        metadata = metadata.replace(before, after)
    return metadata


def _record_digest(data: bytes) -> str:
    """Encode a file hash in the URL-safe format required by wheel RECORD."""
    encoded = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
    return "sha256=" + encoded.decode("ascii")


def _replace_record(files: dict[str, bytes]) -> None:
    """Regenerate every RECORD path, hash and length after metadata changes."""
    record_path = TARGET_DIST_INFO + "RECORD"
    del files[record_path]
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    for name, data in sorted(files.items()):
        writer.writerow((name, _record_digest(data), str(len(data))))
    writer.writerow((record_path, "", ""))
    files[record_path] = stream.getvalue().encode("utf-8")


def repack_wheel(source: Path, output_directory: Path) -> Path:
    """Verify the upstream archive and write a wheel containing unchanged code.

    Args:
        source: Locally downloaded upstream wheel with the pinned digest.
        output_directory: Destination directory for the reproducible wheel.

    Returns:
        Path to the generated wheel.

    Raises:
        ValueError: If the upstream archive or metadata differs from the pin.
    """
    content = source.read_bytes()
    if hashlib.sha256(content).hexdigest() != UPSTREAM_SHA256:
        raise ValueError("Upstream wheel SHA-256 mismatch")
    with zipfile.ZipFile(io.BytesIO(content)) as wheel:
        files = {
            name.replace(SOURCE_DIST_INFO, TARGET_DIST_INFO, 1): wheel.read(name)
            for name in wheel.namelist()
        }
    metadata_path = TARGET_DIST_INFO + "METADATA"
    files[metadata_path] = _patch_metadata(files[metadata_path])
    _replace_record(files)
    output_directory.mkdir(parents=True, exist_ok=True)
    destination = output_directory / WHEEL_NAME
    with zipfile.ZipFile(destination, "w") as wheel:
        for name, data in sorted(files.items()):
            entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            entry.create_system = 3
            entry.external_attr = 0o100644 << 16
            wheel.writestr(entry, data, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
    return destination


def main(argv: list[str] | None = None) -> int:
    """Rebuild the wheel from a separate, verified upstream download.

    Args:
        argv: Command arguments, or None to read them from the process arguments.

    Returns:
        Zero after writing the wheel and printing its SHA-256 digest and path.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Downloaded upstream CCXT 4.5.84 wheel")
    parser.add_argument("--output-dir", type=Path, default=Path("vendor/ccxt"))
    args = parser.parse_args(argv)
    output = repack_wheel(args.source, args.output_dir)
    print(f"{hashlib.sha256(output.read_bytes()).hexdigest()}  {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

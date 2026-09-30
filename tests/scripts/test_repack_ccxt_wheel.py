"""Verify the CCXT packaging patch preserves code and produces valid wheel records."""

import base64
import csv
import hashlib
import io
import runpy
import sys
import zipfile
from pathlib import Path

import pytest

from scripts import repack_ccxt_wheel

METADATA = (
    b"Metadata-Version: 2.4\nName: ccxt\nVersion: 4.5.84\n"
    b"Requires-Dist: urllib3==2.7.0\nRequires-Dist: requests>=2.32,<3\n"
    b"License-Expression: MIT\nLicense-File: LICENSE.txt\n"
)


def _upstream_wheel(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a small upstream-shaped wheel and pin its digest for offline tests."""
    path = tmp_path / "upstream.whl"
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr("ccxt/base/exchange.py", b"class Exchange:\n    pass\n")
        wheel.writestr("ccxt/__init__.py", b"__version__ = '4.5.84'\n")
        wheel.writestr(repack_ccxt_wheel.SOURCE_DIST_INFO + "METADATA", METADATA)
        wheel.writestr(repack_ccxt_wheel.SOURCE_DIST_INFO + "WHEEL", b"Tag: py3-none-any\n")
        wheel.writestr(
            repack_ccxt_wheel.SOURCE_DIST_INFO + "licenses/LICENSE.txt", b"MIT license\n"
        )
        wheel.writestr(repack_ccxt_wheel.SOURCE_DIST_INFO + "RECORD", b"old paths and hashes\n")
    monkeypatch.setattr(
        repack_ccxt_wheel, "UPSTREAM_SHA256", hashlib.sha256(path.read_bytes()).hexdigest()
    )
    return path


def test_repack_preserves_code_license_and_builds_valid_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep all payload bytes while updating distribution metadata and RECORD.

    Given a verified upstream wheel containing package code and a license,
    When the wheel is repackaged into two separate destination directories,
    Then both artifacts match and only metadata and valid RECORD entries change.
    """
    source = _upstream_wheel(tmp_path, monkeypatch)
    first = repack_ccxt_wheel.repack_wheel(source, tmp_path / "first")
    second = repack_ccxt_wheel.repack_wheel(source, tmp_path / "second")
    assert first.read_bytes() == second.read_bytes()
    assert first.name == "ccxt-4.5.84+snapper.1-py3-none-any.whl"
    with zipfile.ZipFile(source) as before, zipfile.ZipFile(first) as after:
        changed: set[str] = set()
        for name in before.namelist():
            renamed = name.replace(
                repack_ccxt_wheel.SOURCE_DIST_INFO, repack_ccxt_wheel.TARGET_DIST_INFO
            )
            if before.read(name) != after.read(renamed):
                changed.add(name)
        assert changed == {
            repack_ccxt_wheel.SOURCE_DIST_INFO + "METADATA",
            repack_ccxt_wheel.SOURCE_DIST_INFO + "RECORD",
        }
        metadata = after.read(repack_ccxt_wheel.TARGET_DIST_INFO + "METADATA")
        assert metadata == METADATA.replace(
            b"Version: 4.5.84\n", b"Version: 4.5.84+snapper.1\n"
        ).replace(b"urllib3==2.7.0", b"urllib3==2.8.0")
        record_path = repack_ccxt_wheel.TARGET_DIST_INFO + "RECORD"
        rows = list(csv.reader(io.StringIO(after.read(record_path).decode())))
        assert {row[0] for row in rows} == set(after.namelist())
        for name, digest, size in rows:
            if name == record_path:
                assert (digest, size) == ("", "")
            else:
                data = after.read(name)
                expected = (
                    base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
                )
                assert digest == "sha256=" + expected
                assert int(size) == len(data)


def test_repack_refuses_modified_source_before_creating_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject changed upstream bytes before producing an unverified artifact.

    Given an upstream wheel modified after its expected digest was recorded,
    When repackaging verifies the downloaded archive,
    Then it rejects the digest mismatch without creating the output directory.
    """
    source = _upstream_wheel(tmp_path, monkeypatch)
    source.write_bytes(source.read_bytes() + b"modified")
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        repack_ccxt_wheel.repack_wheel(source, output)
    assert not output.exists()


@pytest.mark.parametrize(
    "metadata",
    [
        METADATA.replace(b"Version: 4.5.84\n", b""),
        METADATA + b"Version: 4.5.84\n",
        METADATA.replace(b"Requires-Dist: urllib3==2.7.0\n", b""),
        METADATA + b"Requires-Dist: urllib3==2.7.0\n",
    ],
)
def test_patch_metadata_refuses_missing_or_duplicate_headers(metadata: bytes) -> None:
    """Refuse metadata with missing or duplicate patch targets.

    Given a missing or repeated version or urllib3 requirement header,
    When the metadata patch validates its expected input,
    Then it raises an error before returning partially patched metadata.
    """
    with pytest.raises(ValueError, match="metadata does not match"):
        repack_ccxt_wheel._patch_metadata(metadata)


def test_main_reports_artifact_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Expose a verifiable digest and the default vendor destination from the CLI.

    Given a verified upstream wheel and no explicit output directory,
    When the command repackages the wheel successfully,
    Then it returns zero and prints the generated artifact's digest and path.
    """
    source = _upstream_wheel(tmp_path, monkeypatch)
    monkeypatch.chdir(tmp_path)
    assert repack_ccxt_wheel.main([str(source)]) == 0
    output = tmp_path / "vendor/ccxt" / repack_ccxt_wheel.WHEEL_NAME
    assert capsys.readouterr().out == (
        f"{hashlib.sha256(output.read_bytes()).hexdigest()}  vendor/ccxt/{output.name}\n"
    )


def test_module_entrypoint_rejects_unverified_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the pinned upstream integrity check in the real command entrypoint.

    Given a download whose contents do not match the fixed upstream digest,
    When the script runs through its module entrypoint,
    Then it raises the digest mismatch error before generating an artifact.
    """
    source = tmp_path / "unverified.whl"
    source.write_bytes(b"unverified")
    script = Path(repack_ccxt_wheel.__file__)
    monkeypatch.setattr(sys, "argv", [str(script), str(source)])
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        runpy.run_path(str(script), run_name="__main__")

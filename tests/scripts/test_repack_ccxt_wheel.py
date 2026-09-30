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


def _upstream_wheel(monkeypatch: pytest.MonkeyPatch, metadata: bytes = METADATA) -> bytes:
    """Create a small upstream-shaped wheel and pin its digest for offline tests."""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as wheel:
        wheel.writestr("ccxt/base/exchange.py", b"class Exchange:\n    pass\n")
        wheel.writestr("ccxt/__init__.py", b"__version__ = '4.5.84'\n")
        wheel.writestr(repack_ccxt_wheel.SOURCE_DIST_INFO + "METADATA", metadata)
        wheel.writestr(repack_ccxt_wheel.SOURCE_DIST_INFO + "WHEEL", b"Tag: py3-none-any\n")
        wheel.writestr(
            repack_ccxt_wheel.SOURCE_DIST_INFO + "licenses/LICENSE.txt", b"MIT license\n"
        )
        wheel.writestr(repack_ccxt_wheel.SOURCE_DIST_INFO + "RECORD", b"old paths and hashes\n")
    content = stream.getvalue()
    monkeypatch.setattr(repack_ccxt_wheel, "UPSTREAM_SHA256", hashlib.sha256(content).hexdigest())
    return content


def test_repack_preserves_code_license_and_builds_valid_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep all payload bytes while updating distribution metadata and RECORD.

    Given a verified upstream wheel containing package code and a license,
    When the wheel bytes are repackaged twice,
    Then both artifacts match and only metadata and valid RECORD entries change.
    """
    source = _upstream_wheel(monkeypatch)
    first = repack_ccxt_wheel.repack_wheel(source)
    second = repack_ccxt_wheel.repack_wheel(source)
    assert first == second
    with zipfile.ZipFile(io.BytesIO(source)) as before, zipfile.ZipFile(io.BytesIO(first)) as after:
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


def test_repack_refuses_modified_source(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject changed upstream bytes before producing an unverified artifact.

    Given an upstream wheel modified after its expected digest was recorded,
    When repackaging verifies the downloaded archive,
    Then it rejects the digest mismatch instead of returning wheel bytes.
    """
    source = _upstream_wheel(monkeypatch) + b"modified"
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        repack_ccxt_wheel.repack_wheel(source)


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

    Given verified upstream wheel bytes on standard input and a different working directory,
    When the command repackages the wheel successfully,
    Then it returns zero and prints the generated artifact's digest and path.
    """
    expected_directory = Path(repack_ccxt_wheel.__file__).resolve().parents[1] / "vendor/ccxt"
    assert expected_directory == repack_ccxt_wheel.OUTPUT_DIRECTORY
    source = _upstream_wheel(monkeypatch)
    output_directory = tmp_path / "repository/vendor/ccxt"
    monkeypatch.setattr(repack_ccxt_wheel, "OUTPUT_DIRECTORY", output_directory)
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(source)))
    monkeypatch.chdir(tmp_path)
    assert repack_ccxt_wheel.main([]) == 0
    output = output_directory / "ccxt-4.5.84+snapper.1-py3-none-any.whl"
    assert output.read_bytes() == repack_ccxt_wheel.repack_wheel(source)
    assert capsys.readouterr().out == (
        f"{hashlib.sha256(output.read_bytes()).hexdigest()}  {output}\n"
    )
    assert not (tmp_path / "vendor").exists()


def test_main_refuses_modified_source_before_creating_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the fixed destination absent when upstream integrity verification fails.

    Given modified wheel bytes and a nonexistent output directory,
    When the CLI consumes the bytes from standard input,
    Then it rejects their digest before creating any output directory or artifact.
    """
    source = _upstream_wheel(monkeypatch) + b"modified"
    output_directory = tmp_path / "vendor/ccxt"
    monkeypatch.setattr(repack_ccxt_wheel, "OUTPUT_DIRECTORY", output_directory)
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(source)))
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        repack_ccxt_wheel.main([])
    assert not output_directory.parent.exists()


def test_main_preserves_existing_output_when_metadata_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve the previous wheel when verified input fails metadata validation.

    Given an existing output wheel and input with an unexpected metadata header,
    When the CLI accepts the pinned digest but refuses the metadata,
    Then the previous wheel remains byte-identical and no extra files appear.
    """
    previous = repack_ccxt_wheel.repack_wheel(_upstream_wheel(monkeypatch))
    output_directory = tmp_path / "vendor/ccxt"
    output_directory.mkdir(parents=True)
    output = output_directory / repack_ccxt_wheel.WHEEL_NAME
    output.write_bytes(previous)
    source = _upstream_wheel(monkeypatch, METADATA.replace(b"Version: 4.5.84\n", b""))
    monkeypatch.setattr(repack_ccxt_wheel, "OUTPUT_DIRECTORY", output_directory)
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(source)))
    with pytest.raises(ValueError, match="metadata does not match"):
        repack_ccxt_wheel.main([])
    assert output.read_bytes() == previous
    assert list(output_directory.iterdir()) == [output]


@pytest.mark.parametrize("arguments", [["untrusted.whl"], ["--output-dir", "elsewhere"]])
def test_main_rejects_path_arguments_before_reading_input(
    arguments: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject caller-supplied filesystem paths before consuming or writing anything.

    Given a positional source path or output-directory option,
    When the CLI parses its arguments,
    Then it reports a usage error with standard input unread and no output created.
    """
    stream = io.BytesIO(_upstream_wheel(monkeypatch))
    output_directory = tmp_path / "vendor/ccxt"
    monkeypatch.setattr(repack_ccxt_wheel, "OUTPUT_DIRECTORY", output_directory)
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(stream))
    with pytest.raises(SystemExit) as error:
        repack_ccxt_wheel.main(arguments)
    assert error.value.code == 2
    assert stream.tell() == 0
    assert not output_directory.parent.exists()


def test_module_entrypoint_rejects_unverified_download(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the pinned upstream integrity check in the real command entrypoint.

    Given a download whose contents do not match the fixed upstream digest,
    When the script runs through its module entrypoint,
    Then it raises the digest mismatch error before generating an artifact.
    """
    script = Path(repack_ccxt_wheel.__file__)
    monkeypatch.setattr(sys, "argv", [str(script)])
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(b"unverified")))
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        runpy.run_path(str(script), run_name="__main__")

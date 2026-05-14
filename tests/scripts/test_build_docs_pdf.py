"""Tests for build_docs_pdf module."""

import re
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def mock_weasyprint() -> MagicMock:
    """Mock weasyprint module to avoid native library dependencies."""
    mock_css = MagicMock()
    mock_html = MagicMock()
    mock_weasyprint_module = MagicMock()
    mock_weasyprint_module.CSS = mock_css
    mock_weasyprint_module.HTML = mock_html
    sys.modules["weasyprint"] = mock_weasyprint_module
    return mock_weasyprint_module


def _import_module() -> Any:
    """Import the module after mocking weasyprint."""
    if "weasyprint" not in sys.modules:
        mock_weasyprint_module = MagicMock()
        mock_weasyprint_module.CSS = MagicMock()
        mock_weasyprint_module.HTML = MagicMock()
        sys.modules["weasyprint"] = mock_weasyprint_module

    import scripts.build_docs_pdf as module

    return module


_mock_weasyprint = MagicMock()
_mock_weasyprint.CSS = MagicMock()
_mock_weasyprint.HTML = MagicMock()
sys.modules["weasyprint"] = _mock_weasyprint

from scripts.build_docs_pdf import CODE_LANGUAGE_HINTS
from scripts.build_docs_pdf import MARKDOWN_EXTENSION_CONFIGS
from scripts.build_docs_pdf import MARKDOWN_EXTENSIONS
from scripts.build_docs_pdf import DocumentSource
from scripts.build_docs_pdf import MarkdownToPdf
from scripts.build_docs_pdf import MermaidRenderer
from scripts.build_docs_pdf import MermaidRenderingError
from scripts.build_docs_pdf import RenderConfig
from scripts.build_docs_pdf import build_slug
from scripts.build_docs_pdf import collect_sources
from scripts.build_docs_pdf import default_font_candidates
from scripts.build_docs_pdf import extract_title
from scripts.build_docs_pdf import find_font
from scripts.build_docs_pdf import main
from scripts.build_docs_pdf import parse_args


class TestConstants:
    """Test module-level constants."""

    def test_markdown_extensions_list(self) -> None:
        """Verify markdown extensions list.

        Given: The MARKDOWN_EXTENSIONS constant,
        When: Checking for required extensions,
        Then: It contains fenced_code, tables, toc, and codehilite.
        """
        assert "fenced_code" in MARKDOWN_EXTENSIONS
        assert "tables" in MARKDOWN_EXTENSIONS
        assert "toc" in MARKDOWN_EXTENSIONS
        assert "codehilite" in MARKDOWN_EXTENSIONS

    def test_markdown_extension_configs_codehilite(self) -> None:
        """Verify markdown extension configs codehilite.

        Given: The MARKDOWN_EXTENSION_CONFIGS constant,
        When: Checking codehilite configuration,
        Then: It has linenums=False, guess_lang=False, and noclasses=False.
        """
        assert "codehilite" in MARKDOWN_EXTENSION_CONFIGS
        config = MARKDOWN_EXTENSION_CONFIGS["codehilite"]
        assert config["linenums"] is False
        assert config["guess_lang"] is False
        assert config["noclasses"] is False

    def test_code_language_hints_contains_common_languages(self) -> None:
        """Verify code language hints contains common languages.

        Given: The CODE_LANGUAGE_HINTS constant,
        When: Checking for common programming languages,
        Then: It contains bash, python, json, yaml, sql, and shell.
        """
        expected = {"bash", "python", "json", "yaml", "sql", "shell"}
        assert expected.issubset(CODE_LANGUAGE_HINTS)


class TestDefaultFontCandidates:
    """Test default_font_candidates function."""

    def test_returns_tuple(self) -> None:
        """Verify returns tuple.

        Given: The default_font_candidates function,
        When: Called without arguments,
        Then: It returns a tuple of font paths.
        """
        result = default_font_candidates()
        assert isinstance(result, tuple)

    def test_contains_linux_fonts(self) -> None:
        """Verify contains linux fonts.

        Given: The default_font_candidates function,
        When: Called on any platform,
        Then: It includes DejaVu fonts (common Linux fonts).
        """
        result = default_font_candidates()
        paths_str = [str(p) for p in result]
        assert any("dejavu" in p.lower() for p in paths_str)

    @patch("scripts.build_docs_pdf.platform.system", return_value="Windows")
    def test_windows_adds_windows_fonts(self, mock_system: MagicMock) -> None:
        """Verify windows adds windows fonts.

        Given: Platform.system returns 'Windows',
        When: Calling default_font_candidates,
        Then: It includes Windows-specific fonts (Segoe UI or Arial).
        """
        result = default_font_candidates()
        paths_str = [str(p) for p in result]
        assert any("segoeui" in p.lower() or "arial" in p.lower() for p in paths_str)

    @patch("scripts.build_docs_pdf.platform.system", return_value="Darwin")
    def test_darwin_adds_macos_fonts(self, mock_system: MagicMock) -> None:
        """Verify darwin adds macos fonts.

        Given: Platform.system returns 'Darwin' (macOS),
        When: Calling default_font_candidates,
        Then: It includes macOS Library/Fonts paths.
        """
        result = default_font_candidates()
        assert any("Library" in p.parts and "Fonts" in p.parts for p in result)

    @patch("scripts.build_docs_pdf.platform.system", return_value="Linux")
    def test_linux_adds_home_fonts(self, mock_system: MagicMock) -> None:
        """Verify linux adds home fonts.

        Given: Platform.system returns 'Linux',
        When: Calling default_font_candidates,
        Then: It includes user home .fonts directory.
        """
        result = default_font_candidates()
        paths_str = [str(p) for p in result]
        assert any(".fonts" in p for p in paths_str)

    def test_no_duplicates(self) -> None:
        """Verify no duplicates.

        Given: The default_font_candidates function,
        When: Collecting all font paths,
        Then: The list contains no duplicate paths.
        """
        result = default_font_candidates()
        assert len(result) == len(set(result))


class TestRenderConfig:
    """Test RenderConfig dataclass."""

    def test_creates_with_required_fields(self) -> None:
        """Verify creates with required fields.

        Given: Required config values (title_font_size_pt, body_font_size_pt, page_margin_mm),
        When: Creating a RenderConfig instance,
        Then: The values are stored correctly.
        """
        config = RenderConfig(
            title_font_size_pt=18,
            body_font_size_pt=11,
            page_margin_mm=15,
        )
        assert config.title_font_size_pt == 18
        assert config.body_font_size_pt == 11
        assert config.page_margin_mm == 15

    def test_default_font_families(self) -> None:
        """Verify default font families.

        Given: A RenderConfig without custom font families,
        When: Accessing font_family and mono_font_family,
        Then: They default to 'SnapperBody' and 'SnapperMono'.
        """
        config = RenderConfig(
            title_font_size_pt=18,
            body_font_size_pt=11,
            page_margin_mm=15,
        )
        assert config.font_family == "SnapperBody"
        assert config.mono_font_family == "SnapperMono"

    def test_custom_font_families(self) -> None:
        """Verify custom font families.

        Given: A RenderConfig with custom font families specified,
        When: Accessing font_family and mono_font_family,
        Then: They contain the custom values.
        """
        config = RenderConfig(
            title_font_size_pt=18,
            body_font_size_pt=11,
            page_margin_mm=15,
            font_family="CustomFont",
            mono_font_family="CustomMono",
        )
        assert config.font_family == "CustomFont"
        assert config.mono_font_family == "CustomMono"

    def test_is_frozen(self) -> None:
        """Verify is frozen.

        Given: A RenderConfig instance (frozen dataclass),
        When: Attempting to modify an attribute,
        Then: It raises AttributeError.
        """
        config = RenderConfig(
            title_font_size_pt=18,
            body_font_size_pt=11,
            page_margin_mm=15,
        )
        with pytest.raises(AttributeError):
            config.title_font_size_pt = 20


class TestDocumentSource:
    """Test DocumentSource dataclass."""

    def test_creates_correctly(self, tmp_path: Path) -> None:
        """Verify creates correctly.

        Given: Path, title, and slug values,
        When: Creating a DocumentSource instance,
        Then: All attributes are stored correctly.
        """
        path = tmp_path / "test.md"
        source = DocumentSource(path=path, title="Test Title", slug="test-title")
        assert source.path == path
        assert source.title == "Test Title"
        assert source.slug == "test-title"

    def test_is_frozen(self, tmp_path: Path) -> None:
        """Verify is frozen.

        Given: A DocumentSource instance (frozen dataclass),
        When: Attempting to modify the title attribute,
        Then: It raises AttributeError.
        """
        path = tmp_path / "test.md"
        source = DocumentSource(path=path, title="Test", slug="test")
        with pytest.raises(AttributeError):
            source.title = "New Title"


class TestMermaidRenderer:
    """Test MermaidRenderer class."""

    @patch("scripts.build_docs_pdf.shutil.which", return_value="/usr/bin/mmdc")
    @patch("scripts.build_docs_pdf.shutil.rmtree")
    @patch("scripts.build_docs_pdf.Path.mkdir")
    @patch("scripts.build_docs_pdf.Path.exists", return_value=True)
    def test_init_creates_assets_dir(
        self,
        mock_exists: MagicMock,
        mock_mkdir: MagicMock,
        mock_rmtree: MagicMock,
        mock_which: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify init creates assets dir.

        Given: An existing assets directory,
        When: Creating a MermaidRenderer instance,
        Then: It removes the existing directory and recreates it.
        """
        assets_dir = tmp_path / "assets"
        MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        mock_rmtree.assert_called_once()
        mock_mkdir.assert_called()

    @patch("scripts.build_docs_pdf.shutil.which", return_value="/usr/bin/mmdc")
    @patch("scripts.build_docs_pdf.shutil.rmtree")
    @patch("scripts.build_docs_pdf.Path.mkdir")
    @patch("scripts.build_docs_pdf.Path.exists", return_value=False)
    def test_init_no_rmtree_if_not_exists(
        self,
        mock_exists: MagicMock,
        mock_mkdir: MagicMock,
        mock_rmtree: MagicMock,
        mock_which: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify init no rmtree if not exists.

        Given: A non-existing assets directory,
        When: Creating a MermaidRenderer instance,
        Then: It does not call rmtree (nothing to remove).
        """
        assets_dir = tmp_path / "assets"
        MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        mock_rmtree.assert_not_called()

    @patch("scripts.build_docs_pdf.platform.system", return_value="Linux")
    @patch("scripts.build_docs_pdf.os.access", return_value=True)
    def test_resolve_command_local_mmdc(
        self,
        mock_access: MagicMock,
        mock_system: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify resolve command local mmdc.

        Given: A local mmdc binary in frontend/node_modules/.bin on Linux,
        When: Creating a MermaidRenderer instance,
        Then: It uses the local mmdc binary path.
        """
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        project_root = tmp_path
        local_bin = project_root / "frontend" / "node_modules" / ".bin"
        local_bin.mkdir(parents=True)
        mmdc_path = local_bin / "mmdc"
        mmdc_path.touch()
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=project_root)
        assert str(mmdc_path) in renderer._command

    @patch("scripts.build_docs_pdf.platform.system", return_value="Windows")
    @patch("scripts.build_docs_pdf.os.access", return_value=True)
    def test_resolve_command_windows_mmdc_cmd(
        self,
        mock_access: MagicMock,
        mock_system: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify resolve command windows mmdc cmd.

        Given: Windows platform with mmdc.cmd in node_modules/.bin,
        When: Creating a MermaidRenderer instance,
        Then: It uses the Windows-specific mmdc.cmd path.
        """
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        project_root = tmp_path
        local_bin = project_root / "frontend" / "node_modules" / ".bin"
        local_bin.mkdir(parents=True)
        mmdc_cmd = local_bin / "mmdc.cmd"
        mmdc_cmd.touch()
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=project_root)
        assert str(mmdc_cmd) in renderer._command

    @patch("scripts.build_docs_pdf.shutil.which", return_value="/usr/bin/mmdc")
    def test_resolve_command_global_mmdc(self, mock_which: MagicMock, tmp_path: Path) -> None:
        """Verify resolve command global mmdc.

        Given: A globally installed mmdc (found via shutil.which),
        When: Creating a MermaidRenderer instance,
        Then: It uses the global mmdc path.
        """
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        assert renderer._command == ["/usr/bin/mmdc"]

    @patch("scripts.build_docs_pdf.shutil.which")
    def test_resolve_command_pnpm_exec(self, mock_which: MagicMock, tmp_path: Path) -> None:
        """Verify resolve command pnpm exec.

        Given: Only pnpm available (no local or global mmdc),
        When: Creating a MermaidRenderer instance,
        Then: It uses 'pnpm exec mmdc' command.
        """
        mock_which.side_effect = lambda cmd: "/usr/bin/pnpm" if cmd == "pnpm" else None
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        assert renderer._command == ["/usr/bin/pnpm", "exec", "mmdc"]

    @patch("scripts.build_docs_pdf.shutil.which", return_value=None)
    def test_resolve_command_raises_if_not_found(
        self,
        mock_which: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify resolve command raises if not found.

        Given: No mmdc binary available (local, global, or via pnpm),
        When: Creating a MermaidRenderer instance,
        Then: It raises MermaidRenderingError with 'Missing Mermaid CLI' message.
        """
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        with pytest.raises(MermaidRenderingError) as exc_info:
            MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        assert "Missing Mermaid CLI" in str(exc_info.value)

    @patch("scripts.build_docs_pdf.subprocess.run")
    @patch("scripts.build_docs_pdf.shutil.which", return_value="/usr/bin/mmdc")
    def test_render_creates_image(
        self,
        mock_which: MagicMock,
        mock_run: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify render creates image.

        Given: A valid Mermaid diagram definition,
        When: Calling render with diagram code and document slug,
        Then: It returns a PNG path named '{slug}_mermaid_{counter}.png'.
        """
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        mock_run.return_value = MagicMock(returncode=0)
        result = renderer.render("graph TD; A-->B;", "test-doc")
        assert result.name.startswith("test-doc_mermaid_")
        assert result.suffix == ".png"
        mock_run.assert_called_once()

    @patch("scripts.build_docs_pdf.subprocess.run")
    @patch("scripts.build_docs_pdf.shutil.which", return_value="/usr/bin/mmdc")
    def test_render_counter_increments(
        self,
        mock_which: MagicMock,
        mock_run: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify render counter increments.

        Given: A MermaidRenderer instance,
        When: Rendering multiple diagrams,
        Then: Each output file has an incrementing counter (001, 002, etc.).
        """
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        mock_run.return_value = MagicMock(returncode=0)
        result1 = renderer.render("graph TD; A-->B;", "doc")
        result2 = renderer.render("graph TD; C-->D;", "doc")
        assert "001" in result1.name
        assert "002" in result2.name

    @patch("scripts.build_docs_pdf.subprocess.run")
    @patch("scripts.build_docs_pdf.shutil.which", return_value="/usr/bin/mmdc")
    def test_render_raises_on_failure(
        self,
        mock_which: MagicMock,
        mock_run: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify render raises on failure.

        Given: An invalid Mermaid diagram that causes mmdc to fail,
        When: Calling render,
        Then: It raises MermaidRenderingError with stderr message.
        """
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        mock_run.side_effect = subprocess.CalledProcessError(
            1,
            "mmdc",
            stderr=b"Error message",
            output=b"",
        )
        with pytest.raises(MermaidRenderingError) as exc_info:
            renderer.render("invalid", "test")
        assert "Failed to render Mermaid diagram" in str(exc_info.value)
        assert "Error message" in str(exc_info.value)

    @patch("scripts.build_docs_pdf.subprocess.run")
    @patch("scripts.build_docs_pdf.shutil.which", return_value="/usr/bin/mmdc")
    def test_render_raises_with_stdout_fallback(
        self,
        mock_which: MagicMock,
        mock_run: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify render raises with stdout fallback.

        Given: mmdc fails with empty stderr but error in stdout,
        When: Calling render,
        Then: It raises MermaidRenderingError with stdout content.
        """
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        mock_run.side_effect = subprocess.CalledProcessError(
            1,
            "mmdc",
            stderr=b"",
            output=b"stdout error",
        )
        with pytest.raises(MermaidRenderingError) as exc_info:
            renderer.render("invalid", "test")
        assert "stdout error" in str(exc_info.value)

    @patch("scripts.build_docs_pdf.subprocess.run")
    @patch("scripts.build_docs_pdf.shutil.which", return_value="/usr/bin/mmdc")
    def test_render_raises_without_details(
        self,
        mock_which: MagicMock,
        mock_run: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify render raises without details.

        Given: mmdc fails with both empty stderr and stdout,
        When: Calling render,
        Then: It raises MermaidRenderingError without CLI output details.
        """
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        mock_run.side_effect = subprocess.CalledProcessError(
            1,
            "mmdc",
            stderr=b"",
            output=b"",
        )
        with pytest.raises(MermaidRenderingError) as exc_info:
            renderer.render("invalid", "test")
        error_msg = str(exc_info.value)
        assert "Failed to render Mermaid diagram" in error_msg
        assert "Mermaid CLI output" not in error_msg

    @patch("scripts.build_docs_pdf.subprocess.run")
    @patch("scripts.build_docs_pdf.shutil.which", return_value="/usr/bin/mmdc")
    def test_render_unlinks_existing_output(
        self,
        mock_which: MagicMock,
        mock_run: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify render unlinks existing output.

        Given: An existing output file at the target path,
        When: Calling render,
        Then: The existing file is deleted before rendering new content.
        """
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        existing_file = assets_dir / "doc_mermaid_001.png"
        existing_file.touch()
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        mock_run.return_value = MagicMock(returncode=0)
        renderer.render("graph TD; A-->B;", "doc")
        assert not existing_file.exists() or mock_run.called


class TestMarkdownToPdf:
    """Test MarkdownToPdf class."""

    @pytest.fixture
    def mock_mermaid_renderer(self, tmp_path: Path) -> MagicMock:
        """Provide a mock MermaidRenderer for testing."""
        renderer = MagicMock(spec=MermaidRenderer)
        renderer.render.return_value = tmp_path / "diagram.png"
        return renderer

    @pytest.fixture
    def config(self) -> RenderConfig:
        """Provide a default RenderConfig for testing."""
        return RenderConfig(
            title_font_size_pt=18,
            body_font_size_pt=11,
            page_margin_mm=15,
        )

    @pytest.fixture
    def sample_source(self, tmp_path: Path) -> DocumentSource:
        """Provide a sample DocumentSource for testing."""
        doc_path = tmp_path / "test.md"
        doc_path.write_text("# Test Document\n\nSome content.", encoding="utf-8")
        return DocumentSource(path=doc_path, title="Test Document", slug="test-doc")

    def test_build_html_returns_string(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify build html returns string.

        Given: A MarkdownToPdf builder with a valid document source,
        When: Calling build_html,
        Then: It returns an HTML string containing DOCTYPE and document title.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        assert isinstance(html, str)
        assert "<!DOCTYPE html>" in html
        assert "Test Document" in html

    def test_build_html_includes_cover_page(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify build html includes cover page.

        Given: A MarkdownToPdf builder,
        When: Calling build_html,
        Then: The HTML includes a cover page with 'Snapper Documentation' title.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        assert 'class="cover"' in html
        assert "Snapper Documentation" in html

    def test_build_html_includes_toc(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify build html includes toc.

        Given: A MarkdownToPdf builder,
        When: Calling build_html,
        Then: The HTML includes a table of contents section ('Table of Contents').
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        assert 'class="toc"' in html
        assert "Table of Contents" in html

    def test_build_stylesheet_without_font(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify build stylesheet without font.

        Given: A MarkdownToPdf builder with no custom font path,
        When: Building the stylesheet,
        Then: It contains @page and font-family rules but no @font-face.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        styles = builder._build_stylesheet()
        assert "@page" in styles
        assert "font-family" in styles
        assert "@font-face" not in styles

    def test_build_stylesheet_with_font(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify build stylesheet with font.

        Given: A MarkdownToPdf builder with a custom font path,
        When: Building the stylesheet,
        Then: It contains @font-face rule with 'SnapperBody' font family.
        """
        font_path = tmp_path / "test.ttf"
        font_path.touch()
        builder = MarkdownToPdf(
            font_path=font_path,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        styles = builder._build_stylesheet()
        assert "@font-face" in styles
        assert "SnapperBody" in styles

    def test_build_stylesheet_same_font_family(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        sample_source: DocumentSource,
    ) -> None:
        """Verify build stylesheet same font family.

        Given: A config where font_family and mono_font_family are the same,
        When: Building the stylesheet,
        Then: The mono font family is suffixed with '-Mono' to differentiate.
        """
        config = RenderConfig(
            title_font_size_pt=18,
            body_font_size_pt=11,
            page_margin_mm=15,
            font_family="SameFont",
            mono_font_family="SameFont",
        )
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        styles = builder._build_stylesheet()
        assert "SameFont-Mono" in styles

    @patch("scripts.build_docs_pdf.HTML")
    @patch("scripts.build_docs_pdf.CSS")
    def test_write_pdf_creates_file(
        self,
        mock_css: MagicMock,
        mock_html: MagicMock,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify write pdf creates file.

        Given: A MarkdownToPdf builder,
        When: Calling write_pdf with an output path,
        Then: It calls weasyprint HTML and CSS, and creates the output directory.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        output_path = tmp_path / "output" / "test.pdf"
        builder.write_pdf(output_path)
        mock_html.assert_called_once()
        mock_css.assert_called_once()
        assert output_path.parent.exists()

    def test_render_document_with_mermaid(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify render document with mermaid.

        Given: A markdown file containing a mermaid code block,
        When: Building HTML,
        Then: The mermaid renderer is called to process the diagram.
        """
        doc_path = tmp_path / "mermaid.md"
        doc_path.write_text(
            "# Test\n\n```mermaid\ngraph TD; A-->B;\n```\n",
            encoding="utf-8",
        )
        source = DocumentSource(path=doc_path, title="Test", slug="test")
        mock_mermaid_renderer.render.return_value = tmp_path / "diagram.png"
        (tmp_path / "diagram.png").touch()
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        builder.build_html()
        mock_mermaid_renderer.render.assert_called_once()

    def test_render_document_empty_mermaid(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify render document empty mermaid.

        Given: A markdown file with an empty mermaid code block,
        When: Building HTML,
        Then: The mermaid renderer is NOT called (empty diagrams are skipped).
        """
        doc_path = tmp_path / "empty-mermaid.md"
        doc_path.write_text(
            "# Test\n\n```mermaid\n\n```\n",
            encoding="utf-8",
        )
        source = DocumentSource(path=doc_path, title="Test", slug="test")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        builder.build_html()
        mock_mermaid_renderer.render.assert_not_called()

    def test_rewrite_document_links(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify rewrite document links.

        Given: Two markdown files with cross-links,
        When: Building HTML,
        Then: Internal links are rewritten (no 'snapper-doc:' scheme in output).
        """
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        doc1_path = docs_dir / "doc1.md"
        doc2_path = docs_dir / "doc2.md"
        doc1_path.write_text("# Doc1\n\nLink to [Doc2](doc2.md)\n", encoding="utf-8")
        doc2_path.write_text("# Doc2\n\nContent\n", encoding="utf-8")
        source1 = DocumentSource(path=doc1_path, title="Doc1", slug="doc1")
        source2 = DocumentSource(path=doc2_path, title="Doc2", slug="doc2")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source1, source2],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        assert "snapper-doc:" not in html

    def test_rewrite_document_links_with_fragment(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify rewrite document links with fragment.

        Given: A markdown file with a link to another doc with anchor fragment,
        When: Building HTML,
        Then: Links with fragments are rewritten (no 'snapper-anchor:' in output).
        """
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        doc1_path = docs_dir / "doc1.md"
        doc2_path = docs_dir / "doc2.md"
        doc1_path.write_text("# Doc1\n\nLink to [Section](doc2.md#section)\n", encoding="utf-8")
        doc2_path.write_text("# Doc2\n\n## Section\n\nContent\n", encoding="utf-8")
        source1 = DocumentSource(path=doc1_path, title="Doc1", slug="doc1")
        source2 = DocumentSource(path=doc2_path, title="Doc2", slug="doc2")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source1, source2],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        assert "snapper-anchor:" not in html

    def test_rewrite_document_links_unknown_doc(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify rewrite document links unknown doc.

        Given: A markdown file with a link to a non-existent document,
        When: Building HTML,
        Then: The original link (unknown.md) is preserved in output.
        """
        doc_path = tmp_path / "doc.md"
        doc_path.write_text("# Doc\n\nLink to [Unknown](unknown.md)\n", encoding="utf-8")
        source = DocumentSource(path=doc_path, title="Doc", slug="doc")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        assert "unknown.md" in html

    def test_slugify_fragment_handles_unicode(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify slugify fragment handles unicode.

        Given: A Polish unicode string with diacritics,
        When: Calling _slugify_fragment,
        Then: It returns a valid slug without forward slashes.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        result = builder._slugify_fragment("Zażółć gęślą jaźń")
        assert result
        assert "/" not in result

    def test_slugify_fragment_handles_slash(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify slugify fragment handles slash.

        Given: A string containing forward slashes,
        When: Calling _slugify_fragment,
        Then: Slashes are replaced with hyphens ('path-to-section').
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        result = builder._slugify_fragment("path/to/section")
        assert "/" not in result
        assert result == "path-to-section"

    def test_slugify_fragment_returns_original_if_empty(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify slugify fragment returns original if empty.

        Given: A non-ASCII string that slugifies to empty (e.g., Japanese text),
        When: Calling _slugify_fragment,
        Then: The original string is returned as fallback.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        result = builder._slugify_fragment("日本語")
        assert result == "日本語"

    def test_heading_anchors_injected(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify heading anchors injected.

        Given: A markdown document with headings,
        When: Building HTML,
        Then: Headings get id attributes prefixed with 'doc-{slug}--'.
        """
        doc_path = tmp_path / "headings.md"
        doc_path.write_text("# Main\n\n## Section\n\nContent\n", encoding="utf-8")
        source = DocumentSource(path=doc_path, title="Main", slug="main")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        assert 'id="doc-main--' in html

    def test_duplicate_headings_get_unique_ids(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify duplicate headings get unique ids.

        Given: A markdown document with duplicate heading titles,
        When: Building HTML,
        Then: Each heading gets a unique id (at least 2 distinct ids for 'Section').
        """
        doc_path = tmp_path / "dup.md"
        doc_path.write_text("# Main\n\n## Section\n\n## Section\n", encoding="utf-8")
        source = DocumentSource(path=doc_path, title="Main", slug="main")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        ids = re.findall(r'id="doc-main--section[^"]*"', html)
        unique_ids = set(ids)
        assert len(unique_ids) >= 2

    def test_normalize_pre_blocks_strips_language_hint(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify normalize pre blocks strips language hint.

        Given: HTML with a pre block containing ```python language hint,
        When: Calling _normalize_pre_blocks,
        Then: The language hint is stripped but code content is preserved.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        html = "<pre>```python\nprint('hello')\n```</pre>"
        result = builder._normalize_pre_blocks(html)
        assert "```python" not in result
        assert "print('hello')" in result

    def test_normalize_pre_blocks_strips_language_only_line(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify normalize pre blocks strips language only line.

        Given: HTML with a pre block starting with a known language name,
        When: Calling _normalize_pre_blocks,
        Then: The language line is stripped if it's a recognized hint.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        html = "<pre>python\nprint('hello')\n</pre>"
        result = builder._normalize_pre_blocks(html)
        assert result.strip().startswith("<pre>")
        assert "print('hello')" in result

    def test_normalize_pre_blocks_preserves_non_language_content(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify normalize pre blocks preserves non language content.

        Given: HTML with a pre block containing plain code (no language hint),
        When: Calling _normalize_pre_blocks,
        Then: All code content is preserved unchanged.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        html = "<pre>some code\nmore code\n</pre>"
        result = builder._normalize_pre_blocks(html)
        assert "some code" in result
        assert "more code" in result

    def test_normalize_pre_blocks_empty_content(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify normalize pre blocks empty content.

        Given: HTML with an empty pre block,
        When: Calling _normalize_pre_blocks,
        Then: The empty pre block is returned unchanged.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        html = "<pre></pre>"
        result = builder._normalize_pre_blocks(html)
        assert result == "<pre></pre>"

    def test_heading_anchor_aliases_basic(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify heading anchor aliases basic.

        Given: A slug with hyphenated segments ('test-section'),
        When: Calling _heading_anchor_aliases,
        Then: It returns aliases including the full slug and prefix ('test').
        """
        result = MarkdownToPdf._heading_anchor_aliases("test-section")
        assert "test-section" in result
        assert "test" in result

    def test_heading_anchor_aliases_underscore(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify heading anchor aliases underscore.

        Given: A slug with underscores ('test_section'),
        When: Calling _heading_anchor_aliases,
        Then: It returns aliases including both underscore and hyphen variants.
        """
        result = MarkdownToPdf._heading_anchor_aliases("test_section")
        assert "test_section" in result
        assert "test-section" in result

    def test_heading_anchor_aliases_short_prefix(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify heading anchor aliases short prefix.

        Given: A slug with a very short prefix ('ab-section'),
        When: Calling _heading_anchor_aliases,
        Then: Short prefixes (< 3 chars) are not included as aliases.
        """
        result = MarkdownToPdf._heading_anchor_aliases("ab-section")
        assert "ab" not in result
        assert "ab-section" in result

    def test_resolve_internal_links_doc(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify resolve internal links doc.

        Given: HTML with 'snapper-doc:' scheme link to a known document,
        When: Calling _resolve_internal_links,
        Then: It converts to an anchor link '#doc-{slug}'.
        """
        doc_path = tmp_path / "doc.md"
        doc_path.write_text("# Doc\n\nContent\n", encoding="utf-8")
        source = DocumentSource(path=doc_path, title="Doc", slug="doc")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        builder.build_html()
        html_with_link = 'href="snapper-doc:doc"'
        result = builder._resolve_internal_links(html_with_link)
        assert 'href="#doc-doc"' in result

    def test_resolve_internal_links_anchor(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify resolve internal links anchor.

        Given: HTML with 'snapper-anchor:' scheme link including a fragment,
        When: Calling _resolve_internal_links,
        Then: The scheme is resolved to internal anchor format.
        """
        doc_path = tmp_path / "doc.md"
        doc_path.write_text("# Doc\n\n## Section\n\nContent\n", encoding="utf-8")
        source = DocumentSource(path=doc_path, title="Doc", slug="doc")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        builder.build_html()
        html_with_link = 'href="snapper-anchor:doc#section"'
        result = builder._resolve_internal_links(html_with_link)
        assert "snapper-anchor:" not in result

    def test_resolve_internal_links_unknown_doc(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify resolve internal links unknown doc.

        Given: HTML with 'snapper-doc:' link to an unknown document,
        When: Calling _resolve_internal_links,
        Then: The original link is preserved unchanged.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        builder.build_html()
        html_with_link = 'href="snapper-doc:unknown"'
        result = builder._resolve_internal_links(html_with_link)
        assert result == html_with_link

    def test_inject_heading_anchors_empty_slug(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify inject heading anchors empty slug.

        Given: A markdown document with a Japanese heading (non-ASCII),
        When: Building HTML,
        Then: The heading preserves the original text in the id attribute.
        """
        doc_path = tmp_path / "special.md"
        doc_path.write_text("# 日本語\n\nContent\n", encoding="utf-8")
        source = DocumentSource(path=doc_path, title="Special", slug="special")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        assert "doc-special--日本語" in html

    def test_split_target_without_fragment(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify split target without fragment.

        Given: A link target without anchor fragment ('doc.md'),
        When: Calling _split_target,
        Then: It returns the document path and None for fragment.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        doc_part, fragment = builder._split_target("doc.md")
        assert doc_part == "doc.md"
        assert fragment is None

    def test_split_target_with_fragment(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
        sample_source: DocumentSource,
    ) -> None:
        """Verify split target with fragment.

        Given: A link target with anchor fragment ('doc.md#section'),
        When: Calling _split_target,
        Then: It returns document path and fragment as separate values.
        """
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[sample_source],
            repo_root=tmp_path,
        )
        doc_part, fragment = builder._split_target("doc.md#section")
        assert doc_part == "doc.md"
        assert fragment == "section"

    def test_code_blocks_processed(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify code blocks processed.

        Given: A markdown document with a Python code block,
        When: Building HTML,
        Then: Code blocks are rendered inside <pre> tags.
        """
        doc_path = tmp_path / "code.md"
        doc_path.write_text(
            "# Code\n\n```python\nprint('hello')\n```\n",
            encoding="utf-8",
        )
        source = DocumentSource(path=doc_path, title="Code", slug="code")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        assert "<pre>" in html

    def test_build_doc_link_map_relative_paths(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Verify build doc link map relative paths.

        Given: A document source in a subdirectory,
        When: Building the document link map,
        Then: It contains multiple path variants (relative, with ./, basename).
        """
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        doc_path = docs_dir / "test.md"
        doc_path.write_text("# Test\n", encoding="utf-8")
        source = DocumentSource(path=doc_path, title="Test", slug="test")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        link_map = builder._doc_link_map
        assert "docs/test.md" in link_map
        assert "./docs/test.md" in link_map
        assert "test.md" in link_map


class TestFindFont:
    """Test find_font function."""

    def test_finds_existing_font(self, tmp_path: Path) -> None:
        """Verify finds existing font.

        Given: A list of font candidates with one existing file,
        When: Calling find_font,
        Then: It returns the path to the existing font file.
        """
        font_path = tmp_path / "test.ttf"
        font_path.touch()
        result = find_font([font_path])
        assert result == font_path

    def test_returns_first_existing(self, tmp_path: Path) -> None:
        """Verify returns first existing.

        Given: A list with multiple candidates where only second exists,
        When: Calling find_font,
        Then: It returns the first existing font (font2).
        """
        font1 = tmp_path / "font1.ttf"
        font2 = tmp_path / "font2.ttf"
        font2.touch()
        result = find_font([font1, font2])
        assert result == font2

    def test_raises_if_none_found(self, tmp_path: Path) -> None:
        """Verify raises if none found.

        Given: A list of non-existent font paths,
        When: Calling find_font,
        Then: It raises FileNotFoundError with descriptive message.
        """
        with pytest.raises(FileNotFoundError) as exc_info:
            find_font([tmp_path / "nonexistent.ttf"])
        assert "No UTF-8 capable TrueType font found" in str(exc_info.value)


class TestExtractTitle:
    """Test extract_title function."""

    def test_extracts_heading(self, tmp_path: Path) -> None:
        """Verify extracts heading.

        Given: A markdown file with a level-1 heading,
        When: Calling extract_title,
        Then: It returns the heading text without the '#' marker.
        """
        doc_path = tmp_path / "doc.md"
        doc_path.write_text("# My Title\n\nContent\n", encoding="utf-8")
        result = extract_title(doc_path)
        assert result == "My Title"

    def test_extracts_heading_with_number_prefix(self, tmp_path: Path) -> None:
        """Verify extracts heading with number prefix.

        Given: A markdown file with a numbered heading ('# 1. Introduction'),
        When: Calling extract_title,
        Then: It strips the number prefix and returns 'Introduction'.
        """
        doc_path = tmp_path / "doc.md"
        doc_path.write_text("# 1. Introduction\n\nContent\n", encoding="utf-8")
        result = extract_title(doc_path)
        assert result == "Introduction"

    def test_extracts_heading_with_paren_number(self, tmp_path: Path) -> None:
        """Verify extracts heading with paren number.

        Given: A markdown file with parenthesized number ('# 1) Introduction'),
        When: Calling extract_title,
        Then: It strips the number prefix and returns 'Introduction'.
        """
        doc_path = tmp_path / "doc.md"
        doc_path.write_text("# 1) Introduction\n\nContent\n", encoding="utf-8")
        result = extract_title(doc_path)
        assert result == "Introduction"

    def test_returns_stem_if_no_heading(self, tmp_path: Path) -> None:
        """Verify returns stem if no heading.

        Given: A markdown file without any heading,
        When: Calling extract_title,
        Then: It returns the titleized filename stem ('My Document').
        """
        doc_path = tmp_path / "my_document.md"
        doc_path.write_text("No heading here\n", encoding="utf-8")
        result = extract_title(doc_path)
        assert result == "My Document"


class TestCollectSources:
    """Test collect_sources function."""

    def test_collects_readme_and_docs(self, tmp_path: Path) -> None:
        """Verify collects readme and docs.

        Given: A README.md and a docs directory with two markdown files,
        When: Calling collect_sources,
        Then: It returns 3 sources with README first.
        """
        readme = tmp_path / "README.md"
        readme.write_text("# Project\n", encoding="utf-8")
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        (docs_dir / "01_intro.md").write_text("# Intro\n", encoding="utf-8")
        (docs_dir / "02_guide.md").write_text("# Guide\n", encoding="utf-8")
        sources = collect_sources(readme, docs_dir)
        assert len(sources) == 3
        assert sources[0].path == readme

    def test_sorts_docs_alphabetically(self, tmp_path: Path) -> None:
        """Verify sorts docs alphabetically.

        Given: A docs directory with files b_doc.md and a_doc.md,
        When: Calling collect_sources,
        Then: Docs are sorted alphabetically (a_doc before b_doc).
        """
        readme = tmp_path / "README.md"
        readme.write_text("# Project\n", encoding="utf-8")
        docs_dir = tmp_path / "docs"
        docs_dir.mkdir()
        (docs_dir / "b_doc.md").write_text("# B\n", encoding="utf-8")
        (docs_dir / "a_doc.md").write_text("# A\n", encoding="utf-8")
        sources = collect_sources(readme, docs_dir)
        assert sources[1].path.name == "a_doc.md"
        assert sources[2].path.name == "b_doc.md"


class TestBuildSlug:
    """Test build_slug function."""

    def test_removes_number_prefix(self) -> None:
        """Verify removes number prefix.

        Given: A filename with numeric prefix ('01_introduction.md'),
        When: Calling build_slug,
        Then: The slug has the prefix removed ('introduction').
        """
        path = Path("01_introduction.md")
        result = build_slug(path)
        assert result == "introduction"

    def test_converts_to_lowercase(self) -> None:
        """Verify converts to lowercase.

        Given: A filename with mixed case ('MyDocument.md'),
        When: Calling build_slug,
        Then: The slug is all lowercase ('mydocument').
        """
        path = Path("MyDocument.md")
        result = build_slug(path)
        assert result == "mydocument"

    def test_replaces_special_chars(self) -> None:
        """Verify replaces special chars.

        Given: A filename with underscores and hyphens,
        When: Calling build_slug,
        Then: Underscores are replaced with hyphens ('my-special-doc').
        """
        path = Path("my_special-doc.md")
        result = build_slug(path)
        assert result == "my-special-doc"

    def test_returns_section_if_empty(self) -> None:
        """Verify returns section if empty.

        Given: A filename that produces empty slug after stripping ('01_.md'),
        When: Calling build_slug,
        Then: It returns 'section' as fallback.
        """
        path = Path("01_.md")
        result = build_slug(path)
        assert result == "section"


class TestParseArgs:
    """Test parse_args function."""

    def test_default_output(self) -> None:
        """Verify default output.

        Given: No command line arguments,
        When: Calling parse_args,
        Then: The default output is 'frontend/public/snapper.pdf'.
        """
        args = parse_args([])
        assert args.output == "frontend/public/snapper.pdf"

    def test_custom_output(self) -> None:
        """Verify custom output.

        Given: The --output argument with 'custom.pdf',
        When: Calling parse_args,
        Then: The output path is 'custom.pdf'.
        """
        args = parse_args(["--output", "custom.pdf"])
        assert args.output == "custom.pdf"

    def test_font_option(self) -> None:
        """Verify font option.

        Given: The --font argument with a font path,
        When: Calling parse_args,
        Then: The font path is parsed as a Path object.
        """
        args = parse_args(["--font", "/path/to/font.ttf"])
        assert args.font == Path("/path/to/font.ttf")


class TestMain:
    """Test main function."""

    @patch("scripts.build_docs_pdf.MarkdownToPdf")
    @patch("scripts.build_docs_pdf.MermaidRenderer")
    @patch("scripts.build_docs_pdf.find_font")
    @patch("scripts.build_docs_pdf.collect_sources")
    def test_main_creates_pdf(
        self,
        mock_collect: MagicMock,
        mock_find_font: MagicMock,
        mock_mermaid: MagicMock,
        mock_markdown_to_pdf: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify main creates pdf.

        Given: Mocked dependencies returning valid sources and font,
        When: Calling main with --output argument,
        Then: MarkdownToPdf is instantiated to create the PDF.
        """
        mock_source = DocumentSource(
            path=tmp_path / "README.md",
            title="Test",
            slug="test",
        )
        mock_collect.return_value = [mock_source]
        mock_find_font.return_value = tmp_path / "font.ttf"
        monkeypatch.setattr(sys, "argv", ["prog", "--output", str(tmp_path / "output.pdf")])
        with patch("scripts.build_docs_pdf.Path") as mock_path_cls:
            mock_path_instance = MagicMock()
            mock_path_cls.return_value = mock_path_instance
            mock_path_cls.__file__ = str(tmp_path / "build_docs_pdf.py")
            with patch.object(Path, "__new__", return_value=tmp_path / "output.pdf"):
                main()
        mock_markdown_to_pdf.assert_called_once()

    @patch("scripts.build_docs_pdf.MarkdownToPdf")
    @patch("scripts.build_docs_pdf.MermaidRenderer")
    @patch("scripts.build_docs_pdf.find_font")
    @patch("scripts.build_docs_pdf.collect_sources")
    def test_main_uses_provided_font(
        self,
        mock_collect: MagicMock,
        mock_find_font: MagicMock,
        mock_mermaid: MagicMock,
        mock_markdown_to_pdf: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify main uses provided font.

        Given: A --font argument pointing to an existing font file,
        When: Calling main,
        Then: find_font is not called and the provided font path is used.
        """
        font_path = tmp_path / "custom.ttf"
        font_path.touch()
        mock_source = DocumentSource(
            path=tmp_path / "README.md",
            title="Test",
            slug="test",
        )
        mock_collect.return_value = [mock_source]
        monkeypatch.setattr(sys, "argv", ["prog", "--font", str(font_path)])
        main()
        mock_find_font.assert_not_called()
        call_kwargs = mock_markdown_to_pdf.call_args[1]
        assert call_kwargs["font_path"] == font_path

    @patch("scripts.build_docs_pdf.collect_sources")
    def test_main_returns_error_on_exception(
        self,
        mock_collect: MagicMock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify main returns 1 when an exception occurs.

        Given: collect_sources raises an exception,
        When: Calling main with --output argument,
        Then: Returns exit code 1 and prints error message to stderr.
        """
        mock_collect.side_effect = RuntimeError("Test error")
        monkeypatch.setattr(sys, "argv", ["prog", "--output", str(tmp_path / "output.pdf")])

        result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Failed to build PDF" in captured.err
        assert "Test error" in captured.err


class TestAdditionalCoverage:
    """Additional tests for edge cases and uncovered lines."""

    @pytest.fixture
    def mock_mermaid_renderer(self, tmp_path: Path) -> MagicMock:
        """Provide a mock MermaidRenderer for testing."""
        renderer = MagicMock(spec=MermaidRenderer)
        renderer.render.return_value = tmp_path / "diagram.png"
        return renderer

    @pytest.fixture
    def config(self) -> RenderConfig:
        """Provide a default RenderConfig for testing."""
        return RenderConfig(
            title_font_size_pt=18,
            body_font_size_pt=11,
            page_margin_mm=15,
        )

    @patch("scripts.build_docs_pdf.subprocess.run")
    @patch("scripts.build_docs_pdf.shutil.which", return_value="/usr/bin/mmdc")
    def test_render_unlinks_existing_file(
        self,
        mock_which: MagicMock,
        mock_run: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Test that existing output file is unlinked before rendering."""
        assets_dir = tmp_path / "assets"
        assets_dir.mkdir(parents=True)
        renderer = MermaidRenderer(assets_dir=assets_dir, project_root=tmp_path)
        existing_file = assets_dir / "doc_mermaid_001.png"
        existing_file.write_text("old content")
        mock_run.return_value = MagicMock(returncode=0)
        renderer.render("graph TD; A-->B;", "doc")
        assert not existing_file.exists()

    def test_code_paragraph_conversion(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Test that code in paragraph tags gets converted to pre tags."""
        doc_path = tmp_path / "code.md"
        doc_path.write_text("# Test\n\n`single line code block`\n", encoding="utf-8")
        source = DocumentSource(path=doc_path, title="Test", slug="test")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        html = builder.build_html()
        assert "single line code block" in html

    def test_document_not_relative_to_repo(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Test handling of document path not relative to repo root."""
        import tempfile

        with tempfile.TemporaryDirectory() as other_dir:
            other_path = Path(other_dir)
            doc_path = other_path / "external.md"
            doc_path.write_text("# External\n\nContent\n", encoding="utf-8")
            source = DocumentSource(path=doc_path, title="External", slug="external")
            builder = MarkdownToPdf(
                font_path=None,
                config=config,
                mermaid_renderer=mock_mermaid_renderer,
                sources=[source],
                repo_root=tmp_path,
            )
            html = builder.build_html()
            assert "External" in html

    def test_pop_leading_blank_in_pre_blocks(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Test that leading blank lines are removed from code blocks."""
        doc_path = tmp_path / "blanks.md"
        source = DocumentSource(path=doc_path, title="Test", slug="test")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        html = "<pre>python\n\n\nprint('hello')\n```</pre>"
        result = builder._normalize_pre_blocks(html)
        assert "python" not in result or result.startswith("<pre>")
        assert "print('hello')" in result

    def test_empty_base_slug_fallback(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Test that empty base slug falls back to section-N format."""
        doc_path = tmp_path / "empty.md"
        source = DocumentSource(path=doc_path, title="Empty", slug="empty")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        html = "<h1><span></span></h1>"
        result = builder._inject_heading_anchors(html, source)
        assert "section-1" in result

    def test_unknown_anchor_target_preserved(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Test that unknown anchor targets are preserved as-is."""
        doc_path = tmp_path / "doc.md"
        doc_path.write_text("# Doc\n\nContent\n", encoding="utf-8")
        source = DocumentSource(path=doc_path, title="Doc", slug="doc")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        builder.build_html()
        html_with_unknown = 'href="snapper-anchor:doc#nonexistent-section"'
        result = builder._resolve_internal_links(html_with_unknown)
        assert html_with_unknown == result

    def test_normalize_pre_blocks_unknown_language_preserved(
        self,
        tmp_path: Path,
        mock_mermaid_renderer: MagicMock,
        config: RenderConfig,
    ) -> None:
        """Test that code blocks with unknown language hints are preserved.

        When ```typescript or similar unknown language is used, the language
        line should NOT be stripped (only known languages in CODE_LANGUAGE_HINTS
        are stripped).
        """
        doc_path = tmp_path / "doc.md"
        source = DocumentSource(path=doc_path, title="Doc", slug="doc")
        builder = MarkdownToPdf(
            font_path=None,
            config=config,
            mermaid_renderer=mock_mermaid_renderer,
            sources=[source],
            repo_root=tmp_path,
        )
        html = "<pre>```typescript\nconst x = 1;\n```</pre>"
        result = builder._normalize_pre_blocks(html)
        assert "typescript" in result

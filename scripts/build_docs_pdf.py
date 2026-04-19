"""Build PDF documentation from Markdown files.

Converts README.md and docs/*.md into a single PDF using WeasyPrint.
Supports syntax highlighting, custom fonts, and table of contents.
"""

import argparse
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unicodedata
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import cast

from markdown import markdown
from pygments.formatters import HtmlFormatter
from weasyprint import CSS
from weasyprint import HTML


def default_font_candidates() -> tuple[Path, ...]:
    """Return a tuple of candidate font paths for the current platform.

    Returns:
        Tuple of candidate font paths ordered by platform-specific preference.
    """
    candidates: list[Path] = [
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"),
        Path("/usr/share/fonts/truetype/freefont/FreeSans.ttf"),
    ]
    system_name = platform.system().lower()
    if system_name == "windows":
        fonts_dir = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts"
        candidates.extend(
            [
                fonts_dir / "segoeui.ttf",
                fonts_dir / "arial.ttf",
                fonts_dir / "calibri.ttf",
            ]
        )
    elif system_name == "darwin":
        fonts_dir = Path("/Library/Fonts")
        candidates.extend(
            [
                fonts_dir / "Arial Unicode.ttf",
                fonts_dir / "Arial.ttf",
                fonts_dir / "Helvetica.ttc",
            ]
        )
    else:
        home_fonts = Path.home() / ".fonts"
        candidates.append(home_fonts / "DejaVuSans.ttf")
    return tuple(dict.fromkeys(candidates))


MARKDOWN_EXTENSIONS = ["fenced_code", "tables", "toc", "codehilite"]
MARKDOWN_EXTENSION_CONFIGS: dict[str, dict[str, Any]] = {
    "codehilite": {
        "linenums": False,
        "guess_lang": False,
        "noclasses": False,
    }
}
CODE_LANGUAGE_HINTS = {
    "bash",
    "shell",
    "sh",
    "zsh",
    "fish",
    "python",
    "py",
    "json",
    "yaml",
    "yml",
    "toml",
    "ini",
    "sql",
    "ps1",
    "powershell",
    "cmd",
}


class MermaidRenderingError(RuntimeError):
    """Exception raised when Mermaid diagram rendering fails."""

    pass


class MermaidRenderer:
    """Render Mermaid diagrams to PNG images."""

    def __init__(
        self,
        assets_dir: Path,
        project_root: Path,
        *,
        width_px: int = 1200,
        scale: float = 1.5,
    ) -> None:
        """Initialize the instance.

        Args:
            assets_dir: Directory path for storing generated diagram assets.
            project_root: Root path of the project for locating Mermaid CLI.
            width_px: Width in pixels for rendered diagrams.
            scale: Scale factor for diagram rendering quality.
        """
        self._assets_dir = assets_dir
        if self._assets_dir.exists():
            shutil.rmtree(self._assets_dir)
        self._assets_dir.mkdir(parents=True, exist_ok=True)
        self._command = self._resolve_command(project_root)
        self._counter = 0
        self._width_px = width_px
        self._scale = scale

    def _resolve_command(self, project_root: Path) -> list[str]:
        """Resolve the Mermaid CLI command for the current environment.

        Args:
            project_root: Root path of the project for locating local CLI.

        Returns:
            Command arguments for invoking Mermaid CLI.
        """
        local_bin_dir = project_root / "frontend" / "node_modules" / ".bin"
        local_candidates = [local_bin_dir / "mmdc"]
        if platform.system().lower() == "windows":
            local_candidates.insert(0, local_bin_dir / "mmdc.cmd")
        for candidate in local_candidates:
            if candidate.exists() and os.access(candidate, os.X_OK):
                return [str(candidate)]
        binary = shutil.which("mmdc")
        if binary:
            return [binary]
        pnpm = shutil.which("pnpm")
        if pnpm:
            return [pnpm, "exec", "mmdc"]
        raise MermaidRenderingError(
            "Missing Mermaid CLI. Install '@mermaid-js/mermaid-cli' "
            "(for example `pnpm add -D @mermaid-js/mermaid-cli`) or ensure `mmdc` is on PATH."
        )

    def render(self, code: str, basename: str) -> Path:
        """Render Mermaid code and return path to output image.

        Args:
            code: Mermaid diagram source code to render.
            basename: Base name for the output image file.

        Returns:
            Path to the generated PNG image file.
        """
        self._counter += 1
        output_path = self._assets_dir / f"{basename}_mermaid_{self._counter:03d}.png"
        if output_path.exists():
            output_path.unlink()
        with tempfile.NamedTemporaryFile(
            "w",
            suffix=".mmd",
            delete=False,
            encoding="utf-8",
        ) as tmp_file:
            tmp_file.write(code)
            tmp_input = Path(tmp_file.name)
        command = [
            *self._command,
            "-i",
            str(tmp_input),
            "-o",
            str(output_path),
            "-b",
            "transparent",
            "--width",
            str(self._width_px),
            "--scale",
            str(self._scale),
        ]
        try:
            subprocess.run(command, check=True, capture_output=True)
        except subprocess.CalledProcessError as exc:
            stderr = exc.stderr.decode("utf-8", errors="ignore").strip()
            stdout = exc.stdout.decode("utf-8", errors="ignore").strip()
            details = stderr or stdout or ""
            raise MermaidRenderingError(
                "Failed to render Mermaid diagram. Ensure Mermaid CLI is installed and working."
                + (f"\n\nMermaid CLI output:\n{details}" if details else "")
            ) from exc
        finally:
            tmp_input.unlink(missing_ok=True)
        return output_path


@dataclass(frozen=True)
class RenderConfig:
    """Configuration for PDF rendering parameters."""

    title_font_size_pt: int
    body_font_size_pt: int
    page_margin_mm: int
    font_family: str = "SnapperBody"
    mono_font_family: str = "SnapperMono"


@dataclass(frozen=True)
class DocumentSource:
    """Represents a Markdown source document with metadata."""

    path: Path
    title: str
    slug: str


class MarkdownToPdf:
    """Convert Markdown documents to a single PDF file."""

    def __init__(
        self,
        font_path: Path | None,
        config: RenderConfig,
        mermaid_renderer: MermaidRenderer,
        *,
        sources: Sequence[DocumentSource],
        repo_root: Path,
    ) -> None:
        """Initialize the instance.

        Args:
            font_path: Path to a TrueType font file, or None for system default.
            config: Rendering configuration for PDF output.
            mermaid_renderer: Renderer instance for Mermaid diagrams.
            sources: Sequence of document sources to include in the PDF.
            repo_root: Root path of the repository for resolving relative paths.
        """
        self._font_path = font_path
        self._config = config
        self._mermaid_renderer = mermaid_renderer
        self._repo_root = repo_root
        self._sources = list(sources)
        self._doc_link_map = self._build_doc_link_map(repo_root)
        self._anchor_targets: dict[tuple[str, str], str] = {}
        self._doc_top_ids: dict[str, str] = {}
        self._heading_slug_usage: dict[tuple[str, str], int] = defaultdict(int)
        self._debug_html_dir = repo_root / "build" / "docs" / "pdf-debug"
        self._combined_debug_name = "__combined.html"
        self._styles = self._build_stylesheet()

    def build_html(self) -> str:
        """Build combined HTML from all document sources.

        Returns:
            Combined HTML string with cover page, table of contents, and documents.
        """
        sections: list[str] = [self._render_cover_page(), self._render_table_of_contents()]
        document_sections: list[str] = []
        for document in self._sources:
            section_html = self._render_document(document)
            document_sections.append(section_html)
        sections.extend(document_sections)
        combined_html = self._wrap_html("\n".join(sections))
        combined_html = self._resolve_internal_links(combined_html)
        self._write_combined_debug(combined_html)
        return combined_html

    def write_pdf(self, destination: Path) -> None:
        """Generate PDF from HTML and write to destination.

        Args:
            destination: File path where the PDF will be written.
        """
        html_content = self.build_html()
        destination.parent.mkdir(parents=True, exist_ok=True)
        html = HTML(string=html_content, base_url=str(self._repo_root))
        css = CSS(string=self._styles, base_url=str(self._repo_root))
        html_obj = cast(Any, html)
        html_obj.write_pdf(target=str(destination), stylesheets=[css])

    def _build_stylesheet(self) -> str:
        font_face_block = ""
        if self._font_path is not None:
            font_uri = self._font_path.resolve().as_uri()
            font_face_block = textwrap.dedent(f"""
                @font-face {{
                    font-family: '{self._config.font_family}';
                    src: url('{font_uri}');
                }}
                """)
        mono_family = (
            self._config.mono_font_family
            if self._config.mono_font_family != self._config.font_family
            else f"{self._config.font_family}-Mono"
        )
        formatter = cast(Any, HtmlFormatter(style="xcode"))
        highlight_css: str = formatter.get_style_defs(".codehilite")
        base_css = textwrap.dedent(f"""
            {font_face_block}
            @page {{
                margin: {self._config.page_margin_mm}mm;
            }}
            body {{
                orphans: 3;
                widows: 3;
                font-family: '{self._config.font_family}',
                    'DejaVu Sans', 'Liberation Sans', sans-serif;
                color: #111827;
                line-height: 1.6;
                font-size: {self._config.body_font_size_pt}pt;
            }}
            section.cover {{
                text-align: center;
                display: flex;
                flex-direction: column;
                justify-content: center;
                height: 100vh;
                page-break-after: always;
            }}
            section.cover h1 {{
                font-size: {self._config.title_font_size_pt + 6}pt;
                margin-bottom: 0.5em;
                color: #0f172a;
            }}
            section.cover p {{
                font-size: {self._config.body_font_size_pt + 4}pt;
                margin: 0.2em 0;
            }}
            section.toc {{
            }}
            section.toc h2 {{
                font-size: {self._config.title_font_size_pt}pt;
                color: #0f172a;
            }}
            section.toc ul {{
                list-style-type: none;
                padding-left: 0;
            }}
            section.toc li {{
                margin: 0.3em 0;
                color: inherit;
            }}
            section.document {{
                page-break-before: always;
            }}
            h1, h2, h3, h4, h5, h6 {{
                color: #0f172a;
                margin: 1.2em 0 0.6em 0;
                font-weight: 600;
                page-break-after: avoid;
            }}
            h1 {{
                font-size: {self._config.title_font_size_pt}pt;
                border-bottom: 1px solid #e5e7eb;
                padding-bottom: 0.3em;
            }}
            h2 {{
                font-size: {self._config.title_font_size_pt - 2}pt;
            }}
            h3 {{
                font-size: {self._config.title_font_size_pt - 4}pt;
            }}
            p {{
                margin: 0.6em 0;
            }}
            p > strong:only-child {{
                page-break-after: avoid;
            }}
            ul, ol {{
                color: inherit;
                margin: 0.4em 0 0.4em 1.4em;
                padding-left: 0.6em;
            }}
            li {{
                margin: 0.2em 0;
                color: inherit;
            }}
            pre {{
                background: #f3f4f6;
                border: 1px solid #e5e7eb;
                border-radius: 4px;
                padding: 10px;
                margin: 0.8em 0;
                white-space: pre-wrap;
                font-family: '{mono_family}', 'DejaVu Sans Mono', 'Liberation Mono', monospace;
                font-size: {max(self._config.body_font_size_pt - 1, 6)}pt;
                color: #111827;
            }}
            code {{
                font-family: '{mono_family}', 'DejaVu Sans Mono', 'Liberation Mono', monospace;
                background: #f3f4f6;
                padding: 0.1em 0.3em;
                border-radius: 3px;
            }}
            .codehilite {{
                background: #f8f8f8;
                color: #1f2937;
                border-radius: 4px;
                margin: 0.8em 0;
                border: 1px solid #d1d5db;
            }}
            .codehilite pre {{
                background: transparent;
                border: none;
                margin: 0;
                padding: 10px;
                white-space: pre-wrap;
                overflow-x: auto;
                color: inherit;
                font-family: '{mono_family}', 'DejaVu Sans Mono', 'Liberation Mono', monospace;
                font-size: {max(self._config.body_font_size_pt - 1, 6)}pt;
            }}
            figure.diagram {{
                display: flex;
                justify-content: center;
                margin: 1.2em 0;
            }}
            figure.diagram img {{
                max-width: 100%;
                max-height: 85vh;
                height: auto;
                object-fit: contain;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
                margin: 1.2em 0;
                font-size: {self._config.body_font_size_pt - 1}pt;
            }}
            table th, table td {{
                border: 1px solid #e5e7eb;
                padding: 6px 8px;
                text-align: left;
            }}
            a {{
                color: #0d6efd;
                text-decoration: none;
            }}
            a:hover {{
                text-decoration: underline;
            }}
            .document-meta {{
                color: #4b5563;
                font-size: {self._config.body_font_size_pt - 2}pt;
                margin-bottom: 1em;
            }}
            """)
        return base_css + "\n" + highlight_css

    def _render_cover_page(self) -> str:
        generated_on = datetime.now().strftime("%Y-%m-%d")
        return textwrap.dedent(f"""
            <section class="cover">
                <h1>Snapper Documentation</h1>
                <p>Comprehensive project guide</p>
                <p class="document-meta">Generated on {generated_on}</p>
            </section>
            """).strip()

    def _render_table_of_contents(self) -> str:
        items = "".join(
            f'<li><a href="#doc-{doc.slug}">{doc.title}</a></li>' for doc in self._sources
        )
        return textwrap.dedent(f"""
            <section class="toc">
                <h2>Table of Contents</h2>
                <ul>
                    {items}
                </ul>
            </section>
            """).strip()

    def _render_document(self, source: DocumentSource) -> str:
        content = source.path.read_text(encoding="utf-8")
        sanitized = self._normalize_text(content, source)
        html_body = markdown(
            sanitized,
            extensions=MARKDOWN_EXTENSIONS,
            extension_configs=MARKDOWN_EXTENSION_CONFIGS,
        )
        html_body = re.sub(
            r"<pre><code[^>]*>(.*?)</code></pre>",
            r"<pre>\1</pre>",
            html_body,
            flags=re.DOTALL,
        )

        def _convert_code_paragraph(match: re.Match[str]) -> str:
            inner = match.group(1)
            return f"<pre>{inner}</pre>"

        html_body = re.sub(
            r"<p><code[^>]*>((?:(?!</?code).)*?)</code>\s*</p>",
            _convert_code_paragraph,
            html_body,
            flags=re.DOTALL,
        )
        html_body = re.sub(r"<code[^>]*>", "", html_body)
        html_body = html_body.replace("</code>", "")
        html_body = self._normalize_pre_blocks(html_body)
        html_body = self._inject_heading_anchors(html_body, source)
        top_anchor_id = f"doc-{source.slug}"
        self._doc_top_ids[source.slug] = top_anchor_id
        section_html = textwrap.dedent(f"""
            <section class="document">
                <a id="{top_anchor_id}"></a>
                {html_body}
            </section>
            """)
        self._dump_debug_html(source, section_html)
        return section_html.strip()

    def _wrap_html(self, body: str) -> str:
        return textwrap.dedent(f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="utf-8" />
                <title>Snapper Documentation</title>
            </head>
            <body>
                {body}
            </body>
            </html>
            """).strip()

    def _dump_debug_html(self, document: DocumentSource, html_content: str) -> None:
        self._debug_html_dir.mkdir(parents=True, exist_ok=True)
        target_path = self._debug_html_dir / f"{document.slug}.html"
        target_path.write_text(html_content, encoding="utf-8")

    def _write_combined_debug(self, html_content: str) -> None:
        self._debug_html_dir.mkdir(parents=True, exist_ok=True)
        target_path = self._debug_html_dir / self._combined_debug_name
        target_path.write_text(html_content, encoding="utf-8")

    def _normalize_text(self, text: str, source: DocumentSource) -> str:
        result = self._replace_mermaid_blocks(text, source)
        result = self._rewrite_document_links(result)
        return result

    def _replace_mermaid_blocks(self, text: str, source: DocumentSource) -> str:
        pattern = re.compile(r"```mermaid\s*\n(.*?)```", flags=re.DOTALL)

        def render_block(match: re.Match[str]) -> str:
            code = match.group(1).strip()
            if not code:
                return ""
            image_path = self._mermaid_renderer.render(code, source.slug)
            img_src = image_path.resolve().as_uri()
            return (
                '<figure class="diagram">'
                f'<img src="{img_src}" alt="Diagram Mermaid" />'
                "</figure>"
            )

        return pattern.sub(render_block, text)

    def _rewrite_document_links(self, text: str) -> str:
        pattern = re.compile(r"\[([^\]]+)\]\(((?!https?://|mailto:)[^\)#]+\.md(?:#[^\)]*)?)\)")

        def replace(match: re.Match[str]) -> str:
            label = match.group(1)
            target = match.group(2)
            doc_path, fragment = self._split_target(target)
            document = self._doc_link_map.get(doc_path)
            if not document:
                return match.group(0)
            if fragment:
                anchor = self._slugify_fragment(fragment)
                return f"[{label}](snapper-anchor:{document.slug}#{anchor})"
            return f"[{label}](snapper-doc:{document.slug})"

        return pattern.sub(replace, text)

    @staticmethod
    def _split_target(target: str) -> tuple[str, str | None]:
        if "#" not in target:
            return target, None
        doc_part, fragment = target.split("#", 1)
        return doc_part, fragment

    def _slugify_fragment(self, fragment: str) -> str:
        normalized = unicodedata.normalize("NFKD", fragment)
        ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
        ascii_only = ascii_only.replace("/", " ")
        cleaned = re.sub(r"[^a-zA-Z0-9\s_-]", "", ascii_only)
        collapsed = re.sub(r"[\s_]+", "-", cleaned.strip().lower())
        return collapsed or fragment

    def _build_doc_link_map(self, repo_root: Path) -> dict[str, DocumentSource]:
        mapping: dict[str, DocumentSource] = {}
        for document in self._sources:
            try:
                relative = document.path.relative_to(repo_root)
                mapping[relative.as_posix()] = document
                mapping[f"./{relative.as_posix()}"] = document
            except ValueError:
                pass
            mapping[document.path.name] = document
            mapping[document.path.as_posix()] = document
        return mapping

    @staticmethod
    def _is_removable_first_line(first_line: str) -> bool:
        """Check whether the first line of a pre block is a removable language hint.

        Args:
            first_line: Stripped first line of a pre block.

        Returns:
            True if the line is a fenced-code opener or bare language hint.
        """
        if first_line.startswith("```"):
            language = first_line[3:].strip().lower()
            return not language or language in CODE_LANGUAGE_HINTS
        return first_line.lower() in CODE_LANGUAGE_HINTS

    @staticmethod
    def _strip_language_hint(content: str) -> str:
        """Strip fenced-code language hints and dedent pre-block content.

        Args:
            content: Raw inner content of a ``<pre>`` block.

        Returns:
            Cleaned content with language hints removed and whitespace normalized.
        """
        working = content.lstrip("\n")
        prefix_newlines = content[: len(content) - len(working)]
        lines = working.splitlines()
        if not lines:
            return prefix_newlines
        modified = False
        first_line = lines[0].strip()
        if MarkdownToPdf._is_removable_first_line(first_line):
            lines = lines[1:]
            modified = True
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
            modified = True
        if modified:
            while lines and not lines[0].strip():
                lines.pop(0)
        body = "\n".join(lines)
        dedented = textwrap.dedent(body).lstrip("\n")
        if modified or dedented != body:
            return prefix_newlines + dedented
        return content

    def _normalize_pre_blocks(self, html: str) -> str:
        """Normalize ``<pre>`` blocks by stripping language hints and dedenting.

        Args:
            html: HTML string with ``<pre>`` blocks to normalize.

        Returns:
            HTML with cleaned ``<pre>`` blocks.
        """
        pattern = re.compile(r"<pre>(.*?)</pre>", flags=re.DOTALL)

        def repl(match: re.Match[str]) -> str:
            body = match.group(1)
            cleaned = self._strip_language_hint(body)
            return f"<pre>{cleaned}</pre>"

        return pattern.sub(repl, html)

    def _resolve_heading_slug(self, source_slug: str, base_slug: str) -> str:
        """Resolve a unique heading slug using occurrence tracking.

        Increments the usage counter and appends a suffix for duplicates.

        Args:
            source_slug: Document slug for namespace isolation.
            base_slug: Base slug derived from heading text.

        Returns:
            Final slug string (unique within the document).
        """
        usage_key = (source_slug, base_slug)
        occurrence = self._heading_slug_usage[usage_key]
        final_slug = base_slug if occurrence == 0 else f"{base_slug}-{occurrence + 1}"
        self._heading_slug_usage[usage_key] = occurrence + 1
        return final_slug

    def _register_heading_aliases(
        self, source_slug: str, base_slug: str, final_slug: str, final_id: str
    ) -> str:
        """Build alias anchor tags and register them in the anchor targets map.

        Args:
            source_slug: Document slug for namespace isolation.
            base_slug: Base slug derived from heading text.
            final_slug: Resolved unique slug.
            final_id: Full anchor ID for the heading element.

        Returns:
            HTML string of alias anchor tags.
        """
        aliases = {base_slug, final_slug}
        aliases.update(self._heading_anchor_aliases(base_slug))
        aliases.update(self._heading_anchor_aliases(final_slug))
        for alias in aliases:
            self._anchor_targets[(source_slug, alias)] = final_id
        return "".join(f'<a id="doc-{source_slug}--{alias}"></a>' for alias in sorted(aliases))

    def _inject_heading_anchors(self, html: str, source: DocumentSource) -> str:
        """Inject unique anchor IDs and alias tags into heading elements.

        Args:
            html: HTML string containing heading elements.
            source: Document source providing the slug namespace.

        Returns:
            HTML with heading anchors and alias tags injected.
        """
        pattern = re.compile(r"<h([1-6])([^>]*)>(.*?)</h\1>", flags=re.DOTALL)

        def repl(match: re.Match[str]) -> str:
            level = match.group(1)
            attrs = match.group(2)
            content = match.group(3)
            attrs_without_id = re.sub(r"\s*id=\"[^\"]*\"", "", attrs)
            text_content = re.sub(r"<[^>]+>", "", content)
            base_slug = self._slugify_fragment(text_content) or f"section-{level}"
            final_slug = self._resolve_heading_slug(source.slug, base_slug)
            final_id = f"doc-{source.slug}--{final_slug}"
            alias_tags = self._register_heading_aliases(
                source.slug, base_slug, final_slug, final_id
            )
            return f'{alias_tags}<h{level}{attrs_without_id} id="{final_id}">{content}</h{level}>'

        return pattern.sub(repl, html)

    @staticmethod
    def _heading_anchor_aliases(anchor: str) -> set[str]:
        aliases: set[str] = {anchor}
        hyphen_variant = anchor.replace("_", "-")
        aliases.add(hyphen_variant)
        if "-" in anchor:
            prefix = anchor.split("-", 1)[0]
            if len(prefix) >= 3:
                aliases.add(prefix)
        return {value for value in aliases if value}

    def _resolve_internal_links(self, html: str) -> str:
        def doc_repl(match: re.Match[str]) -> str:
            slug = match.group(1)
            target = self._doc_top_ids.get(slug)
            if not target:
                return match.group(0)
            return f'href="#{target}"'

        html = re.sub(r'href="snapper-doc:([^\"]+)"', doc_repl, html)

        def anchor_repl(match: re.Match[str]) -> str:
            slug = match.group(1)
            anchor = match.group(2)
            normalized = self._slugify_fragment(anchor)
            target = self._anchor_targets.get((slug, normalized))
            if target:
                return f'href="#{target}"'
            return match.group(0)

        return re.sub(r'href="snapper-anchor:([^"#]+)#([^"#]+)"', anchor_repl, html)


def find_font(candidate_paths: Sequence[Path]) -> Path:
    """Find the first existing font from candidate paths.

    Args:
        candidate_paths: Sequence of font file paths to search.

    Returns:
        Path to the first existing font file found.
    """
    for path in candidate_paths:
        if path.exists():
            return path
    raise FileNotFoundError(
        "No UTF-8 capable TrueType font found. "
        "Install a Unicode TrueType font or provide a path via --font."
    )


def extract_title(path: Path) -> str:
    """Extract title from Markdown file's first heading.

    Args:
        path: Path to the Markdown file.

    Returns:
        Extracted title string, or a formatted stem if no heading found.
    """
    content = path.read_text(encoding="utf-8")
    for line in content.splitlines():
        if line.startswith("#"):
            heading = line.lstrip("# ").strip()
            return re.sub(r"^\d+[\.)]\s*", "", heading)
    return path.stem.replace("_", " ").title()


def collect_sources(root: Path, docs_dir: Path) -> list[DocumentSource]:
    """Collect all Markdown sources from root and docs directory.

    Args:
        root: Path to the root Markdown file (typically README.md).
        docs_dir: Directory containing additional documentation files.

    Returns:
        List of DocumentSource objects for all discovered Markdown files.
    """
    files: list[Path] = [root]
    files.extend(sorted(docs_dir.glob("*.md")))
    return [
        DocumentSource(path=path, title=extract_title(path), slug=build_slug(path))
        for path in files
    ]


def build_slug(path: Path) -> str:
    """Build a URL-safe slug from a file path.

    Args:
        path: File path to convert to a slug.

    Returns:
        URL-safe slug string derived from the file stem.
    """
    stem = re.sub(r"^\d+_", "", path.stem)
    slug = re.sub(r"[^a-z0-9]+", "-", stem.lower()).strip("-")
    return slug or "section"


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    """Parse command-line arguments for PDF generation.

    Args:
        argv: Command-line argument list to parse.

    Returns:
        Namespace containing parsed argument values.
    """
    parser = argparse.ArgumentParser(description="Export Markdown documentation to a PDF file")
    parser.add_argument(
        "--output",
        default="snapper.pdf",
        help="Path to the target PDF file (default: snapper.pdf)",
    )
    parser.add_argument(
        "--font",
        type=Path,
        help="Path to a UTF-8 capable TrueType font file",
    )
    return parser.parse_args(argv)


def main() -> int:
    """Entry point for the PDF generation script.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    print("Building documentation PDF...")
    try:
        args = parse_args(sys.argv[1:])
        repo_root = Path(__file__).resolve().parents[1]
        readme = repo_root / "README.md"
        docs_dir = repo_root / "docs"
        sources = collect_sources(readme, docs_dir)
        font_path = args.font or find_font(default_font_candidates())
        config = RenderConfig(
            title_font_size_pt=18,
            body_font_size_pt=11,
            page_margin_mm=15,
        )
        assets_dir = repo_root / "build" / "docs" / "pdf-assets"
        mermaid_renderer = MermaidRenderer(assets_dir=assets_dir, project_root=repo_root)
        builder = MarkdownToPdf(
            font_path=font_path,
            config=config,
            mermaid_renderer=mermaid_renderer,
            sources=sources,
            repo_root=repo_root,
        )
        builder.write_pdf(Path(args.output))
        print(f"PDF generated: {args.output}")
        return 0
    except Exception as exc:
        print(f"Failed to build PDF: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

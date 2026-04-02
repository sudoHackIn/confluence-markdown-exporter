"""Unit tests for DrawIO macro conversion in Page.Converter."""

from unittest.mock import MagicMock
from unittest.mock import patch

from bs4 import BeautifulSoup

from confluence_markdown_exporter.confluence import Page


class TestDrawIOMacroConversion:
    """Test DrawIO macro conversion behavior."""

    @patch("confluence_markdown_exporter.confluence.settings")
    def test_convert_drawio_prefers_embedded_mermaid(self, mock_settings: MagicMock) -> None:
        """When Mermaid is available in .drawio, use it before image-link fallback."""
        mock_settings.export.include_document_title = False
        mock_settings.export.attachment_href = "relative"

        page = MagicMock(spec=Page)
        page.id = 123
        page.title = "DrawIO page"
        page.html = ""
        page.labels = []
        page.ancestors = []
        page.editor2 = ""
        page.get_attachments_by_title.return_value = []

        converter = Page.Converter(page)
        converter._convert_drawio_embedded_mermaid = MagicMock(  # type: ignore[attr-defined]
            return_value="```mermaid\\ngraph TD; A-->B\\n```"
        )

        html = '<div data-macro-name="drawio">|diagramName=diagram.drawio|</div>'
        el = BeautifulSoup(html, "html.parser").find("div")

        result = converter.convert_drawio(el, "", [])

        assert "```mermaid" in result
        assert "graph TD; A-->B" in result
        converter._convert_drawio_embedded_mermaid.assert_called_once_with(  # type: ignore[attr-defined]
            "diagram.drawio.png"
        )

    @patch("confluence_markdown_exporter.confluence.settings")
    def test_convert_drawio_falls_back_to_preview_link(self, mock_settings: MagicMock) -> None:
        """Fallback to preview image linking original drawio when Mermaid extraction fails."""
        mock_settings.export.include_document_title = False
        mock_settings.export.attachment_href = "relative"

        drawio_attachment = MagicMock()
        drawio_attachment.export_path = "attachments/diagram.drawio"
        preview_attachment = MagicMock()
        preview_attachment.export_path = "attachments/diagram.drawio.png"

        page = MagicMock(spec=Page)
        page.id = 123
        page.title = "DrawIO page"
        page.html = ""
        page.labels = []
        page.ancestors = []
        page.editor2 = ""
        page.get_attachments_by_title.side_effect = [
            [drawio_attachment],
            [preview_attachment],
        ]

        converter = Page.Converter(page)
        converter._convert_drawio_embedded_mermaid = MagicMock(return_value=None)  # type: ignore[attr-defined]

        html = '<div data-macro-name="drawio">|diagramName=diagram.drawio|</div>'
        el = BeautifulSoup(html, "html.parser").find("div")

        result = converter.convert_drawio(el, "", [])

        assert "![diagram.drawio](" in result
        assert "diagram.drawio.png" in result
        assert "](../../../attachments/diagram.drawio)" in result

"""Tests for the table_converter module."""

import pytest
from bs4 import BeautifulSoup

import confluence_markdown_exporter.utils.table_converter as table_converter_module
from confluence_markdown_exporter.utils.table_converter import TableConverter


class TestTableConverter:
    """Test TableConverter class."""

    def test_pipe_character_in_cell(self) -> None:
        """Test that pipe characters are escaped in table cells."""
        html = """
        <table>
            <tr>
                <th>Column 1</th>
                <th>Column 2</th>
            </tr>
            <tr>
                <td>Value with | pipe</td>
                <td>Normal value</td>
            </tr>
        </table>
        """
        BeautifulSoup(html, "html.parser")
        converter = TableConverter()
        result = converter.convert(html)

        # The pipe character should be escaped
        assert "\\|" in result
        # The result should still have proper table structure
        assert "Column 1" in result
        assert "Column 2" in result
        assert "Value with" in result
        assert "pipe" in result

    def test_multiple_pipes_in_cell(self) -> None:
        """Test that multiple pipe characters are escaped in table cells."""
        html = """
        <table>
            <tr>
                <th>Header</th>
            </tr>
            <tr>
                <td>Value | with | multiple | pipes</td>
            </tr>
        </table>
        """
        BeautifulSoup(html, "html.parser")
        converter = TableConverter()
        result = converter.convert(html)

        # All pipe characters should be escaped (3 pipes in the content)
        assert result.count("\\|") == 3
        assert "Value" in result
        assert "with" in result
        assert "multiple" in result
        assert "pipes" in result

    def test_pipe_character_in_header(self) -> None:
        """Test that pipe characters are escaped in table header cells."""
        html = """
        <table>
            <tr>
                <th>Column | 1</th>
                <th>Column | 2</th>
            </tr>
            <tr>
                <td>Value 1</td>
                <td>Value 2</td>
            </tr>
        </table>
        """
        converter = TableConverter()
        result = converter.convert(html)

        # The pipe characters in headers should be escaped (2 pipes)
        assert result.count("\\|") == 2
        assert "Column" in result
        assert "Value 1" in result
        assert "Value 2" in result

    def test_table_without_pipes(self) -> None:
        """Test normal table conversion without pipe characters."""
        html = """
        <table>
            <tr>
                <th>Name</th>
                <th>Age</th>
            </tr>
            <tr>
                <td>John</td>
                <td>30</td>
            </tr>
        </table>
        """
        converter = TableConverter()
        result = converter.convert(html)

        assert "Name" in result
        assert "Age" in result
        assert "John" in result
        assert "30" in result
        # Should have proper table structure
        assert "|" in result
        assert "---" in result
        # Should have no escaped pipes
        assert "\\|" not in result

    def test_skip_oversized_table_by_estimated_cells(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Skip table when estimated expanded size exceeds safety limit."""
        monkeypatch.setattr(table_converter_module, "MAX_TABLE_CELLS", 4)
        html = """
        <table>
            <tr><th>A</th><th>B</th></tr>
            <tr><td rowspan="5">x</td><td>y</td></tr>
        </table>
        """
        converter = TableConverter()
        result = converter.convert(html)

        assert "Skipped oversized table" in result

    def test_skip_oversized_table_after_padding(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Skip table when real expanded cell count exceeds limit."""
        monkeypatch.setattr(table_converter_module, "MAX_TABLE_CELLS", 3)
        monkeypatch.setattr(table_converter_module, "_estimate_table_cells", lambda _rows: 1)
        html = """
        <table>
            <tr><th>A</th><th>B</th><th>C</th></tr>
            <tr><td>1</td><td>2</td><td>3</td></tr>
        </table>
        """
        converter = TableConverter()
        result = converter.convert(html)

        assert "Skipped oversized table after expansion" in result

    def test_skip_oversized_table_by_markdown_bytes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Skip table when generated markdown payload exceeds byte limit."""
        monkeypatch.setattr(table_converter_module, "MAX_TABLE_CELLS", 1000)
        monkeypatch.setattr(table_converter_module, "MAX_TABLE_MARKDOWN_BYTES", 10)
        html = """
        <table>
            <tr><th>A</th></tr>
            <tr><td>value value value</td></tr>
        </table>
        """
        converter = TableConverter()
        result = converter.convert(html)

        assert "Skipped oversized markdown table" in result

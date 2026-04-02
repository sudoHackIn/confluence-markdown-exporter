import logging
import os
from typing import cast

from bs4 import BeautifulSoup
from bs4 import Tag
from markdownify import MarkdownConverter
from tabulate import tabulate

logger = logging.getLogger(__name__)
MAX_TABLE_CELLS = int(os.getenv("CME_MAX_TABLE_CELLS", "1000"))
MAX_TABLE_MARKDOWN_BYTES = int(os.getenv("CME_MAX_TABLE_MARKDOWN_BYTES", str(1024 * 1024)))
MAX_SPAN = int(os.getenv("CME_MAX_TABLE_SPAN", "50"))


def _get_int_attr(cell: Tag, attr: str, default: str = "1") -> int:
    val = cell.get(attr, default)
    if isinstance(val, list):
        val = val[0] if val else default
    try:
        parsed = int(str(val))
        return max(1, min(parsed, MAX_SPAN))
    except (ValueError, TypeError):
        return int(default)


def _estimate_table_cells(rows: list[list[Tag]]) -> int:
    """Estimate expanded table size with bounded row/col spans."""
    estimated = 0
    for row in rows:
        for cell in row:
            rs = _get_int_attr(cell, "rowspan", "1")
            cs = _get_int_attr(cell, "colspan", "1")
            estimated += rs * cs
            if estimated > MAX_TABLE_CELLS:
                return estimated
    return estimated


def pad(rows: list[list[Tag]]) -> list[list[Tag]]:
    """Pad table rows to handle rowspan and colspan for markdown conversion."""
    padded: list[list[Tag]] = []
    occ: dict[tuple[int, int], Tag] = {}
    for r, row in enumerate(rows):
        if not row:
            continue
        cur: list[Tag] = []
        c = 0
        for cell in row:
            while (r, c) in occ:
                cur.append(occ.pop((r, c)))
                c += 1
            rs = _get_int_attr(cell, "rowspan", "1")
            cs = _get_int_attr(cell, "colspan", "1")
            cur.append(cell)
            # Append extra cells for colspan
            if cs > 1:
                cur.extend(make_empty_cell() for _ in range(1, cs))
            # Mark future cells for rowspan and colspan
            for i in range(rs):
                for j in range(cs):
                    if i or j:
                        occ[(r + i, c + j)] = make_empty_cell()
            c += cs
        while (r, c) in occ:
            cur.append(occ.pop((r, c)))
            c += 1
        padded.append(cur)
    return padded


def make_empty_cell() -> Tag:
    """Return an empty <td> Tag."""
    return Tag(name="td")


def _normalize_table_cell_text(text: str) -> str:
    return (
        text.replace("|", "\\|")  # Escape pipe characters to prevent breaking table formatting
        .replace("\n", "<br/>")  # Replace newlines with <br/> to preserve line breaks in tables
        .removesuffix("<br/>")  # Remove trailing <br/> that may be added by the last cell in a row
        .removeprefix("<br/>")  # Remove leading <br/> that may be added by the first cell in a row
    )


class TableConverter(MarkdownConverter):
    """Custom MarkdownConverter for converting HTML tables to markdown tables."""

    def convert_table(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
        rows = [
            cast("list[Tag]", tr.find_all(["td", "th"]))
            for tr in cast("list[Tag]", el.find_all("tr"))
            if tr
        ]

        if not rows:
            return ""

        estimated_cells = _estimate_table_cells(rows)
        if estimated_cells > MAX_TABLE_CELLS:
            logger.warning(
                "Skipping oversized table (estimated cells=%d, limit=%d)",
                estimated_cells,
                MAX_TABLE_CELLS,
            )
            return (
                "\n<!-- Skipped oversized table: "
                f"estimated {estimated_cells} cells exceeds limit {MAX_TABLE_CELLS}. -->\n"
            )

        padded_rows = pad(rows)
        actual_cells = sum(len(row) for row in padded_rows)
        if actual_cells > MAX_TABLE_CELLS:
            logger.warning(
                "Skipping oversized table after padding (cells=%d, limit=%d)",
                actual_cells,
                MAX_TABLE_CELLS,
            )
            return (
                "\n<!-- Skipped oversized table after expansion: "
                f"{actual_cells} cells exceeds limit {MAX_TABLE_CELLS}. -->\n"
            )

        converted = [[self.convert(str(cell)) for cell in row] for row in padded_rows]

        has_header = all(cell.name == "th" for cell in rows[0])
        markdown_table = ""
        if has_header:
            markdown_table = tabulate(converted[1:], headers=converted[0], tablefmt="pipe")
        else:
            markdown_table = tabulate(converted, headers=[""] * len(converted[0]), tablefmt="pipe")

        markdown_bytes = len(markdown_table.encode("utf-8"))
        if markdown_bytes > MAX_TABLE_MARKDOWN_BYTES:
            logger.warning(
                "Skipping oversized markdown table payload (bytes=%d, limit=%d)",
                markdown_bytes,
                MAX_TABLE_MARKDOWN_BYTES,
            )
            return (
                "\n<!-- Skipped oversized markdown table: "
                f"{markdown_bytes} bytes exceeds limit {MAX_TABLE_MARKDOWN_BYTES}. -->\n"
            )

        return markdown_table

    def convert_th(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
        """This method is empty because we want a No-Op for the <th> tag."""
        return _normalize_table_cell_text(text)

    def convert_tr(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
        """This method is empty because we want a No-Op for the <tr> tag."""
        return text

    def convert_td(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
        """This method is empty because we want a No-Op for the <td> tag."""
        return _normalize_table_cell_text(text)

    def convert_thead(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
        """This method is empty because we want a No-Op for the <thead> tag."""
        return text

    def convert_tbody(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
        """This method is empty because we want a No-Op for the <tbody> tag."""
        return text

    def convert_ol(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
        if "td" in parent_tags:
            return str(el)
        return super().convert_ol(el, text, parent_tags)

    def convert_ul(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
        if "td" in parent_tags:
            return str(el)
        return super().convert_ul(el, text, parent_tags)

    def convert_p(self, el: BeautifulSoup, text: str, parent_tags: list[str]) -> str:
        md = super().convert_p(el, text, parent_tags)
        if "td" in parent_tags:
            md = md.replace("\n", "") + "<br/>"
        return md

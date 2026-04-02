"""Tests for frontmatter provider composition."""

from types import SimpleNamespace

from confluence_markdown_exporter.utils.frontmatter import FrontmatterContext
from confluence_markdown_exporter.utils.frontmatter import build_frontmatter_data
from confluence_markdown_exporter.utils.frontmatter import build_frontmatter_markdown


def _make_context(markdown_body: str = "") -> FrontmatterContext:
    page = SimpleNamespace(
        id=123,
        title="Architecture Notes",
        export_path="SPACE/Architecture Notes.md",
        space=SimpleNamespace(key="SPACE", name="Engineering Space"),
        version=SimpleNamespace(
            number=7,
            when="2026-04-03T10:00:00Z",
            by=SimpleNamespace(display_name="Vlad"),
        ),
        ancestors=[
            SimpleNamespace(id=10, title="Home"),
            SimpleNamespace(id=20, title="RFC"),
        ],
    )
    return FrontmatterContext(
        page=page,
        page_properties={"owner": "platform"},
        labels=["#obsidian", "#confluence"],
        markdown_body=markdown_body,
    )


def test_build_frontmatter_data_with_multiple_providers() -> None:
    context = _make_context(markdown_body="```plantuml\n@startuml\n@enduml\n```")

    data = build_frontmatter_data(
        context=context,
        provider_names=["page_properties", "base", "obsidian", "diagnostics"],
    )

    assert data["owner"] == "platform"
    assert data["cme"]["page_id"] == 123
    assert data["tags"] == ["obsidian", "confluence"]
    assert data["diagnostics"]["has_plantuml"] is True


def test_build_frontmatter_markdown_empty_when_no_providers() -> None:
    context = _make_context()
    markdown = build_frontmatter_markdown(context=context, provider_names=[])
    assert markdown == ""


def test_build_frontmatter_markdown_contains_yaml_block() -> None:
    context = _make_context(markdown_body="<!-- Drawio diagram missing -->")

    markdown = build_frontmatter_markdown(
        context=context,
        provider_names=["base", "obsidian", "diagnostics"],
    )

    assert markdown.startswith("---\n")
    assert "cme:" in markdown
    assert "diagnostics:" in markdown
    assert "warnings_count: 1" in markdown
    assert markdown.endswith("\n---")


def test_obsidian_flat_provider_exposes_top_level_search_fields() -> None:
    context = _make_context(
        markdown_body="```mermaid\ngraph TD; A-->B\n```\n<!-- warning -->",
    )

    data = build_frontmatter_data(
        context=context,
        provider_names=["obsidian_flat"],
    )

    assert data["page_id"] == 123
    assert data["space_key"] == "SPACE"
    assert data["has_mermaid"] is True
    assert data["has_plantuml"] is False
    assert data["warnings_count"] == 1
    assert data["ancestor_ids"] == [10, 20]
    assert data["ancestor_titles"] == ["Home", "RFC"]
    assert data["parent_id"] == 20
    assert data["parent_title"] == "RFC"


def test_obsidian_links_provider_exposes_wikilinks() -> None:
    context = _make_context()
    data = build_frontmatter_data(
        context=context,
        provider_names=["obsidian_links"],
    )

    assert data["ancestor_links"] == ["[[Home]]", "[[RFC]]"]
    assert data["parent_link"] == "[[RFC]]"
    assert data["page_link"] == "[[Architecture Notes]]"

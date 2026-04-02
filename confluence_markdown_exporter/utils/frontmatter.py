"""Frontmatter builders for exported Markdown documents."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Any

import yaml

from confluence_markdown_exporter.utils.export import sanitize_key

if TYPE_CHECKING:
    from confluence_markdown_exporter.confluence import Page

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FrontmatterContext:
    """Context passed to frontmatter providers."""

    page: Page
    page_properties: dict[str, object]
    labels: list[str]
    markdown_body: str


def _provider_page_properties(context: FrontmatterContext) -> dict[str, object]:
    return dict(context.page_properties)


def _provider_base(context: FrontmatterContext) -> dict[str, object]:
    ancestors = [ancestor.title for ancestor in context.page.ancestors]
    version = context.page.version
    return {
        "cme": {
            "page_id": context.page.id,
            "page_title": context.page.title,
            "space_key": context.page.space.key,
            "space_name": context.page.space.name,
            "export_path": str(context.page.export_path),
            "version": version.number,
            "updated_at": version.when,
            "updated_by": version.by.display_name,
            "ancestors": ancestors,
        }
    }


def _provider_obsidian(context: FrontmatterContext) -> dict[str, object]:
    tags = [label.lstrip("#") for label in context.labels]
    return {
        "aliases": [context.page.title],
        "tags": tags,
        "cssclasses": ["confluence-export"],
    }


def _collect_diagnostics(markdown_body: str) -> dict[str, object]:
    comments = re.findall(r"<!--\s*(.*?)\s*-->", markdown_body, flags=re.DOTALL)
    cleaned_comments = [
        comment.strip().replace("\n", " ")
        for comment in comments
        if comment.strip()
    ]
    return {
        "has_mermaid": "```mermaid" in markdown_body,
        "has_plantuml": "```plantuml" in markdown_body,
        "has_drawio_reference": ".drawio" in markdown_body,
        "warnings_count": len(cleaned_comments),
        "warnings": cleaned_comments,
    }


def _provider_diagnostics(context: FrontmatterContext) -> dict[str, object]:
    return {
        "diagnostics": _collect_diagnostics(context.markdown_body)
    }


def _provider_obsidian_flat(context: FrontmatterContext) -> dict[str, object]:
    diagnostics = _collect_diagnostics(context.markdown_body)
    ancestor_ids = [ancestor.id for ancestor in context.page.ancestors]
    ancestor_titles = [ancestor.title for ancestor in context.page.ancestors]

    return {
        "page_id": context.page.id,
        "page_title": context.page.title,
        "space_key": context.page.space.key,
        "space_name": context.page.space.name,
        "version": context.page.version.number,
        "updated_at": context.page.version.when,
        "updated_by": context.page.version.by.display_name,
        "has_mermaid": diagnostics["has_mermaid"],
        "has_plantuml": diagnostics["has_plantuml"],
        "has_drawio_reference": diagnostics["has_drawio_reference"],
        "warnings_count": diagnostics["warnings_count"],
        "ancestor_ids": ancestor_ids,
        "ancestor_titles": ancestor_titles,
        "parent_id": ancestor_ids[-1] if ancestor_ids else None,
        "parent_title": ancestor_titles[-1] if ancestor_titles else None,
    }


def _provider_obsidian_links(context: FrontmatterContext) -> dict[str, object]:
    ancestor_titles = [ancestor.title for ancestor in context.page.ancestors]
    ancestor_links = [f"[[{title}]]" for title in ancestor_titles]
    parent_link = ancestor_links[-1] if ancestor_links else None
    return {
        "ancestor_links": ancestor_links,
        "parent_link": parent_link,
        "page_link": f"[[{context.page.title}]]",
    }


FRONTMATTER_PROVIDERS: dict[str, Any] = {
    "page_properties": _provider_page_properties,
    "base": _provider_base,
    "obsidian": _provider_obsidian,
    "diagnostics": _provider_diagnostics,
    "obsidian_flat": _provider_obsidian_flat,
    "obsidian_links": _provider_obsidian_links,
}


def _deep_merge_dicts(dst: dict[str, object], src: dict[str, object]) -> dict[str, object]:
    for key, value in src.items():
        sanitized_key = sanitize_key(key)
        if (
            sanitized_key in dst
            and isinstance(dst[sanitized_key], dict)
            and isinstance(value, dict)
        ):
            _deep_merge_dicts(dst[sanitized_key], value)
            continue
        dst[sanitized_key] = value
    return dst


def build_frontmatter_data(
    *,
    context: FrontmatterContext,
    provider_names: list[str],
) -> dict[str, object]:
    """Build frontmatter dict by combining selected providers."""
    result: dict[str, object] = {}
    for provider_name in provider_names:
        provider = FRONTMATTER_PROVIDERS.get(provider_name)
        if provider is None:
            logger.warning("Unknown frontmatter provider: %s", provider_name)
            continue
        data = provider(context)
        _deep_merge_dicts(result, data)
    return result


def build_frontmatter_markdown(
    *,
    context: FrontmatterContext,
    provider_names: list[str],
    indent: int = 2,
) -> str:
    """Build YAML frontmatter markdown from provider names."""
    data = build_frontmatter_data(context=context, provider_names=provider_names)
    if not data:
        return ""

    yml = yaml.dump(
        data,
        indent=indent,
        allow_unicode=True,
        sort_keys=False,
    ).strip()
    yml = re.sub(r"^( *)(- )", r"\1" + " " * indent + r"\2", yml, flags=re.MULTILINE)
    return f"---\n{yml}\n---"

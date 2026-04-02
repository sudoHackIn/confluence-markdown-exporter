import os
from pathlib import Path
from typing import Annotated

import typer

from confluence_markdown_exporter import __version__
from confluence_markdown_exporter.utils.app_data_store import get_settings
from confluence_markdown_exporter.utils.app_data_store import set_setting
from confluence_markdown_exporter.utils.config_interactive import main_config_menu_loop
from confluence_markdown_exporter.utils.lockfile import LockfileManager
from confluence_markdown_exporter.utils.measure_time import measure
from confluence_markdown_exporter.utils.platform_compat import handle_powershell_tilde_expansion
from confluence_markdown_exporter.utils.type_converter import str_to_bool

DEBUG: bool = str_to_bool(os.getenv("DEBUG", "False"))

app = typer.Typer()


def override_output_path_config(value: Path | None) -> None:
    """Override the default output path if provided."""
    if value is not None:
        set_setting("export.output_path", value)


@app.command(help="Export one or more Confluence pages by ID or URL to Markdown.")
def pages(
    pages: Annotated[list[str], typer.Argument(help="Page ID(s) or URL(s)")],
    output_path: Annotated[
        Path | None,
        typer.Option(
            help="Directory to write exported Markdown files to. Overrides config if set."
        ),
    ] = None,
) -> None:
    from confluence_markdown_exporter.confluence import Page
    from confluence_markdown_exporter.confluence import sync_removed_pages

    with measure(f"Export pages {', '.join(pages)}"):
        override_output_path_config(output_path)
        LockfileManager.init()
        for page in pages:
            _page = Page.from_id(int(page)) if page.isdigit() else Page.from_url(page)
            _page.export()
            LockfileManager.record_page(_page)
        sync_removed_pages()


@app.command(help="Export Confluence pages and their descendant pages by ID or URL to Markdown.")
def pages_with_descendants(
    pages: Annotated[list[str], typer.Argument(help="Page ID(s) or URL(s)")],
    output_path: Annotated[
        Path | None,
        typer.Option(
            help="Directory to write exported Markdown files to. Overrides config if set."
        ),
    ] = None,
) -> None:
    from confluence_markdown_exporter.confluence import Page
    from confluence_markdown_exporter.confluence import sync_removed_pages

    with measure(f"Export pages {', '.join(pages)} with descendants"):
        override_output_path_config(output_path)
        LockfileManager.init()
        for page in pages:
            _page = Page.from_id(int(page)) if page.isdigit() else Page.from_url(page)
            _page.export_with_descendants()
        sync_removed_pages()


@app.command(help="Export all Confluence pages of one or more spaces to Markdown.")
def spaces(
    space_keys: Annotated[list[str], typer.Argument()],
    output_path: Annotated[
        Path | None,
        typer.Option(
            help="Directory to write exported Markdown files to. Overrides config if set."
        ),
    ] = None,
) -> None:
    from confluence_markdown_exporter.confluence import Space
    from confluence_markdown_exporter.confluence import sync_removed_pages

    # Personal Confluence spaces start with ~. Exporting them on Windows leads to
    # Powershell expanding tilde to the Users directory, which is handled here
    normalized_space_keys = [handle_powershell_tilde_expansion(key) for key in space_keys]

    with measure(f"Export spaces {', '.join(normalized_space_keys)}"):
        override_output_path_config(output_path)
        LockfileManager.init()
        for space_key in normalized_space_keys:
            space = Space.from_key(space_key)
            space.export()
        sync_removed_pages()


@app.command(help="Export all Confluence pages across all spaces to Markdown.")
def all_spaces(
    output_path: Annotated[
        Path | None,
        typer.Option(
            help="Directory to write exported Markdown files to. Overrides config if set."
        ),
    ] = None,
) -> None:
    from confluence_markdown_exporter.confluence import Organization
    from confluence_markdown_exporter.confluence import sync_removed_pages

    with measure("Export all spaces"):
        override_output_path_config(output_path)
        LockfileManager.init()
        org = Organization.from_api()
        org.export()
        sync_removed_pages()


@app.command(help="Open the interactive configuration menu or display current configuration.")
def config(
    jump_to: Annotated[
        str | None,
        typer.Option(help="Jump directly to a config submenu, e.g. 'auth.confluence'"),
    ] = None,
    *,
    show: Annotated[
        bool,
        typer.Option(
            "--show",
            help="Display current configuration as YAML instead of opening the interactive menu",
        ),
    ] = False,
) -> None:
    """Interactive configuration menu or display current configuration."""
    if show:
        current_settings = get_settings()
        json_output = current_settings.model_dump_json(indent=2)
        typer.echo(f"```json\n{json_output}\n```")
    else:
        main_config_menu_loop(jump_to)


@app.command(help="Show the current version of confluence-markdown-exporter.")
def version() -> None:
    """Display the current version."""
    typer.echo(f"confluence-markdown-exporter {__version__}")


@app.command(
    help=(
        "Run V2 sync orchestrator with SQLite checkpointing "
        "(incremental/full/resume)."
    )
)
def sync(
    mode: Annotated[
        str | None,
        typer.Option(help="Sync mode override. Defaults to config.v2.mode"),
    ] = None,
    from_ts: Annotated[
        str | None,
        typer.Option(
            help=(
                "Incremental lower bound timestamp in ISO format, "
                "or 'auto'. Defaults to config.v2.from_ts."
            )
        ),
    ] = None,
    state_db_path: Annotated[
        Path | None,
        typer.Option(help="Override SQLite state DB path (config.v2.state_db_path by default)."),
    ] = None,
    space_keys: Annotated[
        list[str] | None,
        typer.Option(
            help=(
                "Optional repeatable space key filter, "
                "e.g. --space-keys ENG --space-keys DOCS"
            )
        ),
    ] = None,
    max_fetch_workers: Annotated[
        int | None,
        typer.Option(help="Override max fetch workers."),
    ] = None,
    max_convert_workers: Annotated[
        int | None,
        typer.Option(help="Override max convert workers."),
    ] = None,
    max_attachment_workers: Annotated[
        int | None,
        typer.Option(help="Override max write/attachment workers."),
    ] = None,
    global_rps: Annotated[
        float | None,
        typer.Option(help="Override global request rate limit (requests/sec)."),
    ] = None,
    max_retries: Annotated[
        int | None,
        typer.Option(help="Override retry budget per page."),
    ] = None,
    timeout_seconds: Annotated[
        int | None,
        typer.Option(help="Override total timeout for the run in seconds."),
    ] = None,
) -> None:
    """Run V2 incremental sync."""
    from confluence_markdown_exporter.v2_sync import run_v2_sync

    if mode is not None and mode not in {"incremental", "full", "resume"}:
        msg = "Invalid --mode. Expected one of: incremental, full, resume."
        raise typer.BadParameter(msg)

    result = run_v2_sync(
        mode=mode,
        from_ts=from_ts,
        space_keys=space_keys,
        state_db_path=state_db_path,
        max_fetch_workers=max_fetch_workers,
        max_convert_workers=max_convert_workers,
        max_attachment_workers=max_attachment_workers,
        global_rps=global_rps,
        max_retries=max_retries,
        timeout_seconds=timeout_seconds,
    )

    typer.echo(
        f"V2 run {result.run_id} ({result.mode}) completed: "
        f"discovered={result.discovered}, enqueued={result.enqueued}, "
        f"processed={result.processed}, updated={result.updated}, failed={result.failed}, "
        f"from_ts={result.from_ts or 'none'}, to_ts={result.to_ts}"
    )


if __name__ == "__main__":
    app()

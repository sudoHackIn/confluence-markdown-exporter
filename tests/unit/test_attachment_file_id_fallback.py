"""Tests for attachment file_id fallback behavior."""

from confluence_markdown_exporter.confluence import Attachment
from confluence_markdown_exporter.confluence import Space
from confluence_markdown_exporter.confluence import User
from confluence_markdown_exporter.confluence import Version


def _make_attachment(file_id: str) -> Attachment:
    user = User(
        account_id="a1",
        username="u",
        display_name="User",
        public_name="User",
        email="u@example.com",
    )
    version = Version(number=1, by=user, when="2026-04-03T00:00:00Z", friendly_when="")
    return Attachment(
        id="12345",
        title="file",
        space=Space(key="ENG", name="Engineering", description="", homepage=None),
        ancestors=[],
        version=version,
        file_size=1,
        media_type="image/png",
        media_type_description="",
        file_id=file_id,
        collection_name="",
        download_link="",
        comment="",
    )


def test_attachment_uses_file_id_when_present() -> None:
    attachment = _make_attachment("abc123")

    assert attachment.export_file_id == "abc123"
    assert attachment.filename == "abc123.png"
    assert attachment._template_vars["attachment_file_id"] == "abc123"


def test_attachment_falls_back_to_id_when_file_id_missing() -> None:
    attachment = _make_attachment("")

    assert attachment.export_file_id == "12345"
    assert attachment.filename == "12345.png"
    assert attachment._template_vars["attachment_file_id"] == "12345"

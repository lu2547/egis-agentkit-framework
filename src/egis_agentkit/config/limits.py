"""Shared defaults for archive validation and registry management."""

# Strictly below 50 MiB; retain a separate limit for ordinary workspace uploads.
DEFAULT_SKILL_ARCHIVE_MAX_BYTES = 50 * 1024 * 1024 - 1
